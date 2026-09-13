// Package forward relays decoded ingest payloads to a backend over the
// same protobuf-over-HTTP shape this collector accepts from the agent:
// same content type, same body shape, same paths, no new protocol
// invented. It plays the role an OTel Collector exporter plays: forward
// what came in, don't reinterpret it.
package forward

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/internal/ingest"
	"github.com/LukeDevOps/yukon-collector/internal/metrics"
)

const (
	defaultShards    = 8
	defaultQueueSize = 64

	defaultRequestTimeout       = 10 * time.Second
	defaultRetryInitialInterval = 5 * time.Second
	defaultRetryMaxInterval     = 30 * time.Second
	defaultRetryMaxElapsedTime  = 300 * time.Second

	// maxResponseBytes bounds how much of a backend response body is read
	// before the connection is released. A 202 carries no body worth
	// reading; the read only exists to let the connection be reused.
	maxResponseBytes = 64 << 10
)

// Config configures a ForwardingSink. URL is required; every other field
// falls back to a default matched to the OTLP HTTP exporter's own
// defaults when left zero.
type Config struct {
	// URL is the backend's base URL, with an http or https scheme and a
	// host. ForwardingSink posts the re-marshaled payload to
	// URL+ingest.DeltaBatchPath and URL+ingest.ManifestPath. Trailing
	// slashes are removed so the joined path has a single separator.
	URL string

	// AuthToken authenticates the collector to the backend, sent as
	// "Authorization: Bearer <AuthToken>" when set. This is a separate
	// trust boundary from the agent-facing bearer token the collector
	// itself checks: the agent authenticates to the collector, the
	// collector authenticates to the backend, and the two need not share
	// a secret.
	AuthToken string

	// Shards is the number of independent worker/queue pairs. A payload is
	// routed to a shard by hashing its service identity, so one
	// instance's stuck backend retries can't hold up delivery for every
	// other instance's healthy traffic. Defaults to defaultShards.
	Shards int

	// QueueSize bounds each shard's queue. A full shard drops the newest
	// item, with a Warn log, rather than blocking the caller. Defaults to
	// defaultQueueSize.
	QueueSize int

	// HTTPClient sends the relayed requests. Defaults to a client whose
	// transport keeps one idle connection per shard to the backend, so
	// concurrent workers do not churn connections the way
	// http.DefaultTransport's two-per-host limit would. Each request
	// carries its own timeout via context, so the client itself does not
	// need one.
	HTTPClient *http.Client

	// Logger receives Warn logs for dropped payloads. Defaults to
	// slog.Default().
	Logger *slog.Logger

	// RequestTimeout bounds a single HTTP attempt. Defaults to
	// defaultRequestTimeout.
	RequestTimeout time.Duration

	// RetryInitialInterval, RetryMaxInterval, and RetryMaxElapsedTime
	// shape the retry backoff between attempts at delivering one payload.
	// Default to defaultRetryInitialInterval, defaultRetryMaxInterval, and
	// defaultRetryMaxElapsedTime, matching the OTLP HTTP exporter's own
	// defaults (1.5x multiplier, 30s max interval, 300s max elapsed time).
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration
	RetryMaxElapsedTime  time.Duration
}

func (c Config) withDefaults() Config {
	if c.Shards <= 0 {
		c.Shards = defaultShards
	}
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.HTTPClient == nil {
		c.HTTPClient = newHTTPClient(c.Shards)
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = defaultRequestTimeout
	}
	if c.RetryInitialInterval <= 0 {
		c.RetryInitialInterval = defaultRetryInitialInterval
	}
	if c.RetryMaxInterval <= 0 {
		c.RetryMaxInterval = defaultRetryMaxInterval
	}
	if c.RetryMaxElapsedTime <= 0 {
		c.RetryMaxElapsedTime = defaultRetryMaxElapsedTime
	}
	return c
}

// newHTTPClient returns a client sized for shards concurrent workers all
// talking to one backend host.
func newHTTPClient(shards int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = shards
	if transport.MaxIdleConns < shards {
		transport.MaxIdleConns = shards
	}
	return &http.Client{Transport: transport}
}

// queuedItem is a payload already marshaled to wire bytes, ready to POST.
// Marshaling happens once, in Accept, not on every retry attempt. key is
// the service identity the item was sharded on; it names the owner in
// log lines when the item is dropped, so an operator can tell whose data
// went missing.
type queuedItem struct {
	key  string
	path string
	body []byte
}

// shard is one independent queue and worker goroutine. ForwardingSink
// routes a payload to a shard by hashing its service identity, so a
// stuck backend for one instance only ever occupies that instance's
// shard, never every other instance's.
type shard struct {
	queue chan queuedItem
}

// ForwardingSink is a Sink that relays decoded payloads to a backend
// instead of just logging them. Accept methods enqueue and return
// immediately; background workers, one per shard, do the actual HTTP
// relay, so the agent-facing handler never blocks on the backend's
// availability.
type ForwardingSink struct {
	cfg    Config
	shards []*shard

	// ctx bounds every worker delivery attempt. Shutdown cancels it once
	// its own deadline passes, so a slow in-flight request cannot outlive
	// the shutdown budget.
	ctx    context.Context
	cancel context.CancelFunc

	closed       atomic.Bool
	stopping     chan struct{}
	shutdownOnce sync.Once
	wg           sync.WaitGroup
}

var _ ingest.Sink = (*ForwardingSink)(nil)

// NewForwardingSink starts cfg.Shards worker goroutines and returns a
// ready-to-use ForwardingSink. Call Shutdown to drain and stop them. It
// returns an error for a cfg.URL that could never reach a backend, so a
// bad setting stops the collector at startup rather than dropping every
// payload as a permanent failure.
func NewForwardingSink(cfg Config) (*ForwardingSink, error) {
	cfg = cfg.withDefaults()

	baseURL, err := validateURL(cfg.URL)
	if err != nil {
		return nil, err
	}
	cfg.URL = baseURL
	if strings.HasPrefix(cfg.URL, "http://") && cfg.AuthToken != "" {
		cfg.Logger.Warn("forward URL uses plain http; the backend auth token is sent unencrypted", "url", cfg.URL)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &ForwardingSink{
		cfg:      cfg,
		shards:   make([]*shard, cfg.Shards),
		ctx:      ctx,
		cancel:   cancel,
		stopping: make(chan struct{}),
	}
	for i := range s.shards {
		sh := &shard{queue: make(chan queuedItem, cfg.QueueSize)}
		s.shards[i] = sh
		s.wg.Add(1)
		go s.runShard(sh)
	}
	return s, nil
}

// validateURL checks that raw is an absolute http or https URL with a
// host, and returns it with any trailing slashes removed.
func validateURL(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("forward URL is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("forward URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("forward URL %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("forward URL %q: missing host", raw)
	}
	return strings.TrimRight(raw, "/"), nil
}

// AcceptDeltaBatch marshals batch and queues it on the shard for its
// service instance. It returns without waiting for delivery.
func (s *ForwardingSink) AcceptDeltaBatch(batch *yukonpb.DeltaBatch) {
	body, err := proto.Marshal(batch)
	if err != nil {
		s.cfg.Logger.Warn("dropping delta batch: marshal failed", "error", err)
		metrics.ForwardDropped.Inc("deltas", "marshal")
		return
	}
	key := batch.GetResource().GetServiceName() + "/" + batch.GetResource().GetServiceInstanceId()
	s.enqueue(queuedItem{key: key, path: ingest.DeltaBatchPath, body: body})
}

// AcceptManifest marshals manifest and queues it on the shard for its
// service. It returns without waiting for delivery.
func (s *ForwardingSink) AcceptManifest(manifest *yukonpb.ProbeManifest) {
	body, err := proto.Marshal(manifest)
	if err != nil {
		s.cfg.Logger.Warn("dropping manifest: marshal failed", "error", err)
		metrics.ForwardDropped.Inc("manifest", "marshal")
		return
	}
	// ProbeManifest carries no instance ID: a manifest describes a
	// service's probe set, not one running instance of it.
	key := manifest.GetServiceName()
	s.enqueue(queuedItem{key: key, path: ingest.ManifestPath, body: body})
}

func (s *ForwardingSink) enqueue(item queuedItem) {
	if s.closed.Load() {
		s.dropped(item, "shutting_down", nil)
		return
	}
	sh := s.shards[shardIndex(item.key, len(s.shards))]
	select {
	case sh.queue <- item:
	default:
		s.dropped(item, "queue_full", nil)
	}
}

// dropReasons maps a drop reason label to the text logged for it.
var dropReasons = map[string]string{
	"shutting_down":           "sink is shutting down",
	"queue_full":              "shard queue full",
	"permanent":               "permanent failure",
	"retry_exhausted":         "retry budget exhausted",
	"shutdown_deadline":       "shutdown deadline exceeded",
	"shutdown_attempt_failed": "shutdown drain attempt failed",
}

// dropped records one discarded payload: a Warn log naming the service it
// belonged to, and a count under reason.
func (s *ForwardingSink) dropped(item queuedItem, reason string, err error) {
	attrs := []any{"service", item.key, "path", item.path}
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	s.cfg.Logger.Warn("dropping payload: "+dropReasons[reason], attrs...)
	metrics.ForwardDropped.Inc(ingest.PayloadLabel(item.path), reason)
}

// shardIndex maps key to a shard deterministically: the same key always
// lands on the same shard, so one instance's payloads are always ordered
// against each other, even though shards themselves run independently.
func shardIndex(key string, numShards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(numShards))
}

func (s *ForwardingSink) runShard(sh *shard) {
	defer s.wg.Done()
	for {
		// Check for shutdown before looking at the queue. A select with
		// both cases ready picks one at random, which would let the
		// worker keep pulling items that Shutdown's drain should own.
		select {
		case <-s.stopping:
			return
		default:
		}
		select {
		case item := <-sh.queue:
			s.deliver(item)
		case <-s.stopping:
			return
		}
	}
}

// deliver attempts item with the configured retry backoff, until it
// succeeds, hits a permanent (non-retryable) failure, exhausts the retry
// budget, or the sink starts shutting down.
func (s *ForwardingSink) deliver(item queuedItem) {
	b := newBackoff(s.cfg.RetryInitialInterval, s.cfg.RetryMaxInterval, s.cfg.RetryMaxElapsedTime)
	payload := ingest.PayloadLabel(item.path)
	for {
		retryable, retryAfter, err := s.attempt(s.ctx, item)
		if err == nil {
			metrics.ForwardDelivered.Inc(payload)
			return
		}
		if !retryable {
			s.dropped(item, "permanent", err)
			return
		}
		wait, ok := b.next(retryAfter)
		if !ok {
			s.dropped(item, "retry_exhausted", err)
			return
		}
		select {
		case <-time.After(wait):
			metrics.ForwardRetries.Inc(payload)
		case <-s.stopping:
			return
		}
	}
}

// attempt makes exactly one HTTP delivery attempt, bounded by
// cfg.RequestTimeout (or whatever deadline ctx already carries, if
// sooner). retryable tells the caller whether this failure is worth
// retrying at all: only 429/502/503/504 are, per the OTLP spec as
// otlphttpexporter actually implements it, plus any error that means no
// response came back (a plain 500 is not retried; the collector doesn't
// assume "5xx means retry"). retryAfter is the wait the backend asked
// for in a Retry-After header, or zero.
func (s *ForwardingSink) attempt(ctx context.Context, item queuedItem) (retryable bool, retryAfter time.Duration, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.cfg.URL+item.path, bytes.NewReader(item.body))
	if err != nil {
		return false, 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if s.cfg.AuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+s.cfg.AuthToken)
	}

	resp, err := s.cfg.HTTPClient.Do(req)
	if err != nil {
		return true, 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false, 0, nil
	}
	err = fmt.Errorf("backend returned %s", resp.Status)
	if !isRetryableStatus(resp.StatusCode) {
		return false, 0, err
	}
	return true, parseRetryAfter(resp.Header, time.Now()), err
}

func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// Shutdown stops accepting new payloads, cuts off any in-flight retry
// wait, and gives every payload still sitting in a shard's queue exactly
// one more delivery attempt, bounded by ctx, instead of the full retry
// sequence. This matches QueueBatch.Shutdown in the OTel Collector: a
// bounded best-effort drain, not a guarantee every payload is delivered.
//
// An attempt already in flight when Shutdown is called is allowed to
// finish, but only within ctx: once ctx expires the attempt is cancelled,
// so a hung backend cannot hold up the drain or leak the worker past
// Shutdown's return.
//
// Only the first call does anything. Later calls return at once.
func (s *ForwardingSink) Shutdown(ctx context.Context) {
	first := false
	s.shutdownOnce.Do(func() { first = true })
	if !first {
		return
	}
	s.closed.Store(true)
	close(s.stopping)

	waitDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-ctx.Done():
		s.cancel()
		<-waitDone
	}

	for _, sh := range s.shards {
		s.drainOnce(ctx, sh)
	}
	s.cancel()
}

// drainOnce empties sh's queue, giving each item exactly one attempt.
// It never blocks waiting for more items: anything enqueued after
// Shutdown started (which enqueue itself already rejects) is not this
// method's concern.
func (s *ForwardingSink) drainOnce(ctx context.Context, sh *shard) {
	for {
		select {
		case item := <-sh.queue:
			if ctx.Err() != nil {
				s.dropped(item, "shutdown_deadline", nil)
				continue
			}
			if _, _, err := s.attempt(ctx, item); err != nil {
				s.dropped(item, "shutdown_attempt_failed", err)
			} else {
				metrics.ForwardDelivered.Inc(ingest.PayloadLabel(item.path))
			}
		default:
			return
		}
	}
}
