// Package forward relays decoded ingest payloads to a backend. It uses the
// shape this collector accepts from the agent: the same content type, body
// and paths. It plays the role of an OTel Collector exporter. It forwards
// what came in and does not change it.
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
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/ingest"
	"github.com/otherlodehq/otherlode-collector/metrics"
)

const (
	defaultShards     = 8
	defaultQueueSize  = 64
	defaultQueueBytes = 64 << 20 // 64 MiB per shard

	defaultRequestTimeout       = 10 * time.Second
	defaultRetryInitialInterval = 5 * time.Second
	defaultRetryMaxInterval     = 30 * time.Second
	defaultRetryMaxElapsedTime  = 300 * time.Second

	// maxResponseBytes bounds how much of a backend response body is read
	// before the connection is released. A 202 carries no body worth
	// reading; the read only exists to let the connection be reused.
	maxResponseBytes = 64 << 10
)

// Config configures a ForwardingSink. URL is required. Every other field
// falls back to a default when left zero. The timeout and retry defaults
// match the OTLP HTTP exporter.
type Config struct {
	// URL is the backend's base URL, with an http or https scheme and a
	// host. NewForwardingSink rejects a URL with a query, a fragment or
	// user info. ForwardingSink posts the re-marshaled payload to
	// URL+ingest.DeltaBatchPath, URL+ingest.ManifestPath, and
	// URL+ingest.StaticBaselinePath. Trailing slashes are removed so the
	// joined path has a single separator.
	URL string

	// AuthToken yields the key that authenticates the collector to the
	// backend. The sink asks for it before every request and sends it as
	// "Authorization: Bearer <key>" when it is not empty. A nil AuthToken
	// sends no header. This is a separate trust boundary from the
	// agent-facing bearer token the collector itself checks: the agent
	// authenticates to the collector, the collector authenticates to the
	// backend, and the two need not share a secret.
	AuthToken TokenSource

	// Shards is the number of independent worker/queue pairs. A payload is
	// routed to a shard by hashing its namespace, service name and
	// instance ID, so one instance's stuck backend retries cannot hold up
	// delivery for every other instance's healthy traffic. Defaults to
	// defaultShards.
	Shards int

	// QueueSize bounds each shard's queue by item count. A full shard
	// refuses the newest item, returning ErrQueueFull, rather than
	// blocking the caller. Defaults to defaultQueueSize.
	QueueSize int

	// QueueBytes bounds each shard's marshaled payload bytes, counted
	// from enqueue until the worker is done with the item. That includes
	// the item in flight. A shard refuses an item, returning
	// ErrQueueFull, when its held bytes plus the item's size would pass
	// QueueBytes. A shard that holds nothing accepts any item, so a
	// payload larger than QueueBytes can still pass through an idle
	// shard. Worst-case memory for queued bodies is Shards times the
	// larger of QueueBytes and the largest payload. Defaults to
	// defaultQueueBytes.
	QueueBytes int

	// HTTPClient sends the relayed requests. Defaults to a client whose
	// transport keeps one idle connection per shard to the backend, so
	// concurrent workers do not churn connections the way
	// http.DefaultTransport's two-per-host limit would. Each request
	// carries its own timeout via context, so the client itself does not
	// need one. The default client does not follow redirects: a 3xx
	// response is a permanent failure. A client you supply should do the
	// same, because a followed redirect can turn the POST into a GET and a
	// 2xx from the target would count as delivered.
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

// TokenSource yields the key a ForwardingSink sends to the backend.
// The sink calls Token before every request, so the key can change
// while the sink runs. Token must be safe for concurrent use.
type TokenSource interface {
	Token() string
}

// StaticToken is a TokenSource whose key never changes.
type StaticToken string

// Token returns the fixed key.
func (t StaticToken) Token() string { return string(t) }

// authToken returns the key to send, or "" when there is none.
func (c Config) authToken() string {
	if c.AuthToken == nil {
		return ""
	}
	return c.AuthToken.Token()
}

func (c Config) withDefaults() Config {
	if c.Shards <= 0 {
		c.Shards = defaultShards
	}
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.QueueBytes <= 0 {
		c.QueueBytes = defaultQueueBytes
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
// talking to one backend host. It does not follow redirects, so a 3xx
// response reaches attempt as a failure.
func newHTTPClient(shards int) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = shards
	if transport.MaxIdleConns < shards {
		transport.MaxIdleConns = shards
	}
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// queuedItem is a payload already marshaled to wire bytes, ready to POST.
// Marshaling happens once, in Accept, not on every retry attempt. owner
// is the instance the item was sharded on. It names the owner in log
// lines when the item is dropped, so an operator can tell whose data went
// missing.
type queuedItem struct {
	owner instance
	path  string
	body  []byte
}

// shard is one independent queue and worker goroutine. ForwardingSink
// routes a payload to a shard by hashing its instance's shard key, so a
// stuck backend for one instance only ever occupies that instance's
// shard, never every other instance's.
//
// bytes counts the body bytes of every item the shard holds, queued or
// in flight. mu guards it, because enqueues to one shard can run
// concurrently.
type shard struct {
	queue chan queuedItem

	mu    sync.Mutex
	bytes int
}

// reserve adds n bytes to the shard's count and reports true, unless the
// count would pass limit. An empty shard accepts any n, so an item larger
// than limit is never refused for good.
func (sh *shard) reserve(n, limit int) bool {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if sh.bytes > 0 && sh.bytes+n > limit {
		return false
	}
	sh.bytes += n
	return true
}

func (sh *shard) release(n int) {
	sh.mu.Lock()
	sh.bytes -= n
	sh.mu.Unlock()
}

// heldBytes returns the shard's byte count. Tests use it to check the
// accounting.
func (sh *shard) heldBytes() int {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.bytes
}

// ErrQueueFull is the error AcceptDeltaBatch, AcceptManifest, and
// AcceptStaticBaseline return when the payload's shard queue has no room,
// either in item count (Config.QueueSize) or in bytes (Config.QueueBytes).
// The ingest handler answers 503 for it, so the agent keeps its counts
// and sends the payload again on its next flush.
var ErrQueueFull = errors.New("forward: shard queue is full")

// ErrShuttingDown is the error AcceptDeltaBatch, AcceptManifest, and
// AcceptStaticBaseline return once Shutdown has started. The ingest
// handler answers 503 for it, so the agent keeps its counts and sends
// the payload again on its next flush.
var ErrShuttingDown = errors.New("forward: sink is shutting down")

// ForwardingSink is a Sink that relays decoded payloads to a backend
// instead of only logging them. Accept methods queue the payload and
// return at once. Background workers, one per shard, do the HTTP relay,
// so the agent-facing handler never blocks on the backend.
//
// An Accept method returns an error when the payload's shard queue is
// full by item count or by bytes, or the sink is shutting down. The
// handler then answers 503, and the agent is never told that the
// collector took data it did not queue.
type ForwardingSink struct {
	cfg    Config
	shards []*shard

	// ctx bounds every worker delivery attempt. Shutdown cancels it once
	// its own deadline passes, so a slow in-flight request cannot outlive
	// the shutdown budget.
	ctx    context.Context
	cancel context.CancelFunc

	// mu holds enqueue's shutdown check and its queue send together
	// against Shutdown setting closed. An item that passed the check is
	// queued before any worker starts its final drain, not after it, where
	// it would be answered 202 and never forwarded. The send never
	// blocks, so the read lock is brief.
	mu       sync.RWMutex
	closed   bool
	stopping chan struct{}

	// beforeSend, when set by a test, runs in enqueue between the
	// shutdown check and the queue send.
	beforeSend   func()
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

	baseURL, scheme, err := validateURL(cfg.URL)
	if err != nil {
		return nil, err
	}
	cfg.URL = baseURL
	if scheme == "http" && cfg.authToken() != "" {
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
// host and no query, fragment or user info. The sink joins paths onto the
// URL by string concatenation, so a query or fragment would end up in the
// middle of the request URL. User info would be logged as written. It
// returns raw with trailing slashes removed, and the lowercase scheme.
func validateURL(raw string) (base, scheme string, err error) {
	if raw == "" {
		return "", "", errors.New("forward URL is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", fmt.Errorf("forward URL %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", "", fmt.Errorf("forward URL %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return "", "", fmt.Errorf("forward URL %q: missing host", raw)
	}
	if u.User != nil {
		return "", "", errors.New("forward URL must not contain user info")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", "", errors.New("forward URL must not contain a query")
	}
	if u.Fragment != "" || strings.Contains(raw, "#") {
		return "", "", errors.New("forward URL must not contain a fragment")
	}
	return strings.TrimRight(raw, "/"), u.Scheme, nil
}

// instance names the service instance a payload came from: the service's
// identity, which is its namespace and name, plus the instance ID. An
// empty namespace is the unspecified one.
type instance struct {
	namespace string
	service   string
	id        string
}

// instanceOf reads the instance from res. It trims the namespace and
// service name, as the backend does when it keys a service, so one
// service never splits across shards. A nil res gives the zero instance.
func instanceOf(res *otherlodepb.ResourceAttributes) instance {
	return instance{
		namespace: strings.TrimSpace(res.GetServiceNamespace()),
		service:   strings.TrimSpace(res.GetServiceName()),
		id:        res.GetServiceInstanceId(),
	}
}

// shardKey builds the key every payload type is routed on. It writes
// each part as its byte length, a colon, then the part itself. Any part
// may hold "/" or ":", or be empty, so a plain separator could give two
// distinct instances one key. With the length prefix, each key decodes
// one way only.
//
// The run ID is left out on purpose. A restarted instance gets a
// different run ID, and a key with the run ID in it would move the
// instance to another shard. The single worker per shard keeps one
// instance's payloads in order, and that order would be lost across the
// restart.
func (i instance) shardKey() string {
	var b strings.Builder
	for _, part := range [...]string{i.namespace, i.service, i.id} {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

// String names the instance for people to read in errors and logs. It
// quotes each part, so a reader can tell where one part ends.
func (i instance) String() string {
	if i.namespace == "" {
		return fmt.Sprintf("service %q instance %q", i.service, i.id)
	}
	return fmt.Sprintf("namespace %q service %q instance %q", i.namespace, i.service, i.id)
}

// AcceptDeltaBatch marshals batch and queues it on the shard for its
// service instance. It returns without waiting for delivery, and does not
// use ctx: the request it comes from ends when the handler returns, long
// before the queued item is delivered in the background.
func (s *ForwardingSink) AcceptDeltaBatch(_ context.Context, batch *otherlodepb.DeltaBatch) error {
	return s.marshalAndEnqueue(batch, instanceOf(batch.GetResource()), ingest.DeltaBatchPath)
}

// AcceptManifest marshals manifest and queues it on the shard for its
// service instance. It returns without waiting for delivery, and does not
// use ctx: the request it comes from ends when the handler returns, long
// before the queued item is delivered in the background.
func (s *ForwardingSink) AcceptManifest(_ context.Context, manifest *otherlodepb.ProbeManifest) error {
	return s.marshalAndEnqueue(manifest, instanceOf(manifest.GetResource()), ingest.ManifestPath)
}

// AcceptStaticBaseline marshals baseline and queues it on the shard for
// its service instance. It returns without waiting for delivery, and does
// not use ctx: the request it comes from ends when the handler returns,
// long before the queued item is delivered in the background.
func (s *ForwardingSink) AcceptStaticBaseline(_ context.Context, baseline *otherlodepb.StaticBaseline) error {
	return s.marshalAndEnqueue(baseline, instanceOf(baseline.GetResource()), ingest.StaticBaselinePath)
}

// marshalAndEnqueue marshals msg and queues it for path. A marshal failure
// is logged and counted, and returned as nil. Resending the same payload
// cannot fix it, so a 503 would only make the agent retry it forever.
func (s *ForwardingSink) marshalAndEnqueue(msg proto.Message, owner instance, path string) error {
	payload := ingest.PayloadLabel(path)
	body, err := proto.Marshal(msg)
	if err != nil {
		s.cfg.Logger.Warn("dropping payload: marshal failed", "payload", payload, "error", err)
		metrics.ForwardDropped.Inc(payload, "marshal")
		return nil
	}
	return s.enqueue(queuedItem{owner: owner, path: path, body: body})
}

// enqueue puts item on its shard's queue. It returns an error wrapping
// ErrShuttingDown when Shutdown has started, or ErrQueueFull when the
// shard has no room by item count or by bytes. It does not log the
// error: the ingest handler logs every sink error it answers with 503.
func (s *ForwardingSink) enqueue(item queuedItem) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		s.refused(item, "shutting_down")
		return fmt.Errorf("%w (%s)", ErrShuttingDown, item.owner)
	}
	if s.beforeSend != nil {
		s.beforeSend()
	}
	sh := s.shards[shardIndex(item.owner.shardKey(), len(s.shards))]
	if !sh.reserve(len(item.body), s.cfg.QueueBytes) {
		s.refused(item, "queue_full")
		return fmt.Errorf("%w (%s)", ErrQueueFull, item.owner)
	}
	select {
	case sh.queue <- item:
		return nil
	default:
		sh.release(len(item.body))
		s.refused(item, "queue_full")
		return fmt.Errorf("%w (%s)", ErrQueueFull, item.owner)
	}
}

// refused counts one payload the sink did not take, under reason
// "shutting_down" or "queue_full". The sender still holds the payload and
// is expected to send it again, so this is not a ForwardDropped count.
func (s *ForwardingSink) refused(item queuedItem, reason string) {
	metrics.ForwardRefused.Inc(ingest.PayloadLabel(item.path), reason)
}

// dropReasons maps a drop reason label to the text logged for it.
var dropReasons = map[string]string{
	"permanent":               "permanent failure",
	"retry_exhausted":         "retry budget exhausted",
	"shutdown_deadline":       "shutdown deadline exceeded",
	"shutdown_attempt_failed": "shutdown drain attempt failed",
}

// dropped records one discarded payload: a Warn log naming the instance
// it belonged to, and a count under reason.
func (s *ForwardingSink) dropped(item queuedItem, reason string, err error) {
	attrs := []any{
		"namespace", item.owner.namespace,
		"service", item.owner.service,
		"instance", item.owner.id,
		"path", item.path,
	}
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

// runShard delivers sh's items in order until Shutdown closes stopping.
// It then drains sh's queue, giving each item one final attempt, and
// returns.
func (s *ForwardingSink) runShard(sh *shard) {
	defer s.wg.Done()
	for {
		// Check for shutdown before looking at the queue. A select with
		// both cases ready picks one at random, which would let the
		// worker keep running full retry sequences after Shutdown began.
		select {
		case <-s.stopping:
			s.drain(sh)
			return
		default:
		}
		select {
		case item := <-sh.queue:
			s.deliver(item)
			sh.release(len(item.body))
		case <-s.stopping:
			s.drain(sh)
			return
		}
	}
}

// drain empties sh's queue, giving each item one final attempt bounded by
// s.ctx. It does not wait for more items. enqueue refuses new items once
// Shutdown has set closed, so the queue cannot refill.
func (s *ForwardingSink) drain(sh *shard) {
	for {
		select {
		case item := <-sh.queue:
			s.finalDeliveryAttempt(item)
			sh.release(len(item.body))
		default:
			return
		}
	}
}

// deliver attempts item with the configured retry backoff, until it
// succeeds, hits a permanent (non-retryable) failure, or exhausts the
// retry budget. Shutdown can start while deliver holds item, either in
// the retry wait or right after a retryable failure. deliver then gives
// item one final attempt bounded by s.ctx, so every item ends as a
// counted delivery or a counted drop.
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
		select {
		case <-s.stopping:
			s.finalDeliveryAttempt(item)
			return
		default:
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
			s.finalDeliveryAttempt(item)
			return
		}
	}
}

// attempt makes one HTTP delivery attempt, bounded by cfg.RequestTimeout or
// ctx's deadline, whichever is sooner. Only 429, 502, 503 and 504 are
// retryable, as in otlphttpexporter, plus any error with no response. A
// plain 500 is not retried. retryAfter is the wait from a Retry-After
// header, or zero.
func (s *ForwardingSink) attempt(ctx context.Context, item queuedItem) (retryable bool, retryAfter time.Duration, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.RequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.cfg.URL+item.path, bytes.NewReader(item.body))
	if err != nil {
		return false, 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	if token := s.cfg.authToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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

// Shutdown stops accepting new payloads and gives every payload the sink
// still holds one more delivery attempt instead of the full retry
// sequence. That covers a payload in a shard's queue and a payload a
// worker holds, either in a retry wait or right after a retryable failure.
// Each worker drains its own shard, so the shards drain in parallel and
// each shard keeps its order. This matches QueueBatch.Shutdown in the OTel
// Collector: a bounded best-effort drain, not a guarantee every payload
// is delivered.
//
// ctx bounds the whole drain. When it ends, Shutdown cancels the attempt
// in flight on each shard, and every payload not yet attempted is dropped
// as "shutdown_deadline". Shutdown still waits for the workers to exit, so
// it does not leak them past its return.
//
// Only the first call does anything. Later calls return at once.
func (s *ForwardingSink) Shutdown(ctx context.Context) {
	first := false
	s.shutdownOnce.Do(func() { first = true })
	if !first {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
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
	s.cancel()
}

// finalDeliveryAttempt gives item its one shutdown attempt and records
// the outcome: a delivery, a "shutdown_deadline" drop when s.ctx has
// already ended, or a "shutdown_attempt_failed" drop. Shutdown cancels
// s.ctx when its own deadline passes.
func (s *ForwardingSink) finalDeliveryAttempt(item queuedItem) {
	if s.ctx.Err() != nil {
		s.dropped(item, "shutdown_deadline", nil)
		return
	}
	if _, _, err := s.attempt(s.ctx, item); err != nil {
		s.dropped(item, "shutdown_attempt_failed", err)
		return
	}
	metrics.ForwardDelivered.Inc(ingest.PayloadLabel(item.path))
}
