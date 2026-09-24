package forward

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/ingest"
	"github.com/LukeDevOps/yukon-collector/metrics"
)

// discardLogger keeps test output free of expected Warn lines (queue-full,
// permanent-failure, retry-exhausted) that these tests deliberately trigger.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(nopWriter{}, nil))
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// testConfig returns a Config tuned for fast, deterministic tests: small
// timeouts and retry budgets instead of the multi-second production
// defaults.
func testConfig(url string) Config {
	return Config{
		URL:                  url,
		Shards:               1,
		QueueSize:            8,
		Logger:               discardLogger(),
		RequestTimeout:       200 * time.Millisecond,
		RetryInitialInterval: 5 * time.Millisecond,
		RetryMaxInterval:     20 * time.Millisecond,
		RetryMaxElapsedTime:  200 * time.Millisecond,
	}
}

// mustNewSink constructs a sink for a config the test expects to be valid.
func mustNewSink(t *testing.T, cfg Config) *ForwardingSink {
	t.Helper()
	sink, err := NewForwardingSink(cfg)
	if err != nil {
		t.Fatalf("NewForwardingSink: %v", err)
	}
	return sink
}

func manifestWithService(name string) *yukonpb.ProbeManifest {
	return &yukonpb.ProbeManifest{Resource: &yukonpb.ResourceAttributes{ServiceName: name, ServiceInstanceId: "instance-1", RunId: "run-1"}}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met before timeout")
	}
}

func TestForwardingSink_Manifest_RelayedToBackendPath(t *testing.T) {
	var gotPath, gotContentType, gotAuth string
	var gotBody []byte
	var received atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotAuth = r.Header.Get("Authorization")
		gotBody, _ = io.ReadAll(r.Body)
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.AuthToken = "backend-secret"
	sink := mustNewSink(t, cfg)
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("demo-service"))

	waitFor(t, time.Second, received.Load)

	if gotPath != ingest.ManifestPath {
		t.Errorf("path = %q, want %q", gotPath, ingest.ManifestPath)
	}
	if gotContentType != "application/x-protobuf" {
		t.Errorf("content-type = %q, want application/x-protobuf", gotContentType)
	}
	if gotAuth != "Bearer backend-secret" {
		t.Errorf("authorization = %q, want %q", gotAuth, "Bearer backend-secret")
	}

	var manifest yukonpb.ProbeManifest
	if err := proto.Unmarshal(gotBody, &manifest); err != nil {
		t.Fatalf("unmarshal relayed body: %v", err)
	}
	if manifest.GetResource().GetServiceName() != "demo-service" {
		t.Errorf("relayed service name = %q, want %q", manifest.GetResource().GetServiceName(), "demo-service")
	}
}

func TestForwardingSink_DeltaBatch_RelayedToBackendPath(t *testing.T) {
	var gotPath string
	var received atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptDeltaBatch(context.Background(), &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	})

	waitFor(t, time.Second, received.Load)
	if gotPath != ingest.DeltaBatchPath {
		t.Errorf("path = %q, want %q", gotPath, ingest.DeltaBatchPath)
	}
}

func TestForwardingSink_StaticBaseline_RelayedToBackendPath(t *testing.T) {
	var gotPath, gotContentType string
	var gotBody []byte
	var received atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sent := &yukonpb.StaticBaseline{
		Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkIndex: 1,
		ChunkCount: 2,
	}
	sink.AcceptStaticBaseline(context.Background(), sent)

	waitFor(t, time.Second, received.Load)
	if gotPath != ingest.StaticBaselinePath {
		t.Errorf("path = %q, want %q", gotPath, ingest.StaticBaselinePath)
	}
	if gotContentType != "application/x-protobuf" {
		t.Errorf("content-type = %q, want application/x-protobuf", gotContentType)
	}

	var got yukonpb.StaticBaseline
	if err := proto.Unmarshal(gotBody, &got); err != nil {
		t.Fatalf("unmarshal relayed body: %v", err)
	}
	if got.GetChunkIndex() != sent.GetChunkIndex() || got.GetChunkCount() != sent.GetChunkCount() || got.GetScannedAt() != sent.GetScannedAt() {
		t.Errorf("relayed baseline = %v, want it to match %v", &got, sent)
	}
}

func TestForwardingSink_RetryableStatus_RetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("demo-service"))

	waitFor(t, time.Second, func() bool { return attempts.Load() == 3 })
	// Give a moment to confirm no further attempts happen after success.
	time.Sleep(30 * time.Millisecond)
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want exactly 3", got)
	}
}

func TestForwardingSink_PermanentStatus_DroppedWithoutRetry(t *testing.T) {
	var attempts atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("demo-service"))

	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (a plain 500 must not be retried)", got)
	}
}

func TestForwardingSink_RetryBudgetExhausted_StopsRetrying(t *testing.T) {
	var attempts atomic.Int32

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("demo-service"))

	// RetryMaxElapsedTime is 200ms with a 5ms initial interval: the retry
	// loop gives up well within a second. Confirm it stops growing after
	// that, rather than retrying forever.
	time.Sleep(400 * time.Millisecond)
	stalled := attempts.Load()
	time.Sleep(100 * time.Millisecond)
	if got := attempts.Load(); got != stalled {
		t.Fatalf("attempts grew from %d to %d after the retry budget should have been spent", stalled, got)
	}
	if stalled < 2 {
		t.Fatalf("attempts = %d, want at least 2 (budget should allow more than one try)", stalled)
	}
}

func TestForwardingSink_QueueFull_RefusesNewestWithoutBlocking(t *testing.T) {
	blockBackend := make(chan struct{})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockBackend
		w.WriteHeader(http.StatusOK)
	}))
	// t.Cleanup, not defer: cleanups run after the test body's own defers,
	// so registering backend.Close via defer here would make it run before
	// the unblock-then-shutdown cleanup below, and hang waiting for the
	// still-blocked handler to return.
	t.Cleanup(backend.Close)

	cfg := testConfig(backend.URL)
	cfg.QueueSize = 1
	cfg.RequestTimeout = time.Hour // don't let the timeout unblock the worker under test
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		close(blockBackend) // unblock the handler before Shutdown waits on the worker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	refusedBefore := metrics.ForwardRefused.Value("manifest", "queue_full")
	droppedBefore := metrics.ForwardDropped.Value("manifest", "queue_full")

	// Same key -> same shard -> same queue: item 1 gets picked up by the
	// single worker (which then blocks in the handler above), item 2 fills
	// the queue, item 3 finds the queue full and must be refused.
	var errs [3]error
	done := make(chan struct{})
	go func() {
		errs[0] = sink.AcceptManifest(context.Background(), manifestWithService("svc"))
		errs[1] = sink.AcceptManifest(context.Background(), manifestWithService("svc"))
		errs[2] = sink.AcceptManifest(context.Background(), manifestWithService("svc"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Accept calls blocked instead of refusing the overflow item")
	}

	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("first two Accepts = %v, %v, want nil, nil", errs[0], errs[1])
	}
	if !errors.Is(errs[2], ErrQueueFull) {
		t.Fatalf("third Accept error = %v, want it to wrap ErrQueueFull", errs[2])
	}
	if !strings.Contains(errs[2].Error(), "svc/instance-1") {
		t.Fatalf("third Accept error = %q, want it to name the service", errs[2])
	}

	if got := metrics.ForwardRefused.Value("manifest", "queue_full"); got != refusedBefore+1 {
		t.Fatalf("refused count = %d, want %d", got, refusedBefore+1)
	}
	if got := metrics.ForwardDropped.Value("manifest", "queue_full"); got != droppedBefore {
		t.Fatalf("dropped count = %d, want unchanged at %d", got, droppedBefore)
	}
}

// TestForwardingSink_RunIdChange_KeepsInstanceOnItsShard sends one
// instance's three payload types under three run IDs. With the run ID in
// the shard key, these three would hash to three different shards out of
// 16, and none would be refused. On one shard with a queue of one, the
// third finds the queue full.
func TestForwardingSink_RunIdChange_KeepsInstanceOnItsShard(t *testing.T) {
	blockBackend := make(chan struct{})
	entered := make(chan struct{}, 3)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-blockBackend
		w.WriteHeader(http.StatusOK)
	}))
	// t.Cleanup, not defer: see the comment in the queue-full test above.
	t.Cleanup(backend.Close)

	cfg := testConfig(backend.URL)
	cfg.Shards = 16
	cfg.QueueSize = 1
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		close(blockBackend)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	resource := func(runID string) *yukonpb.ResourceAttributes {
		return &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "instance-1", RunId: runID}
	}

	if err := sink.AcceptDeltaBatch(context.Background(), &yukonpb.DeltaBatch{Resource: resource("run-a")}); err != nil {
		t.Fatalf("delta batch: unexpected error: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("backend never received the delta batch")
	}
	if err := sink.AcceptManifest(context.Background(), &yukonpb.ProbeManifest{Resource: resource("run-b")}); err != nil {
		t.Fatalf("manifest: unexpected error: %v", err)
	}
	baseline := &yukonpb.StaticBaseline{Resource: resource("run-c"), ScannedAt: 1700000000, ChunkCount: 1}
	if err := sink.AcceptStaticBaseline(context.Background(), baseline); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("static baseline error = %v, want it to wrap ErrQueueFull (all three runs share one shard)", err)
	}
}

func TestForwardingSink_DifferentShards_OneStuckDoesNotBlockAnother(t *testing.T) {
	const numShards = 4
	keyA, keyB := findKeysInDifferentShards(t, numShards)

	blockA := make(chan struct{})
	var bReceived atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var manifest yukonpb.ProbeManifest
		body, _ := io.ReadAll(r.Body)
		_ = proto.Unmarshal(body, &manifest)
		if manifest.GetResource().GetServiceName() == keyA {
			<-blockA
			w.WriteHeader(http.StatusOK)
			return
		}
		bReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	// t.Cleanup, not defer: see the comment in the queue-full test above.
	t.Cleanup(backend.Close)

	cfg := testConfig(backend.URL)
	cfg.Shards = numShards
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		close(blockA)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	sink.AcceptManifest(context.Background(), manifestWithService(keyA))
	sink.AcceptManifest(context.Background(), manifestWithService(keyB))

	waitFor(t, time.Second, bReceived.Load)
}

func TestForwardingSink_Shutdown_DrainsQueueWithOneAttemptEach(t *testing.T) {
	var attempts atomic.Int32
	release := make(chan struct{})

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		<-release
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.QueueSize = 4
	cfg.RequestTimeout = 5 * time.Second
	cfg.RetryMaxElapsedTime = time.Hour // would retry effectively forever if not for Shutdown
	sink := mustNewSink(t, cfg)

	sink.AcceptManifest(context.Background(), manifestWithService("svc-1")) // picked up by the worker, blocks in the handler
	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })
	sink.AcceptManifest(context.Background(), manifestWithService("svc-2")) // sits queued behind it
	sink.AcceptManifest(context.Background(), manifestWithService("svc-3")) // sits queued behind it

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(release) // let the in-flight request return its 503
	}()

	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		sink.Shutdown(ctx)
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return promptly; it may have run the full retry sequence per item")
	}
	wg.Wait()

	// svc-1's in-flight attempt (already running when Shutdown was
	// called) comes back retryable, so it gets one final attempt of its
	// own, same as a queued item would: 2 attempts. Plus exactly one
	// drain attempt each for svc-2 and svc-3: 4 total, never a
	// multi-attempt retry sequence for any of them.
	if got := attempts.Load(); got != 4 {
		t.Fatalf("attempts = %d, want exactly 4 (svc-1's initial and final attempt, one each for svc-2 and svc-3, no retries during drain)", got)
	}
}

// findKeysInDifferentShards returns two service names whose manifests
// land on different shards, derived the same way AcceptManifest derives
// them, so the isolation under test is the one that happens in
// production.
func findKeysInDifferentShards(t *testing.T, numShards int) (string, string) {
	t.Helper()
	candidates := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f", "svc-g", "svc-h"}
	for i := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			a, b := manifestWithService(candidates[i]), manifestWithService(candidates[j])
			keyA := instanceKey(a.GetResource().GetServiceName(), a.GetResource().GetServiceInstanceId())
			keyB := instanceKey(b.GetResource().GetServiceName(), b.GetResource().GetServiceInstanceId())
			if shardIndex(keyA, numShards) != shardIndex(keyB, numShards) {
				return candidates[i], candidates[j]
			}
		}
	}
	t.Fatalf("no two candidate keys hash to different shards out of %d shards", numShards)
	return "", ""
}

func TestForwardingSink_Shutdown_CancelsInFlightAttemptAtDeadline(t *testing.T) {
	var cancelled atomic.Bool
	handlerEntered := make(chan struct{}, 1)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The server only watches for a client disconnect once the body
		// has been consumed, so read it before waiting on the context.
		_, _ = io.ReadAll(r.Body)
		handlerEntered <- struct{}{}
		select {
		case <-r.Context().Done():
			cancelled.Store(true)
		case <-time.After(5 * time.Second):
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.RequestTimeout = time.Hour // only the shutdown deadline may end the attempt
	sink := mustNewSink(t, cfg)

	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	select {
	case <-handlerEntered:
	case <-time.After(time.Second):
		t.Fatal("backend never received the in-flight request")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	sink.Shutdown(ctx)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Shutdown took %v, want it bounded by the 100ms deadline", took)
	}
	waitFor(t, time.Second, cancelled.Load)
}

func TestForwardingSink_Shutdown_SecondCallIsANoOp(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	sink.Shutdown(context.Background())
	sink.Shutdown(context.Background()) // must not panic on the closed stop channel
}

func TestNewForwardingSink_InvalidURL_ReturnsError(t *testing.T) {
	for _, raw := range []string{"", "backend.example.com", "ftp://backend.example.com", "http://", "://bad"} {
		cfg := testConfig(raw)
		if _, err := NewForwardingSink(cfg); err == nil {
			t.Errorf("URL %q: expected an error, got nil", raw)
		}
	}
}

func TestForwardingSink_TrailingSlashURL_PostsToCleanPath(t *testing.T) {
	var gotPath string
	var received atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		received.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL+"/"))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	waitFor(t, time.Second, received.Load)
	if gotPath != ingest.ManifestPath {
		t.Fatalf("path = %q, want %q (no doubled slash from the trailing slash)", gotPath, ingest.ManifestPath)
	}
}

func TestForwardingSink_RetryAfterHeader_DelaysNextAttempt(t *testing.T) {
	var attempts atomic.Int32
	var firstAt, secondAt atomic.Int64 // unix nanos, written by the handler goroutine

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch attempts.Add(1) {
		case 1:
			firstAt.Store(time.Now().UnixNano())
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			secondAt.Store(time.Now().UnixNano())
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.RetryMaxElapsedTime = 10 * time.Second // room for the 1s hint
	sink := mustNewSink(t, cfg)
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	waitFor(t, 3*time.Second, func() bool { return attempts.Load() == 2 })

	// The backoff alone would retry within tens of milliseconds; the
	// header must stretch that to at least a second.
	if gap := time.Duration(secondAt.Load() - firstAt.Load()); gap < time.Second {
		t.Fatalf("second attempt came %v after the first, want at least the 1s Retry-After", gap)
	}
}

func TestForwardingSink_DropLog_NamesTheService(t *testing.T) {
	var logs bytes.Buffer
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	sink := mustNewSink(t, cfg)

	sink.AcceptDeltaBatch(context.Background(), &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	})
	sink.Shutdown(context.Background())

	if got := logs.String(); !strings.Contains(got, "service=demo-service/instance-1") {
		t.Fatalf("drop log does not name the service:\n%s", got)
	}
}

func TestNewHTTPClient_IdleConnsMatchShardCount(t *testing.T) {
	client := newHTTPClient(8)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", client.Transport)
	}
	if transport.MaxIdleConnsPerHost != 8 {
		t.Fatalf("MaxIdleConnsPerHost = %d, want 8", transport.MaxIdleConnsPerHost)
	}
	if transport.MaxIdleConns < 8 {
		t.Fatalf("MaxIdleConns = %d, want at least 8", transport.MaxIdleConns)
	}
}

func TestForwardingSink_LargeResponseBody_DoesNotBlockDelivery(t *testing.T) {
	deliveredBefore := metrics.ForwardDelivered.Value("manifest")
	var received atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Store(true)
		w.WriteHeader(http.StatusOK)
		// Several times the read bound; the sink must not try to drain it all.
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for i := 0; i < 4; i++ {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	waitFor(t, time.Second, received.Load)

	done := make(chan struct{})
	go func() {
		sink.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return; the worker is likely still reading the oversized response")
	}
	if got := metrics.ForwardDelivered.Value("manifest"); got != deliveredBefore+1 {
		t.Fatalf("delivered count = %d, want %d (a 200 with a big body is still a success)", got, deliveredBefore+1)
	}
}

func TestForwardingSink_AcceptAfterShutdown_RefusedWithoutBlocking(t *testing.T) {
	var attempts atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	sink.Shutdown(context.Background())

	refusedBefore := metrics.ForwardRefused.Value("manifest", "shutting_down")
	droppedBefore := metrics.ForwardDropped.Value("manifest", "shutting_down")
	var acceptErr error
	done := make(chan struct{})
	go func() {
		acceptErr = sink.AcceptManifest(context.Background(), manifestWithService("svc"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Accept after Shutdown blocked")
	}

	if !errors.Is(acceptErr, ErrShuttingDown) {
		t.Fatalf("Accept after Shutdown error = %v, want it to wrap ErrShuttingDown", acceptErr)
	}
	if got := metrics.ForwardRefused.Value("manifest", "shutting_down"); got != refusedBefore+1 {
		t.Fatalf("refused count = %d, want %d", got, refusedBefore+1)
	}
	if got := metrics.ForwardDropped.Value("manifest", "shutting_down"); got != droppedBefore {
		t.Fatalf("dropped count = %d, want unchanged at %d", got, droppedBefore)
	}
	time.Sleep(50 * time.Millisecond)
	if attempts.Load() != 0 {
		t.Fatalf("backend received %d requests after Shutdown, want 0", attempts.Load())
	}
}

// retryWaitConfig returns a Config whose backoff parks a delivery in its
// retry wait for far longer than any of these tests run, so Shutdown is
// what ends the wait, not the timer.
func retryWaitConfig(url string) Config {
	cfg := testConfig(url)
	cfg.RetryInitialInterval = time.Hour
	cfg.RetryMaxInterval = time.Hour
	cfg.RetryMaxElapsedTime = 24 * time.Hour
	return cfg
}

func TestForwardingSink_Shutdown_DuringRetryWait_FinalAttemptSucceeds(t *testing.T) {
	var attempts atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, retryWaitConfig(backend.URL))
	deliveredBefore := metrics.ForwardDelivered.Value("manifest")

	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sink.Shutdown(ctx)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("backend saw %d requests, want exactly 2 (the initial failure and Shutdown's final attempt)", got)
	}
	if got := metrics.ForwardDelivered.Value("manifest"); got != deliveredBefore+1 {
		t.Fatalf("delivered count = %d, want %d", got, deliveredBefore+1)
	}
}

func TestForwardingSink_Shutdown_DuringRetryWait_FinalAttemptAlsoFails(t *testing.T) {
	var attempts atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	sink := mustNewSink(t, retryWaitConfig(backend.URL))
	droppedBefore := metrics.ForwardDropped.Value("manifest", "shutdown_attempt_failed")

	sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sink.Shutdown(ctx)

	if got := attempts.Load(); got != 2 {
		t.Fatalf("backend saw %d requests, want exactly 2 (the initial failure and Shutdown's final attempt)", got)
	}
	if got := metrics.ForwardDropped.Value("manifest", "shutdown_attempt_failed"); got != droppedBefore+1 {
		t.Fatalf("shutdown_attempt_failed count = %d, want %d", got, droppedBefore+1)
	}
}
