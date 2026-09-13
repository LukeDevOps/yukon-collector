package forward

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/internal/ingest"
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

func manifestWithService(name string) *yukonpb.ProbeManifest {
	return &yukonpb.ProbeManifest{ServiceName: name}
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
	sink := NewForwardingSink(cfg)
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(manifestWithService("demo-service"))

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
	if manifest.GetServiceName() != "demo-service" {
		t.Errorf("relayed service name = %q, want %q", manifest.GetServiceName(), "demo-service")
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

	sink := NewForwardingSink(testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptDeltaBatch(&yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"},
	})

	waitFor(t, time.Second, received.Load)
	if gotPath != ingest.DeltaBatchPath {
		t.Errorf("path = %q, want %q", gotPath, ingest.DeltaBatchPath)
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

	sink := NewForwardingSink(testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(manifestWithService("demo-service"))

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

	sink := NewForwardingSink(testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(manifestWithService("demo-service"))

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

	sink := NewForwardingSink(testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	sink.AcceptManifest(manifestWithService("demo-service"))

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

func TestForwardingSink_QueueFull_DropsNewestWithoutBlocking(t *testing.T) {
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
	sink := NewForwardingSink(cfg)
	t.Cleanup(func() {
		close(blockBackend) // unblock the handler before Shutdown waits on the worker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	// Same key -> same shard -> same queue: item 1 gets picked up by the
	// single worker (which then blocks in the handler above), item 2 fills
	// the queue, item 3 finds the queue full and must be dropped.
	done := make(chan struct{})
	go func() {
		sink.AcceptManifest(manifestWithService("svc"))
		sink.AcceptManifest(manifestWithService("svc"))
		sink.AcceptManifest(manifestWithService("svc"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Accept calls blocked instead of dropping the overflow item")
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
		if manifest.GetServiceName() == keyA {
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
	sink := NewForwardingSink(cfg)
	t.Cleanup(func() {
		close(blockA)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	sink.AcceptManifest(manifestWithService(keyA))
	sink.AcceptManifest(manifestWithService(keyB))

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
	sink := NewForwardingSink(cfg)

	sink.AcceptManifest(manifestWithService("svc-1")) // picked up by the worker, blocks in the handler
	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })
	sink.AcceptManifest(manifestWithService("svc-2")) // sits queued behind it
	sink.AcceptManifest(manifestWithService("svc-3")) // sits queued behind it

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

	// One attempt for svc-1 (already in flight when Shutdown was called),
	// plus exactly one drain attempt each for svc-2 and svc-3: 3 total,
	// never a multi-attempt retry sequence for any of them.
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want exactly 3 (one per payload, no retries during drain)", got)
	}
}

func findKeysInDifferentShards(t *testing.T, numShards int) (string, string) {
	t.Helper()
	candidates := []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e", "svc-f", "svc-g", "svc-h"}
	for i := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			if shardIndex(candidates[i], numShards) != shardIndex(candidates[j], numShards) {
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
	sink := NewForwardingSink(cfg)

	sink.AcceptManifest(manifestWithService("svc"))
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
