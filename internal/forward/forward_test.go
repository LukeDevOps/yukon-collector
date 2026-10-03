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

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/ingest"
	"github.com/otherlodehq/otherlode-collector/metrics"
)

// testConfig returns a Config with small timeouts and retry budgets, in
// place of the multi-second production defaults. Its logger discards the
// Warn lines these tests trigger on purpose.
func testConfig(url string) Config {
	return Config{
		URL:                  url,
		Shards:               1,
		QueueSize:            8,
		Logger:               slog.New(slog.DiscardHandler),
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

func manifestWithService(name string) *otherlodepb.ProbeManifest {
	return &otherlodepb.ProbeManifest{Resource: &otherlodepb.ResourceAttributes{ServiceName: name, ServiceInstanceId: "instance-1", RunId: "run-1"}}
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

// droppedReasons lists every reason the sink counts in ForwardDropped. It
// is built from dropReasons, so a new reason is counted without an edit
// here.
var droppedReasons = func() []string {
	reasons := []string{"marshal"}
	for reason := range dropReasons {
		reasons = append(reasons, reason)
	}
	return reasons
}()

// droppedTotal sums ForwardDropped for payload over every reason the sink
// writes.
func droppedTotal(payload string) int64 {
	var total int64
	for _, reason := range droppedReasons {
		total += metrics.ForwardDropped.Value(payload, reason)
	}
	return total
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
	cfg.AuthToken = StaticToken("backend-secret")
	sink := mustNewSink(t, cfg)
	defer sink.Shutdown(context.Background())

	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

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

	var manifest otherlodepb.ProbeManifest
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

	if err := sink.AcceptDeltaBatch(context.Background(), &otherlodepb.DeltaBatch{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	}); err != nil {
		t.Fatalf("AcceptDeltaBatch: %v", err)
	}

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

	sent := &otherlodepb.StaticBaseline{
		Resource:   &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkIndex: 1,
		ChunkCount: 2,
	}
	if err := sink.AcceptStaticBaseline(context.Background(), sent); err != nil {
		t.Fatalf("AcceptStaticBaseline: %v", err)
	}

	waitFor(t, time.Second, received.Load)
	if gotPath != ingest.StaticBaselinePath {
		t.Errorf("path = %q, want %q", gotPath, ingest.StaticBaselinePath)
	}
	if gotContentType != "application/x-protobuf" {
		t.Errorf("content-type = %q, want application/x-protobuf", gotContentType)
	}

	var got otherlodepb.StaticBaseline
	if err := proto.Unmarshal(gotBody, &got); err != nil {
		t.Fatalf("unmarshal relayed body: %v", err)
	}
	if got.GetChunkIndex() != sent.GetChunkIndex() || got.GetChunkCount() != sent.GetChunkCount() || got.GetScannedAt() != sent.GetScannedAt() {
		t.Errorf("relayed baseline = %v, want it to match %v", &got, sent)
	}
}

func TestForwardingSink_RetryableStatus_RetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	deliveredBefore := metrics.ForwardDelivered.Value("manifest")

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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

	waitFor(t, time.Second, func() bool { return metrics.ForwardDelivered.Value("manifest") == deliveredBefore+1 })
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want exactly 3", got)
	}
}

func TestForwardingSink_PermanentStatus_DroppedWithoutRetry(t *testing.T) {
	var attempts atomic.Int32
	droppedBefore := metrics.ForwardDropped.Value("manifest", "permanent")

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

	waitFor(t, time.Second, func() bool { return metrics.ForwardDropped.Value("manifest", "permanent") == droppedBefore+1 })
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (a plain 500 must not be retried)", got)
	}
}

func TestForwardingSink_RetryBudgetExhausted_StopsRetrying(t *testing.T) {
	var attempts atomic.Int32
	droppedBefore := metrics.ForwardDropped.Value("manifest", "retry_exhausted")

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	defer sink.Shutdown(context.Background())

	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

	// RetryMaxElapsedTime is 200ms with a 5ms initial interval, so the
	// sink gives up well within the wait below.
	waitFor(t, 2*time.Second, func() bool { return metrics.ForwardDropped.Value("manifest", "retry_exhausted") == droppedBefore+1 })
	if got := attempts.Load(); got < 2 {
		t.Fatalf("attempts = %d, want at least 2 (the budget allows more than one try)", got)
	}
}

func TestForwardingSink_QueueFull_RefusesNewestWithoutBlocking(t *testing.T) {
	blockBackend := make(chan struct{})
	entered := make(chan struct{}, 1)

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
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
	cfg.RequestTimeout = time.Hour // keep the timeout from unblocking the worker under test
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		close(blockBackend) // unblock the handler before Shutdown waits on the worker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	refusedBefore := metrics.ForwardRefused.Value("manifest", "queue_full")
	droppedBefore := droppedTotal("manifest")

	// Same key -> same shard -> same queue: item 1 gets picked up by the
	// single worker (which then blocks in the handler above), item 2 fills
	// the queue, item 3 finds the queue full and must be refused. Item 2
	// waits until the handler holds item 1. Before that, item 1 can still
	// sit in the queue, and item 2 is refused in its place.
	var errs [3]error
	errs[0] = sink.AcceptManifest(context.Background(), manifestWithService("svc"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("backend never received the first manifest")
	}
	done := make(chan struct{})
	go func() {
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
	if !strings.Contains(errs[2].Error(), `service "svc" instance "instance-1"`) {
		t.Fatalf("third Accept error = %q, want it to name the service", errs[2])
	}

	if got := metrics.ForwardRefused.Value("manifest", "queue_full"); got != refusedBefore+1 {
		t.Fatalf("refused count = %d, want %d", got, refusedBefore+1)
	}
	if got := droppedTotal("manifest"); got != droppedBefore {
		t.Fatalf("dropped count = %d, want unchanged at %d (a refused payload is not a drop)", got, droppedBefore)
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

	resource := func(runID string) *otherlodepb.ResourceAttributes {
		return &otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "instance-1", RunId: runID}
	}

	if err := sink.AcceptDeltaBatch(context.Background(), &otherlodepb.DeltaBatch{Resource: resource("run-a")}); err != nil {
		t.Fatalf("delta batch: unexpected error: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("backend never received the delta batch")
	}
	if err := sink.AcceptManifest(context.Background(), &otherlodepb.ProbeManifest{Resource: resource("run-b")}); err != nil {
		t.Fatalf("manifest: unexpected error: %v", err)
	}
	baseline := &otherlodepb.StaticBaseline{Resource: resource("run-c"), ScannedAt: 1700000000, ChunkCount: 1}
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
		var manifest otherlodepb.ProbeManifest
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService(keyA)); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	if err := sink.AcceptManifest(context.Background(), manifestWithService(keyB)); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

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

	// The worker picks svc-1 up and blocks in the handler.
	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc-1")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	waitFor(t, time.Second, func() bool { return attempts.Load() == 1 })
	// svc-2 and svc-3 sit queued behind it.
	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc-2")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc-3")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}

	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		sink.Shutdown(ctx)
		close(shutdownDone)
	}()

	// Release the in-flight request only after Shutdown has started.
	// Otherwise svc-1 could retry before the sink stops and add attempts.
	select {
	case <-sink.stopping:
	case <-time.After(time.Second):
		t.Fatal("Shutdown never started")
	}
	close(release)

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return promptly; it may have run the full retry sequence per item")
	}

	// svc-1's in-flight attempt (already running when Shutdown was
	// called) comes back retryable, so it gets one final attempt of its
	// own, same as a queued item would: 2 attempts. Plus exactly one
	// drain attempt each for svc-2 and svc-3: 4 total, never a
	// multi-attempt retry sequence for any of them.
	if got := attempts.Load(); got != 4 {
		t.Fatalf("attempts = %d, want exactly 4 (svc-1's initial and final attempt, one each for svc-2 and svc-3, no retries during drain)", got)
	}
}

// TestForwardingSink_Shutdown_DrainsShardsInParallel holds one item per
// shard in the queue. The backend answers each drain request only once it
// has a drain request from both shards in flight at the same time. A serial
// drain never reaches that state, so its items time out and fail.
func TestForwardingSink_Shutdown_DrainsShardsInParallel(t *testing.T) {
	const numShards = 4
	svcA, svcB := findKeysInDifferentShards(t, numShards)

	release := make(chan struct{})
	var seen sync.Map
	var drainInFlight atomic.Int32
	var barrierTimedOut atomic.Bool

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var manifest otherlodepb.ProbeManifest
		body, _ := io.ReadAll(r.Body)
		_ = proto.Unmarshal(body, &manifest)
		if _, again := seen.LoadOrStore(manifest.GetResource().GetServiceName(), true); !again {
			<-release
			w.WriteHeader(http.StatusOK)
			return
		}
		drainInFlight.Add(1)
		deadline := time.Now().Add(2 * time.Second)
		for drainInFlight.Load() < 2 {
			if time.Now().After(deadline) {
				barrierTimedOut.Store(true)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			time.Sleep(time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	cfg := testConfig(backend.URL)
	cfg.Shards = numShards
	cfg.RequestTimeout = 5 * time.Second
	sink := mustNewSink(t, cfg)
	deliveredBefore := metrics.ForwardDelivered.Value("manifest")

	// The first item per shard is held by its worker, blocked in the
	// backend. The second item per shard waits in the queue.
	for _, svc := range []string{svcA, svcB} {
		if err := sink.AcceptManifest(context.Background(), manifestWithService(svc)); err != nil {
			t.Fatalf("AcceptManifest %s: %v", svc, err)
		}
	}
	waitFor(t, time.Second, func() bool {
		_, a := seen.Load(svcA)
		_, b := seen.Load(svcB)
		return a && b
	})
	for _, svc := range []string{svcA, svcB} {
		if err := sink.AcceptManifest(context.Background(), manifestWithService(svc)); err != nil {
			t.Fatalf("AcceptManifest %s: %v", svc, err)
		}
	}

	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		sink.Shutdown(ctx)
		close(shutdownDone)
	}()
	select {
	case <-sink.stopping:
	case <-time.After(time.Second):
		t.Fatal("Shutdown never started")
	}
	close(release)
	select {
	case <-shutdownDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return")
	}

	if barrierTimedOut.Load() {
		t.Fatal("drain requests for two shards were never in flight together, so the drain is serial")
	}
	if got := metrics.ForwardDelivered.Value("manifest"); got != deliveredBefore+4 {
		t.Fatalf("delivered count = %d, want %d", got, deliveredBefore+4)
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
			keyA := instanceOf(a.GetResource()).shardKey()
			keyB := instanceOf(b.GetResource()).shardKey()
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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

func TestForwardingSink_Redirect_IsPermanentFailure(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	var attempts atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer backend.Close()

	var logs bytes.Buffer
	cfg := testConfig(backend.URL)
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	sink := mustNewSink(t, cfg)
	defer sink.Shutdown(context.Background())
	deliveredBefore := metrics.ForwardDelivered.Value("manifest")
	droppedBefore := metrics.ForwardDropped.Value("manifest", "permanent")

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	waitFor(t, time.Second, func() bool { return metrics.ForwardDropped.Value("manifest", "permanent") == droppedBefore+1 })

	if got := targetHits.Load(); got != 0 {
		t.Fatalf("redirect target received %d requests, want 0", got)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want exactly 1 (a redirect must not be retried)", got)
	}
	if got := metrics.ForwardDelivered.Value("manifest"); got != deliveredBefore {
		t.Fatalf("delivered count = %d, want unchanged at %d", got, deliveredBefore)
	}
	if !strings.Contains(logs.String(), "backend returned 302 Found") {
		t.Fatalf("drop log does not name the 302:\n%s", logs.String())
	}
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
	for _, raw := range invalidURLs {
		cfg := testConfig(raw)
		if _, err := NewForwardingSink(cfg); err == nil {
			t.Errorf("URL %q: expected an error, got nil", raw)
		}
	}
}

var invalidURLs = []string{
	"", "backend.example.com", "ftp://backend.example.com", "http://", "://bad",
	"https://h/base?x=1", "https://h/base?", "https://h/base#frag", "https://h/base#",
	"https://user:pass@h", "https://user@h/base",
}

func TestValidateURL(t *testing.T) {
	for _, raw := range invalidURLs {
		if _, _, err := validateURL(raw); err == nil {
			t.Errorf("validateURL(%q): expected an error, got nil", raw)
		}
	}

	valid := []struct {
		raw        string
		wantBase   string
		wantScheme string
	}{
		{"https://h", "https://h", "https"},
		{"https://h/base", "https://h/base", "https"},
		{"https://h/base//", "https://h/base", "https"},
		{"http://h:8080/", "http://h:8080", "http"},
		{"HTTP://h/base/", "HTTP://h/base", "http"},
	}
	for _, tt := range valid {
		base, scheme, err := validateURL(tt.raw)
		if err != nil {
			t.Errorf("validateURL(%q): unexpected error: %v", tt.raw, err)
			continue
		}
		if base != tt.wantBase || scheme != tt.wantScheme {
			t.Errorf("validateURL(%q) = %q, %q, want %q, %q", tt.raw, base, scheme, tt.wantBase, tt.wantScheme)
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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

	resource := &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}
	resource.SetServiceNamespace("team-a")
	if err := sink.AcceptDeltaBatch(context.Background(), &otherlodepb.DeltaBatch{Resource: resource}); err != nil {
		t.Fatalf("AcceptDeltaBatch: %v", err)
	}
	sink.Shutdown(context.Background())

	got := logs.String()
	for _, want := range []string{"namespace=team-a", "service=demo-service", "instance=instance-1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("drop log does not contain %q:\n%s", want, got)
		}
	}
}

func TestInstance_ShardKey_DistinctInstancesNeverCollide(t *testing.T) {
	instances := []instance{
		{namespace: "a/b", service: "c", id: "d"},
		{namespace: "a", service: "b/c", id: "d"},
		{namespace: "a", service: "b", id: "c/d"},
		{namespace: "", service: "a/b/c", id: "d"},
		{namespace: "", service: "a", id: "b"},
		{namespace: "a", service: "b", id: ""},
		{namespace: "a", service: "", id: "b"},
		{namespace: "", service: "a/b", id: ""},
		{namespace: "1:a", service: "b", id: "c"},
		{namespace: "1", service: "a1:b", id: "c"},
		{namespace: "", service: "", id: "1:a1:b"},
		{namespace: "a:", service: "b", id: "c"},
		{namespace: "a", service: ":b", id: "c"},
	}
	seen := make(map[string]instance, len(instances))
	for _, in := range instances {
		key := in.shardKey()
		if other, ok := seen[key]; ok {
			t.Fatalf("%+v and %+v share the shard key %q", other, in, key)
		}
		seen[key] = in
	}
}

func TestInstance_ShardKey_SameInstanceSameKey(t *testing.T) {
	a := instanceOf(&otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: "run-1"})
	b := instanceOf(&otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: "run-2"})
	if a.shardKey() != b.shardKey() {
		t.Fatalf("shard keys differ across runs of one instance: %q and %q", a.shardKey(), b.shardKey())
	}
}

func TestInstance_ShardKey_NamespaceTellsInstancesApart(t *testing.T) {
	res := &otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: "run-1"}
	unspecified := instanceOf(res).shardKey()
	res.SetServiceNamespace("team-a")
	if named := instanceOf(res).shardKey(); named == unspecified {
		t.Fatalf("namespace %q and the unspecified namespace share the shard key %q", "team-a", named)
	}
}

func TestInstance_ShardKey_SurroundingSpacesKeepOneKey(t *testing.T) {
	trimmed := &otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: "run-1"}
	trimmed.SetServiceNamespace("team-a")
	padded := &otherlodepb.ResourceAttributes{ServiceName: " svc ", ServiceInstanceId: "i1", RunId: "run-1"}
	padded.SetServiceNamespace(" team-a ")
	if a, b := instanceOf(trimmed).shardKey(), instanceOf(padded).shardKey(); a != b {
		t.Fatalf("surrounding spaces split one service across shard keys %q and %q", a, b)
	}
}

func TestInstanceOf_NilResource_ZeroInstance(t *testing.T) {
	if got := instanceOf(nil); got != (instance{}) {
		t.Fatalf("instanceOf(nil) = %+v, want the zero instance", got)
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
	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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
	droppedBefore := droppedTotal("manifest")
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
	if got := droppedTotal("manifest"); got != droppedBefore {
		t.Fatalf("dropped count = %d, want unchanged at %d (a refused payload is not a drop)", got, droppedBefore)
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
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

// An Accept that has passed the shutdown check when Shutdown starts must
// still reach the backend: the handler answers 202 for it, so the agent
// will not send it again. beforeSend holds the Accept between the check
// and the queue send long enough for an unguarded Shutdown to finish its
// drain first.
func TestForwardingSink_AcceptRacingShutdown_AcceptedPayloadIsDelivered(t *testing.T) {
	var delivered atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	sink := mustNewSink(t, testConfig(backend.URL))
	shutdownDone := make(chan struct{})
	sink.beforeSend = func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			sink.Shutdown(ctx)
			close(shutdownDone)
		}()
		time.Sleep(100 * time.Millisecond)
	}

	if err := sink.AcceptManifest(context.Background(), manifestWithService("svc")); err != nil {
		t.Fatalf("AcceptManifest = %v, want nil: it passed the shutdown check before Shutdown began", err)
	}
	<-shutdownDone
	if got := delivered.Load(); got != 1 {
		t.Fatalf("backend received %d payloads, want the 1 accepted", got)
	}
}

// swappableToken is a TokenSource a test can change while the sink runs.
type swappableToken struct {
	v atomic.Value
}

func (s *swappableToken) Token() string {
	token, _ := s.v.Load().(string)
	return token
}

func TestForwardingSink_AuthTokenChanges_NextRequestSendsNewKey(t *testing.T) {
	auths := make(chan string, 8)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auths <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	token := new(swappableToken)
	token.v.Store("old-key")
	cfg := testConfig(backend.URL)
	cfg.AuthToken = token
	sink := mustNewSink(t, cfg)
	defer sink.Shutdown(context.Background())

	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	if got := <-auths; got != "Bearer old-key" {
		t.Fatalf("first authorization = %q, want %q", got, "Bearer old-key")
	}

	token.v.Store("new-key")
	if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
		t.Fatalf("AcceptManifest: %v", err)
	}
	if got := <-auths; got != "Bearer new-key" {
		t.Fatalf("second authorization = %q, want %q", got, "Bearer new-key")
	}
}

func TestForwardingSink_EmptyAuthToken_SendsNoHeader(t *testing.T) {
	for name, source := range map[string]TokenSource{
		"nil source":   nil,
		"empty static": StaticToken(""),
	} {
		t.Run(name, func(t *testing.T) {
			auths := make(chan []string, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auths <- r.Header.Values("Authorization")
				w.WriteHeader(http.StatusOK)
			}))
			defer backend.Close()

			cfg := testConfig(backend.URL)
			cfg.AuthToken = source
			sink := mustNewSink(t, cfg)
			defer sink.Shutdown(context.Background())

			if err := sink.AcceptManifest(context.Background(), manifestWithService("demo-service")); err != nil {
				t.Fatalf("AcceptManifest: %v", err)
			}
			if got := <-auths; len(got) != 0 {
				t.Fatalf("authorization headers = %q, want none", got)
			}
		})
	}
}

func TestNewForwardingSink_PlainHTTPWithKey_Warns(t *testing.T) {
	token := new(swappableToken)
	token.v.Store("file-key")

	tests := map[string]struct {
		url      string
		token    TokenSource
		wantWarn bool
	}{
		"http with static key":   {url: "http://backend.example.com", token: StaticToken("backend-secret"), wantWarn: true},
		"http with changing key": {url: "http://backend.example.com", token: token, wantWarn: true},
		"http without key":       {url: "http://backend.example.com", token: nil, wantWarn: false},
		"https with key":         {url: "https://backend.example.com", token: StaticToken("backend-secret"), wantWarn: false},
		"upper-case HTTP":        {url: "HTTP://backend.example.com", token: StaticToken("backend-secret"), wantWarn: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			cfg := testConfig(tt.url)
			cfg.AuthToken = tt.token
			cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			sink := mustNewSink(t, cfg)
			sink.Shutdown(context.Background())

			warned := strings.Contains(logs.String(), "plain http")
			if warned != tt.wantWarn {
				t.Fatalf("plain http warning logged = %v, want %v:\n%s", warned, tt.wantWarn, logs.String())
			}
			if strings.Contains(logs.String(), "backend-secret") || strings.Contains(logs.String(), "file-key") {
				t.Fatalf("log shows the key:\n%s", logs.String())
			}
		})
	}
}

// blockingBackend starts a backend that holds every request until the
// returned release function runs. entered receives one value per request.
func blockingBackend(t *testing.T) (url string, entered chan struct{}, release func()) {
	t.Helper()
	gate := make(chan struct{})
	entered = make(chan struct{}, 16)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-gate
		w.WriteHeader(http.StatusOK)
	}))
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(backend.Close)
	t.Cleanup(release)
	return backend.URL, entered, release
}

func waitEntered(t *testing.T, entered chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("backend never received a request")
	}
}

func TestForwardingSink_QueueBytes_RefusesItemOverBudgetUntilWorkerFinishes(t *testing.T) {
	url, entered, release := blockingBackend(t)
	item := manifestWithService("svc")
	size := proto.Size(item)

	cfg := testConfig(url)
	cfg.QueueBytes = size + size/2
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	refusedBefore := metrics.ForwardRefused.Value("manifest", "queue_full")
	droppedBefore := droppedTotal("manifest")

	if err := sink.AcceptManifest(context.Background(), item); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	waitEntered(t, entered)

	err := sink.AcceptManifest(context.Background(), item)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second Accept error = %v, want it to wrap ErrQueueFull while the first is in flight", err)
	}
	if got := metrics.ForwardRefused.Value("manifest", "queue_full"); got != refusedBefore+1 {
		t.Errorf("refused count = %d, want %d", got, refusedBefore+1)
	}
	if got := sink.shards[0].heldBytes(); got != size {
		t.Errorf("held bytes after a refusal = %d, want %d (the refused item must not stay counted)", got, size)
	}

	release()
	waitFor(t, time.Second, func() bool { return sink.shards[0].heldBytes() == 0 })
	if err := sink.AcceptManifest(context.Background(), item); err != nil {
		t.Fatalf("Accept after the worker finished: %v", err)
	}
	if got := droppedTotal("manifest"); got != droppedBefore {
		t.Errorf("dropped count = %d, want unchanged at %d", got, droppedBefore)
	}
}

func TestForwardingSink_QueueBytes_OversizedItemPassesThroughIdleShard(t *testing.T) {
	url, entered, release := blockingBackend(t)
	item := manifestWithService("svc")

	cfg := testConfig(url)
	cfg.QueueBytes = 1
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	if err := sink.AcceptManifest(context.Background(), item); err != nil {
		t.Fatalf("oversized item on an idle shard: %v", err)
	}
	waitEntered(t, entered)
	if err := sink.AcceptManifest(context.Background(), item); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second oversized item error = %v, want it to wrap ErrQueueFull while the shard is busy", err)
	}

	release()
	waitFor(t, time.Second, func() bool { return sink.shards[0].heldBytes() == 0 })
	if err := sink.AcceptManifest(context.Background(), item); err != nil {
		t.Fatalf("oversized item after the shard went idle: %v", err)
	}
}

func TestForwardingSink_QueueBytes_ConcurrentEnqueuesNeverPassBudget(t *testing.T) {
	url, entered, release := blockingBackend(t)
	item := manifestWithService("svc")
	size := proto.Size(item)

	cfg := testConfig(url)
	cfg.QueueSize = 64
	cfg.QueueBytes = 3 * size
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(func() {
		release()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		sink.Shutdown(ctx)
	})

	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sink.AcceptManifest(context.Background(), item); err == nil {
				accepted.Add(1)
			} else if !errors.Is(err, ErrQueueFull) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	waitEntered(t, entered)

	if got := accepted.Load(); got != 3 {
		t.Errorf("accepted = %d, want 3 (budget holds three items)", got)
	}
	if got := sink.shards[0].heldBytes(); got != 3*size {
		t.Errorf("held bytes = %d, want %d", got, 3*size)
	}
}

func TestForwardingSink_QueueBytes_ReturnToZeroAfterShutdown(t *testing.T) {
	url, entered, release := blockingBackend(t)
	item := manifestWithService("svc")

	cfg := testConfig(url)
	cfg.RequestTimeout = time.Hour
	sink := mustNewSink(t, cfg)
	t.Cleanup(release)

	for range 3 {
		if err := sink.AcceptManifest(context.Background(), item); err != nil {
			t.Fatalf("Accept: %v", err)
		}
	}
	waitEntered(t, entered)

	// The deadline ends while one item is in flight and two are queued.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	sink.Shutdown(ctx)

	if got := sink.shards[0].heldBytes(); got != 0 {
		t.Errorf("held bytes after Shutdown = %d, want 0", got)
	}
}
