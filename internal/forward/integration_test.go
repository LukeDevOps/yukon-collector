package forward

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/ingest"
)

// captureSink is an ingest.Sink that records what it receives, standing in
// for a backend that actually decodes the relayed payload instead of just
// checking the raw HTTP request the way the httptest fakes above do. Every
// other test in this package proves ForwardingSink sends the right bytes to
// the right path; these prove that a real backend's own ingest.Handler
// decodes those bytes back into the same message the agent-facing Handler
// started with.
type captureSink struct {
	mu           sync.Mutex
	deltaBatches []*yukonpb.DeltaBatch
	manifests    []*yukonpb.ProbeManifest
	baselines    []*yukonpb.StaticBaseline
}

func (c *captureSink) AcceptDeltaBatch(_ context.Context, batch *yukonpb.DeltaBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deltaBatches = append(c.deltaBatches, batch)
	return nil
}

func (c *captureSink) AcceptManifest(_ context.Context, manifest *yukonpb.ProbeManifest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifests = append(c.manifests, manifest)
	return nil
}

func (c *captureSink) AcceptStaticBaseline(_ context.Context, baseline *yukonpb.StaticBaseline) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baselines = append(c.baselines, baseline)
	return nil
}

func (c *captureSink) deltaBatchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deltaBatches)
}

func (c *captureSink) manifestCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.manifests)
}

func (c *captureSink) baselineCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.baselines)
}

func (c *captureSink) lastDeltaBatch() *yukonpb.DeltaBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deltaBatches[len(c.deltaBatches)-1]
}

func (c *captureSink) lastManifest() *yukonpb.ProbeManifest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.manifests[len(c.manifests)-1]
}

func (c *captureSink) baselinesSnapshot() []*yukonpb.StaticBaseline {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*yukonpb.StaticBaseline(nil), c.baselines...)
}

// newReferenceBackend stands up a real ingest.Handler, the same one the
// collector itself runs, in front of a capturing Sink. It speaks the
// collector's own protobuf-over-HTTP shape well enough to prove
// ForwardingSink's output is valid input to it, without being a real
// storage backend.
func newReferenceBackend() (*httptest.Server, *captureSink) {
	sink := &captureSink{}
	mux := http.NewServeMux()
	ingest.NewHandler(sink, discardLogger()).Register(mux)
	return httptest.NewServer(mux), sink
}

// newFrontCollector stands up the agent-facing side: a real ingest.Handler
// backed by a ForwardingSink pointed at backendURL, the same wiring
// cmd/yukon-collector uses when YUKON_COLLECTOR_FORWARD_URL is set.
func newFrontCollector(t *testing.T, backendURL string) *httptest.Server {
	t.Helper()
	fwd := mustNewSink(t, testConfig(backendURL))
	t.Cleanup(func() { fwd.Shutdown(t.Context()) })
	mux := http.NewServeMux()
	ingest.NewHandler(fwd, discardLogger()).Register(mux)
	return httptest.NewServer(mux)
}

func TestIntegration_DeltaBatch_RoundTripsThroughRealHandlerOnBothEnds(t *testing.T) {
	backend, sink := newReferenceBackend()
	defer backend.Close()

	front := newFrontCollector(t, backend.URL)
	defer front.Close()

	sent := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{
			ServiceName:       "checkout",
			ServiceInstanceId: "instance-7",
			RunId:             "run-1",
		},
		Deltas: []*yukonpb.ProbeDelta{
			{ClassId: 3, ProbeIndex: 1, Kind: yukonpb.ProbeKind_BRANCH, HitsTotal: 42},
		},
		EndpointDeltas: []*yukonpb.EndpointDelta{
			{EndpointId: 5, FirstSeenAt: 1700000000, HitsTotal: 9},
		},
		DependencyDeltas: []*yukonpb.DependencyDelta{
			{DependencyId: 2, FirstLoadedAt: 1700000000, LoadedClassesTotal: 378},
		},
	}
	body, err := proto.Marshal(sent)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(front.URL+ingest.DeltaBatchPath, "application/x-protobuf", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post to front collector: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("front collector status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	waitFor(t, time.Second, func() bool { return sink.deltaBatchCount() == 1 })
	if got := sink.lastDeltaBatch(); !proto.Equal(got, sent) {
		t.Errorf("backend decoded %v, want %v", got, sent)
	}
}

func TestIntegration_Manifest_RoundTripsThroughRealHandlerOnBothEnds(t *testing.T) {
	backend, sink := newReferenceBackend()
	defer backend.Close()

	front := newFrontCollector(t, backend.URL)
	defer front.Close()

	sent := &yukonpb.ProbeManifest{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "checkout", ServiceInstanceId: "instance-7", RunId: "run-1"},
		Probes: []*yukonpb.ProbeLocation{
			{ClassId: 3, ProbeIndex: 1, Kind: yukonpb.ProbeKind_BRANCH, ClassName: "CheckoutService", MethodName: "applyDiscount"},
			{
				ClassId: 3, ProbeIndex: 0, Kind: yukonpb.ProbeKind_METHOD, ClassName: "CheckoutService", MethodName: "checkout",
				Calls: []*yukonpb.CallEdge{
					{ClassName: "CheckoutService", MethodName: "applyDiscount", MethodDescriptor: "()V", Virtual: true},
				},
				ReferencedClasses: []string{"tools.jackson.databind.json.JsonMapper"},
			},
		},
		ClassLocations: []*yukonpb.ClassLocation{
			{ClassId: 3, SuperClassName: "java.lang.Object", InterfaceNames: []string{"Service"}},
		},
		Endpoints: []*yukonpb.EndpointLocation{
			{
				EndpointId:        5,
				Verb:              "POST",
				RouteTemplate:     "/checkout/{id}",
				VerbatimTemplate:  "/checkout/{id}",
				Framework:         "spring-mvc",
				DiscoverySource:   yukonpb.EndpointDiscoverySource_REGISTRATION,
				HandlerClass:      proto.String("CheckoutService"),
				HandlerMethod:     proto.String("applyDiscount"),
				HandlerDescriptor: proto.String("()V"),
			},
		},
		DisabledEndpointModules: []*yukonpb.DisabledEndpointModule{
			{Module: "ktor-2", Reason: "no supported framework class on the classpath", DisabledAt: 1700000000},
		},
		Dependencies: []*yukonpb.DependencyLocation{
			{
				DependencyId:    2,
				Identities:      []*yukonpb.DependencyIdentity{{GroupId: "tools.jackson.core", ArtifactId: "jackson-databind", Version: "3.1.5"}},
				IdentitySource:  yukonpb.DependencyIdentitySource_POM_PROPERTIES,
				Location:        "BOOT-INF/lib/jackson-databind-3.1.5.jar",
				DiscoverySource: yukonpb.DependencyDiscoverySource_STARTUP_CLASSPATH,
				ClassCount:      proto.Int32(880),
			},
		},
		ClassReferences: []*yukonpb.ClassReferences{
			{ClassId: 3, ReferencedClasses: []string{"org.springframework.stereotype.Service"}},
		},
		ExternalClasses: []*yukonpb.ExternalClass{
			{ClassName: "tools.jackson.databind.json.JsonMapper", DependencyId: proto.Int32(2)},
			{ClassName: "org.example.Missing", Absent: true},
		},
		ReferencesRecorded: true,
	}
	body, err := proto.Marshal(sent)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	resp, err := http.Post(front.URL+ingest.ManifestPath, "application/x-protobuf", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post to front collector: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("front collector status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	waitFor(t, time.Second, func() bool { return sink.manifestCount() == 1 })
	if got := sink.lastManifest(); !proto.Equal(got, sent) {
		t.Errorf("backend decoded %v, want %v", got, sent)
	}
}

func TestIntegration_StaticBaseline_RoundTripsThroughRealHandlerOnBothEnds(t *testing.T) {
	backend, sink := newReferenceBackend()
	defer backend.Close()

	front := newFrontCollector(t, backend.URL)
	defer front.Close()

	resource := &yukonpb.ResourceAttributes{ServiceName: "checkout", ServiceInstanceId: "instance-7", RunId: "run-1"}
	const scannedAt = 1700000000

	chunk0 := &yukonpb.StaticBaseline{
		Resource:   resource,
		ScannedAt:  scannedAt,
		ChunkIndex: 0,
		ChunkCount: 2,
		DeclaredClasses: []*yukonpb.DeclaredClass{
			{
				ClassName:         "CheckoutService",
				Methods:           []*yukonpb.DeclaredMethod{{MethodName: "applyDiscount", MethodDescriptor: "()V", ReferencedClasses: []string{"tools.jackson.databind.json.JsonMapper"}}},
				ReferencedClasses: []string{"org.springframework.stereotype.Service"},
			},
		},
	}
	chunk1 := &yukonpb.StaticBaseline{
		Resource:   resource,
		ScannedAt:  scannedAt,
		ChunkIndex: 1,
		ChunkCount: 2,
		DeclaredClasses: []*yukonpb.DeclaredClass{
			{ClassName: "PaymentService", Methods: []*yukonpb.DeclaredMethod{{MethodName: "charge", MethodDescriptor: "()V"}}},
		},
	}

	for _, chunk := range []*yukonpb.StaticBaseline{chunk0, chunk1} {
		body, err := proto.Marshal(chunk)
		if err != nil {
			t.Fatalf("marshal baseline chunk %d: %v", chunk.GetChunkIndex(), err)
		}
		resp, err := http.Post(front.URL+ingest.StaticBaselinePath, "application/x-protobuf", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("post chunk %d to front collector: %v", chunk.GetChunkIndex(), err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("front collector status for chunk %d = %d, want %d", chunk.GetChunkIndex(), resp.StatusCode, http.StatusAccepted)
		}
	}

	waitFor(t, time.Second, func() bool { return sink.baselineCount() == 2 })

	got := sink.baselinesSnapshot()
	byChunk := map[int32]*yukonpb.StaticBaseline{got[0].GetChunkIndex(): got[0], got[1].GetChunkIndex(): got[1]}
	if !proto.Equal(byChunk[0], chunk0) {
		t.Errorf("backend decoded chunk 0 as %v, want %v", byChunk[0], chunk0)
	}
	if !proto.Equal(byChunk[1], chunk1) {
		t.Errorf("backend decoded chunk 1 as %v, want %v", byChunk[1], chunk1)
	}
	if byChunk[0].GetScannedAt() != byChunk[1].GetScannedAt() {
		t.Errorf("chunk scanned_at mismatch: %d vs %d", byChunk[0].GetScannedAt(), byChunk[1].GetScannedAt())
	}
}

// TestIntegration_QueueFull_RefusesWithServiceUnavailable proves the
// backpressure path end to end: a real ingest.Handler in front of a
// ForwardingSink answers 503, not 202, once the shard queue behind it has
// no room, so the agent knows to keep its counts and resend.
func TestIntegration_QueueFull_RefusesWithServiceUnavailable(t *testing.T) {
	blockBackend := make(chan struct{})
	backendReceivedFirst := make(chan struct{})
	var signalFirst sync.Once

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signalFirst.Do(func() { close(backendReceivedFirst) })
		<-blockBackend
		w.WriteHeader(http.StatusOK)
	}))
	// t.Cleanup, not defer: cleanups run after the test body's own defers,
	// so registering backend.Close via defer here would make it run before
	// the unblock-then-shutdown cleanup below, and hang waiting for the
	// still-blocked handler to return.
	t.Cleanup(backend.Close)

	cfg := testConfig(backend.URL)
	cfg.Shards = 1
	cfg.QueueSize = 1
	cfg.RequestTimeout = time.Hour // don't let the timeout unblock the worker under test
	fwd := mustNewSink(t, cfg)
	t.Cleanup(func() {
		close(blockBackend) // unblock the handler before Shutdown waits on the worker
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fwd.Shutdown(ctx)
	})

	mux := http.NewServeMux()
	ingest.NewHandler(fwd, discardLogger()).Register(mux)
	front := httptest.NewServer(mux)
	t.Cleanup(front.Close)

	post := func(serviceName string) *http.Response {
		t.Helper()
		body, err := proto.Marshal(manifestWithService(serviceName))
		if err != nil {
			t.Fatalf("marshal manifest for %s: %v", serviceName, err)
		}
		resp, err := http.Post(front.URL+ingest.ManifestPath, "application/x-protobuf", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("post manifest for %s: %v", serviceName, err)
		}
		return resp
	}

	// svc-1 is picked up by the single worker, which then blocks in the
	// backend handler above, holding the shard's only in-flight slot.
	resp1 := post("svc-1")
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first post status = %d, want %d", resp1.StatusCode, http.StatusAccepted)
	}

	// Wait for the worker to have actually pulled svc-1 off the queue
	// before posting more: otherwise svc-2 could be the one that finds
	// the queue full instead of svc-3.
	select {
	case <-backendReceivedFirst:
	case <-time.After(time.Second):
		t.Fatal("backend never received the first request")
	}

	// svc-2 fills the now-empty queue.
	resp2 := post("svc-2")
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("second post status = %d, want %d", resp2.StatusCode, http.StatusAccepted)
	}

	// svc-3 finds the queue full and must be refused, not acknowledged.
	resp3 := post("svc-3")
	resp3.Body.Close()
	if resp3.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third post status = %d, want %d", resp3.StatusCode, http.StatusServiceUnavailable)
	}
}
