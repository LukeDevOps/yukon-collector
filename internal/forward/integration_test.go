package forward

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/internal/ingest"
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

func (c *captureSink) AcceptDeltaBatch(batch *yukonpb.DeltaBatch) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deltaBatches = append(c.deltaBatches, batch)
}

func (c *captureSink) AcceptManifest(manifest *yukonpb.ProbeManifest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.manifests = append(c.manifests, manifest)
}

func (c *captureSink) AcceptStaticBaseline(baseline *yukonpb.StaticBaseline) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.baselines = append(c.baselines, baseline)
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
		},
		Deltas: []*yukonpb.ProbeDelta{
			{ClassId: 3, ProbeIndex: 1, Kind: yukonpb.ProbeKind_BRANCH, HitsTotal: 42},
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
		ServiceName:       "checkout",
		ServiceInstanceId: "instance-7",
		Probes: []*yukonpb.ProbeLocation{
			{ClassId: 3, ProbeIndex: 1, Kind: yukonpb.ProbeKind_BRANCH, ClassName: "CheckoutService", MethodName: "applyDiscount"},
		},
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

	resource := &yukonpb.ResourceAttributes{ServiceName: "checkout", ServiceInstanceId: "instance-7"}
	const scannedAt = 1700000000

	chunk0 := &yukonpb.StaticBaseline{
		Resource:   resource,
		ScannedAt:  scannedAt,
		ChunkIndex: 0,
		ChunkCount: 2,
		DeclaredClasses: []*yukonpb.DeclaredClass{
			{ClassName: "CheckoutService", Methods: []*yukonpb.DeclaredMethod{{MethodName: "applyDiscount", MethodDescriptor: "()V"}}},
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
