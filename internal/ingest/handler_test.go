package ingest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"
)

type fakeSink struct {
	deltaBatches []*yukonpb.DeltaBatch
	manifests    []*yukonpb.ProbeManifest
}

func (f *fakeSink) AcceptDeltaBatch(batch *yukonpb.DeltaBatch) {
	f.deltaBatches = append(f.deltaBatches, batch)
}

func (f *fakeSink) AcceptManifest(manifest *yukonpb.ProbeManifest) {
	f.manifests = append(f.manifests, manifest)
}

func newTestServer(sink Sink) *httptest.Server {
	mux := http.NewServeMux()
	NewHandler(sink, nil).Register(mux)
	return httptest.NewServer(mux)
}

func TestHandleDeltaBatch_ValidPayload_ReachesSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	batch := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
		},
		Deltas: []*yukonpb.ProbeDelta{
			{ClassId: 1, ProbeIndex: 0, Kind: yukonpb.ProbeKind_METHOD, HitsSinceLastFlush: 5},
		},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/x-protobuf", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.deltaBatches) != 1 {
		t.Fatalf("sink received %d batches, want 1", len(sink.deltaBatches))
	}
	if sink.deltaBatches[0].GetResource().GetServiceName() != "demo-service" {
		t.Errorf("service name = %q, want %q", sink.deltaBatches[0].GetResource().GetServiceName(), "demo-service")
	}
}

func TestHandleDeltaBatch_MalformedBody_RejectedWithoutReachingSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/x-protobuf", strings.NewReader("not a protobuf message"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if len(sink.deltaBatches) != 0 {
		t.Fatalf("sink received %d batches, want 0", len(sink.deltaBatches))
	}
}

func TestHandleDeltaBatch_WrongContentType_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	batch := &yukonpb.DeltaBatch{Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service"}}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnsupportedMediaType)
	}
	if len(sink.deltaBatches) != 0 {
		t.Fatalf("sink received %d batches, want 0", len(sink.deltaBatches))
	}
}

func TestHandleDeltaBatch_BodyTooLarge_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	oversized := strings.Repeat("x", maxBodyBytes+1)

	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/x-protobuf", strings.NewReader(oversized))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
	if len(sink.deltaBatches) != 0 {
		t.Fatalf("sink received %d batches, want 0", len(sink.deltaBatches))
	}
}

func TestHandleManifest_ValidPayload_ReachesSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &yukonpb.ProbeManifest{
		ServiceName: "demo-service",
		Probes: []*yukonpb.ProbeLocation{
			{ClassId: 1, ProbeIndex: 0, Kind: yukonpb.ProbeKind_METHOD, ClassName: "com.example.Foo"},
		},
	}
	body, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/yukon/manifest", "application/x-protobuf", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.manifests) != 1 {
		t.Fatalf("sink received %d manifests, want 1", len(sink.manifests))
	}
}
