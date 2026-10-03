package ingest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

type fakeSink struct {
	deltaBatches []*otherlodepb.DeltaBatch
	manifests    []*otherlodepb.ProbeManifest
	baselines    []*otherlodepb.StaticBaseline
}

func (f *fakeSink) AcceptDeltaBatch(_ context.Context, batch *otherlodepb.DeltaBatch) error {
	f.deltaBatches = append(f.deltaBatches, batch)
	return nil
}

func (f *fakeSink) AcceptManifest(_ context.Context, manifest *otherlodepb.ProbeManifest) error {
	f.manifests = append(f.manifests, manifest)
	return nil
}

func (f *fakeSink) AcceptStaticBaseline(_ context.Context, baseline *otherlodepb.StaticBaseline) error {
	f.baselines = append(f.baselines, baseline)
	return nil
}

// failingSink refuses every payload, standing in for a backend that could
// not take the write (a database outage, for example).
type failingSink struct{}

func (failingSink) AcceptDeltaBatch(context.Context, *otherlodepb.DeltaBatch) error {
	return errors.New("sink unavailable")
}

func (failingSink) AcceptManifest(context.Context, *otherlodepb.ProbeManifest) error {
	return errors.New("sink unavailable")
}

func (failingSink) AcceptStaticBaseline(context.Context, *otherlodepb.StaticBaseline) error {
	return errors.New("sink unavailable")
}

// ctxKey avoids collisions with any key another package might set on the
// same context.
type ctxKey string

// ctxCheckSink asserts that the context it receives carries the value set
// under key, proving the handler passes the request's own context through
// rather than a detached one. calls counts the payloads it received.
type ctxCheckSink struct {
	t     *testing.T
	key   ctxKey
	calls *atomic.Int32
}

func (s ctxCheckSink) checkContext(ctx context.Context) {
	s.calls.Add(1)
	if got := ctx.Value(s.key); got != "request-scoped" {
		s.t.Errorf("sink saw context value %v, want %q", got, "request-scoped")
	}
}

func (s ctxCheckSink) AcceptDeltaBatch(ctx context.Context, _ *otherlodepb.DeltaBatch) error {
	s.checkContext(ctx)
	return nil
}

func (s ctxCheckSink) AcceptManifest(ctx context.Context, _ *otherlodepb.ProbeManifest) error {
	s.checkContext(ctx)
	return nil
}

func (s ctxCheckSink) AcceptStaticBaseline(ctx context.Context, _ *otherlodepb.StaticBaseline) error {
	s.checkContext(ctx)
	return nil
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

	batch := &otherlodepb.DeltaBatch{
		Resource: &otherlodepb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		Deltas: []*otherlodepb.ProbeDelta{
			{ClassId: 1, ProbeIndex: 0, Kind: otherlodepb.ProbeKind_METHOD, HitsTotal: 5},
		},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/otherlode/deltas", "application/x-protobuf", strings.NewReader(string(body)))
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

	resp, err := http.Post(server.URL+"/v1/otherlode/deltas", "application/x-protobuf", strings.NewReader("not a protobuf message"))
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

	batch := &otherlodepb.DeltaBatch{Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/otherlode/deltas", "application/json", strings.NewReader(string(body)))
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

	resp, err := http.Post(server.URL+"/v1/otherlode/deltas", "application/x-protobuf", strings.NewReader(oversized))
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

func TestHandleDeltaBatch_EndpointDeltas_ReachSinkIntact(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	batch := &otherlodepb.DeltaBatch{
		Resource: &otherlodepb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		EndpointDeltas: []*otherlodepb.EndpointDelta{
			{EndpointId: 1, FirstSeenAt: 1700000000, HitsTotal: 7},
			{EndpointId: 2, FirstSeenAt: 1700000100, HitsTotal: 0},
		},
	}

	resp := postProto(t, server.URL+DeltaBatchPath, batch)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.deltaBatches) != 1 {
		t.Fatalf("sink received %d batches, want 1", len(sink.deltaBatches))
	}
	if !proto.Equal(sink.deltaBatches[0], batch) {
		t.Errorf("sink received %v, want %v", sink.deltaBatches[0], batch)
	}
}

func TestHandleManifest_ValidPayload_ReachesSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &otherlodepb.ProbeManifest{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		Probes: []*otherlodepb.ProbeLocation{
			{ClassId: 1, ProbeIndex: 0, Kind: otherlodepb.ProbeKind_METHOD, ClassName: "com.example.Foo"},
		},
	}
	body, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/otherlode/manifest", "application/x-protobuf", strings.NewReader(string(body)))
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
	res := sink.manifests[0].GetResource()
	if res.GetServiceName() != "demo-service" || res.GetServiceInstanceId() != "instance-1" || res.GetRunId() != "run-1" {
		t.Errorf("manifest resource = %v, want demo-service/instance-1/run-1", res)
	}
}

func TestHandleManifest_Endpoints_ReachSinkIntact(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &otherlodepb.ProbeManifest{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		Endpoints: []*otherlodepb.EndpointLocation{
			{
				EndpointId:        1,
				Verb:              "GET",
				RouteTemplate:     "/checkout/{id}",
				VerbatimTemplate:  "/checkout/{id}",
				Framework:         "spring-mvc",
				DiscoverySource:   otherlodepb.EndpointDiscoverySource_REGISTRATION,
				HandlerClass:      proto.String("com.example.CheckoutController"),
				HandlerMethod:     proto.String("get"),
				HandlerDescriptor: proto.String("(Ljava/lang/String;)Lorg/springframework/http/ResponseEntity;"),
			},
			{
				EndpointId:       2,
				Verb:             "POST",
				RouteTemplate:    "/promo",
				VerbatimTemplate: "/promo",
				Framework:        "spring-mvc",
				DiscoverySource:  otherlodepb.EndpointDiscoverySource_DISPATCH,
			},
		},
		DisabledEndpointModules: []*otherlodepb.DisabledEndpointModule{
			{Module: "jdk-httpserver", Reason: "no supported framework class on the classpath", DisabledAt: 1700000000},
		},
	}

	resp := postProto(t, server.URL+ManifestPath, manifest)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.manifests) != 1 {
		t.Fatalf("sink received %d manifests, want 1", len(sink.manifests))
	}
	if !proto.Equal(sink.manifests[0], manifest) {
		t.Errorf("sink received %v, want %v", sink.manifests[0], manifest)
	}
}

func TestHandleManifest_EndpointsWithoutInstanceId_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &otherlodepb.ProbeManifest{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"},
		Endpoints: []*otherlodepb.EndpointLocation{
			{EndpointId: 1, Verb: "GET", RouteTemplate: "/checkout/{id}"},
		},
	}

	resp := postProto(t, server.URL+ManifestPath, manifest)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d (endpoints present does not excuse the missing service_instance_id)", resp.StatusCode, http.StatusBadRequest)
	}
	if len(sink.manifests) != 0 {
		t.Fatalf("sink received %d manifests, want 0", len(sink.manifests))
	}
}

func TestHandleManifest_MalformedBody_RejectedWithoutReachingSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp, err := http.Post(server.URL+"/v1/otherlode/manifest", "application/x-protobuf", strings.NewReader("not a protobuf message"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if len(sink.manifests) != 0 {
		t.Fatalf("sink received %d manifests, want 0", len(sink.manifests))
	}
}

func TestHandleManifest_WrongContentType_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &otherlodepb.ProbeManifest{Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}}
	body, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/otherlode/manifest", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnsupportedMediaType)
	}
	if len(sink.manifests) != 0 {
		t.Fatalf("sink received %d manifests, want 0", len(sink.manifests))
	}
}

func TestHandleManifest_BodyTooLarge_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	oversized := strings.Repeat("x", maxBodyBytes+1)

	resp, err := http.Post(server.URL+"/v1/otherlode/manifest", "application/x-protobuf", strings.NewReader(oversized))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
	if len(sink.manifests) != 0 {
		t.Fatalf("sink received %d manifests, want 0", len(sink.manifests))
	}
}

func TestHandleDeltaBatch_ContentTypeWithParameters_Accepted(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	batch := &otherlodepb.DeltaBatch{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/otherlode/deltas", "application/x-protobuf; charset=utf-8", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (media type parameters must be ignored)", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.deltaBatches) != 1 {
		t.Fatalf("sink received %d batches, want 1", len(sink.deltaBatches))
	}
}

func postProto(t *testing.T, url string, msg proto.Message) *http.Response {
	t.Helper()
	body, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/x-protobuf", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestHandleDeltaBatch_MissingIdentity_RejectedWithoutReachingSink(t *testing.T) {
	for name, batch := range map[string]*otherlodepb.DeltaBatch{
		"empty body":          {},
		"no resource":         {Deltas: []*otherlodepb.ProbeDelta{{ClassId: 1}}},
		"no service name":     {Resource: &otherlodepb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}},
		"no service instance": {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}},
		"no run id":           {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+"/v1/otherlode/deltas", batch)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			if len(sink.deltaBatches) != 0 {
				t.Fatalf("sink received %d batches, want 0", len(sink.deltaBatches))
			}
		})
	}
}

func TestHandleManifest_MissingIdentity_RejectedWithoutReachingSink(t *testing.T) {
	for name, manifest := range map[string]*otherlodepb.ProbeManifest{
		"empty body":          {},
		"no resource":         {Probes: []*otherlodepb.ProbeLocation{{ClassId: 1}}},
		"no service name":     {Resource: &otherlodepb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}},
		"no service instance": {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}},
		"no run id":           {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+"/v1/otherlode/manifest", manifest)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			if len(sink.manifests) != 0 {
				t.Fatalf("sink received %d manifests, want 0", len(sink.manifests))
			}
		})
	}
}

func TestHandler_EmptyRunId_RejectedForEveryPayload(t *testing.T) {
	res := &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}
	for path, msg := range map[string]proto.Message{
		DeltaBatchPath:     &otherlodepb.DeltaBatch{Resource: res},
		ManifestPath:       &otherlodepb.ProbeManifest{Resource: res},
		StaticBaselinePath: &otherlodepb.StaticBaseline{Resource: res, ScannedAt: 1700000000, ChunkCount: 1},
	} {
		t.Run(path, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+path, msg)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if !strings.Contains(string(body), "resource.run_id is empty") {
				t.Errorf("body = %q, want it to name resource.run_id", body)
			}
			if n := len(sink.deltaBatches) + len(sink.manifests) + len(sink.baselines); n != 0 {
				t.Fatalf("sink received %d payloads, want 0", n)
			}
		})
	}
}

func TestHandler_UnnameableServiceIdentity_RejectedForEveryPayload(t *testing.T) {
	withNamespace := func(namespace string) *otherlodepb.ResourceAttributes {
		res := &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}
		res.SetServiceNamespace(namespace)
		return res
	}
	for name, tc := range map[string]struct {
		res  *otherlodepb.ResourceAttributes
		want string
	}{
		"blank service name": {&otherlodepb.ResourceAttributes{ServiceName: "   ", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name is empty"},
		"dot service name":   {&otherlodepb.ResourceAttributes{ServiceName: ".", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name"},
		"dots service name":  {&otherlodepb.ResourceAttributes{ServiceName: " .. ", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name"},
		"dot namespace":      {withNamespace("."), "resource.service_namespace"},
		"dots namespace":     {withNamespace(" .. "), "resource.service_namespace"},
	} {
		for path, msg := range map[string]proto.Message{
			DeltaBatchPath:     &otherlodepb.DeltaBatch{Resource: tc.res},
			ManifestPath:       &otherlodepb.ProbeManifest{Resource: tc.res},
			StaticBaselinePath: &otherlodepb.StaticBaseline{Resource: tc.res, ScannedAt: 1700000000, ChunkCount: 1},
		} {
			t.Run(name+" "+path, func(t *testing.T) {
				sink := &fakeSink{}
				server := newTestServer(sink)
				defer server.Close()

				resp := postProto(t, server.URL+path, msg)
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
				}
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("read body: %v", err)
				}
				if !strings.Contains(string(body), tc.want) {
					t.Errorf("body = %q, want it to name %s", body, tc.want)
				}
				if n := len(sink.deltaBatches) + len(sink.manifests) + len(sink.baselines); n != 0 {
					t.Fatalf("sink received %d payloads, want 0", n)
				}
			})
		}
	}
}

func TestHandler_DotsInsideAName_Accepted(t *testing.T) {
	res := &otherlodepb.ResourceAttributes{ServiceName: "checkout.v2", ServiceInstanceId: "instance-1", RunId: "run-1"}
	res.SetServiceNamespace("...")
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp := postProto(t, server.URL+DeltaBatchPath, &otherlodepb.DeltaBatch{Resource: res})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
}

func TestHandler_WrongMethod_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	for _, path := range []string{DeltaBatchPath, ManifestPath, StaticBaselinePath} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("get %s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("GET %s: status = %d, want %d", path, resp.StatusCode, http.StatusMethodNotAllowed)
		}
		if allow := resp.Header.Get("Allow"); !strings.Contains(allow, http.MethodPost) {
			t.Errorf("GET %s: Allow = %q, want it to list POST", path, allow)
		}
	}
	if len(sink.deltaBatches)+len(sink.manifests)+len(sink.baselines) != 0 {
		t.Fatal("a non-POST request reached the sink")
	}
}

func TestHandleStaticBaseline_ValidPayload_ReachesSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	baseline := &otherlodepb.StaticBaseline{
		Resource: &otherlodepb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		ScannedAt:  1700000000,
		ChunkIndex: 0,
		ChunkCount: 2,
		DeclaredClasses: []*otherlodepb.DeclaredClass{
			{
				ClassName: "com.example.Foo",
				Methods:   []*otherlodepb.DeclaredMethod{{MethodName: "bar", MethodDescriptor: "()V"}},
			},
		},
		StaticallyUnsafeClasses: []*otherlodepb.StaticallyUnsafeClass{
			{ClassName: "com.example.Unsafe", Reason: "annotation not legal on a type"},
		},
		UnreadableClasses: []*otherlodepb.UnreadableClass{
			{ClassName: "com.example.Unreadable", Reason: "corrupt class file"},
		},
		UnprobedClasses: []*otherlodepb.UnprobedClass{
			{ClassName: "com.example.Unprobed", Reason: "interface with no concrete methods"},
		},
	}

	resp := postProto(t, server.URL+StaticBaselinePath, baseline)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.baselines) != 1 {
		t.Fatalf("sink received %d baselines, want 1", len(sink.baselines))
	}
	if !proto.Equal(sink.baselines[0], baseline) {
		t.Errorf("sink received %v, want %v", sink.baselines[0], baseline)
	}
}

func TestHandleStaticBaseline_MalformedBody_RejectedWithoutReachingSink(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp, err := http.Post(server.URL+StaticBaselinePath, "application/x-protobuf", strings.NewReader("not a protobuf message"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if len(sink.baselines) != 0 {
		t.Fatalf("sink received %d baselines, want 0", len(sink.baselines))
	}
}

func TestHandleStaticBaseline_WrongContentType_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	baseline := &otherlodepb.StaticBaseline{
		Resource:   &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkCount: 1,
	}
	body, err := proto.Marshal(baseline)
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}

	resp, err := http.Post(server.URL+StaticBaselinePath, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnsupportedMediaType)
	}
	if len(sink.baselines) != 0 {
		t.Fatalf("sink received %d baselines, want 0", len(sink.baselines))
	}
}

func TestHandleStaticBaseline_BodyTooLarge_Rejected(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	oversized := strings.Repeat("x", maxBodyBytes+1)

	resp, err := http.Post(server.URL+StaticBaselinePath, "application/x-protobuf", strings.NewReader(oversized))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusRequestEntityTooLarge)
	}
	if len(sink.baselines) != 0 {
		t.Fatalf("sink received %d baselines, want 0", len(sink.baselines))
	}
}

func TestHandleStaticBaseline_InvalidFields_RejectedWithoutReachingSink(t *testing.T) {
	validResource := &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}

	for name, baseline := range map[string]*otherlodepb.StaticBaseline{
		"empty body":                       {},
		"no resource":                      {ScannedAt: 1700000000, ChunkCount: 1},
		"no service name":                  {Resource: &otherlodepb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}, ScannedAt: 1700000000, ChunkCount: 1},
		"no service instance":              {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}, ScannedAt: 1700000000, ChunkCount: 1},
		"no run id":                        {Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}, ScannedAt: 1700000000, ChunkCount: 1},
		"zero scanned_at":                  {Resource: validResource, ScannedAt: 0, ChunkCount: 1},
		"zero chunk_count":                 {Resource: validResource, ScannedAt: 1700000000, ChunkCount: 0},
		"negative chunk_index":             {Resource: validResource, ScannedAt: 1700000000, ChunkCount: 1, ChunkIndex: -1},
		"chunk_index equal to chunk_count": {Resource: validResource, ScannedAt: 1700000000, ChunkCount: 2, ChunkIndex: 2},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+StaticBaselinePath, baseline)
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			if len(sink.baselines) != 0 {
				t.Fatalf("sink received %d baselines, want 0", len(sink.baselines))
			}
		})
	}
}

func TestHandleStaticBaseline_LastChunk_Accepted(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp := postProto(t, server.URL+StaticBaselinePath, &otherlodepb.StaticBaseline{
		Resource:   &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkIndex: 2,
		ChunkCount: 3,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (chunk_index == chunk_count-1 is the last chunk, not out of range)", resp.StatusCode, http.StatusAccepted)
	}
}

func TestHandleStaticBaseline_EmptyScan_Accepted(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp := postProto(t, server.URL+StaticBaselinePath, &otherlodepb.StaticBaseline{
		Resource:   &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkIndex: 0,
		ChunkCount: 1,
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d (a scan with no classes in any bucket is still a valid statement)", resp.StatusCode, http.StatusAccepted)
	}
	if len(sink.baselines) != 1 {
		t.Fatalf("sink received %d baselines, want 1", len(sink.baselines))
	}
}

func TestHandler_SinkError_Returns503AndIncrementsRejected(t *testing.T) {
	server := newTestServer(failingSink{})
	defer server.Close()

	cases := []struct {
		name  string
		path  string
		label string
		msg   proto.Message
	}{
		{
			name:  "delta batch",
			path:  DeltaBatchPath,
			label: "deltas",
			msg: &otherlodepb.DeltaBatch{
				Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
			},
		},
		{
			name:  "manifest",
			path:  ManifestPath,
			label: "manifest",
			msg: &otherlodepb.ProbeManifest{
				Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
			},
		},
		{
			name:  "static baseline",
			path:  StaticBaselinePath,
			label: "static_baseline",
			msg: &otherlodepb.StaticBaseline{
				Resource:   &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
				ScannedAt:  1700000000,
				ChunkCount: 1,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rejectedBefore := metrics.IngestRejected.Value(c.label, "sink")
			acceptedBefore := metrics.IngestAccepted.Value(c.label)

			resp := postProto(t, server.URL+c.path, c.msg)
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			if len(body) != 0 {
				t.Errorf("body = %q, want empty", body)
			}
			if got := metrics.IngestRejected.Value(c.label, "sink"); got != rejectedBefore+1 {
				t.Errorf("IngestRejected(%q, sink) = %d, want %d", c.label, got, rejectedBefore+1)
			}
			if got := metrics.IngestAccepted.Value(c.label); got != acceptedBefore {
				t.Errorf("IngestAccepted(%q) = %d, want unchanged at %d", c.label, got, acceptedBefore)
			}
		})
	}
}

func TestHandler_PassesRequestContextToSink(t *testing.T) {
	const key ctxKey = "test-key"
	res := &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}

	cases := []struct {
		name string
		path string
		msg  proto.Message
	}{
		{"delta batch", DeltaBatchPath, &otherlodepb.DeltaBatch{Resource: res}},
		{"manifest", ManifestPath, &otherlodepb.ProbeManifest{Resource: res}},
		{"static baseline", StaticBaselinePath, &otherlodepb.StaticBaseline{Resource: res, ScannedAt: 1700000000, ChunkCount: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sink := ctxCheckSink{t: t, key: key, calls: &atomic.Int32{}}
			mux := http.NewServeMux()
			NewHandler(sink, nil).Register(mux)

			body, err := proto.Marshal(c.msg)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(string(body)))
			req.Header.Set("Content-Type", "application/x-protobuf")
			req = req.WithContext(context.WithValue(req.Context(), key, "request-scoped"))

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusAccepted)
			}
			if got := sink.calls.Load(); got != 1 {
				t.Fatalf("sink received %d payloads, want 1", got)
			}
		})
	}
}

func TestHandler_RejectedRequests_CountedByReason(t *testing.T) {
	server := newTestServer(&fakeSink{})
	defer server.Close()

	routes := []struct{ name, path, label string }{
		{"delta batch", DeltaBatchPath, "deltas"},
		{"manifest", ManifestPath, "manifest"},
		{"static baseline", StaticBaselinePath, "static_baseline"},
	}
	// An empty message has no resource, so it fails validation on every route.
	empty, err := proto.Marshal(&otherlodepb.DeltaBatch{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	reasons := []struct {
		name        string
		reason      string
		status      int
		contentType string
		encodings   []string
		body        string
	}{
		{"wrong content type", "content_type", http.StatusUnsupportedMediaType, "application/json", nil, string(empty)},
		{"gzip content encoding", "content_type", http.StatusUnsupportedMediaType, "application/x-protobuf", []string{"gzip"}, string(empty)},
		{"identity then gzip on two lines", "content_type", http.StatusUnsupportedMediaType, "application/x-protobuf", []string{"identity", "gzip"}, string(empty)},
		{"body too large", "too_large", http.StatusRequestEntityTooLarge, "application/x-protobuf", nil, strings.Repeat("x", maxBodyBytes+1)},
		{"malformed body", "malformed", http.StatusBadRequest, "application/x-protobuf", nil, "not a protobuf message"},
		{"missing resource", "invalid", http.StatusBadRequest, "application/x-protobuf", nil, string(empty)},
	}

	for _, route := range routes {
		for _, c := range reasons {
			t.Run(route.name+"/"+c.name, func(t *testing.T) {
				before := metrics.IngestRejected.Value(route.label, c.reason)
				acceptedBefore := metrics.IngestAccepted.Value(route.label)

				req, err := http.NewRequest(http.MethodPost, server.URL+route.path, strings.NewReader(c.body))
				if err != nil {
					t.Fatalf("new request: %v", err)
				}
				req.Header.Set("Content-Type", c.contentType)
				for _, enc := range c.encodings {
					req.Header.Add("Content-Encoding", enc)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatalf("post: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != c.status {
					t.Fatalf("status = %d, want %d", resp.StatusCode, c.status)
				}
				if got := metrics.IngestRejected.Value(route.label, c.reason); got != before+1 {
					t.Errorf("IngestRejected(%q, %q) = %d, want %d", route.label, c.reason, got, before+1)
				}
				if got := metrics.IngestAccepted.Value(route.label); got != acceptedBefore {
					t.Errorf("IngestAccepted(%q) = %d, want unchanged at %d", route.label, got, acceptedBefore)
				}
			})
		}
	}
}

func TestHandler_IdentityContentEncoding_Accepted(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	body, err := proto.Marshal(&otherlodepb.DeltaBatch{Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, server.URL+DeltaBatchPath, strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "identity")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
}

func TestPayloadLabel(t *testing.T) {
	for path, want := range map[string]string{
		DeltaBatchPath:     "deltas",
		ManifestPath:       "manifest",
		StaticBaselinePath: "static_baseline",
		"/unknown":         "deltas",
	} {
		if got := PayloadLabel(path); got != want {
			t.Errorf("PayloadLabel(%q) = %q, want %q", path, got, want)
		}
	}
}

// gateSink holds every manifest in AcceptManifest until the test lets it
// go. It records how many calls started and the most that ran at once.
type gateSink struct {
	fakeSink
	entered chan struct{}
	gate    chan struct{}
	active  atomic.Int32
	maxSeen atomic.Int32
	calls   atomic.Int32
}

func newGateSink() *gateSink {
	return &gateSink{entered: make(chan struct{}, 16), gate: make(chan struct{})}
}

func (g *gateSink) AcceptManifest(context.Context, *otherlodepb.ProbeManifest) error {
	g.calls.Add(1)
	n := g.active.Add(1)
	for {
		m := g.maxSeen.Load()
		if n <= m || g.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	g.entered <- struct{}{}
	<-g.gate
	g.active.Add(-1)
	return nil
}

func manifestRequest(ctx context.Context, t *testing.T) *http.Request {
	t.Helper()
	body, err := proto.Marshal(&otherlodepb.ProbeManifest{
		Resource: &otherlodepb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, ManifestPath, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", contentType)
	return req
}

// serveWithin runs mux.ServeHTTP in a goroutine and fails the test if it
// does not return within five seconds.
func serveWithin(t *testing.T, mux *http.ServeMux, rec *httptest.ResponseRecorder, req *http.Request) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		mux.ServeHTTP(rec, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not return")
	}
}

func TestHandler_MaxConcurrentDecodes_SecondRequestWaitsForFirst(t *testing.T) {
	sink := newGateSink()
	mux := http.NewServeMux()
	NewHandler(sink, nil, WithMaxConcurrentDecodes(1)).Register(mux)

	recorders := []*httptest.ResponseRecorder{httptest.NewRecorder(), httptest.NewRecorder()}
	done := make(chan struct{}, 2)
	serve := func(rec *httptest.ResponseRecorder) {
		mux.ServeHTTP(rec, manifestRequest(context.Background(), t))
		done <- struct{}{}
	}

	go serve(recorders[0])
	<-sink.entered
	go serve(recorders[1])

	select {
	case <-sink.entered:
		t.Fatal("second request reached the sink while the first held the only slot")
	case <-time.After(100 * time.Millisecond):
	}
	if got := sink.calls.Load(); got != 1 {
		t.Fatalf("sink calls while the first request holds the slot = %d, want 1", got)
	}

	sink.gate <- struct{}{}
	<-sink.entered
	sink.gate <- struct{}{}
	<-done
	<-done

	if got := sink.maxSeen.Load(); got != 1 {
		t.Errorf("most concurrent sink calls = %d, want 1", got)
	}
	for i, rec := range recorders {
		if rec.Code != http.StatusAccepted {
			t.Errorf("request %d status = %d, want %d", i, rec.Code, http.StatusAccepted)
		}
	}
}

func TestHandler_MaxConcurrentDecodes_CanceledWaiterCountedAndSkipsSink(t *testing.T) {
	sink := newGateSink()
	mux := http.NewServeMux()
	NewHandler(sink, nil, WithMaxConcurrentDecodes(1)).Register(mux)

	first := make(chan struct{})
	go func() {
		mux.ServeHTTP(httptest.NewRecorder(), manifestRequest(context.Background(), t))
		close(first)
	}()
	<-sink.entered

	before := metrics.IngestRejected.Value("manifest", "canceled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	serveWithin(t, mux, rec, manifestRequest(ctx, t))

	if got := metrics.IngestRejected.Value("manifest", "canceled"); got != before+1 {
		t.Errorf("IngestRejected(manifest, canceled) = %d, want %d", got, before+1)
	}
	if rec.Body.Len() != 0 || rec.Flushed || len(rec.Header()) != 0 {
		t.Errorf("canceled request wrote a response: body %q, headers %v", rec.Body.String(), rec.Header())
	}
	if got := sink.calls.Load(); got != 1 {
		t.Errorf("sink calls = %d, want 1 (the canceled request must not reach it)", got)
	}

	sink.gate <- struct{}{}
	<-first
}

func TestHandler_MaxConcurrentDecodes_BusyWaiterAnswers503AfterSlotWait(t *testing.T) {
	sink := newGateSink()
	mux := http.NewServeMux()
	h := NewHandler(sink, nil, WithMaxConcurrentDecodes(1))
	h.slotWait = 50 * time.Millisecond
	h.Register(mux)

	first := make(chan struct{})
	go func() {
		mux.ServeHTTP(httptest.NewRecorder(), manifestRequest(context.Background(), t))
		close(first)
	}()
	<-sink.entered

	before := metrics.IngestRejected.Value("manifest", "busy")
	rec := httptest.NewRecorder()
	serveWithin(t, mux, rec, manifestRequest(context.Background(), t))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want %q", got, "1")
	}
	if got := metrics.IngestRejected.Value("manifest", "busy"); got != before+1 {
		t.Errorf("IngestRejected(manifest, busy) = %d, want %d", got, before+1)
	}
	if got := sink.calls.Load(); got != 1 {
		t.Errorf("sink calls = %d, want 1 (the busy request must not reach it)", got)
	}

	sink.gate <- struct{}{}
	<-first
}

func TestHandler_MaxConcurrentDecodes_SlotFreedAfterRejection(t *testing.T) {
	sink := newGateSink()
	close(sink.gate)
	mux := http.NewServeMux()
	NewHandler(sink, nil, WithMaxConcurrentDecodes(1)).Register(mux)

	bad := httptest.NewRequest(http.MethodPost, ManifestPath, strings.NewReader("\xff\xff"))
	bad.Header.Set("Content-Type", contentType)
	serveWithin(t, mux, httptest.NewRecorder(), bad)

	rec := httptest.NewRecorder()
	serveWithin(t, mux, rec, manifestRequest(context.Background(), t))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status after a malformed request = %d, want %d", rec.Code, http.StatusAccepted)
	}
}
