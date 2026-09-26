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

	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/metrics"
)

type fakeSink struct {
	deltaBatches []*yukonpb.DeltaBatch
	manifests    []*yukonpb.ProbeManifest
	baselines    []*yukonpb.StaticBaseline
}

func (f *fakeSink) AcceptDeltaBatch(_ context.Context, batch *yukonpb.DeltaBatch) error {
	f.deltaBatches = append(f.deltaBatches, batch)
	return nil
}

func (f *fakeSink) AcceptManifest(_ context.Context, manifest *yukonpb.ProbeManifest) error {
	f.manifests = append(f.manifests, manifest)
	return nil
}

func (f *fakeSink) AcceptStaticBaseline(_ context.Context, baseline *yukonpb.StaticBaseline) error {
	f.baselines = append(f.baselines, baseline)
	return nil
}

// failingSink refuses every payload, standing in for a backend that could
// not take the write (a database outage, for example).
type failingSink struct{}

func (failingSink) AcceptDeltaBatch(context.Context, *yukonpb.DeltaBatch) error {
	return errors.New("sink unavailable")
}

func (failingSink) AcceptManifest(context.Context, *yukonpb.ProbeManifest) error {
	return errors.New("sink unavailable")
}

func (failingSink) AcceptStaticBaseline(context.Context, *yukonpb.StaticBaseline) error {
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

func (s ctxCheckSink) AcceptDeltaBatch(ctx context.Context, _ *yukonpb.DeltaBatch) error {
	s.checkContext(ctx)
	return nil
}

func (s ctxCheckSink) AcceptManifest(ctx context.Context, _ *yukonpb.ProbeManifest) error {
	s.checkContext(ctx)
	return nil
}

func (s ctxCheckSink) AcceptStaticBaseline(ctx context.Context, _ *yukonpb.StaticBaseline) error {
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

	batch := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		Deltas: []*yukonpb.ProbeDelta{
			{ClassId: 1, ProbeIndex: 0, Kind: yukonpb.ProbeKind_METHOD, HitsTotal: 5},
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

	batch := &yukonpb.DeltaBatch{Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}}
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

func TestHandleDeltaBatch_EndpointDeltas_ReachSinkIntact(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	batch := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		EndpointDeltas: []*yukonpb.EndpointDelta{
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

	manifest := &yukonpb.ProbeManifest{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
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
	res := sink.manifests[0].GetResource()
	if res.GetServiceName() != "demo-service" || res.GetServiceInstanceId() != "instance-1" || res.GetRunId() != "run-1" {
		t.Errorf("manifest resource = %v, want demo-service/instance-1/run-1", res)
	}
}

func TestHandleManifest_Endpoints_ReachSinkIntact(t *testing.T) {
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	manifest := &yukonpb.ProbeManifest{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		Endpoints: []*yukonpb.EndpointLocation{
			{
				EndpointId:        1,
				Verb:              "GET",
				RouteTemplate:     "/checkout/{id}",
				VerbatimTemplate:  "/checkout/{id}",
				Framework:         "spring-mvc",
				DiscoverySource:   yukonpb.EndpointDiscoverySource_REGISTRATION,
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
				DiscoverySource:  yukonpb.EndpointDiscoverySource_DISPATCH,
			},
		},
		DisabledEndpointModules: []*yukonpb.DisabledEndpointModule{
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

	manifest := &yukonpb.ProbeManifest{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"},
		Endpoints: []*yukonpb.EndpointLocation{
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

	resp, err := http.Post(server.URL+"/v1/yukon/manifest", "application/x-protobuf", strings.NewReader("not a protobuf message"))
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

	manifest := &yukonpb.ProbeManifest{Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}}
	body, err := proto.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/yukon/manifest", "application/json", strings.NewReader(string(body)))
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

	resp, err := http.Post(server.URL+"/v1/yukon/manifest", "application/x-protobuf", strings.NewReader(oversized))
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

	batch := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/x-protobuf; charset=utf-8", strings.NewReader(string(body)))
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
	for name, batch := range map[string]*yukonpb.DeltaBatch{
		"empty body":          {},
		"no resource":         {Deltas: []*yukonpb.ProbeDelta{{ClassId: 1}}},
		"no service name":     {Resource: &yukonpb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}},
		"no service instance": {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}},
		"no run id":           {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+"/v1/yukon/deltas", batch)
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
	for name, manifest := range map[string]*yukonpb.ProbeManifest{
		"empty body":          {},
		"no resource":         {Probes: []*yukonpb.ProbeLocation{{ClassId: 1}}},
		"no service name":     {Resource: &yukonpb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}},
		"no service instance": {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}},
		"no run id":           {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}},
	} {
		t.Run(name, func(t *testing.T) {
			sink := &fakeSink{}
			server := newTestServer(sink)
			defer server.Close()

			resp := postProto(t, server.URL+"/v1/yukon/manifest", manifest)
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
	res := &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}
	for path, msg := range map[string]proto.Message{
		DeltaBatchPath:     &yukonpb.DeltaBatch{Resource: res},
		ManifestPath:       &yukonpb.ProbeManifest{Resource: res},
		StaticBaselinePath: &yukonpb.StaticBaseline{Resource: res, ScannedAt: 1700000000, ChunkCount: 1},
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
	withNamespace := func(namespace string) *yukonpb.ResourceAttributes {
		res := &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}
		res.SetServiceNamespace(namespace)
		return res
	}
	for name, tc := range map[string]struct {
		res  *yukonpb.ResourceAttributes
		want string
	}{
		"blank service name": {&yukonpb.ResourceAttributes{ServiceName: "   ", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name is empty"},
		"dot service name":   {&yukonpb.ResourceAttributes{ServiceName: ".", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name"},
		"dots service name":  {&yukonpb.ResourceAttributes{ServiceName: " .. ", ServiceInstanceId: "instance-1", RunId: "run-1"}, "resource.service_name"},
		"dot namespace":      {withNamespace("."), "resource.service_namespace"},
		"dots namespace":     {withNamespace(" .. "), "resource.service_namespace"},
	} {
		for path, msg := range map[string]proto.Message{
			DeltaBatchPath:     &yukonpb.DeltaBatch{Resource: tc.res},
			ManifestPath:       &yukonpb.ProbeManifest{Resource: tc.res},
			StaticBaselinePath: &yukonpb.StaticBaseline{Resource: tc.res, ScannedAt: 1700000000, ChunkCount: 1},
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
	res := &yukonpb.ResourceAttributes{ServiceName: "checkout.v2", ServiceInstanceId: "instance-1", RunId: "run-1"}
	res.SetServiceNamespace("...")
	sink := &fakeSink{}
	server := newTestServer(sink)
	defer server.Close()

	resp := postProto(t, server.URL+DeltaBatchPath, &yukonpb.DeltaBatch{Resource: res})
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

	baseline := &yukonpb.StaticBaseline{
		Resource: &yukonpb.ResourceAttributes{
			ServiceName:       "demo-service",
			ServiceInstanceId: "instance-1",
			RunId:             "run-1",
		},
		ScannedAt:  1700000000,
		ChunkIndex: 0,
		ChunkCount: 2,
		DeclaredClasses: []*yukonpb.DeclaredClass{
			{
				ClassName: "com.example.Foo",
				Methods:   []*yukonpb.DeclaredMethod{{MethodName: "bar", MethodDescriptor: "()V"}},
			},
		},
		StaticallyUnsafeClasses: []*yukonpb.StaticallyUnsafeClass{
			{ClassName: "com.example.Unsafe", Reason: "annotation not legal on a type"},
		},
		UnreadableClasses: []*yukonpb.UnreadableClass{
			{ClassName: "com.example.Unreadable", Reason: "corrupt class file"},
		},
		UnprobedClasses: []*yukonpb.UnprobedClass{
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

	baseline := &yukonpb.StaticBaseline{
		Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
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
	validResource := &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}

	for name, baseline := range map[string]*yukonpb.StaticBaseline{
		"empty body":                       {},
		"no resource":                      {ScannedAt: 1700000000, ChunkCount: 1},
		"no service name":                  {Resource: &yukonpb.ResourceAttributes{ServiceInstanceId: "instance-1", RunId: "run-1"}, ScannedAt: 1700000000, ChunkCount: 1},
		"no service instance":              {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", RunId: "run-1"}, ScannedAt: 1700000000, ChunkCount: 1},
		"no run id":                        {Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"}, ScannedAt: 1700000000, ChunkCount: 1},
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

	resp := postProto(t, server.URL+StaticBaselinePath, &yukonpb.StaticBaseline{
		Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
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

	resp := postProto(t, server.URL+StaticBaselinePath, &yukonpb.StaticBaseline{
		Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
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
			msg: &yukonpb.DeltaBatch{
				Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
			},
		},
		{
			name:  "manifest",
			path:  ManifestPath,
			label: "manifest",
			msg: &yukonpb.ProbeManifest{
				Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
			},
		},
		{
			name:  "static baseline",
			path:  StaticBaselinePath,
			label: "static_baseline",
			msg: &yukonpb.StaticBaseline{
				Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
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
	sink := ctxCheckSink{t: t, key: key, calls: &atomic.Int32{}}
	mux := http.NewServeMux()
	NewHandler(sink, nil).Register(mux)

	batch := &yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, DeltaBatchPath, strings.NewReader(string(body)))
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
}

func TestPayloadLabel(t *testing.T) {
	for path, want := range map[string]string{
		DeltaBatchPath:     "deltas",
		ManifestPath:       "manifest",
		StaticBaselinePath: "static_baseline",
	} {
		if got := PayloadLabel(path); got != want {
			t.Errorf("PayloadLabel(%q) = %q, want %q", path, got, want)
		}
	}
}
