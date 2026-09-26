package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/processor"
	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

// deltaRequest builds a POST to the deltas route carrying the smallest
// batch the handler accepts: a resource with a service name and instance
// ID, and no deltas.
func deltaRequest(t *testing.T, serverURL string) *http.Request {
	t.Helper()
	body, err := proto.Marshal(&yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
	})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+"/v1/yukon/deltas", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	return req
}

// staticBaselineRequest builds a POST to the static-baseline route
// carrying the smallest baseline the handler accepts: identity, a
// scanned_at, and a single chunk.
func staticBaselineRequest(t *testing.T, serverURL string) *http.Request {
	t.Helper()
	body, err := proto.Marshal(&yukonpb.StaticBaseline{
		Resource:   &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		ScannedAt:  1700000000,
		ChunkCount: 1,
	})
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, serverURL+"/v1/yukon/static-baseline", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	return req
}

// mustRegisterRoutes wires routes for a config the test expects to be
// valid, with every processor left off.
func mustRegisterRoutes(t *testing.T, mux *http.ServeMux, authToken string, limiter *ratelimit.Limiter, forwardURL, forwardAuthToken string) *forward.ForwardingSink {
	t.Helper()
	return mustRegisterRoutesWithEnv(t, mux, authToken, limiter, forwardURL, forwardAuthToken, processor.EnvironmentConfig{})
}

// mustRegisterRoutesWithEnv is mustRegisterRoutes with an explicit
// envCfg, for tests that care how the environment processor behaves.
func mustRegisterRoutesWithEnv(t *testing.T, mux *http.ServeMux, authToken string, limiter *ratelimit.Limiter, forwardURL, forwardAuthToken string, envCfg processor.EnvironmentConfig) *forward.ForwardingSink {
	t.Helper()
	return mustRegisterRoutesWithProcessors(t, mux, forwardURL, envCfg, processor.NamespaceConfig{}, processor.RedactionConfig{}, authToken, limiter, forwardAuthToken)
}

// mustRegisterRoutesWithProcessors is mustRegisterRoutes with explicit
// processor configs.
func mustRegisterRoutesWithProcessors(t *testing.T, mux *http.ServeMux, forwardURL string, envCfg processor.EnvironmentConfig, nsCfg processor.NamespaceConfig, redactCfg processor.RedactionConfig, authToken string, limiter *ratelimit.Limiter, forwardAuthToken string) *forward.ForwardingSink {
	t.Helper()
	fwd, err := registerRoutes(mux, nil, authToken, limiter, forward.Config{URL: forwardURL, AuthToken: forwardAuthToken}, envCfg, nsCfg, redactCfg)
	if err != nil {
		t.Fatalf("registerRoutes: %v", err)
	}
	return fwd
}

func TestRegisterRoutes_AuthTokenSet_RequiresMatchingHeader(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "s3cret", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	req := deltaRequest(t, server.URL)

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("post without auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	req2 := deltaRequest(t, server.URL)
	req2.Header.Set("Authorization", "Bearer s3cret")

	resp2, err := server.Client().Do(req2)
	if err != nil {
		t.Fatalf("post with auth: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("status with auth = %d, want %d", resp2.StatusCode, http.StatusAccepted)
	}
}

func TestRegisterRoutes_AuthTokenSet_StaticBaselineRequiresMatchingHeader(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "s3cret", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := server.Client().Do(staticBaselineRequest(t, server.URL))
	if err != nil {
		t.Fatalf("post without auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	req2 := staticBaselineRequest(t, server.URL)
	req2.Header.Set("Authorization", "Bearer s3cret")

	resp2, err := server.Client().Do(req2)
	if err != nil {
		t.Fatalf("post with auth: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusAccepted {
		t.Fatalf("status with auth = %d, want %d", resp2.StatusCode, http.StatusAccepted)
	}
}

func TestRegisterRoutes_Healthz_NeverRequiresAuth(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "s3cret", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func postDelta(t *testing.T, serverURL string) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(deltaRequest(t, serverURL))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestRegisterRoutes_LimiterSet_ThrottlesIngestRoutesOverBurst(t *testing.T) {
	limiter := ratelimit.New(rate.Limit(1), 1)
	defer limiter.Stop()

	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "", limiter, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp1 := postDelta(t, server.URL)
	resp1.Body.Close()
	if resp1.StatusCode != http.StatusAccepted {
		t.Fatalf("first request status = %d, want %d", resp1.StatusCode, http.StatusAccepted)
	}

	resp2 := postDelta(t, server.URL)
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", resp2.StatusCode, http.StatusTooManyRequests)
	}
}

func TestRegisterRoutes_LimiterSet_HealthzNeverThrottled(t *testing.T) {
	limiter := ratelimit.New(rate.Limit(1), 1)
	defer limiter.Stop()

	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "", limiter, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	for i := 0; i < 3; i++ {
		resp, err := http.Get(server.URL + "/healthz")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i, resp.StatusCode, http.StatusOK)
		}
	}
}

func TestRegisterRoutes_NoLimiter_IngestUnthrottled(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	for i := 0; i < 5; i++ {
		resp := postDelta(t, server.URL)
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("request %d: status = %d, want %d", i, resp.StatusCode, http.StatusAccepted)
		}
	}
}

func TestRegisterRoutes_NoForwardURL_ReturnsNilForwardingSink(t *testing.T) {
	mux := http.NewServeMux()
	fwd := mustRegisterRoutes(t, mux, "", nil, "", "")
	if fwd != nil {
		t.Fatalf("forwarding sink = %v, want nil when YUKON_COLLECTOR_FORWARD_URL is unset", fwd)
	}
}

func TestRegisterRoutes_ForwardURLSet_ReturnsForwardingSinkAndReachesAcceptedStatus(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	mux := http.NewServeMux()
	fwd := mustRegisterRoutes(t, mux, "", nil, backend.URL, "backend-secret")
	if fwd == nil {
		t.Fatal("forwarding sink = nil, want non-nil when YUKON_COLLECTOR_FORWARD_URL is set")
	}
	defer fwd.Shutdown(context.Background())

	server := httptest.NewServer(mux)
	defer server.Close()

	resp := postDelta(t, server.URL)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}
}

func TestRegisterRoutes_InvalidForwardURL_ReturnsError(t *testing.T) {
	mux := http.NewServeMux()
	if _, err := registerRoutes(mux, nil, "", nil, forward.Config{URL: "not a url"}, processor.EnvironmentConfig{}, processor.NamespaceConfig{}, processor.RedactionConfig{}); err == nil {
		t.Fatal("expected an error for an unusable forward URL, got nil")
	}
}

func TestRegisterRoutes_Metrics_CountsAcceptedIngest(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp := postDelta(t, server.URL)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	metricsResp, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer metricsResp.Body.Close()
	body, _ := io.ReadAll(metricsResp.Body)
	if metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", metricsResp.StatusCode, http.StatusOK)
	}
	if !strings.Contains(string(body), `yukon_collector_ingest_accepted_total{payload="deltas"} `) {
		t.Fatalf("metrics output missing the accepted-deltas series:\n%s", body)
	}
}

// forwardDelta posts a delta batch carrying res to a collector wired
// with envCfg and nsCfg, and returns the batch its backend received.
func forwardDelta(t *testing.T, envCfg processor.EnvironmentConfig, nsCfg processor.NamespaceConfig, res *yukonpb.ResourceAttributes) *yukonpb.DeltaBatch {
	t.Helper()
	received := make(chan *yukonpb.DeltaBatch, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var batch yukonpb.DeltaBatch
		if err := proto.Unmarshal(body, &batch); err != nil {
			t.Errorf("unmarshal forwarded body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received <- &batch
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	mux := http.NewServeMux()
	fwd := mustRegisterRoutesWithProcessors(t, mux, backend.URL, envCfg, nsCfg, processor.RedactionConfig{}, "", nil, "")
	defer fwd.Shutdown(context.Background())

	server := httptest.NewServer(mux)
	defer server.Close()

	body, err := proto.Marshal(&yukonpb.DeltaBatch{Resource: res})
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	resp, err := http.Post(server.URL+"/v1/yukon/deltas", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	select {
	case batch := <-received:
		return batch
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the backend to receive the forwarded batch")
		return nil
	}
}

// demoResource is the smallest resource the handler accepts. It names a
// namespace only when ns is non-empty.
func demoResource(ns string) *yukonpb.ResourceAttributes {
	res := &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"}
	if ns != "" {
		res.SetServiceNamespace(ns)
	}
	return res
}

func TestRegisterRoutes_EnvironmentConfigSet_StampsForwardedDeltaBatch(t *testing.T) {
	batch := forwardDelta(t, processor.EnvironmentConfig{Value: "uat"}, processor.NamespaceConfig{}, demoResource(""))
	if got := batch.GetResource().GetEnvironment(); got != "uat" {
		t.Fatalf("forwarded batch environment = %q, want %q", got, "uat")
	}
}

func TestRegisterRoutes_NamespaceConfigSet_StampsForwardedDeltaBatch(t *testing.T) {
	batch := forwardDelta(t, processor.EnvironmentConfig{}, processor.NamespaceConfig{Value: "team-a"}, demoResource(""))
	if got := batch.GetResource().GetServiceNamespace(); got != "team-a" {
		t.Fatalf("forwarded batch namespace = %q, want %q", got, "team-a")
	}
}

func TestRegisterRoutes_NamespaceUpsert_ReplacesAgentNamespace(t *testing.T) {
	nsCfg := processor.NamespaceConfig{Value: "team-a", Action: processor.Upsert}
	batch := forwardDelta(t, processor.EnvironmentConfig{}, nsCfg, demoResource("team-b"))
	if got := batch.GetResource().GetServiceNamespace(); got != "team-a" {
		t.Fatalf("forwarded batch namespace = %q, want %q", got, "team-a")
	}
}

func TestRegisterRoutes_NamespaceConfigBlank_PassesAgentNamespaceThrough(t *testing.T) {
	for _, agent := range []string{"", "team-b"} {
		batch := forwardDelta(t, processor.EnvironmentConfig{}, processor.NamespaceConfig{}, demoResource(agent))
		if got := batch.GetResource().GetServiceNamespace(); got != agent {
			t.Errorf("agent namespace %q: forwarded namespace = %q, want it unchanged", agent, got)
		}
		if agent == "" && batch.GetResource().HasServiceNamespace() {
			t.Errorf("agent sent no namespace, but the forwarded batch has one set")
		}
	}
}

func TestRegisterRoutes_BothProcessorsSet_StampBothFields(t *testing.T) {
	batch := forwardDelta(t, processor.EnvironmentConfig{Value: "uat"}, processor.NamespaceConfig{Value: "team-a"}, demoResource(""))
	if got := batch.GetResource().GetEnvironment(); got != "uat" {
		t.Fatalf("forwarded batch environment = %q, want %q", got, "uat")
	}
	if got := batch.GetResource().GetServiceNamespace(); got != "team-a" {
		t.Fatalf("forwarded batch namespace = %q, want %q", got, "team-a")
	}
}

// manifestWithLiteral is the smallest manifest the handler accepts, with
// one probe whose branch site tests a string literal. Its top-level
// message and its branch site each carry a field no schema version
// declares.
func manifestWithLiteral() *yukonpb.ProbeManifest {
	unknown := protowire.AppendString(protowire.AppendTag(nil, 9999, protowire.BytesType), "a newer literal")
	site := &yukonpb.BranchSite{Condition: []*yukonpb.ConditionPart{
		{Kind: yukonpb.ConditionPartKind_CODE, Text: "System.getenv("},
		{Kind: yukonpb.ConditionPartKind_STRING_LITERAL, Text: "ENABLE_LEGACY_DISCOUNT"},
		{Kind: yukonpb.ConditionPartKind_CODE, Text: ")"},
	}}
	site.ProtoReflect().SetUnknown(unknown)
	manifest := &yukonpb.ProbeManifest{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1", RunId: "run-1"},
		Probes:   []*yukonpb.ProbeLocation{{ClassName: "com.example.Pricing", MethodName: "price", BranchSites: []*yukonpb.BranchSite{site}}},
	}
	manifest.ProtoReflect().SetUnknown(unknown)
	return manifest
}

// forwardManifest posts manifestWithLiteral to a collector wired with
// redactCfg and returns the manifest its backend received.
func forwardManifest(t *testing.T, redactCfg processor.RedactionConfig) *yukonpb.ProbeManifest {
	t.Helper()
	received := make(chan *yukonpb.ProbeManifest, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var manifest yukonpb.ProbeManifest
		if err := proto.Unmarshal(body, &manifest); err != nil {
			t.Errorf("unmarshal forwarded body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received <- &manifest
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	mux := http.NewServeMux()
	fwd := mustRegisterRoutesWithProcessors(t, mux, backend.URL, processor.EnvironmentConfig{}, processor.NamespaceConfig{}, redactCfg, "", nil, "")
	defer fwd.Shutdown(context.Background())

	server := httptest.NewServer(mux)
	defer server.Close()

	body, err := proto.Marshal(manifestWithLiteral())
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	resp, err := http.Post(server.URL+"/v1/yukon/manifest", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	select {
	case manifest := <-received:
		return manifest
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the backend to receive the forwarded manifest")
		return nil
	}
}

func TestRegisterRoutes_RedactionOn_ForwardsManifestRedactedWithoutUnknownFields(t *testing.T) {
	manifest := forwardManifest(t, processor.RedactionConfig{BlockedValues: []*regexp.Regexp{regexp.MustCompile("LEGACY")}})

	site := manifest.GetProbes()[0].GetBranchSites()[0]
	if got := site.GetCondition()[1].GetText(); got != processor.RedactedText {
		t.Fatalf("forwarded literal = %q, want %q", got, processor.RedactedText)
	}
	if got := site.GetCondition()[0].GetText(); got != "System.getenv(" {
		t.Fatalf("forwarded code part = %q, want it unchanged", got)
	}
	if n := len(manifest.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("forwarded manifest has %d unknown bytes, want 0", n)
	}
	if n := len(site.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("forwarded branch site has %d unknown bytes, want 0", n)
	}
}

func TestRegisterRoutes_RedactionOff_ForwardsManifestWithLiteralAndUnknownFields(t *testing.T) {
	manifest := forwardManifest(t, processor.RedactionConfig{})

	site := manifest.GetProbes()[0].GetBranchSites()[0]
	if got := site.GetCondition()[1].GetText(); got != "ENABLE_LEGACY_DISCOUNT" {
		t.Fatalf("forwarded literal = %q, want it unchanged", got)
	}
	if len(manifest.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("forwarded manifest lost its unknown field, want it passed through")
	}
	if len(site.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("forwarded branch site lost its unknown field, want it passed through")
	}
}
