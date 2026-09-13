package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

// deltaRequest builds a POST to the deltas route carrying the smallest
// batch the handler accepts: a resource with a service name and instance
// ID, and no deltas.
func deltaRequest(t *testing.T, serverURL string) *http.Request {
	t.Helper()
	body, err := proto.Marshal(&yukonpb.DeltaBatch{
		Resource: &yukonpb.ResourceAttributes{ServiceName: "demo-service", ServiceInstanceId: "instance-1"},
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

// mustRegisterRoutes wires routes for a config the test expects to be valid.
func mustRegisterRoutes(t *testing.T, mux *http.ServeMux, authToken string, limiter *ratelimit.Limiter, forwardURL, forwardAuthToken string) *forward.ForwardingSink {
	t.Helper()
	fwd, err := registerRoutes(mux, nil, authToken, limiter, forwardURL, forwardAuthToken)
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

func TestRegisterRoutes_NoAuthToken_IngestUnauthenticated(t *testing.T) {
	mux := http.NewServeMux()
	mustRegisterRoutes(t, mux, "", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := server.Client().Do(deltaRequest(t, server.URL))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
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
	if _, err := registerRoutes(mux, nil, "", nil, "not a url", ""); err == nil {
		t.Fatal("expected an error for an unusable forward URL, got nil")
	}
}
