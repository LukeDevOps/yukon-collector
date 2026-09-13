package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"

	"github.com/LukeDevOps/yukon-collector/internal/ratelimit"
)

func TestRegisterRoutes_AuthTokenSet_RequiresMatchingHeader(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux, nil, "s3cret", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/yukon/deltas", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Body = http.NoBody
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("post without auth: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}

	req2, err := http.NewRequest(http.MethodPost, server.URL+"/v1/yukon/deltas", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req2.Body = http.NoBody
	req2.Header.Set("Content-Type", "application/x-protobuf")
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
	registerRoutes(mux, nil, "", nil, "", "")
	server := httptest.NewServer(mux)
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/yukon/deltas", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Body = http.NoBody
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := server.Client().Do(req)
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
	registerRoutes(mux, nil, "s3cret", nil, "", "")
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
	req, err := http.NewRequest(http.MethodPost, serverURL+"/v1/yukon/deltas", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Body = http.NoBody
	req.Header.Set("Content-Type", "application/x-protobuf")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

func TestRegisterRoutes_LimiterSet_ThrottlesIngestRoutesOverBurst(t *testing.T) {
	limiter := ratelimit.New(rate.Limit(1), 1)
	defer limiter.Stop()

	mux := http.NewServeMux()
	registerRoutes(mux, nil, "", limiter, "", "")
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
	registerRoutes(mux, nil, "", limiter, "", "")
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
	registerRoutes(mux, nil, "", nil, "", "")
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
	fwd := registerRoutes(mux, nil, "", nil, "", "")
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
	fwd := registerRoutes(mux, nil, "", nil, backend.URL, "backend-secret")
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
