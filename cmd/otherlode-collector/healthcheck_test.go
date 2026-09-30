package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/otherlodehq/otherlode-collector/internal/auth"
	"github.com/otherlodehq/otherlode-collector/internal/forward"
	"github.com/otherlodehq/otherlode-collector/internal/processor"
)

func TestResolveAddr(t *testing.T) {
	if got := resolveAddr(""); got != defaultAddr {
		t.Errorf("resolveAddr(\"\") = %q, want %q", got, defaultAddr)
	}
	if got := resolveAddr("127.0.0.1:9000"); got != "127.0.0.1:9000" {
		t.Errorf("resolveAddr(\"127.0.0.1:9000\") = %q, want it unchanged", got)
	}
}

func TestHealthzURL(t *testing.T) {
	cases := []struct {
		addr    string
		want    string
		wantErr bool
	}{
		{addr: ":4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "0.0.0.0:4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "[::]:4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "10.0.0.5:4319", want: "http://10.0.0.5:4319/healthz"},
		{addr: "[::1]:4319", want: "http://[::1]:4319/healthz"},
		{addr: "localhost:4319", want: "http://localhost:4319/healthz"},
		{addr: "[0:0:0:0:0:0:0:0]:4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "[::ffff:0.0.0.0]:4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "[::%eth0]:4319", want: "http://127.0.0.1:4319/healthz"},
		{addr: "[fe80::1%eth0]:4319", want: "http://[fe80::1%25eth0]:4319/healthz"},
		{addr: ":http", want: "http://127.0.0.1:80/healthz"},
		{addr: "nonsense", wantErr: true},
		{addr: ":no-such-service", wantErr: true},
	}
	for _, c := range cases {
		got, err := healthzURL(context.Background(), c.addr)
		if c.wantErr {
			if err == nil {
				t.Errorf("addr %q: expected an error", c.addr)
			}
			continue
		}
		if err != nil {
			t.Errorf("addr %q: unexpected error: %v", c.addr, err)
			continue
		}
		if got != c.want {
			t.Errorf("addr %q: url = %q, want %q", c.addr, got, c.want)
		}
		if _, err := http.NewRequest(http.MethodGet, got, nil); err != nil {
			t.Errorf("addr %q: url %q does not parse: %v", c.addr, got, err)
		}
	}
}

func TestHealthcheckClient_NeverUsesAProxy(t *testing.T) {
	transport, ok := healthcheckClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T, want *http.Transport", healthcheckClient.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("healthcheck transport has a proxy function; the probe must never go through HTTP_PROXY")
	}
}

func TestRunSubcommand(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != healthzPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	downAddr := down.Listener.Addr().String()
	down.Close()
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer hung.Close()
	defer close(release)

	cases := []struct {
		name       string
		args       []string
		addr       string
		wantCode   int
		wantStderr string
	}{
		{name: "healthy", args: []string{"healthcheck"}, addr: up.Listener.Addr().String(), wantCode: 0},
		{name: "unreachable", args: []string{"healthcheck"}, addr: downAddr, wantCode: 1, wantStderr: "healthz:"},
		{name: "bad address", args: []string{"healthcheck"}, addr: "nonsense", wantCode: 1, wantStderr: "healthz url:"},
		{name: "hung", args: []string{"healthcheck"}, addr: hung.Listener.Addr().String(), wantCode: 1, wantStderr: "deadline exceeded"},
		{name: "extra argument", args: []string{"healthcheck", "-v"}, wantCode: 2, wantStderr: "usage:"},
		{name: "unknown subcommand", args: []string{"healthchek"}, wantCode: 2, wantStderr: "usage:"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			getenv := func(key string) string {
				if key == "OTHERLODE_COLLECTOR_ADDR" {
					return c.addr
				}
				return ""
			}
			var stderr strings.Builder
			done := make(chan int, 1)
			go func() { done <- runSubcommand(context.Background(), c.args, getenv, &stderr) }()
			var code int
			select {
			case code = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("probe still running after 3s, want it to give up under the Dockerfile's 3s timeout")
			}
			if code != c.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr %q)", code, c.wantCode, stderr.String())
			}
			if !strings.Contains(stderr.String(), c.wantStderr) {
				t.Fatalf("stderr %q does not contain %q", stderr.String(), c.wantStderr)
			}
		})
	}
}

func TestRunHealthcheck_OK_ReturnsNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if err := runHealthcheck(context.Background(), srv.URL); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunHealthcheck_NonOK_ErrorsNamingStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	err := runHealthcheck(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("expected an error for a 503 response")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("error %q does not name the status", err.Error())
	}
}

// TestRunHealthcheck_Hung_FailsAtDeadline covers a collector that accepts
// the connection and never answers: the probe must fail on its own
// deadline, which healthcheckTimeout keeps under the Dockerfile's 3s.
func TestRunHealthcheck_Hung_FailsAtDeadline(t *testing.T) {
	if healthcheckTimeout >= 3*time.Second {
		t.Fatalf("healthcheckTimeout = %v, must stay under the Dockerfile's HEALTHCHECK --timeout=3s", healthcheckTimeout)
	}
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := runHealthcheck(ctx, srv.URL)
	if err == nil {
		t.Fatal("expected an error for a server that never answers")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error %q is not a deadline error", err.Error())
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("probe took %v, want it to stop at its 100ms deadline", elapsed)
	}
}

func TestRunHealthcheck_Unreachable_Errors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()

	if err := runHealthcheck(context.Background(), srv.URL); err == nil {
		t.Fatal("expected an error for a closed server")
	}
}

// TestRunHealthcheck_AgainstRegisteredRoutes probes the real mux, so the
// subcommand and the /healthz route cannot drift apart.
func TestRunHealthcheck_AgainstRegisteredRoutes(t *testing.T) {
	mux := http.NewServeMux()
	if _, err := registerRoutes(mux, nil, auth.NewTokenSet([]string{"token"}), nil, forward.Config{}, processor.EnvironmentConfig{}, processor.NamespaceConfig{}, processor.RedactionConfig{}); err != nil {
		t.Fatalf("registerRoutes: %v", err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	url, err := healthzURL(context.Background(), srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("healthzURL: %v", err)
	}
	if err := runHealthcheck(context.Background(), url); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
