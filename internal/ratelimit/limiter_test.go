package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"
)

func newTestHandler(l *Limiter) (http.Handler, *int) {
	reached := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		w.WriteHeader(http.StatusOK)
	})
	return l.Middleware(next), &reached
}

func doRequest(handler http.Handler, remoteAddr string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = remoteAddr
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestLimiter_WithinBurst_ReachesNext(t *testing.T) {
	l := New(rate.Limit(1), 3)
	defer l.Stop()
	handler, reached := newTestHandler(l)

	for i := 0; i < 3; i++ {
		rec := doRequest(handler, "10.0.0.1:1234")
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want %d", i, rec.Code, http.StatusOK)
		}
	}
	if *reached != 3 {
		t.Fatalf("next handler reached %d times, want 3", *reached)
	}
}

func TestLimiter_OverBurst_Rejected(t *testing.T) {
	l := New(rate.Limit(1), 2)
	defer l.Stop()
	handler, reached := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1234")
	doRequest(handler, "10.0.0.1:1234")
	rec := doRequest(handler, "10.0.0.1:1234")

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if *reached != 2 {
		t.Fatalf("next handler reached %d times, want 2", *reached)
	}
}

func TestLimiter_DifferentIPs_TrackedSeparately(t *testing.T) {
	l := New(rate.Limit(1), 1)
	defer l.Stop()
	handler, reached := newTestHandler(l)

	rec1 := doRequest(handler, "10.0.0.1:1234")
	rec2 := doRequest(handler, "10.0.0.2:5678")

	if rec1.Code != http.StatusOK {
		t.Fatalf("ip1 status = %d, want %d", rec1.Code, http.StatusOK)
	}
	if rec2.Code != http.StatusOK {
		t.Fatalf("ip2 status = %d, want %d", rec2.Code, http.StatusOK)
	}
	if *reached != 2 {
		t.Fatalf("next handler reached %d times, want 2", *reached)
	}
}

func TestLimiter_MalformedRemoteAddr_FallsBackToRawValue(t *testing.T) {
	l := New(rate.Limit(1), 1)
	defer l.Stop()
	handler, _ := newTestHandler(l)

	rec := doRequest(handler, "not-a-host-port")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}
