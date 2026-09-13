package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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
	if got := rec.Header().Get("Retry-After"); got == "" {
		t.Fatal("Retry-After header missing on 429")
	} else if secs, err := strconv.Atoi(got); err != nil || secs < 1 {
		t.Fatalf("Retry-After = %q, want a whole number of seconds >= 1", got)
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

func doRequestWithHeader(handler http.Handler, remoteAddr, header, value string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = remoteAddr
	req.Header.Set(header, value)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestLimiter_ClientIPHeader_KeysOnHeaderNotRemoteAddr(t *testing.T) {
	l := New(rate.Limit(1), 1, WithClientIPHeader("X-Forwarded-For"))
	defer l.Stop()
	handler, reached := newTestHandler(l)

	// Same proxy address for both, different clients behind it.
	rec1 := doRequestWithHeader(handler, "10.0.0.1:1234", "X-Forwarded-For", "203.0.113.5")
	rec2 := doRequestWithHeader(handler, "10.0.0.1:1234", "X-Forwarded-For", "203.0.113.6")
	rec3 := doRequestWithHeader(handler, "10.0.0.1:1234", "X-Forwarded-For", "203.0.113.5")

	if rec1.Code != http.StatusOK || rec2.Code != http.StatusOK {
		t.Fatalf("distinct clients behind one proxy: statuses %d and %d, want both %d", rec1.Code, rec2.Code, http.StatusOK)
	}
	if rec3.Code != http.StatusTooManyRequests {
		t.Fatalf("repeat client status = %d, want %d", rec3.Code, http.StatusTooManyRequests)
	}
	if *reached != 2 {
		t.Fatalf("next handler reached %d times, want 2", *reached)
	}
}

func TestLimiter_ClientIPHeader_UsesLastForwardedEntry(t *testing.T) {
	l := New(rate.Limit(1), 1, WithClientIPHeader("X-Forwarded-For"))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	// A client can prepend anything it likes; only the entry added by the
	// nearest proxy (the last one) may be trusted.
	doRequestWithHeader(handler, "10.0.0.1:1234", "X-Forwarded-For", "1.1.1.1, 203.0.113.5")
	rec := doRequestWithHeader(handler, "10.0.0.1:1234", "X-Forwarded-For", "2.2.2.2, 203.0.113.5")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d (same last entry must share a bucket)", rec.Code, http.StatusTooManyRequests)
	}
}

func TestLimiter_ClientIPHeader_AbsentFallsBackToRemoteAddr(t *testing.T) {
	l := New(rate.Limit(1), 1, WithClientIPHeader("X-Forwarded-For"))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1234")
	rec := doRequest(handler, "10.0.0.1:1234")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d (missing header must key on RemoteAddr)", rec.Code, http.StatusTooManyRequests)
	}
}

func (l *Limiter) hasVisitor(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.visitors[key]
	return ok
}

func TestLimiter_IdleClientIsEvicted(t *testing.T) {
	l := New(rate.Limit(1), 1, withStaleAfter(20*time.Millisecond))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1234")
	if !l.hasVisitor("10.0.0.1") {
		t.Fatal("visitor not tracked after its first request")
	}

	// Two ticks is enough for the idle threshold to pass and be observed.
	deadline := time.Now().Add(time.Second)
	for l.hasVisitor("10.0.0.1") {
		if time.Now().After(deadline) {
			t.Fatal("idle visitor was never evicted")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLimiter_EvictedClientStartsWithAFullBucket(t *testing.T) {
	l := New(rate.Limit(0.001), 1, withStaleAfter(20*time.Millisecond)) // refill far slower than the test
	defer l.Stop()
	handler, _ := newTestHandler(l)

	if rec := doRequest(handler, "10.0.0.1:1234"); rec.Code != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec := doRequest(handler, "10.0.0.1:1234"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}

	deadline := time.Now().Add(time.Second)
	for l.hasVisitor("10.0.0.1") {
		if time.Now().After(deadline) {
			t.Fatal("idle visitor was never evicted")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if rec := doRequest(handler, "10.0.0.1:1234"); rec.Code != http.StatusOK {
		t.Fatalf("status after eviction = %d, want %d (a new bucket starts full)", rec.Code, http.StatusOK)
	}
}
