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

func TestLimiter_ClientIPHeader_SeparateHeaderLinesShareProxyEntry(t *testing.T) {
	l := New(rate.Limit(0.001), 1, WithClientIPHeader("X-Forwarded-For"))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	// The client sends the first line. The proxy adds the second line.
	codes := make([]int, 5)
	for i := range codes {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.RemoteAddr = "10.0.0.1:1234"
		req.Header.Add("X-Forwarded-For", "198.51.100."+strconv.Itoa(i+1))
		req.Header.Add("X-Forwarded-For", "203.0.113.5")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		codes[i] = rec.Code
	}
	if codes[0] != http.StatusOK {
		t.Fatalf("first request status = %d, want %d", codes[0], http.StatusOK)
	}
	for i, c := range codes[1:] {
		if c != http.StatusTooManyRequests {
			t.Fatalf("request %d status = %d, want %d (forged first line must not pick a bucket)", i+1, c, http.StatusTooManyRequests)
		}
	}
}

func TestLimiter_IPv6Default_KeysPerAddress(t *testing.T) {
	l := New(rate.Limit(0.001), 1)
	defer l.Stop()
	handler, _ := newTestHandler(l)

	if rec := doRequest(handler, "[2001:db8:1:2::1]:1234"); rec.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec := doRequest(handler, "[2001:db8:1:2::2]:1234"); rec.Code != http.StatusOK {
		t.Fatalf("other address in the same /64 status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec := doRequest(handler, "[2001:db8:1:2::1]:1234"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("repeat address status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestLimiter_IPv6Prefix64_SameSlash64ShareBucket(t *testing.T) {
	l := New(rate.Limit(0.001), 1, WithIPv6Prefix(64))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	if rec := doRequest(handler, "[2001:db8:1:2::1]:1234"); rec.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec := doRequest(handler, "[2001:db8:1:2:ffff::9]:1234"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("same /64 status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestLimiter_IPv6Prefix64_DifferentSlash64SeparateBuckets(t *testing.T) {
	l := New(rate.Limit(0.001), 1, WithIPv6Prefix(64))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "[2001:db8:1:2::1]:1234")
	if rec := doRequest(handler, "[2001:db8:1:3::1]:1234"); rec.Code != http.StatusOK {
		t.Fatalf("other /64 status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestLimiter_IPv6Prefix_OutOfRangeKeepsDefault(t *testing.T) {
	for _, bits := range []int{0, -1, 129} {
		l := New(rate.Limit(1), 1, WithIPv6Prefix(bits))
		if l.ipv6Prefix != DefaultIPv6Prefix {
			t.Errorf("bits %d: prefix = %d, want %d", bits, l.ipv6Prefix, DefaultIPv6Prefix)
		}
		l.Stop()
	}
}

func TestLimiter_IPv4MappedIPv6_KeysOnIPv4(t *testing.T) {
	l := New(rate.Limit(0.001), 1)
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1234")
	if rec := doRequest(handler, "[::ffff:10.0.0.1]:1234"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("mapped address status = %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
}

func TestLimiter_FullMap_EvictsLeastRecentlyUsedAndAllowsNewClient(t *testing.T) {
	l := New(rate.Limit(100), 100, withMaxVisitors(2))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1")
	doRequest(handler, "10.0.0.2:1")

	if rec := doRequest(handler, "10.0.0.3:1"); rec.Code != http.StatusOK {
		t.Fatalf("new client status = %d, want %d", rec.Code, http.StatusOK)
	}
	if l.hasVisitor("10.0.0.1") {
		t.Fatal("least recently used client was not evicted")
	}
	if !l.hasVisitor("10.0.0.2") || !l.hasVisitor("10.0.0.3") {
		t.Fatal("a newer client was evicted")
	}
}

func TestLimiter_FullMap_RecentlyUsedClientSurvives(t *testing.T) {
	l := New(rate.Limit(100), 100, withMaxVisitors(2))
	defer l.Stop()
	handler, _ := newTestHandler(l)

	doRequest(handler, "10.0.0.1:1")
	doRequest(handler, "10.0.0.2:1")
	doRequest(handler, "10.0.0.1:1")
	doRequest(handler, "10.0.0.3:1")

	if !l.hasVisitor("10.0.0.1") {
		t.Fatal("recently used client was evicted")
	}
	if l.hasVisitor("10.0.0.2") {
		t.Fatal("least recently used client was not evicted")
	}
}

func TestLimiter_StopTwice_DoesNotPanic(t *testing.T) {
	l := New(rate.Limit(1), 1)
	l.Stop()
	l.Stop()
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
