// Package ratelimit provides per-client-IP request throttling as HTTP
// middleware, for use in front of the collector's agent-facing endpoints.
package ratelimit

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// staleAfter is how long a client's bucket is kept with no requests before
// the cleanup loop evicts it. Idle clients are far more likely than an
// attacker cycling source IPs to avoid throttling, so this only needs to
// bound memory growth, not defend against eviction abuse.
const staleAfter = 10 * time.Minute

// Limiter throttles requests per client IP using a token bucket per key.
// The zero value is not usable; construct with New.
type Limiter struct {
	rate  rate.Limit
	burst int

	mu       sync.Mutex
	visitors map[string]*visitor

	stop chan struct{}
}

type visitor struct {
	bucket   *rate.Limiter
	lastSeen time.Time
}

// New creates a Limiter allowing r requests per second, per client IP, with
// burst as the largest instantaneous spike a single client may send. It
// starts a background goroutine to evict idle clients; call Stop when done
// with it.
func New(r rate.Limit, burst int) *Limiter {
	l := &Limiter{
		rate:     r,
		burst:    burst,
		visitors: make(map[string]*visitor),
		stop:     make(chan struct{}),
	}
	go l.evictStaleLoop()
	return l
}

// Stop ends the background eviction loop.
func (l *Limiter) Stop() {
	close(l.stop)
}

// Middleware wraps next with the rate limit check, keyed on the request's
// client IP. A request over the limit is rejected with 429 before it
// reaches next.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(clientIP(r)) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *Limiter) allow(key string) bool {
	l.mu.Lock()
	v, ok := l.visitors[key]
	if !ok {
		v = &visitor{bucket: rate.NewLimiter(l.rate, l.burst)}
		l.visitors[key] = v
	}
	v.lastSeen = time.Now()
	bucket := v.bucket
	l.mu.Unlock()

	return bucket.Allow()
}

func (l *Limiter) evictStaleLoop() {
	ticker := time.NewTicker(staleAfter)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case now := <-ticker.C:
			l.mu.Lock()
			for key, v := range l.visitors {
				if now.Sub(v.lastSeen) > staleAfter {
					delete(l.visitors, key)
				}
			}
			l.mu.Unlock()
		}
	}
}

// clientIP extracts the request's client IP from RemoteAddr, stripping the
// port. Falls back to the raw RemoteAddr if it isn't a host:port pair.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
