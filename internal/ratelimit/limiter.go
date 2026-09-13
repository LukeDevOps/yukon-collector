// Package ratelimit provides per-client-IP request throttling as HTTP
// middleware, for use in front of the collector's agent-facing endpoints.
package ratelimit

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/LukeDevOps/yukon-collector/internal/metrics"
)

// defaultStaleAfter is how long a client's bucket is kept with no
// requests before the cleanup loop evicts it. Idle clients are far more
// likely than an attacker cycling source IPs to avoid throttling, so this
// only needs to bound memory growth, not defend against eviction abuse.
const defaultStaleAfter = 10 * time.Minute

// Limiter throttles requests per client IP using a token bucket per key.
// The zero value is not usable; construct with New.
type Limiter struct {
	rate  rate.Limit
	burst int

	// clientIPHeader names a request header to read the client IP from
	// instead of RemoteAddr. Empty means RemoteAddr is used.
	clientIPHeader string

	// staleAfter is the idle time after which a client's bucket is
	// evicted, and the interval the eviction loop runs on.
	staleAfter time.Duration

	mu       sync.Mutex
	visitors map[string]*visitor

	stop chan struct{}
}

type visitor struct {
	bucket   *rate.Limiter
	lastSeen time.Time
}

// Option adjusts a Limiter at construction.
type Option func(*Limiter)

// WithClientIPHeader makes the Limiter key requests on the named header
// instead of the connection's remote address. Use it when the collector
// sits behind a reverse proxy or load balancer, where every request
// would otherwise share the proxy's IP and one bucket.
//
// The header is trusted as-is. Only set it when the collector is
// reachable solely through a proxy that overwrites or appends to that
// header; a client that can reach the collector directly could otherwise
// pick its own bucket. For a comma-separated list such as
// X-Forwarded-For, the last entry is used: that is the one written by
// the proxy directly in front of the collector.
func WithClientIPHeader(name string) Option {
	return func(l *Limiter) { l.clientIPHeader = name }
}

// withStaleAfter overrides the idle eviction time. Tests use it to make
// eviction observable without waiting ten minutes.
func withStaleAfter(d time.Duration) Option {
	return func(l *Limiter) { l.staleAfter = d }
}

// New creates a Limiter allowing r requests per second, per client IP, with
// burst as the largest instantaneous spike a single client may send. It
// starts a background goroutine to evict idle clients; call Stop when done
// with it.
func New(r rate.Limit, burst int, opts ...Option) *Limiter {
	l := &Limiter{
		rate:       r,
		burst:      burst,
		staleAfter: defaultStaleAfter,
		visitors:   make(map[string]*visitor),
		stop:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
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
// reaches next, with a Retry-After header saying how many seconds until
// the client's bucket has a token again.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ok, retryAfter := l.allow(l.clientIP(r)); !ok {
			metrics.RateLimited.Inc()
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(retryAfter)))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allow reports whether key may proceed. When it may not, retryAfter is
// how long until the bucket next has a token.
func (l *Limiter) allow(key string) (ok bool, retryAfter time.Duration) {
	now := time.Now()

	l.mu.Lock()
	v, found := l.visitors[key]
	if !found {
		v = &visitor{bucket: rate.NewLimiter(l.rate, l.burst)}
		l.visitors[key] = v
	}
	v.lastSeen = now
	bucket := v.bucket
	l.mu.Unlock()

	res := bucket.ReserveN(now, 1)
	if !res.OK() {
		return false, 0
	}
	delay := res.DelayFrom(now)
	if delay > 0 {
		res.CancelAt(now)
		return false, delay
	}
	return true, 0
}

// retryAfterSeconds rounds d up to whole seconds, with a floor of one so
// the header never tells a client to retry immediately.
func retryAfterSeconds(d time.Duration) int {
	secs := int(math.Ceil(d.Seconds()))
	if secs < 1 {
		secs = 1
	}
	return secs
}

func (l *Limiter) evictStaleLoop() {
	ticker := time.NewTicker(l.staleAfter)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case now := <-ticker.C:
			l.mu.Lock()
			for key, v := range l.visitors {
				if now.Sub(v.lastSeen) > l.staleAfter {
					delete(l.visitors, key)
				}
			}
			l.mu.Unlock()
		}
	}
}

// clientIP returns the key to throttle r on. With a client IP header
// configured and present, that wins; otherwise RemoteAddr with its port
// stripped, or the raw RemoteAddr if it isn't a host:port pair.
func (l *Limiter) clientIP(r *http.Request) string {
	if l.clientIPHeader != "" {
		if ip := lastHeaderValue(r.Header.Get(l.clientIPHeader)); ip != "" {
			return stripPort(ip)
		}
	}
	return stripPort(r.RemoteAddr)
}

// lastHeaderValue returns the last non-empty comma-separated entry of v.
func lastHeaderValue(v string) string {
	parts := strings.Split(v, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		if p := strings.TrimSpace(parts[i]); p != "" {
			return p
		}
	}
	return ""
}

func stripPort(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
