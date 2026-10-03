// Package ratelimit provides per-client-IP request throttling as HTTP
// middleware for the collector's agent-facing endpoints.
package ratelimit

import (
	"container/list"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

// defaultStaleAfter is how long a client's bucket is kept with no
// requests before the cleanup loop evicts it. Eviction frees memory for
// clients that left. defaultMaxVisitors bounds the map against a flood of
// new keys.
const defaultStaleAfter = 10 * time.Minute

// defaultMaxVisitors caps the number of tracked clients. When the map is
// full, a request from a new client evicts the least recently used client.
// An evicted client loses only its bucket and gets a fresh full one on its
// next request. Agents flush every 30 to 60 seconds, so legitimate clients
// stay near the front of the list. A flood of new keys pushes out idle
// clients first, and never locks out a new one.
const defaultMaxVisitors = 100_000

// DefaultIPv6Prefix keys each IPv6 address on its own.
const DefaultIPv6Prefix = 128

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

	maxVisitors int

	// ipv6Prefix is the prefix length, in bits, that an IPv6 address is
	// reduced to before it is used as a key.
	ipv6Prefix int

	mu sync.Mutex
	// visitors maps a key to its element in order. The front of order is
	// the most recently used client.
	visitors map[string]*list.Element
	order    *list.List

	stop     chan struct{}
	stopOnce sync.Once
}

type visitor struct {
	key      string
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

// WithIPv6Prefix sets how many leading bits of an IPv6 address form its
// key. The default is 128, so each address has its own bucket. A smaller
// value makes every address in the same prefix share one bucket. Use 64
// when the collector faces the internet directly, since one sender can
// cycle through the 2^64 addresses of a /64. Keep 128 when many agents
// share a /64, as VPC subnets and per-node pod ranges often do. Values
// outside 1 to 128 are ignored. IPv4 and IPv4-mapped addresses always key
// on the IPv4 address.
func WithIPv6Prefix(bits int) Option {
	return func(l *Limiter) {
		if bits >= 1 && bits <= 128 {
			l.ipv6Prefix = bits
		}
	}
}

// withStaleAfter overrides the idle eviction time. Tests use it to make
// eviction observable without waiting ten minutes.
func withStaleAfter(d time.Duration) Option {
	return func(l *Limiter) { l.staleAfter = d }
}

// withMaxVisitors overrides the client cap. Tests use it to reach the cap
// without creating a hundred thousand clients.
func withMaxVisitors(n int) Option {
	return func(l *Limiter) { l.maxVisitors = n }
}

// New creates a Limiter allowing r requests per second, per client IP, with
// burst as the largest instantaneous spike a single client may send. It
// starts a background goroutine to evict idle clients; call Stop when done
// with it.
func New(r rate.Limit, burst int, opts ...Option) *Limiter {
	l := &Limiter{
		rate:        r,
		burst:       burst,
		staleAfter:  defaultStaleAfter,
		maxVisitors: defaultMaxVisitors,
		ipv6Prefix:  DefaultIPv6Prefix,
		visitors:    make(map[string]*list.Element),
		order:       list.New(),
		stop:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(l)
	}
	go l.evictStaleLoop()
	return l
}

// Stop ends the background eviction loop. It is safe to call more than
// once.
func (l *Limiter) Stop() {
	l.stopOnce.Do(func() { close(l.stop) })
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
// how long until the bucket next has a token. A new key evicts the least
// recently used client when the map is full.
func (l *Limiter) allow(key string) (ok bool, retryAfter time.Duration) {
	l.mu.Lock()
	now := time.Now()
	var v *visitor
	if el, found := l.visitors[key]; found {
		v = el.Value.(*visitor)
		l.order.MoveToFront(el)
	} else {
		for len(l.visitors) >= l.maxVisitors && l.order.Len() > 0 {
			oldest := l.order.Back()
			delete(l.visitors, oldest.Value.(*visitor).key)
			l.order.Remove(oldest)
		}
		v = &visitor{key: key, bucket: rate.NewLimiter(l.rate, l.burst)}
		l.visitors[key] = l.order.PushFront(v)
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
			for el := l.order.Back(); el != nil; el = l.order.Back() {
				v := el.Value.(*visitor)
				if now.Sub(v.lastSeen) <= l.staleAfter {
					break
				}
				delete(l.visitors, v.key)
				l.order.Remove(el)
			}
			l.mu.Unlock()
		}
	}
}

// clientIP returns the key to throttle r on. With a client IP header
// configured and present, that wins. Otherwise it is RemoteAddr with its
// port stripped, or the raw RemoteAddr if it is not a host:port pair. The
// result goes through normalizeIP.
func (l *Limiter) clientIP(r *http.Request) string {
	if l.clientIPHeader != "" {
		// A proxy may add its own header line instead of appending to the
		// client's, so all lines are joined before taking the last entry.
		joined := strings.Join(r.Header.Values(l.clientIPHeader), ",")
		if ip := lastHeaderValue(joined); ip != "" {
			return l.normalizeIP(stripPort(ip))
		}
	}
	return l.normalizeIP(stripPort(r.RemoteAddr))
}

// normalizeIP returns the bucket key for s. IPv4 and IPv4-mapped IPv6
// addresses key on the IPv4 address. IPv6 addresses key on their first
// ipv6Prefix bits. A value that is not an IP keys on itself.
func (l *Limiter) normalizeIP(s string) string {
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return s
	}
	addr = addr.WithZone("").Unmap()
	if addr.Is4() {
		return addr.String()
	}
	prefix, err := addr.Prefix(l.ipv6Prefix)
	if err != nil {
		return s
	}
	return prefix.String()
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
