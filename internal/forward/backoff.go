package forward

import (
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

// backoffMultiplier grows the retry interval between attempts and
// jitterFactor spreads each wait evenly across [interval*(1-jitterFactor),
// interval*(1+jitterFactor)). Both are fixed, not configurable: they are
// part of the retry shape this package matches (otlphttpexporter's default
// retry policy), not per-deployment knobs.
const (
	backoffMultiplier = 1.5
	jitterFactor      = 0.5
)

// backoff produces a time-bounded exponential sequence of wait durations.
// Each call to next grows the interval by backoffMultiplier, capped at max,
// and next reports false once wall-clock time since construction reaches
// maxElapsed. Bounding by total elapsed time rather than attempt count
// matches the OTLP exporter's default retry policy: a slow backend gets a
// fixed retry budget regardless of how many attempts fit in it, and time
// spent inside the attempts themselves counts against that budget.
//
// Waits are jittered so many workers retrying against the same struggling
// backend do not all come back at the same instant.
type backoff struct {
	interval   time.Duration
	max        time.Duration
	maxElapsed time.Duration
	start      time.Time

	// now and rand are injection points for tests. rand returns a value
	// in [0, 1).
	now  func() time.Time
	rand func() float64
}

func newBackoff(initial, max, maxElapsed time.Duration) *backoff {
	if initial > max {
		initial = max
	}
	b := &backoff{
		interval:   initial,
		max:        max,
		maxElapsed: maxElapsed,
		now:        time.Now,
		rand:       rand.Float64,
	}
	b.start = b.now()
	return b
}

// next returns the wait before the next attempt, or false once the retry
// budget is spent. hint is a minimum wait asked for by the server (a
// Retry-After header), or zero when there is none; the jittered interval
// is used when it is longer. The final wait is clipped so the budget is
// spent exactly at maxElapsed, not past it.
func (b *backoff) next(hint time.Duration) (time.Duration, bool) {
	elapsed := b.now().Sub(b.start)
	if elapsed >= b.maxElapsed {
		return 0, false
	}

	wait := b.jittered(b.interval)
	if hint > wait {
		wait = hint
	}
	if elapsed+wait > b.maxElapsed {
		wait = b.maxElapsed - elapsed
	}

	b.interval = time.Duration(float64(b.interval) * backoffMultiplier)
	if b.interval > b.max {
		b.interval = b.max
	}
	return wait, true
}

func (b *backoff) jittered(d time.Duration) time.Duration {
	spread := 1 + jitterFactor*(2*b.rand()-1)
	return time.Duration(float64(d) * spread)
}

// parseRetryAfter reads a Retry-After header as either delay seconds or
// an HTTP date, as RFC 9110 allows. It returns zero for a missing,
// malformed, or already-past value.
func parseRetryAfter(h http.Header, now time.Time) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if at, err := http.ParseTime(v); err == nil {
		if d := at.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
