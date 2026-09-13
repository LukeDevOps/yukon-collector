package forward

import "time"

// backoffMultiplier grows the retry interval between attempts. Fixed, not
// configurable: it is part of the retry shape this package matches
// (otlphttpexporter's default retry policy), not a per-deployment knob.
const backoffMultiplier = 1.5

// backoff produces a time-bounded exponential sequence of wait durations.
// Each call to next grows the interval by backoffMultiplier, capped at max,
// and next reports false once the cumulative wait would reach maxElapsed.
// Bounding by total elapsed time rather than attempt count matches the
// OTLP exporter's default retry policy: a slow backend gets a fixed retry
// budget regardless of how many attempts fit in it.
type backoff struct {
	interval   time.Duration
	elapsed    time.Duration
	max        time.Duration
	maxElapsed time.Duration
}

func newBackoff(initial, max, maxElapsed time.Duration) *backoff {
	if initial > max {
		initial = max
	}
	return &backoff{interval: initial, max: max, maxElapsed: maxElapsed}
}

// next returns the next wait duration, or false once the retry budget is
// spent. The final wait it returns is clipped so the cumulative elapsed
// time lands exactly on maxElapsed, not past it.
func (b *backoff) next() (time.Duration, bool) {
	if b.elapsed >= b.maxElapsed {
		return 0, false
	}
	wait := b.interval
	if b.elapsed+wait > b.maxElapsed {
		wait = b.maxElapsed - b.elapsed
	}
	b.elapsed += wait

	b.interval = time.Duration(float64(b.interval) * backoffMultiplier)
	if b.interval > b.max {
		b.interval = b.max
	}
	return wait, true
}
