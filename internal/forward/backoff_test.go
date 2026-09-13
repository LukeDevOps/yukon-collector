package forward

import (
	"net/http"
	"testing"
	"time"
)

// fakeClock advances only when a test tells it to, so elapsed time is
// exactly what the test says it is.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newTestBackoff returns a backoff with no jitter (rand fixed at 0.5 maps
// to a spread of exactly 1) driven by clock.
func newTestBackoff(clock *fakeClock, initial, max, maxElapsed time.Duration) *backoff {
	b := newBackoff(initial, max, maxElapsed)
	b.now = clock.now
	b.rand = func() float64 { return 0.5 }
	b.start = clock.now()
	return b
}

func TestBackoff_GrowsByMultiplierUpToMax(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 10*time.Millisecond, 40*time.Millisecond, time.Hour)

	want := []time.Duration{10 * time.Millisecond, 15 * time.Millisecond, 22500 * time.Microsecond, 33750 * time.Microsecond, 40 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		got, ok := b.next(0)
		if !ok {
			t.Fatalf("call %d: next() = (_, false), want ok", i)
		}
		if got != w {
			t.Fatalf("call %d: next() = %v, want %v", i, got, w)
		}
		clock.advance(got)
	}
}

func TestBackoff_StopsOnceMaxElapsedReached(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 10*time.Millisecond, 10*time.Millisecond, 25*time.Millisecond)

	var total time.Duration
	calls := 0
	for {
		wait, ok := b.next(0)
		if !ok {
			break
		}
		total += wait
		clock.advance(wait)
		calls++
		if calls > 100 {
			t.Fatal("next() never reported false")
		}
	}
	if total != 25*time.Millisecond {
		t.Fatalf("total wait = %v, want exactly maxElapsed %v", total, 25*time.Millisecond)
	}

	if _, ok := b.next(0); ok {
		t.Fatal("next() after budget exhausted = ok, want false")
	}
}

func TestBackoff_TimeSpentInAttemptsCountsAgainstBudget(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 10*time.Millisecond, 10*time.Millisecond, 100*time.Millisecond)

	// A slow attempt that took most of the budget, with no waits at all.
	clock.advance(95 * time.Millisecond)
	wait, ok := b.next(0)
	if !ok {
		t.Fatal("next() = (_, false) with 5ms of budget left, want ok")
	}
	if wait != 5*time.Millisecond {
		t.Fatalf("wait = %v, want the 5ms remaining in the budget", wait)
	}
	clock.advance(wait)
	if _, ok := b.next(0); ok {
		t.Fatal("next() = ok after attempts used the whole budget, want false")
	}
}

func TestBackoff_InitialAboveMax_ClampedFromTheFirstCall(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 100*time.Millisecond, 10*time.Millisecond, time.Hour)

	wait, ok := b.next(0)
	if !ok {
		t.Fatal("next() = (_, false), want ok")
	}
	if wait != 10*time.Millisecond {
		t.Fatalf("first wait = %v, want clamped to max (10ms)", wait)
	}
}

func TestBackoff_JitterSpreadsWaitAroundInterval(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	for _, tc := range []struct {
		rand float64
		want time.Duration
	}{
		{rand: 0, want: 50 * time.Millisecond},         // interval * (1 - 0.5)
		{rand: 0.5, want: 100 * time.Millisecond},      // interval * 1
		{rand: 0.999, want: 149900 * time.Microsecond}, // just under interval * 1.5
	} {
		b := newTestBackoff(clock, 100*time.Millisecond, time.Second, time.Hour)
		b.rand = func() float64 { return tc.rand }
		got, _ := b.next(0)
		if got != tc.want {
			t.Errorf("rand=%v: wait = %v, want %v", tc.rand, got, tc.want)
		}
	}
}

func TestBackoff_HintLongerThanIntervalWins(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 10*time.Millisecond, 20*time.Millisecond, time.Hour)

	got, _ := b.next(500 * time.Millisecond)
	if got != 500*time.Millisecond {
		t.Fatalf("wait = %v, want the 500ms server hint", got)
	}
	got, _ = b.next(time.Millisecond)
	if got != 15*time.Millisecond {
		t.Fatalf("wait = %v, want the grown 15ms interval over a shorter hint", got)
	}
}

func TestBackoff_HintIsStillClippedToBudget(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	b := newTestBackoff(clock, 10*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond)

	got, ok := b.next(time.Hour)
	if !ok || got != 100*time.Millisecond {
		t.Fatalf("next(1h) = (%v, %v), want the full 100ms budget and ok", got, ok)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"", 0},
		{"3", 3 * time.Second},
		{"0", 0},
		{"-5", 0},
		{"soon", 0},
		{now.Add(90 * time.Second).UTC().Format(http.TimeFormat), 90 * time.Second},
		{now.Add(-90 * time.Second).UTC().Format(http.TimeFormat), 0},
	} {
		h := http.Header{}
		if tc.value != "" {
			h.Set("Retry-After", tc.value)
		}
		if got := parseRetryAfter(h, now); got != tc.want {
			t.Errorf("Retry-After %q: got %v, want %v", tc.value, got, tc.want)
		}
	}
}
