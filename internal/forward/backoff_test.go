package forward

import (
	"testing"
	"time"
)

func TestBackoff_GrowsByMultiplierUpToMax(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 40*time.Millisecond, time.Hour)

	want := []time.Duration{10 * time.Millisecond, 15 * time.Millisecond, 22500 * time.Microsecond, 33750 * time.Microsecond, 40 * time.Millisecond, 40 * time.Millisecond}
	for i, w := range want {
		got, ok := b.next()
		if !ok {
			t.Fatalf("call %d: next() = (_, false), want ok", i)
		}
		if got != w {
			t.Fatalf("call %d: next() = %v, want %v", i, got, w)
		}
	}
}

func TestBackoff_StopsOnceMaxElapsedReached(t *testing.T) {
	b := newBackoff(10*time.Millisecond, 10*time.Millisecond, 25*time.Millisecond)

	var total time.Duration
	calls := 0
	for {
		wait, ok := b.next()
		if !ok {
			break
		}
		total += wait
		calls++
		if calls > 100 {
			t.Fatal("next() never reported false")
		}
	}
	if total != 25*time.Millisecond {
		t.Fatalf("total wait = %v, want exactly maxElapsed %v", total, 25*time.Millisecond)
	}

	if _, ok := b.next(); ok {
		t.Fatal("next() after budget exhausted = ok, want false")
	}
}

func TestBackoff_InitialAboveMax_ClampedFromTheFirstCall(t *testing.T) {
	b := newBackoff(100*time.Millisecond, 10*time.Millisecond, time.Hour)

	wait, ok := b.next()
	if !ok {
		t.Fatal("next() = (_, false), want ok")
	}
	if wait != 10*time.Millisecond {
		t.Fatalf("first wait = %v, want clamped to max (10ms)", wait)
	}
}
