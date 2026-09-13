package main

import (
	"log/slog"
	"testing"

	"golang.org/x/time/rate"
)

func TestResolveAuthToken_TokenSet_ReturnsToken(t *testing.T) {
	token, err := resolveAuthToken("s3cret", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "s3cret" {
		t.Fatalf("token = %q, want %q", token, "s3cret")
	}
}

func TestResolveAuthToken_NoTokenNoOptOut_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveAuthToken_NoTokenExplicitOptOut_ReturnsEmpty(t *testing.T) {
	token, err := resolveAuthToken("", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "" {
		t.Fatalf("token = %q, want empty", token)
	}
}

func TestResolveAuthToken_NoTokenOptOutFalse_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "false")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveAuthToken_NoTokenGarbageOptOut_Errors(t *testing.T) {
	_, err := resolveAuthToken("", "not-a-bool")
	if err == nil {
		t.Fatal("expected error for unparseable opt-out value, got nil")
	}
}

func TestResolveAuthToken_TokenSetAndOptOutSet_TokenWins(t *testing.T) {
	token, err := resolveAuthToken("s3cret", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "s3cret" {
		t.Fatalf("token = %q, want %q", token, "s3cret")
	}
}

func TestResolveRateLimit_BothUnset_UsesDefaults(t *testing.T) {
	rps, burst, err := resolveRateLimit("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rps != rate.Limit(defaultRateLimitRPS) {
		t.Fatalf("rps = %v, want %v", rps, defaultRateLimitRPS)
	}
	if burst != defaultRateLimitBurst {
		t.Fatalf("burst = %d, want %d", burst, defaultRateLimitBurst)
	}
}

func TestResolveRateLimit_BothSet_UsesProvidedValues(t *testing.T) {
	rps, burst, err := resolveRateLimit("10", "50")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rps != rate.Limit(10) {
		t.Fatalf("rps = %v, want 10", rps)
	}
	if burst != 50 {
		t.Fatalf("burst = %d, want 50", burst)
	}
}

func TestResolveRateLimit_RPSZero_DisablesLimiting(t *testing.T) {
	rps, _, err := resolveRateLimit("0", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rps != 0 {
		t.Fatalf("rps = %v, want 0", rps)
	}
}

func TestResolveRateLimit_NegativeRPS_Errors(t *testing.T) {
	_, _, err := resolveRateLimit("-1", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveRateLimit_GarbageRPS_Errors(t *testing.T) {
	_, _, err := resolveRateLimit("not-a-number", "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveRateLimit_GarbageBurst_Errors(t *testing.T) {
	_, _, err := resolveRateLimit("", "not-a-number")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveRateLimit_EnabledWithZeroBurst_Errors(t *testing.T) {
	_, _, err := resolveRateLimit("5", "0")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveRateLimit_NonFiniteRPS_Errors(t *testing.T) {
	for _, raw := range []string{"NaN", "Inf", "+Inf", "-Inf"} {
		if _, _, err := resolveRateLimit(raw, ""); err == nil {
			t.Errorf("rps %q: expected an error, got nil", raw)
		}
	}
}

func TestResolveLogLevel(t *testing.T) {
	for raw, want := range map[string]slog.Level{
		"":      slog.LevelInfo,
		"debug": slog.LevelDebug,
		"INFO":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"Error": slog.LevelError,
	} {
		got, err := resolveLogLevel(raw)
		if err != nil {
			t.Errorf("level %q: unexpected error: %v", raw, err)
			continue
		}
		if got != want {
			t.Errorf("level %q = %v, want %v", raw, got, want)
		}
	}
}

func TestResolveLogLevel_Garbage_Errors(t *testing.T) {
	if _, err := resolveLogLevel("loud"); err == nil {
		t.Fatal("expected an error for an unknown level, got nil")
	}
}
