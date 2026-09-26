package main

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/LukeDevOps/yukon-collector/internal/forward"
	"github.com/LukeDevOps/yukon-collector/internal/processor"
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

func envFrom(vars map[string]string) func(string) string {
	return func(name string) string { return vars[name] }
}

func TestResolveForwardConfig_Unset_LeavesZeroValues(t *testing.T) {
	cfg, err := resolveForwardConfig(envFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != (forward.Config{}) {
		t.Fatalf("config = %+v, want all zero so the forward package applies its defaults", cfg)
	}
}

func TestResolveForwardConfig_AllSet_ParsesEveryField(t *testing.T) {
	cfg, err := resolveForwardConfig(envFrom(map[string]string{
		"YUKON_COLLECTOR_FORWARD_URL":                    "https://backend.example.com",
		"YUKON_COLLECTOR_FORWARD_AUTH_TOKEN":             "backend-secret",
		"YUKON_COLLECTOR_FORWARD_SHARDS":                 "4",
		"YUKON_COLLECTOR_FORWARD_QUEUE_SIZE":             "128",
		"YUKON_COLLECTOR_FORWARD_REQUEST_TIMEOUT":        "15s",
		"YUKON_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL": "2s",
		"YUKON_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL":     "1m",
		"YUKON_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME": "10m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := forward.Config{
		URL:                  "https://backend.example.com",
		AuthToken:            "backend-secret",
		Shards:               4,
		QueueSize:            128,
		RequestTimeout:       15 * time.Second,
		RetryInitialInterval: 2 * time.Second,
		RetryMaxInterval:     time.Minute,
		RetryMaxElapsedTime:  10 * time.Minute,
	}
	if cfg != want {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestResolveEnvironment(t *testing.T) {
	tests := map[string]struct {
		value   string
		action  string
		want    processor.EnvironmentConfig
		wantErr bool
	}{
		"unset": {
			value:  "",
			action: "",
			want:   processor.EnvironmentConfig{},
		},
		"value only": {
			value:  "prod",
			action: "",
			want:   processor.EnvironmentConfig{Value: "prod", Action: processor.Insert},
		},
		"value with surrounding spaces": {
			value:  "  prod  ",
			action: "",
			want:   processor.EnvironmentConfig{Value: "prod", Action: processor.Insert},
		},
		"value and upsert": {
			value:  "prod",
			action: "upsert",
			want:   processor.EnvironmentConfig{Value: "prod", Action: processor.Upsert},
		},
		"bad action": {
			value:   "prod",
			action:  "overwrite",
			wantErr: true,
		},
		"action without value": {
			value:   "",
			action:  "upsert",
			wantErr: true,
		},
		"whitespace-only value with action": {
			value:   "   ",
			action:  "upsert",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := resolveEnvironment(tt.value, tt.action)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("config = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestResolveNamespace(t *testing.T) {
	tests := map[string]struct {
		value   string
		action  string
		want    processor.NamespaceConfig
		wantErr bool
	}{
		"unset": {
			value:  "",
			action: "",
			want:   processor.NamespaceConfig{},
		},
		"value only": {
			value:  "team-a",
			action: "",
			want:   processor.NamespaceConfig{Value: "team-a", Action: processor.Insert},
		},
		"value with surrounding spaces": {
			value:  "  team-a  ",
			action: "",
			want:   processor.NamespaceConfig{Value: "team-a", Action: processor.Insert},
		},
		"value keeps its case": {
			value:  "Team-A",
			action: "",
			want:   processor.NamespaceConfig{Value: "Team-A", Action: processor.Insert},
		},
		"value and insert": {
			value:  "team-a",
			action: "insert",
			want:   processor.NamespaceConfig{Value: "team-a", Action: processor.Insert},
		},
		"value and upsert": {
			value:  "team-a",
			action: "UPSERT",
			want:   processor.NamespaceConfig{Value: "team-a", Action: processor.Upsert},
		},
		"bad action": {
			value:   "team-a",
			action:  "overwrite",
			wantErr: true,
		},
		"dot value": {
			value:   ".",
			action:  "",
			wantErr: true,
		},
		"dots value with spaces": {
			value:   " .. ",
			action:  "upsert",
			wantErr: true,
		},
		"action without value": {
			value:   "",
			action:  "insert",
			wantErr: true,
		},
		"whitespace-only value with action": {
			value:   "   ",
			action:  "upsert",
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := resolveNamespace(tt.value, tt.action)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if !strings.Contains(err.Error(), "YUKON_COLLECTOR_SERVICE_NAMESPACE") {
					t.Fatalf("error %q does not name the variable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("config = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestResolveForwardConfig_BadValues_Error(t *testing.T) {
	for name, value := range map[string]string{
		"YUKON_COLLECTOR_FORWARD_SHARDS":                 "0",
		"YUKON_COLLECTOR_FORWARD_QUEUE_SIZE":             "-1",
		"YUKON_COLLECTOR_FORWARD_REQUEST_TIMEOUT":        "10",
		"YUKON_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL": "soon",
		"YUKON_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL":     "0s",
		"YUKON_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME": "-5m",
	} {
		if _, err := resolveForwardConfig(envFrom(map[string]string{name: value})); err == nil {
			t.Errorf("%s=%q: expected an error, got nil", name, value)
		}
	}
}

func TestResolveRedaction_Unset_Off(t *testing.T) {
	cfg, err := resolveRedaction("", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Enabled() {
		t.Fatalf("config = %+v, want redaction off", cfg)
	}
}

func TestResolveRedaction_PatternsOnePerLine_BlankLinesIgnored(t *testing.T) {
	cfg, err := resolveRedaction("\nLEGACY_[A-Z]+\n   \r\n(?i)secret,token\r\n\n", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got []string
	for _, re := range cfg.BlockedValues {
		got = append(got, re.String())
	}
	want := []string{"LEGACY_[A-Z]+", "(?i)secret,token"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("patterns = %q, want %q", got, want)
	}
	if cfg.AllLiterals {
		t.Fatal("AllLiterals = true, want false")
	}
}

func TestResolveRedaction_InvalidPattern_ErrorNamesVariableAndLine(t *testing.T) {
	_, err := resolveRedaction("ok\n\nbad(", "")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	for _, want := range []string{"YUKON_COLLECTOR_REDACT_BLOCKED_VALUES", "line 3"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

func TestResolveRedaction_AllLiterals(t *testing.T) {
	for raw, want := range map[string]bool{
		"":      false,
		"1":     true,
		"true":  true,
		"TRUE":  true,
		"0":     false,
		"false": false,
	} {
		cfg, err := resolveRedaction("", raw)
		if err != nil {
			t.Errorf("all literals %q: unexpected error: %v", raw, err)
			continue
		}
		if cfg.AllLiterals != want {
			t.Errorf("all literals %q = %v, want %v", raw, cfg.AllLiterals, want)
		}
	}
}

func TestResolveRedaction_AllLiteralsGarbage_Errors(t *testing.T) {
	_, err := resolveRedaction("", "yes please")
	if err == nil {
		t.Fatal("expected an error for an unparsable boolean, got nil")
	}
	if !strings.Contains(err.Error(), "YUKON_COLLECTOR_REDACT_ALL_LITERALS") {
		t.Fatalf("error %q does not name the variable", err)
	}
}
