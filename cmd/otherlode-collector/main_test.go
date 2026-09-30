package main

import (
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/otherlodehq/otherlode-collector/internal/forward"
	"github.com/otherlodehq/otherlode-collector/internal/processor"
)

// writeTokenFile writes content to a new file and returns its path.
func writeTokenFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

// replaceTokenFile swaps path's content through a rename, so a re-read
// never sees a half-written file.
func replaceTokenFile(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename token file: %v", err)
	}
}

func TestParseTokenList(t *testing.T) {
	tests := map[string]struct {
		raw     string
		want    []string
		wantErr bool
	}{
		"single token":         {raw: "s3cret", want: []string{"s3cret"}},
		"several tokens":       {raw: "a,b,c", want: []string{"a", "b", "c"}},
		"spaces trimmed":       {raw: " a , b\t,c ", want: []string{"a", "b", "c"}},
		"inner spaces kept":    {raw: "a b,c", want: []string{"a b", "c"}},
		"empty middle entry":   {raw: "a,,b", wantErr: true},
		"leading comma":        {raw: ",a", wantErr: true},
		"trailing comma":       {raw: "a,", wantErr: true},
		"entry of only spaces": {raw: "a,   ,b", wantErr: true},
		"only spaces":          {raw: "   ", wantErr: true},
		"only a comma":         {raw: ",", wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseTokenList(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("parseTokenList(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestResolveAuthTokens(t *testing.T) {
	goodFile := writeTokenFile(t, "# agents\nfile-a\n\n  file-b  \n")
	emptyFile := writeTokenFile(t, "")
	commentFile := writeTokenFile(t, "# no tokens yet\n\n")
	missingFile := filepath.Join(t.TempDir(), "missing")

	tests := map[string]struct {
		env      map[string]string
		accepted []string
		refused  []string
		noAuth   bool
		wantFile bool
		wantErr  bool
	}{
		"single token": {
			env:      map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "s3cret"},
			accepted: []string{"s3cret"},
			refused:  []string{"other"},
		},
		"token list": {
			env:      map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "old, new"},
			accepted: []string{"old", "new"},
			refused:  []string{"old, new", " new", "other"},
		},
		"token and opt-out, token wins": {
			env:      map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "s3cret", "OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "1"},
			accepted: []string{"s3cret"},
			refused:  []string{""},
		},
		"empty list entry": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "a,,b"},
			wantErr: true,
		},
		"empty list entry with opt-out": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "a,", "OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "1"},
			wantErr: true,
		},
		"token file": {
			env:      map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": goodFile},
			accepted: []string{"file-a", "file-b"},
			refused:  []string{"# agents", "", "other"},
			wantFile: true,
		},
		"both variables set": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN": "s3cret", "OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": goodFile},
			wantErr: true,
		},
		"missing token file": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": missingFile},
			wantErr: true,
		},
		"missing token file with opt-out": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": missingFile, "OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "1"},
			wantErr: true,
		},
		"empty token file with opt-out": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": emptyFile, "OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "1"},
			wantErr: true,
		},
		"comment-only token file with opt-out": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_AUTH_TOKEN_FILE": commentFile, "OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "true"},
			wantErr: true,
		},
		"nothing set": {
			env:     nil,
			wantErr: true,
		},
		"nothing set, explicit opt-out": {
			env:    map[string]string{"OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "1"},
			noAuth: true,
		},
		"nothing set, opt-out false": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "false"},
			wantErr: true,
		},
		"nothing set, unparsable opt-out": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_INSECURE_NO_AUTH": "not-a-bool"},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tokens, file, err := resolveAuthTokens(envFrom(tt.env))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.noAuth {
				if tokens != nil || file != nil {
					t.Fatalf("tokens = %v, file = %v, want both nil for no auth", tokens, file)
				}
				return
			}
			if (file != nil) != tt.wantFile {
				t.Fatalf("file returned = %v, want %v", file != nil, tt.wantFile)
			}
			for _, token := range tt.accepted {
				if !tokens.Matches(token) {
					t.Errorf("token %q refused, want accepted", token)
				}
			}
			for _, token := range tt.refused {
				if tokens.Matches(token) {
					t.Errorf("token %q accepted, want refused", token)
				}
			}
		})
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
	cfg, file, err := resolveForwardConfig(envFrom(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != (forward.Config{}) {
		t.Fatalf("config = %+v, want all zero so the forward package applies its defaults", cfg)
	}
	if file != nil {
		t.Fatal("token file returned with no file configured")
	}
}

func TestResolveForwardConfig_AllSet_ParsesEveryField(t *testing.T) {
	cfg, _, err := resolveForwardConfig(envFrom(map[string]string{
		"OTHERLODE_COLLECTOR_FORWARD_URL":                    "https://backend.example.com",
		"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN":             "backend-secret",
		"OTHERLODE_COLLECTOR_FORWARD_SHARDS":                 "4",
		"OTHERLODE_COLLECTOR_FORWARD_QUEUE_SIZE":             "128",
		"OTHERLODE_COLLECTOR_FORWARD_REQUEST_TIMEOUT":        "15s",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL": "2s",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL":     "1m",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME": "10m",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := forward.Config{
		URL:                  "https://backend.example.com",
		AuthToken:            forward.StaticToken("backend-secret"),
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

func TestResolveForwardConfig_AuthToken(t *testing.T) {
	oneKey := writeTokenFile(t, "# backend key\n  file-key  \n\n")
	noKey := writeTokenFile(t, "# rotating\n")
	twoKeys := writeTokenFile(t, "key-1\nkey-2\n")
	missing := filepath.Join(t.TempDir(), "missing")

	tests := map[string]struct {
		env      map[string]string
		want     string
		wantFile bool
		wantErr  bool
		errHas   string
	}{
		"no key": {
			env:  nil,
			want: "",
		},
		"key from variable, never split": {
			env:  map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN": "a,b"},
			want: "a,b",
		},
		"key from variable, spaces trimmed": {
			env:  map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN": " backend-secret\n"},
			want: "backend-secret",
		},
		"variable with only spaces": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN": " \n"},
			wantErr: true,
		},
		"variable with only spaces and a file": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN": " ", "OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": oneKey},
			wantErr: true,
			errHas:  "both set",
		},
		"key from file": {
			env:      map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": oneKey},
			want:     "file-key",
			wantFile: true,
		},
		"both variables set": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN": "backend-secret", "OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": oneKey},
			wantErr: true,
		},
		"file with no key": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": noKey},
			wantErr: true,
		},
		"file with two keys": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": twoKeys},
			wantErr: true,
		},
		"missing file": {
			env:     map[string]string{"OTHERLODE_COLLECTOR_FORWARD_AUTH_TOKEN_FILE": missing},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, file, err := resolveForwardConfig(envFrom(tt.env))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errHas) {
					t.Fatalf("error %q does not contain %q", err, tt.errHas)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if (file != nil) != tt.wantFile {
				t.Fatalf("file returned = %v, want %v", file != nil, tt.wantFile)
			}
			got := ""
			if cfg.AuthToken != nil {
				got = cfg.AuthToken.Token()
			}
			if got != tt.want {
				t.Fatalf("key = %q, want %q", got, tt.want)
			}
		})
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
				if !strings.Contains(err.Error(), "OTHERLODE_COLLECTOR_SERVICE_NAMESPACE") {
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
		"OTHERLODE_COLLECTOR_FORWARD_SHARDS":                 "0",
		"OTHERLODE_COLLECTOR_FORWARD_QUEUE_SIZE":             "-1",
		"OTHERLODE_COLLECTOR_FORWARD_REQUEST_TIMEOUT":        "10",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_INITIAL_INTERVAL": "soon",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_INTERVAL":     "0s",
		"OTHERLODE_COLLECTOR_FORWARD_RETRY_MAX_ELAPSED_TIME": "-5m",
	} {
		if _, _, err := resolveForwardConfig(envFrom(map[string]string{name: value})); err == nil {
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
	for _, want := range []string{"OTHERLODE_COLLECTOR_REDACT_BLOCKED_VALUES", "line 3"} {
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
	if !strings.Contains(err.Error(), "OTHERLODE_COLLECTOR_REDACT_ALL_LITERALS") {
		t.Fatalf("error %q does not name the variable", err)
	}
}
