package tokenfile

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

// writeFile replaces path's content through a rename, so a reader sees
// the old content or the new, never a half-written file.
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename %s: %v", tmp, err)
	}
}

func tokenPath(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens")
	writeFile(t, path, content)
	return path
}

// holder records what a File applies, safe for a Watch goroutine and a
// test goroutine to share.
type holder struct {
	mu     sync.Mutex
	tokens []string
}

func (h *holder) apply(tokens []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tokens = tokens
}

func (h *holder) get() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.tokens
}

// lockedBuffer is a log sink a Watch goroutine can write while the test
// reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met before timeout")
	}
}

func TestParse(t *testing.T) {
	tests := map[string]struct {
		content string
		want    []string
	}{
		"one token":                 {content: "s3cret", want: []string{"s3cret"}},
		"one token with newline":    {content: "s3cret\n", want: []string{"s3cret"}},
		"several tokens":            {content: "a\nb\nc\n", want: []string{"a", "b", "c"}},
		"spaces trimmed":            {content: "  a  \n\tb\t\n", want: []string{"a", "b"}},
		"carriage returns trimmed":  {content: "a\r\nb\r\n", want: []string{"a", "b"}},
		"blank lines skipped":       {content: "\n\na\n   \n\nb\n\n", want: []string{"a", "b"}},
		"comments skipped":          {content: "# rotated 2026-09\na\n  # old key\nb\n", want: []string{"a", "b"}},
		"hash inside a token kept":  {content: "a#b\n", want: []string{"a#b"}},
		"comma inside a token kept": {content: "a,b\n", want: []string{"a,b"}},
		"inner spaces kept":         {content: "a b\n", want: []string{"a b"}},
		"empty file":                {content: "", want: nil},
		"only comments and blanks":  {content: "# nothing here\n\n   \n", want: nil},
		"byte order mark dropped":   {content: "\xef\xbb\xbfa\nb\n", want: []string{"a", "b"}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := Parse([]byte(tt.content)); !slices.Equal(got, tt.want) {
				t.Fatalf("Parse(%q) = %q, want %q", tt.content, got, tt.want)
			}
		})
	}
}

func TestRead_MissingFile_Errors(t *testing.T) {
	if _, err := Read(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected an error for a missing file, got nil")
	}
}

func TestRead_OversizedFile_Errors(t *testing.T) {
	path := tokenPath(t, strings.Repeat("a", maxFileBytes+1))
	if _, err := Read(path); err == nil {
		t.Fatal("expected an error for an oversized file, got nil")
	}
}

func TestRead_FileAtSizeLimit_Reads(t *testing.T) {
	path := tokenPath(t, strings.Repeat("a", maxFileBytes))
	tokens, err := Read(path)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(tokens) != 1 || len(tokens[0]) != maxFileBytes {
		t.Fatalf("got %d tokens, want one of %d bytes", len(tokens), maxFileBytes)
	}
}

func TestRead_Directory_Errors(t *testing.T) {
	if _, err := Read(t.TempDir()); err == nil {
		t.Fatal("expected an error for a directory, got nil")
	}
}

func TestRead_SymlinkToFile_Reads(t *testing.T) {
	target := tokenPath(t, "a\n")
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks not available: %v", err)
	}
	tokens, err := Read(link)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if want := []string{"a"}; !slices.Equal(tokens, want) {
		t.Fatalf("tokens = %q, want %q", tokens, want)
	}
}

func TestWatch_ShowsZeroFailureSeries(t *testing.T) {
	// Series outlive a test, so each run under -count takes its own label.
	label := fmt.Sprintf("watch-zero-series-%d", time.Now().UnixNano())
	f, err := Open(tokenPath(t, "a\n"), label, AtLeastOne, func([]string) {})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	want := `otherlode_collector_token_reload_failures_total{file="` + label + `"} 0`
	if strings.Contains(metrics.Render(), want) {
		t.Fatalf("metrics show %q before Watch starts", want)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Watch(ctx, time.Hour, slog.New(slog.NewTextHandler(new(bytes.Buffer), nil)))
	}()
	waitFor(t, 5*time.Second, func() bool { return strings.Contains(metrics.Render(), want) })
	cancel()
	<-done
}

func TestOpen(t *testing.T) {
	tests := map[string]struct {
		content string
		missing bool
		check   func([]string) error
		want    []string
		wantErr bool
	}{
		"at least one, one token":     {content: "a\n", check: AtLeastOne, want: []string{"a"}},
		"at least one, two tokens":    {content: "a\nb\n", check: AtLeastOne, want: []string{"a", "b"}},
		"at least one, empty file":    {content: "", check: AtLeastOne, wantErr: true},
		"at least one, only comments": {content: "# none\n", check: AtLeastOne, wantErr: true},
		"at least one, missing file":  {missing: true, check: AtLeastOne, wantErr: true},
		"exactly one, one token":      {content: "# key\nk\n", check: ExactlyOne, want: []string{"k"}},
		"exactly one, empty file":     {content: "\n", check: ExactlyOne, wantErr: true},
		"exactly one, two tokens":     {content: "k1\nk2\n", check: ExactlyOne, wantErr: true},
		"exactly one, missing file":   {missing: true, check: ExactlyOne, wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "missing")
			if !tt.missing {
				path = tokenPath(t, tt.content)
			}
			h := new(holder)
			f, err := Open(path, "auth", tt.check, h.apply)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				if h.get() != nil {
					t.Fatalf("apply called with %q on a failed open", h.get())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if f == nil {
				t.Fatal("Open returned a nil File")
			}
			if got := h.get(); !slices.Equal(got, tt.want) {
				t.Fatalf("applied %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOpen_ErrorNeverShowsTokens(t *testing.T) {
	path := tokenPath(t, "first-secret\nsecond-secret\n")
	_, err := Open(path, "forward", ExactlyOne, func([]string) {})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if strings.Contains(err.Error(), "first-secret") || strings.Contains(err.Error(), "second-secret") {
		t.Fatalf("error shows a token: %v", err)
	}
}

func TestReload_ChangedFile_AppliesAndLogsCountOnly(t *testing.T) {
	path := tokenPath(t, "old-token\n")
	h := new(holder)
	f, err := Open(path, "auth", AtLeastOne, h.apply)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	writeFile(t, path, "old-token\nnew-token\n")
	f.reload(logger)

	if got, want := h.get(), []string{"old-token", "new-token"}; !slices.Equal(got, want) {
		t.Fatalf("applied %q, want %q", got, want)
	}
	if !strings.Contains(logs.String(), "level=INFO") || !strings.Contains(logs.String(), "tokens=2") {
		t.Fatalf("change not logged at info with the token count:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "old-token") || strings.Contains(logs.String(), "new-token") {
		t.Fatalf("log shows a token:\n%s", logs.String())
	}
}

func TestReload_UnchangedFile_DoesNothing(t *testing.T) {
	path := tokenPath(t, "a\nb\n")
	applied := 0
	f, err := Open(path, "auth", AtLeastOne, func([]string) { applied++ })
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var logs bytes.Buffer
	writeFile(t, path, "# same tokens, new comment\na\n\nb\n")
	f.reload(slog.New(slog.NewTextHandler(&logs, nil)))

	if applied != 1 {
		t.Fatalf("apply called %d times, want 1 (the open only)", applied)
	}
	if logs.Len() != 0 {
		t.Fatalf("unchanged re-read logged:\n%s", logs.String())
	}
}

func TestReload_Failure_KeepsLastGoodAndCounts(t *testing.T) {
	tests := map[string]struct {
		label  string
		check  func([]string) error
		start  string
		change func(t *testing.T, path string)
	}{
		"auth file removed": {
			label: "auth", check: AtLeastOne, start: "a\n",
			change: func(t *testing.T, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove: %v", err)
				}
			},
		},
		"auth file emptied": {
			label: "auth", check: AtLeastOne, start: "a\n",
			change: func(t *testing.T, path string) { writeFile(t, path, "") },
		},
		"auth file left with only comments": {
			label: "auth", check: AtLeastOne, start: "a\n",
			change: func(t *testing.T, path string) { writeFile(t, path, "# rotating\n") },
		},
		"forward file removed": {
			label: "forward", check: ExactlyOne, start: "k\n",
			change: func(t *testing.T, path string) {
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove: %v", err)
				}
			},
		},
		"forward file emptied": {
			label: "forward", check: ExactlyOne, start: "k\n",
			change: func(t *testing.T, path string) { writeFile(t, path, "") },
		},
		"forward file with two keys": {
			label: "forward", check: ExactlyOne, start: "k\n",
			change: func(t *testing.T, path string) { writeFile(t, path, "k\nk2\n") },
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			path := tokenPath(t, tt.start)
			h := new(holder)
			f, err := Open(path, tt.label, tt.check, h.apply)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			before := h.get()
			authBefore := metrics.TokenReloadFailures.Value("auth")
			forwardBefore := metrics.TokenReloadFailures.Value("forward")

			var logs bytes.Buffer
			tt.change(t, path)
			f.reload(slog.New(slog.NewTextHandler(&logs, nil)))

			if got := h.get(); !slices.Equal(got, before) {
				t.Fatalf("tokens = %q after a failed re-read, want the last good %q", got, before)
			}
			wantAuth, wantForward := int64(0), int64(0)
			if tt.label == "auth" {
				wantAuth = 1
			} else {
				wantForward = 1
			}
			if got := metrics.TokenReloadFailures.Value("auth") - authBefore; got != wantAuth {
				t.Errorf("auth reload failures rose by %d, want %d", got, wantAuth)
			}
			if got := metrics.TokenReloadFailures.Value("forward") - forwardBefore; got != wantForward {
				t.Errorf("forward reload failures rose by %d, want %d", got, wantForward)
			}
			if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "file="+tt.label) {
				t.Fatalf("failure not logged as a warning naming the file:\n%s", logs.String())
			}
		})
	}
}

func TestReload_RecoversAfterFailure(t *testing.T) {
	path := tokenPath(t, "a\n")
	h := new(holder)
	f, err := Open(path, "auth", AtLeastOne, h.apply)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(new(bytes.Buffer), nil))

	writeFile(t, path, "")
	f.reload(logger)
	writeFile(t, path, "b\n")
	f.reload(logger)

	if got, want := h.get(), []string{"b"}; !slices.Equal(got, want) {
		t.Fatalf("tokens = %q, want %q", got, want)
	}
}

func TestReload_RecoveryWithSameTokens_LogsOnce(t *testing.T) {
	path := tokenPath(t, "a\n")
	h := new(holder)
	f, err := Open(path, "auth", AtLeastOne, h.apply)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	writeFile(t, path, "")
	f.reload(logger)
	writeFile(t, path, "a\n")
	f.reload(logger)
	f.reload(logger)

	if got := strings.Count(logs.String(), "token file re-read recovered"); got != 1 {
		t.Fatalf("recovery logged %d times, want 1; logs:\n%s", got, logs.String())
	}
	if strings.Contains(logs.String(), "token file changed") {
		t.Fatalf("unchanged tokens logged a change; logs:\n%s", logs.String())
	}
}

func TestWatch_PicksUpChangeAndStopsWithContext(t *testing.T) {
	path := tokenPath(t, "old-token\n")
	h := new(holder)
	f, err := Open(path, "auth", AtLeastOne, h.apply)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	logs := new(lockedBuffer)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		f.Watch(ctx, 5*time.Millisecond, slog.New(slog.NewTextHandler(logs, nil)))
		close(done)
	}()

	writeFile(t, path, "new-token\n")
	waitFor(t, 2*time.Second, func() bool { return slices.Equal(h.get(), []string{"new-token"}) })

	failuresBefore := metrics.TokenReloadFailures.Value("auth")
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool { return metrics.TokenReloadFailures.Value("auth") > failuresBefore })
	if got, want := h.get(), []string{"new-token"}; !slices.Equal(got, want) {
		t.Fatalf("tokens = %q after the file was removed, want the last good %q", got, want)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Watch did not return after its context was cancelled")
	}
	if strings.Contains(logs.String(), "old-token") || strings.Contains(logs.String(), "new-token") {
		t.Fatalf("log shows a token:\n%s", logs.String())
	}
}
