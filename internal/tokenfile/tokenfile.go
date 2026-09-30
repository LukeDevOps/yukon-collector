// Package tokenfile reads secret tokens from a file and re-reads the file
// on a fixed interval. A mounted Kubernetes secret changes in place and
// sends no signal, so a signal handler alone would miss a new value.
package tokenfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

// ReloadInterval is how often the collector re-reads a token file.
const ReloadInterval = 30 * time.Second

// maxFileBytes bounds one read. A token file holds a few short lines. A
// path that names something far larger is a mistake, and reading all of
// it every interval would waste memory.
const maxFileBytes = 1 << 20

// utf8BOM is the byte order mark some editors write at the start of a
// file.
var utf8BOM = []byte("\xef\xbb\xbf")

// Parse returns the tokens in data, one per line. It drops a leading
// UTF-8 byte order mark, which strings.TrimSpace keeps. It trims spaces
// around each line. It skips blank lines and lines whose first non-space
// character is '#'.
func Parse(data []byte) []string {
	data = bytes.TrimPrefix(data, utf8BOM)
	var tokens []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		tokens = append(tokens, line)
	}
	return tokens
}

// Read reads the file at path and returns its tokens, as Parse does. It
// refuses anything but a regular file, reached directly or through a
// symlink.
func Read(path string) ([]string, error) {
	// O_NONBLOCK keeps the open from waiting on a named pipe with no
	// writer. It has no effect on a regular file. The type check runs on
	// the open handle, so the path cannot change between check and read.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxFileBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, maxFileBytes)
	}
	return Parse(data), nil
}

// AtLeastOne is a check for File that rejects a file with no token.
func AtLeastOne(tokens []string) error {
	if len(tokens) == 0 {
		return errors.New("no token found")
	}
	return nil
}

// ExactlyOne is a check for File that rejects a file unless it holds
// exactly one token.
func ExactlyOne(tokens []string) error {
	if len(tokens) != 1 {
		return fmt.Errorf("found %d tokens, want exactly one", len(tokens))
	}
	return nil
}

// File is a token file that the collector re-reads. Each read goes
// through a check. A read that passes the check and differs from the
// last good tokens goes to an apply function. A read that fails keeps
// the last good tokens in place.
type File struct {
	path    string
	label   string
	check   func([]string) error
	apply   func([]string)
	last    []string
	failing bool
}

// Open reads the file at path, checks its tokens with check and passes
// them to apply. It returns an error, and never calls apply, when the
// file cannot be read or check rejects its tokens. label names the file
// in logs and in the reload failure counter. It must not be the path,
// since the path can differ on every deployment.
func Open(path, label string, check func([]string) error, apply func([]string)) (*File, error) {
	tokens, err := load(path, check)
	if err != nil {
		return nil, err
	}
	apply(tokens)
	return &File{path: path, label: label, check: check, apply: apply, last: tokens}, nil
}

// Watch re-reads the file every interval until ctx is done. It shows the
// file's reload failure series at 0 from the start, so an alert on its
// increase sees the first failure. Watch must not run in more than one
// goroutine at a time for the same File.
func (f *File) Watch(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	metrics.TokenReloadFailures.Add(0, f.label)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.reload(logger)
		}
	}
}

// reload reads the file once. A failed read or check logs a warning,
// counts a reload failure and keeps the last good tokens. The first good
// read after a failure logs that the file recovered. A good read that
// changes the tokens passes them to apply and logs the new count, never
// the tokens.
func (f *File) reload(logger *slog.Logger) {
	tokens, err := load(f.path, f.check)
	if err != nil {
		f.failing = true
		metrics.TokenReloadFailures.Inc(f.label)
		logger.Warn("token file re-read failed; keeping the last good value", "file", f.label, "error", err)
		return
	}
	if f.failing {
		f.failing = false
		logger.Info("token file re-read recovered", "file", f.label)
	}
	if slices.Equal(tokens, f.last) {
		return
	}
	f.apply(tokens)
	f.last = tokens
	logger.Info("token file changed", "file", f.label, "tokens", len(tokens))
}

func load(path string, check func([]string) error) ([]string, error) {
	tokens, err := Read(path)
	if err != nil {
		return nil, err
	}
	if err := check(tokens); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return tokens, nil
}
