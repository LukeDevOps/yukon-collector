//go:build unix

package tokenfile

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestRead_NamedPipe_ErrorsWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipe")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	errc := make(chan error, 1)
	go func() {
		_, err := Read(path)
		errc <- err
	}()
	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("expected an error for a named pipe, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Read blocked on a named pipe")
	}
}
