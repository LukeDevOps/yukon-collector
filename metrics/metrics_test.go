package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// renderOne renders a single unregistered counter, so tests do not touch
// the process-wide registry and can run more than once per process.
func renderOne(c *Counter) string {
	var b strings.Builder
	c.render(&b)
	return b.String()
}

func TestCounter_RendersSeriesSortedWithLabels(t *testing.T) {
	c := newCounter("test_render_total", "Help text.", "payload", "reason")
	c.Inc("manifest", "b")
	c.Add(2, "deltas", "a")

	want := "# HELP test_render_total Help text.\n" +
		"# TYPE test_render_total counter\n" +
		"test_render_total{payload=\"deltas\",reason=\"a\"} 2\n" +
		"test_render_total{payload=\"manifest\",reason=\"b\"} 1\n"
	if got := renderOne(c); got != want {
		t.Fatalf("rendered output:\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestCounter_NoLabels_RendersBareName(t *testing.T) {
	c := newCounter("test_bare_total", "Bare.")
	c.Inc()
	if got := renderOne(c); !strings.Contains(got, "test_bare_total 1\n") {
		t.Fatalf("bare counter not rendered:\n%s", got)
	}
}

func TestCounter_EscapesLabelValues(t *testing.T) {
	c := newCounter("test_escape_total", "Escape.", "v")
	c.Inc("a\"b\\c\nd")
	if got := renderOne(c); !strings.Contains(got, `test_escape_total{v="a\"b\\c\nd"} 1`) {
		t.Fatalf("label value not escaped:\n%s", got)
	}
}

func TestCounter_WrongLabelCount_Panics(t *testing.T) {
	c := newCounter("test_arity_total", "Arity.", "one")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for the wrong number of label values")
		}
	}()
	c.Inc()
}

func TestNewCounter_DuplicateName_Panics(t *testing.T) {
	// A fresh name per run, since registration is process-wide.
	name := fmt.Sprintf("test_dup_%d_total", time.Now().UnixNano())
	NewCounter(name, "Dup.")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a duplicate counter name")
		}
	}()
	NewCounter(name, "Dup.")
}

func TestRender_IncludesRegisteredCounters(t *testing.T) {
	name := fmt.Sprintf("test_registered_%d_total", time.Now().UnixNano())
	c := NewCounter(name, "Registered.", "k")
	c.Inc("v")
	if got := Render(); !strings.Contains(got, name+`{k="v"} 1`+"\n") {
		t.Fatalf("registered counter missing from Render output:\n%s", got)
	}
}

func TestHandler_ServesTextFormat(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q, want text/plain", ct)
	}
	if !strings.Contains(rec.Body.String(), "# TYPE yukon_collector_ingest_accepted_total counter") {
		t.Fatalf("body missing the collector's own counters:\n%s", rec.Body.String())
	}
}
