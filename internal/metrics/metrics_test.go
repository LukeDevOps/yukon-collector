package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounter_RendersSeriesSortedWithLabels(t *testing.T) {
	c := NewCounter("test_render_total", "Help text.", "payload", "reason")
	c.Inc("manifest", "b")
	c.Add(2, "deltas", "a")

	out := Render()
	want := "# HELP test_render_total Help text.\n" +
		"# TYPE test_render_total counter\n" +
		"test_render_total{payload=\"deltas\",reason=\"a\"} 2\n" +
		"test_render_total{payload=\"manifest\",reason=\"b\"} 1\n"
	if !strings.Contains(out, want) {
		t.Fatalf("rendered output missing expected block.\nwant:\n%s\ngot:\n%s", want, out)
	}
}

func TestCounter_NoLabels_RendersBareName(t *testing.T) {
	c := NewCounter("test_bare_total", "Bare.")
	c.Inc()
	if !strings.Contains(Render(), "test_bare_total 1\n") {
		t.Fatalf("bare counter not rendered:\n%s", Render())
	}
}

func TestCounter_EscapesLabelValues(t *testing.T) {
	c := NewCounter("test_escape_total", "Escape.", "v")
	c.Inc("a\"b\\c\nd")
	if !strings.Contains(Render(), `test_escape_total{v="a\"b\\c\nd"} 1`) {
		t.Fatalf("label value not escaped:\n%s", Render())
	}
}

func TestCounter_WrongLabelCount_Panics(t *testing.T) {
	c := NewCounter("test_arity_total", "Arity.", "one")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for the wrong number of label values")
		}
	}()
	c.Inc()
}

func TestNewCounter_DuplicateName_Panics(t *testing.T) {
	NewCounter("test_dup_total", "Dup.")
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for a duplicate counter name")
		}
	}()
	NewCounter("test_dup_total", "Dup.")
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
