package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/ingest"
)

// TestLogSink_LogsTheTestRunFlag covers the test_run attribute on each of
// the three payloads' log lines (agent ADR 0050).
func TestLogSink_LogsTheTestRunFlag(t *testing.T) {
	var buf bytes.Buffer
	sink := ingest.NewLogSink(slog.New(slog.NewJSONHandler(&buf, nil)))
	resource := &otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "unit-tests", RunId: "run-1", TestRun: true}
	ctx := context.Background()

	if err := sink.AcceptDeltaBatch(ctx, &otherlodepb.DeltaBatch{Resource: resource}); err != nil {
		t.Fatalf("accept delta batch: %v", err)
	}
	if err := sink.AcceptManifest(ctx, &otherlodepb.ProbeManifest{Resource: resource}); err != nil {
		t.Fatalf("accept manifest: %v", err)
	}
	if err := sink.AcceptStaticBaseline(ctx, &otherlodepb.StaticBaseline{Resource: resource}); err != nil {
		t.Fatalf("accept static baseline: %v", err)
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("logged %d lines, want 3: %s", len(lines), buf.String())
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("decode log line %s: %v", line, err)
		}
		if record["test_run"] != true {
			t.Errorf("log line %s has test_run %v, want true", line, record["test_run"])
		}
	}
}
