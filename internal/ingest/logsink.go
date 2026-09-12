package ingest

import (
	"log/slog"

	yukonpb "github.com/LukeDevOps/yukon-collector/internal/proto/yukon"
)

// LogSink logs every payload instead of storing it. It is a placeholder
// until a real backend (storage, multi-tenant aggregation) is wired in.
type LogSink struct {
	logger *slog.Logger
}

func NewLogSink(logger *slog.Logger) *LogSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSink{logger: logger}
}

func (s *LogSink) AcceptDeltaBatch(batch *yukonpb.DeltaBatch) {
	s.logger.Info("received delta batch",
		"service", batch.GetResource().GetServiceName(),
		"instance", batch.GetResource().GetServiceInstanceId(),
		"deltas", len(batch.GetDeltas()),
	)
}

func (s *LogSink) AcceptManifest(manifest *yukonpb.ProbeManifest) {
	s.logger.Info("received probe manifest",
		"service", manifest.GetServiceName(),
		"probes", len(manifest.GetProbes()),
		"skipped_classes", len(manifest.GetSkippedClasses()),
	)
}
