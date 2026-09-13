package ingest

import (
	"log/slog"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"
)

// LogSink logs every payload instead of storing it. It is a placeholder
// until a real backend (storage, multi-tenant aggregation) is wired in.
type LogSink struct {
	logger *slog.Logger
}

// NewLogSink returns a LogSink writing to logger, or slog.Default() when
// logger is nil.
func NewLogSink(logger *slog.Logger) *LogSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogSink{logger: logger}
}

// AcceptDeltaBatch logs the batch's service identity and delta count.
func (s *LogSink) AcceptDeltaBatch(batch *yukonpb.DeltaBatch) {
	s.logger.Info("received delta batch",
		"service", batch.GetResource().GetServiceName(),
		"instance", batch.GetResource().GetServiceInstanceId(),
		"deltas", len(batch.GetDeltas()),
	)
}

// AcceptManifest logs the manifest's service name and probe counts.
func (s *LogSink) AcceptManifest(manifest *yukonpb.ProbeManifest) {
	s.logger.Info("received probe manifest",
		"service", manifest.GetServiceName(),
		"probes", len(manifest.GetProbes()),
		"skipped_classes", len(manifest.GetSkippedClasses()),
	)
}

// AcceptStaticBaseline logs the baseline's service identity, scan
// identity, chunk position, and class counts.
func (s *LogSink) AcceptStaticBaseline(baseline *yukonpb.StaticBaseline) {
	s.logger.Info("received static baseline",
		"service", baseline.GetResource().GetServiceName(),
		"instance", baseline.GetResource().GetServiceInstanceId(),
		"scanned_at", baseline.GetScannedAt(),
		"chunk", baseline.GetChunkIndex(),
		"chunk_count", baseline.GetChunkCount(),
		"declared_classes", len(baseline.GetDeclaredClasses()),
		"statically_unsafe_classes", len(baseline.GetStaticallyUnsafeClasses()),
		"unreadable_classes", len(baseline.GetUnreadableClasses()),
		"unprobed_classes", len(baseline.GetUnprobedClasses()),
	)
}
