package ingest

import (
	"context"
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

// AcceptDeltaBatch logs the batch's service identity, run ID,
// environment, delta count, endpoint delta count, and dependency delta
// count. It never fails.
func (s *LogSink) AcceptDeltaBatch(_ context.Context, batch *yukonpb.DeltaBatch) error {
	s.logger.Info("received delta batch",
		"service", batch.GetResource().GetServiceName(),
		"instance", batch.GetResource().GetServiceInstanceId(),
		"run", batch.GetResource().GetRunId(),
		"environment", batch.GetResource().GetEnvironment(),
		"deltas", len(batch.GetDeltas()),
		"endpoint_deltas", len(batch.GetEndpointDeltas()),
		"dependency_deltas", len(batch.GetDependencyDeltas()),
	)
	return nil
}

// AcceptManifest logs the manifest's service identity, run ID,
// environment, probe counts, call edge and supertype counts, endpoint
// count, disabled endpoint module count, dependency and reference
// counts, and whether the instance records references. It never fails.
func (s *LogSink) AcceptManifest(_ context.Context, manifest *yukonpb.ProbeManifest) error {
	s.logger.Info("received probe manifest",
		"service", manifest.GetResource().GetServiceName(),
		"instance", manifest.GetResource().GetServiceInstanceId(),
		"run", manifest.GetResource().GetRunId(),
		"environment", manifest.GetResource().GetEnvironment(),
		"probes", len(manifest.GetProbes()),
		"call_edges", callEdgeCount(manifest.GetProbes()),
		"class_supertypes", len(manifest.GetClassSupertypes()),
		"skipped_classes", len(manifest.GetSkippedClasses()),
		"endpoints", len(manifest.GetEndpoints()),
		"disabled_endpoint_modules", len(manifest.GetDisabledEndpointModules()),
		"dependencies", len(manifest.GetDependencies()),
		"referenced_classes", referencedClassCount(manifest.GetProbes()),
		"class_references", len(manifest.GetClassReferences()),
		"external_classes", len(manifest.GetExternalClasses()),
		"references_recorded", manifest.GetReferencesRecorded(),
	)
	return nil
}

// AcceptStaticBaseline logs the baseline's service identity, run ID,
// environment, scan identity, chunk position, and class counts. It never
// fails.
func (s *LogSink) AcceptStaticBaseline(_ context.Context, baseline *yukonpb.StaticBaseline) error {
	s.logger.Info("received static baseline",
		"service", baseline.GetResource().GetServiceName(),
		"instance", baseline.GetResource().GetServiceInstanceId(),
		"run", baseline.GetResource().GetRunId(),
		"environment", baseline.GetResource().GetEnvironment(),
		"scanned_at", baseline.GetScannedAt(),
		"chunk", baseline.GetChunkIndex(),
		"chunk_count", baseline.GetChunkCount(),
		"declared_classes", len(baseline.GetDeclaredClasses()),
		"declared_call_edges", declaredCallEdgeCount(baseline.GetDeclaredClasses()),
		"declared_referenced_classes", declaredReferencedClassCount(baseline.GetDeclaredClasses()),
		"statically_unsafe_classes", len(baseline.GetStaticallyUnsafeClasses()),
		"unreadable_classes", len(baseline.GetUnreadableClasses()),
		"unprobed_classes", len(baseline.GetUnprobedClasses()),
	)
	return nil
}

// callEdgeCount sums the call edges carried by probes.
func callEdgeCount(probes []*yukonpb.ProbeLocation) int {
	n := 0
	for _, p := range probes {
		n += len(p.GetCalls())
	}
	return n
}

// declaredCallEdgeCount sums the call edges carried by every method of
// every declared class.
func declaredCallEdgeCount(classes []*yukonpb.DeclaredClass) int {
	n := 0
	for _, c := range classes {
		for _, m := range c.GetMethods() {
			n += len(m.GetCalls())
		}
	}
	return n
}

// referencedClassCount sums the referenced class names carried by probes.
func referencedClassCount(probes []*yukonpb.ProbeLocation) int {
	n := 0
	for _, p := range probes {
		n += len(p.GetReferencedClasses())
	}
	return n
}

// declaredReferencedClassCount sums the referenced class names carried by
// every declared class and every method of it.
func declaredReferencedClassCount(classes []*yukonpb.DeclaredClass) int {
	n := 0
	for _, c := range classes {
		n += len(c.GetReferencedClasses())
		for _, m := range c.GetMethods() {
			n += len(m.GetReferencedClasses())
		}
	}
	return n
}
