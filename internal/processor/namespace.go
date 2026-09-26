package processor

import (
	"context"
	"log/slog"
	"strings"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/ingest"
	"github.com/LukeDevOps/yukon-collector/metrics"
)

// NamespaceConfig configures a Namespace processor. Value is the service
// namespace to stamp onto payloads. An empty Value turns the processor
// off.
type NamespaceConfig struct {
	Value  string
	Action Action
}

// Namespace is an ingest.Sink that writes a configured service namespace
// onto the resource of each delta batch, manifest and static baseline.
// It then passes the payload to next. A service is known by its
// namespace and its name. One collector often serves one team, so an
// operator can set the team's namespace once here instead of on every
// agent.
//
// Namespace compares values after it trims surrounding spaces. It keeps
// case, since a namespace is part of a service's identity.
//
// It changes the resource of the decoded message in place. This is safe
// because ingest.Handler decodes a fresh message for each request and
// nothing else holds a reference to it.
type Namespace struct {
	next   ingest.Sink
	cfg    NamespaceConfig
	logger *slog.Logger
}

var _ ingest.Sink = (*Namespace)(nil)

// NewNamespace returns a Namespace that applies cfg before it passes
// payloads to next. It logs to logger, or to slog.Default() when logger
// is nil.
func NewNamespace(next ingest.Sink, cfg NamespaceConfig, logger *slog.Logger) *Namespace {
	if logger == nil {
		logger = slog.Default()
	}
	return &Namespace{next: next, cfg: cfg, logger: logger}
}

// AcceptDeltaBatch stamps the batch's resource with the configured
// namespace, then passes the batch to next.
func (n *Namespace) AcceptDeltaBatch(ctx context.Context, batch *yukonpb.DeltaBatch) error {
	n.stamp(batch.GetResource(), "deltas")
	return n.next.AcceptDeltaBatch(ctx, batch)
}

// AcceptManifest stamps the manifest's resource with the configured
// namespace, then passes the manifest to next.
func (n *Namespace) AcceptManifest(ctx context.Context, manifest *yukonpb.ProbeManifest) error {
	n.stamp(manifest.GetResource(), "manifest")
	return n.next.AcceptManifest(ctx, manifest)
}

// AcceptStaticBaseline stamps the baseline's resource with the
// configured namespace, then passes the baseline to next.
func (n *Namespace) AcceptStaticBaseline(ctx context.Context, baseline *yukonpb.StaticBaseline) error {
	n.stamp(baseline.GetResource(), "static_baseline")
	return n.next.AcceptStaticBaseline(ctx, baseline)
}

// stamp applies the configured namespace to res. Under both actions it
// fills in a namespace that is empty or only spaces. When the agent
// named a different namespace, stamp counts and logs the mismatch. It
// overwrites the agent's value only under Upsert. A nil res is left
// alone: ingest.Handler rejects such a payload before any sink sees it,
// but a Namespace used without the handler must not panic on one.
func (n *Namespace) stamp(res *yukonpb.ResourceAttributes, payload string) {
	if res == nil {
		return
	}
	agentNamespace := res.GetServiceNamespace()
	agent := strings.TrimSpace(agentNamespace)
	collector := strings.TrimSpace(n.cfg.Value)
	if agent == "" {
		res.SetServiceNamespace(collector)
		return
	}
	if agent == collector {
		return
	}

	metrics.NamespaceMismatch.Inc(payload)
	n.logger.Debug("agent namespace does not match the collector's configured namespace",
		"service", res.GetServiceName(),
		"instance", res.GetServiceInstanceId(),
		"run", res.GetRunId(),
		"payload", payload,
		"agent_namespace", agentNamespace,
		"collector_namespace", collector,
		"action", n.cfg.Action.String(),
	)
	if n.cfg.Action == Upsert {
		res.SetServiceNamespace(collector)
	}
}
