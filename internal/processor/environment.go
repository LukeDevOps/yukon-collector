// Package processor holds processors. A processor is an ingest.Sink that
// wraps another ingest.Sink. It changes or inspects a decoded payload,
// then passes it on, as a processor does in an OTel Collector pipeline.
package processor

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/ingest"
	"github.com/LukeDevOps/yukon-collector/metrics"
)

// Action says what Environment does when a payload already names an
// environment.
type Action int

const (
	// Insert keeps an agent-set environment and only fills in an absent
	// one. It is the zero value and the default.
	Insert Action = iota
	// Upsert always writes the collector's configured environment, even
	// over one the agent already set.
	Upsert
)

// ParseAction parses raw as an Action. "" and "insert" mean Insert,
// "upsert" means Upsert, matched case-insensitively. Any other value is
// an error listing the valid options.
func ParseAction(raw string) (Action, error) {
	switch strings.ToLower(raw) {
	case "", "insert":
		return Insert, nil
	case "upsert":
		return Upsert, nil
	default:
		return 0, fmt.Errorf("invalid action %q: must be %q or %q", raw, Insert, Upsert)
	}
}

// String returns "insert" or "upsert".
func (a Action) String() string {
	if a == Upsert {
		return "upsert"
	}
	return "insert"
}

// EnvironmentConfig configures an Environment processor. Value is the
// environment name to stamp onto payloads; an empty Value means the
// processor is off.
type EnvironmentConfig struct {
	Value  string
	Action Action
}

// Environment is an ingest.Sink that writes a configured environment
// name onto the resource of each delta batch, manifest and static
// baseline, then passes the payload to next. A backend can then expect
// an environment on every payload, even from an agent whose own config
// names none.
//
// It changes the resource of the decoded message in place. This is safe
// because ingest.Handler decodes a fresh message for each request and
// nothing else holds a reference to it.
type Environment struct {
	next   ingest.Sink
	cfg    EnvironmentConfig
	logger *slog.Logger
}

var _ ingest.Sink = (*Environment)(nil)

// NewEnvironment returns an Environment that applies cfg before passing
// payloads to next, logging to logger, or slog.Default() when logger is
// nil.
func NewEnvironment(next ingest.Sink, cfg EnvironmentConfig, logger *slog.Logger) *Environment {
	if logger == nil {
		logger = slog.Default()
	}
	return &Environment{next: next, cfg: cfg, logger: logger}
}

// AcceptDeltaBatch stamps the batch's resource with the configured
// environment, then passes the batch to next.
func (e *Environment) AcceptDeltaBatch(ctx context.Context, batch *yukonpb.DeltaBatch) error {
	e.stamp(batch.GetResource(), "deltas")
	return e.next.AcceptDeltaBatch(ctx, batch)
}

// AcceptStaticBaseline stamps the baseline's resource with the
// configured environment, then passes the baseline to next.
func (e *Environment) AcceptStaticBaseline(ctx context.Context, baseline *yukonpb.StaticBaseline) error {
	e.stamp(baseline.GetResource(), "static_baseline")
	return e.next.AcceptStaticBaseline(ctx, baseline)
}

// AcceptManifest stamps the manifest's resource with the configured
// environment, then passes the manifest to next.
func (e *Environment) AcceptManifest(ctx context.Context, manifest *yukonpb.ProbeManifest) error {
	e.stamp(manifest.GetResource(), "manifest")
	return e.next.AcceptManifest(ctx, manifest)
}

// stamp applies the configured environment to res. It fills in an empty
// environment under both actions. When the agent named a different
// environment, stamp counts and logs the mismatch, and overwrites the
// agent's value only under Upsert. A nil res is left alone:
// ingest.Handler rejects such a payload before any sink sees it, but an
// Environment used without the handler must not panic on one.
func (e *Environment) stamp(res *yukonpb.ResourceAttributes, payload string) {
	if res == nil {
		return
	}
	agentEnv := res.GetEnvironment()
	if agentEnv == "" {
		res.SetEnvironment(e.cfg.Value)
		return
	}
	if agentEnv == e.cfg.Value {
		return
	}

	metrics.EnvironmentMismatch.Inc(payload)
	e.logger.Debug("agent environment does not match the collector's configured environment",
		"service", res.GetServiceName(),
		"instance", res.GetServiceInstanceId(),
		"run", res.GetRunId(),
		"payload", payload,
		"agent_environment", agentEnv,
		"collector_environment", e.cfg.Value,
		"action", e.cfg.Action.String(),
	)
	if e.cfg.Action == Upsert {
		res.SetEnvironment(e.cfg.Value)
	}
}
