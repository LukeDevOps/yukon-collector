package processor

import (
	"context"
	"log/slog"
	"regexp"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/LukeDevOps/yukon-collector/ingest"
	"github.com/LukeDevOps/yukon-collector/metrics"
)

// RedactedText is the text a redacted literal part carries.
const RedactedText = "…"

// RedactionConfig configures a Redaction processor. A string literal part
// is redacted when AllLiterals is true or any pattern in BlockedValues
// matches some of its text. A zero RedactionConfig means the processor is
// off.
type RedactionConfig struct {
	BlockedValues []*regexp.Regexp
	AllLiterals   bool
}

// Enabled reports whether the config redacts anything.
func (c RedactionConfig) Enabled() bool {
	return c.AllLiterals || len(c.BlockedValues) > 0
}

// Redaction is an ingest.Sink that hides string literals before a payload
// leaves the collector, then passes the payload to next. See ADR 0001.
//
// It looks only at ConditionPart values of kind STRING_LITERAL. These sit
// in each branch site's condition and in each outcome's case label, on
// manifest probes and on static baseline methods. A redacted part keeps
// its kind, and its text becomes RedactedText. Code parts, placeholder
// parts, names and files pass through unchanged.
//
// It also drops unknown fields from every message of every payload. A
// field the collector's bindings do not know could carry a literal that
// this processor cannot see.
//
// Like Environment, it changes the decoded message in place.
type Redaction struct {
	next   ingest.Sink
	cfg    RedactionConfig
	logger *slog.Logger
}

var _ ingest.Sink = (*Redaction)(nil)

// NewRedaction returns a Redaction that applies cfg before passing
// payloads to next, logging to logger, or slog.Default() when logger is
// nil.
func NewRedaction(next ingest.Sink, cfg RedactionConfig, logger *slog.Logger) *Redaction {
	if logger == nil {
		logger = slog.Default()
	}
	return &Redaction{next: next, cfg: cfg, logger: logger}
}

// AcceptDeltaBatch drops the batch's unknown fields, then passes the
// batch to next. A delta batch holds no condition parts.
func (r *Redaction) AcceptDeltaBatch(ctx context.Context, batch *yukonpb.DeltaBatch) error {
	dropUnknown(batch.ProtoReflect())
	return r.next.AcceptDeltaBatch(ctx, batch)
}

// AcceptManifest redacts the literals in the branch sites of every probe,
// drops unknown fields, then passes the manifest to next.
func (r *Redaction) AcceptManifest(ctx context.Context, manifest *yukonpb.ProbeManifest) error {
	n := 0
	for _, probe := range manifest.GetProbes() {
		n += r.redactSites(probe.GetBranchSites())
	}
	r.record(manifest.GetResource(), "manifest", n)
	dropUnknown(manifest.ProtoReflect())
	return r.next.AcceptManifest(ctx, manifest)
}

// AcceptStaticBaseline redacts the literals in the branch sites of every
// declared method, drops unknown fields, then passes the baseline to next.
func (r *Redaction) AcceptStaticBaseline(ctx context.Context, baseline *yukonpb.StaticBaseline) error {
	n := 0
	for _, class := range baseline.GetDeclaredClasses() {
		for _, method := range class.GetMethods() {
			n += r.redactSites(method.GetBranchSites())
		}
	}
	r.record(baseline.GetResource(), "static_baseline", n)
	dropUnknown(baseline.ProtoReflect())
	return r.next.AcceptStaticBaseline(ctx, baseline)
}

// redactSites redacts each site's condition and each of its outcomes'
// case labels. It returns the number of parts it replaced.
func (r *Redaction) redactSites(sites []*yukonpb.BranchSite) int {
	n := 0
	for _, site := range sites {
		n += r.redactParts(site.GetCondition())
		for _, outcome := range site.GetOutcomes() {
			n += r.redactParts(outcome.GetCaseLabel())
		}
	}
	return n
}

// redactParts replaces the text of each literal part that the config
// blocks. It returns the number of parts it replaced.
func (r *Redaction) redactParts(parts []*yukonpb.ConditionPart) int {
	n := 0
	for _, p := range parts {
		if p.GetKind() != yukonpb.ConditionPartKind_STRING_LITERAL {
			continue
		}
		if r.blocks(p.GetText()) {
			p.SetText(RedactedText)
			n++
		}
	}
	return n
}

// blocks reports whether text must be redacted.
func (r *Redaction) blocks(text string) bool {
	if r.cfg.AllLiterals {
		return true
	}
	for _, re := range r.cfg.BlockedValues {
		if re.MatchString(text) {
			return true
		}
	}
	return false
}

// record counts n replaced parts and logs them once for the payload. It
// never logs a literal's text.
func (r *Redaction) record(res *yukonpb.ResourceAttributes, payload string, n int) {
	if n == 0 {
		return
	}
	metrics.RedactedLiterals.Add(int64(n), payload)
	r.logger.Debug("redacted string literals",
		"service", res.GetServiceName(),
		"instance", res.GetServiceInstanceId(),
		"run", res.GetRunId(),
		"payload", payload,
		"redacted", n,
	)
}

// dropUnknown clears the unknown fields of m and of every message
// nested in it, through singular, repeated and map fields.
func dropUnknown(m protoreflect.Message) {
	if len(m.GetUnknown()) > 0 {
		m.SetUnknown(nil)
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsMap():
			if fd.MapValue().Message() != nil {
				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					dropUnknown(mv.Message())
					return true
				})
			}
		case fd.IsList():
			if fd.Message() != nil {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					dropUnknown(list.Get(i).Message())
				}
			}
		case fd.Message() != nil:
			dropUnknown(v.Message())
		}
		return true
	})
}
