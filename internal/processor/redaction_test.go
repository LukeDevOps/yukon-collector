package processor

import (
	"context"
	"errors"
	"regexp"
	"testing"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/LukeDevOps/yukon-collector/metrics"
)

func part(kind yukonpb.ConditionPartKind, text string) *yukonpb.ConditionPart {
	return &yukonpb.ConditionPart{Kind: kind, Text: text}
}

func code(text string) *yukonpb.ConditionPart {
	return part(yukonpb.ConditionPartKind_CODE, text)
}

func literal(text string) *yukonpb.ConditionPart {
	return part(yukonpb.ConditionPartKind_STRING_LITERAL, text)
}

func placeholder(text string) *yukonpb.ConditionPart {
	return part(yukonpb.ConditionPartKind_PLACEHOLDER, text)
}

// site builds a branch site with the given condition and one CASE
// outcome carrying caseLabel.
func site(condition []*yukonpb.ConditionPart, caseLabel []*yukonpb.ConditionPart) *yukonpb.BranchSite {
	return &yukonpb.BranchSite{
		Condition: condition,
		Outcomes: []*yukonpb.BranchOutcome{
			{Role: yukonpb.BranchRole_CASE, CaseLabel: caseLabel},
			{Role: yukonpb.BranchRole_DEFAULT},
		},
	}
}

func resourceFor(run string) *yukonpb.ResourceAttributes {
	return &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: run}
}

func manifestWith(sites ...*yukonpb.BranchSite) *yukonpb.ProbeManifest {
	return &yukonpb.ProbeManifest{
		Resource: resourceFor("run-1"),
		Probes:   []*yukonpb.ProbeLocation{{ClassName: "com.example.Pricing", MethodName: "price", BranchSites: sites}},
	}
}

func baselineWith(sites ...*yukonpb.BranchSite) *yukonpb.StaticBaseline {
	return &yukonpb.StaticBaseline{
		Resource:   resourceFor("run-1"),
		ScannedAt:  1700000000,
		ChunkCount: 1,
		DeclaredClasses: []*yukonpb.DeclaredClass{{
			ClassName: "com.example.Pricing",
			Methods:   []*yukonpb.DeclaredMethod{{MethodName: "price", BranchSites: sites}},
		}},
	}
}

func blocked(t *testing.T, patterns ...string) RedactionConfig {
	t.Helper()
	cfg := RedactionConfig{}
	for _, p := range patterns {
		cfg.BlockedValues = append(cfg.BlockedValues, regexp.MustCompile(p))
	}
	return cfg
}

func texts(parts []*yukonpb.ConditionPart) []string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = p.GetText()
	}
	return out
}

func assertTexts(t *testing.T, what string, parts []*yukonpb.ConditionPart, want ...string) {
	t.Helper()
	got := texts(parts)
	if len(got) != len(want) {
		t.Fatalf("%s = %q, want %q", what, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s = %q, want %q", what, got, want)
		}
	}
}

func TestRedaction_BlockedPatternMatchesLiteral_Replaced(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "LEGACY"), nil)

	manifest := manifestWith(site(
		[]*yukonpb.ConditionPart{code("System.getenv("), literal("ENABLE_LEGACY_DISCOUNT"), code(") == "), literal("true")},
		nil,
	))
	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := next.manifests[0].GetProbes()[0].GetBranchSites()[0].GetCondition()
	assertTexts(t, "condition", got, "System.getenv(", "…", ") == ", "true")
	if kind := got[1].GetKind(); kind != yukonpb.ConditionPartKind_STRING_LITERAL {
		t.Fatalf("redacted part kind = %v, want STRING_LITERAL", kind)
	}
}

func TestRedaction_NoPatternMatchesLiteral_Kept(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "^secret$", "password"), nil)

	manifest := manifestWith(site(
		[]*yukonpb.ConditionPart{code("mode == "), literal("fast")},
		[]*yukonpb.ConditionPart{literal("slow")},
	))
	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := next.manifests[0].GetProbes()[0].GetBranchSites()[0]
	assertTexts(t, "condition", s.GetCondition(), "mode == ", "fast")
	assertTexts(t, "case label", s.GetOutcomes()[0].GetCaseLabel(), "slow")
}

func TestRedaction_CodeAndPlaceholderParts_NeverTouched(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "token"), nil)

	manifest := manifestWith(site(
		[]*yukonpb.ConditionPart{code("token.isEmpty() && "), placeholder("token"), code(" == "), literal("token-1")},
		[]*yukonpb.ConditionPart{code("Token.NONE")},
	))
	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := next.manifests[0].GetProbes()[0].GetBranchSites()[0]
	assertTexts(t, "condition", s.GetCondition(), "token.isEmpty() && ", "token", " == ", "…")
	assertTexts(t, "case label", s.GetOutcomes()[0].GetCaseLabel(), "Token.NONE")
}

func TestRedaction_BlockedPatternMatchesUnanchored(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "[0-9]{4}"), nil)

	baseline := baselineWith(site([]*yukonpb.ConditionPart{literal("card 4111 1111")}, nil))
	if err := r.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := next.baselines[0].GetDeclaredClasses()[0].GetMethods()[0].GetBranchSites()[0].GetCondition()
	assertTexts(t, "condition", got, "…")
}

func TestRedaction_AllLiterals_ManifestConditionsAndCaseLabelsReplaced(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, RedactionConfig{AllLiterals: true}, nil)

	manifest := manifestWith(
		site([]*yukonpb.ConditionPart{code("name == "), literal("alice")}, []*yukonpb.ConditionPart{literal("bob")}),
		site([]*yukonpb.ConditionPart{code("x > 0")}, []*yukonpb.ConditionPart{code("Color.RED")}),
	)
	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sites := next.manifests[0].GetProbes()[0].GetBranchSites()
	assertTexts(t, "first condition", sites[0].GetCondition(), "name == ", "…")
	assertTexts(t, "first case label", sites[0].GetOutcomes()[0].GetCaseLabel(), "…")
	assertTexts(t, "second condition", sites[1].GetCondition(), "x > 0")
	assertTexts(t, "second case label", sites[1].GetOutcomes()[0].GetCaseLabel(), "Color.RED")
}

func TestRedaction_AllLiterals_StaticBaselineConditionsAndCaseLabelsReplaced(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, RedactionConfig{AllLiterals: true}, nil)

	baseline := baselineWith(site(
		[]*yukonpb.ConditionPart{code("System.getenv("), literal("MODE"), code(")")},
		[]*yukonpb.ConditionPart{literal("legacy")},
	))
	if err := r.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s := next.baselines[0].GetDeclaredClasses()[0].GetMethods()[0].GetBranchSites()[0]
	assertTexts(t, "condition", s.GetCondition(), "System.getenv(", "…", ")")
	assertTexts(t, "case label", s.GetOutcomes()[0].GetCaseLabel(), "…")
}

func TestRedaction_Counter_CountsReplacedPartsPerPayload(t *testing.T) {
	beforeManifest := metrics.RedactedLiterals.Value("manifest")
	beforeBaseline := metrics.RedactedLiterals.Value("static_baseline")

	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "secret"), nil)

	manifest := manifestWith(site(
		[]*yukonpb.ConditionPart{literal("secret-a"), code(" + "), literal("secret-b"), literal("public")},
		[]*yukonpb.ConditionPart{literal("top-secret")},
	))
	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	baseline := baselineWith(site([]*yukonpb.ConditionPart{literal("secret-c"), code("secret")}, nil))
	if err := r.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := metrics.RedactedLiterals.Value("manifest") - beforeManifest; got != 3 {
		t.Fatalf("manifest counter increased by %d, want 3", got)
	}
	if got := metrics.RedactedLiterals.Value("static_baseline") - beforeBaseline; got != 1 {
		t.Fatalf("static_baseline counter increased by %d, want 1", got)
	}
}

// unknownBytes is one field that no message in the schema declares.
func unknownBytes() []byte {
	b := protowire.AppendTag(nil, 9999, protowire.BytesType)
	return protowire.AppendString(b, "a newer literal")
}

func TestRedaction_On_UnknownFieldsDroppedFromManifest(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, blocked(t, "never-matches"), nil)

	s := site([]*yukonpb.ConditionPart{code("ok")}, []*yukonpb.ConditionPart{literal("x")})
	s.ProtoReflect().SetUnknown(unknownBytes())
	s.GetOutcomes()[0].GetCaseLabel()[0].ProtoReflect().SetUnknown(unknownBytes())
	manifest := manifestWith(s)
	manifest.ProtoReflect().SetUnknown(unknownBytes())

	if err := r.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := next.manifests[0]
	if n := len(got.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("manifest still has %d unknown bytes", n)
	}
	gotSite := got.GetProbes()[0].GetBranchSites()[0]
	if n := len(gotSite.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("branch site still has %d unknown bytes", n)
	}
	if n := len(gotSite.GetOutcomes()[0].GetCaseLabel()[0].ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("case label part still has %d unknown bytes", n)
	}
}

func TestRedaction_On_UnknownFieldsDroppedFromStaticBaseline(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, RedactionConfig{AllLiterals: true}, nil)

	s := site([]*yukonpb.ConditionPart{code("ok")}, nil)
	s.ProtoReflect().SetUnknown(unknownBytes())
	baseline := baselineWith(s)
	baseline.ProtoReflect().SetUnknown(unknownBytes())

	if err := r.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := next.baselines[0]
	if n := len(got.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("baseline still has %d unknown bytes", n)
	}
	gotSite := got.GetDeclaredClasses()[0].GetMethods()[0].GetBranchSites()[0]
	if n := len(gotSite.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("branch site still has %d unknown bytes", n)
	}
}

func TestRedaction_On_UnknownFieldsDroppedFromDeltaBatch(t *testing.T) {
	next := &recordingSink{}
	r := NewRedaction(next, RedactionConfig{AllLiterals: true}, nil)

	delta := &yukonpb.ProbeDelta{ClassId: 7, HitsTotal: 3}
	delta.ProtoReflect().SetUnknown(unknownBytes())
	batch := &yukonpb.DeltaBatch{Resource: resourceFor("run-1"), Deltas: []*yukonpb.ProbeDelta{delta}}
	batch.GetResource().ProtoReflect().SetUnknown(unknownBytes())
	batch.ProtoReflect().SetUnknown(unknownBytes())

	if err := r.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := next.deltaBatches[0]
	if n := len(got.ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("batch still has %d unknown bytes", n)
	}
	if n := len(got.GetResource().ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("resource still has %d unknown bytes", n)
	}
	if n := len(got.GetDeltas()[0].ProtoReflect().GetUnknown()); n != 0 {
		t.Fatalf("delta still has %d unknown bytes", n)
	}
	if got.GetDeltas()[0].GetHitsTotal() != 3 {
		t.Fatalf("known field lost: hits_total = %d, want 3", got.GetDeltas()[0].GetHitsTotal())
	}
}

func TestRedaction_NextError_Returned(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	next := &recordingSink{err: wantErr}
	r := NewRedaction(next, RedactionConfig{AllLiterals: true}, nil)

	if err := r.AcceptManifest(context.Background(), manifestWith()); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestRedactionConfig_Enabled(t *testing.T) {
	if (RedactionConfig{}).Enabled() {
		t.Fatal("zero config reports enabled, want off")
	}
	if !(RedactionConfig{AllLiterals: true}).Enabled() {
		t.Fatal("AllLiterals config reports off, want enabled")
	}
	if !blocked(t, "x").Enabled() {
		t.Fatal("config with a blocked pattern reports off, want enabled")
	}
}
