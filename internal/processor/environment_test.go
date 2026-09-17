package processor

import (
	"context"
	"errors"
	"testing"

	yukonpb "buf.build/gen/go/lukedevops-oss/yukon/protocolbuffers/go"

	"github.com/LukeDevOps/yukon-collector/metrics"
)

// recordingSink records every payload it receives, standing in for the
// real sink an Environment processor wraps.
type recordingSink struct {
	deltaBatches []*yukonpb.DeltaBatch
	manifests    []*yukonpb.ProbeManifest
	baselines    []*yukonpb.StaticBaseline
	err          error
}

func (s *recordingSink) AcceptDeltaBatch(_ context.Context, batch *yukonpb.DeltaBatch) error {
	s.deltaBatches = append(s.deltaBatches, batch)
	return s.err
}

func (s *recordingSink) AcceptManifest(_ context.Context, manifest *yukonpb.ProbeManifest) error {
	s.manifests = append(s.manifests, manifest)
	return s.err
}

func (s *recordingSink) AcceptStaticBaseline(_ context.Context, baseline *yukonpb.StaticBaseline) error {
	s.baselines = append(s.baselines, baseline)
	return s.err
}

func TestEnvironment_AcceptDeltaBatch_AbsentEnvironment_Stamped(t *testing.T) {
	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	batch := &yukonpb.DeltaBatch{Resource: &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}}
	if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := batch.GetResource().GetEnvironment(); got != "prod" {
		t.Fatalf("environment = %q, want %q", got, "prod")
	}
	if len(next.deltaBatches) != 1 || next.deltaBatches[0] != batch {
		t.Fatalf("next did not receive the same batch pointer")
	}
}

func TestEnvironment_AcceptStaticBaseline_AbsentEnvironment_Stamped(t *testing.T) {
	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	baseline := &yukonpb.StaticBaseline{Resource: &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}}
	if err := env.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := baseline.GetResource().GetEnvironment(); got != "prod" {
		t.Fatalf("environment = %q, want %q", got, "prod")
	}
	if len(next.baselines) != 1 || next.baselines[0] != baseline {
		t.Fatalf("next did not receive the same baseline pointer")
	}
}

func TestEnvironment_Insert_ExplicitEmptyEnvironment_Stamped(t *testing.T) {
	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	res := &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}
	res.SetEnvironment("")
	batch := &yukonpb.DeltaBatch{Resource: res}

	if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := batch.GetResource().GetEnvironment(); got != "prod" {
		t.Fatalf("environment = %q, want %q", got, "prod")
	}
}

func TestEnvironment_Insert_DifferentAgentValue_KeptAndMismatchCounted(t *testing.T) {
	before := metrics.EnvironmentMismatch.Value("deltas")

	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	res := &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}
	res.SetEnvironment("uat")
	batch := &yukonpb.DeltaBatch{Resource: res}

	if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := batch.GetResource().GetEnvironment(); got != "uat" {
		t.Fatalf("environment = %q, want %q (insert must not overwrite the agent's value)", got, "uat")
	}
	if got := metrics.EnvironmentMismatch.Value("deltas") - before; got != 1 {
		t.Fatalf("mismatch counter increased by %d, want 1", got)
	}
}

func TestEnvironment_Insert_DifferentAgentValue_StaticBaselineMismatchCounted(t *testing.T) {
	before := metrics.EnvironmentMismatch.Value("static_baseline")

	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	res := &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}
	res.SetEnvironment("uat")
	baseline := &yukonpb.StaticBaseline{Resource: res}

	if err := env.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := metrics.EnvironmentMismatch.Value("static_baseline") - before; got != 1 {
		t.Fatalf("mismatch counter increased by %d, want 1", got)
	}
}

func TestEnvironment_EqualValue_CounterUnchanged(t *testing.T) {
	for _, action := range []Action{Insert, Upsert} {
		t.Run(action.String(), func(t *testing.T) {
			before := metrics.EnvironmentMismatch.Value("deltas")

			next := &recordingSink{}
			env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: action}, nil)

			res := &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}
			res.SetEnvironment("prod")
			batch := &yukonpb.DeltaBatch{Resource: res}

			if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := batch.GetResource().GetEnvironment(); got != "prod" {
				t.Fatalf("environment = %q, want %q", got, "prod")
			}
			if got := metrics.EnvironmentMismatch.Value("deltas") - before; got != 0 {
				t.Fatalf("mismatch counter increased by %d, want 0", got)
			}
		})
	}
}

func TestEnvironment_Upsert_DifferentAgentValue_OverwrittenAndMismatchCounted(t *testing.T) {
	before := metrics.EnvironmentMismatch.Value("deltas")

	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Upsert}, nil)

	res := &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}
	res.SetEnvironment("uat")
	batch := &yukonpb.DeltaBatch{Resource: res}

	if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := batch.GetResource().GetEnvironment(); got != "prod" {
		t.Fatalf("environment = %q, want %q (upsert must overwrite the agent's value)", got, "prod")
	}
	if got := metrics.EnvironmentMismatch.Value("deltas") - before; got != 1 {
		t.Fatalf("mismatch counter increased by %d, want 1", got)
	}
}

func TestEnvironment_AcceptManifest_ReachesNextUnchanged(t *testing.T) {
	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	manifest := &yukonpb.ProbeManifest{ServiceName: "svc", ServiceInstanceId: "i1"}
	if err := env.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.manifests) != 1 || next.manifests[0] != manifest {
		t.Fatalf("next did not receive the same manifest pointer")
	}
}

func TestEnvironment_NilResource_NoPanicReachesNext(t *testing.T) {
	next := &recordingSink{}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	batch := &yukonpb.DeltaBatch{}
	if err := env.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.deltaBatches) != 1 || next.deltaBatches[0] != batch {
		t.Fatalf("next did not receive the same batch pointer")
	}
}

func TestEnvironment_NextError_Returned(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	next := &recordingSink{err: wantErr}
	env := NewEnvironment(next, EnvironmentConfig{Value: "prod", Action: Insert}, nil)

	batch := &yukonpb.DeltaBatch{Resource: &yukonpb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1"}}
	if err := env.AcceptDeltaBatch(context.Background(), batch); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestParseAction(t *testing.T) {
	tests := map[string]Action{
		"":       Insert,
		"insert": Insert,
		"INSERT": Insert,
		"upsert": Upsert,
		"Upsert": Upsert,
	}
	for raw, want := range tests {
		got, err := ParseAction(raw)
		if err != nil {
			t.Errorf("ParseAction(%q): unexpected error: %v", raw, err)
			continue
		}
		if got != want {
			t.Errorf("ParseAction(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestParseAction_BadValue_Errors(t *testing.T) {
	if _, err := ParseAction("overwrite"); err == nil {
		t.Fatal("expected an error for an unknown action, got nil")
	}
}
