package processor

import (
	"context"
	"errors"
	"testing"

	otherlodepb "buf.build/gen/go/otherlode/otherlode/protocolbuffers/go/otherlode/v1"

	"github.com/otherlodehq/otherlode-collector/metrics"
)

// resourceWithNamespace returns a valid resource. It sets the namespace
// only when ns is non-nil, so a test can send an absent one.
func resourceWithNamespace(ns *string) *otherlodepb.ResourceAttributes {
	res := &otherlodepb.ResourceAttributes{ServiceName: "svc", ServiceInstanceId: "i1", RunId: "run-1"}
	if ns != nil {
		res.SetServiceNamespace(*ns)
	}
	return res
}

func ptr(s string) *string { return &s }

// sendEach passes one payload of each kind through ns. Each payload
// carries a resource built by newRes. sendEach returns each payload's
// resource under its payload label.
func sendEach(t *testing.T, ns *Namespace, newRes func() *otherlodepb.ResourceAttributes) map[string]*otherlodepb.ResourceAttributes {
	t.Helper()
	batch := &otherlodepb.DeltaBatch{Resource: newRes()}
	manifest := &otherlodepb.ProbeManifest{Resource: newRes()}
	baseline := &otherlodepb.StaticBaseline{Resource: newRes()}
	if err := ns.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("deltas: unexpected error: %v", err)
	}
	if err := ns.AcceptManifest(context.Background(), manifest); err != nil {
		t.Fatalf("manifest: unexpected error: %v", err)
	}
	if err := ns.AcceptStaticBaseline(context.Background(), baseline); err != nil {
		t.Fatalf("static_baseline: unexpected error: %v", err)
	}
	return map[string]*otherlodepb.ResourceAttributes{
		"deltas":          batch.GetResource(),
		"manifest":        manifest.GetResource(),
		"static_baseline": baseline.GetResource(),
	}
}

func mismatchCounts() map[string]int64 {
	counts := make(map[string]int64)
	for _, payload := range []string{"deltas", "manifest", "static_baseline"} {
		counts[payload] = metrics.NamespaceMismatch.Value(payload)
	}
	return counts
}

func TestNamespace_EveryPayload_ReachesNextAsSamePointer(t *testing.T) {
	next := &recordingSink{}
	ns := NewNamespace(next, NamespaceConfig{Value: "team-a"}, nil)

	batch := &otherlodepb.DeltaBatch{Resource: resourceWithNamespace(nil)}
	manifest := &otherlodepb.ProbeManifest{Resource: resourceWithNamespace(nil)}
	baseline := &otherlodepb.StaticBaseline{Resource: resourceWithNamespace(nil)}
	_ = ns.AcceptDeltaBatch(context.Background(), batch)
	_ = ns.AcceptManifest(context.Background(), manifest)
	_ = ns.AcceptStaticBaseline(context.Background(), baseline)

	if len(next.deltaBatches) != 1 || next.deltaBatches[0] != batch {
		t.Fatalf("next did not receive the same batch pointer")
	}
	if len(next.manifests) != 1 || next.manifests[0] != manifest {
		t.Fatalf("next did not receive the same manifest pointer")
	}
	if len(next.baselines) != 1 || next.baselines[0] != baseline {
		t.Fatalf("next did not receive the same baseline pointer")
	}
}

func TestNamespace_AgentSentNone_Stamped(t *testing.T) {
	tests := map[string]*string{
		"absent":      nil,
		"empty":       ptr(""),
		"only spaces": ptr("   "),
	}
	for _, action := range []Action{Insert, Upsert} {
		for name, agent := range tests {
			t.Run(action.String()+"/"+name, func(t *testing.T) {
				before := mismatchCounts()
				ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a", Action: action}, nil)

				resources := sendEach(t, ns, func() *otherlodepb.ResourceAttributes { return resourceWithNamespace(agent) })

				for payload, res := range resources {
					if got := res.GetServiceNamespace(); got != "team-a" {
						t.Errorf("%s namespace = %q, want %q", payload, got, "team-a")
					}
				}
				after := mismatchCounts()
				for payload, n := range after {
					if d := n - before[payload]; d != 0 {
						t.Errorf("%s mismatch counter moved by %d, want 0", payload, d)
					}
				}
			})
		}
	}
}

func TestNamespace_Insert_DifferentAgentValue_KeptAndMismatchCounted(t *testing.T) {
	before := mismatchCounts()
	ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a", Action: Insert}, nil)

	resources := sendEach(t, ns, func() *otherlodepb.ResourceAttributes { return resourceWithNamespace(ptr("team-b")) })

	after := mismatchCounts()
	for payload, res := range resources {
		if got := res.GetServiceNamespace(); got != "team-b" {
			t.Errorf("%s namespace = %q, want %q (insert must not overwrite the agent's value)", payload, got, "team-b")
		}
		if d := after[payload] - before[payload]; d != 1 {
			t.Errorf("%s mismatch counter moved by %d, want 1", payload, d)
		}
	}
}

func TestNamespace_Upsert_DifferentAgentValue_OverwrittenAndMismatchCounted(t *testing.T) {
	before := mismatchCounts()
	ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a", Action: Upsert}, nil)

	resources := sendEach(t, ns, func() *otherlodepb.ResourceAttributes { return resourceWithNamespace(ptr("team-b")) })

	after := mismatchCounts()
	for payload, res := range resources {
		if got := res.GetServiceNamespace(); got != "team-a" {
			t.Errorf("%s namespace = %q, want %q (upsert must overwrite the agent's value)", payload, got, "team-a")
		}
		if d := after[payload] - before[payload]; d != 1 {
			t.Errorf("%s mismatch counter moved by %d, want 1", payload, d)
		}
	}
}

func TestNamespace_EqualAfterTrim_KeptAndNotCounted(t *testing.T) {
	for _, action := range []Action{Insert, Upsert} {
		t.Run(action.String(), func(t *testing.T) {
			before := mismatchCounts()
			ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a", Action: action}, nil)

			resources := sendEach(t, ns, func() *otherlodepb.ResourceAttributes { return resourceWithNamespace(ptr(" team-a ")) })

			after := mismatchCounts()
			for payload, res := range resources {
				if got := res.GetServiceNamespace(); got != " team-a " {
					t.Errorf("%s namespace = %q, want the agent's %q left alone", payload, got, " team-a ")
				}
				if d := after[payload] - before[payload]; d != 0 {
					t.Errorf("%s mismatch counter moved by %d, want 0", payload, d)
				}
			}
		})
	}
}

func TestNamespace_CaseDiffers_IsAMismatch(t *testing.T) {
	before := metrics.NamespaceMismatch.Value("deltas")
	ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a", Action: Upsert}, nil)

	batch := &otherlodepb.DeltaBatch{Resource: resourceWithNamespace(ptr("Team-A"))}
	if err := ns.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := batch.GetResource().GetServiceNamespace(); got != "team-a" {
		t.Fatalf("namespace = %q, want %q (case must count as a difference)", got, "team-a")
	}
	if d := metrics.NamespaceMismatch.Value("deltas") - before; d != 1 {
		t.Fatalf("mismatch counter moved by %d, want 1", d)
	}
}

func TestNamespace_MismatchNotCountedAsEnvironmentMismatch(t *testing.T) {
	before := metrics.EnvironmentMismatch.Value("deltas")
	ns := NewNamespace(&recordingSink{}, NamespaceConfig{Value: "team-a"}, nil)

	batch := &otherlodepb.DeltaBatch{Resource: resourceWithNamespace(ptr("team-b"))}
	if err := ns.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d := metrics.EnvironmentMismatch.Value("deltas") - before; d != 0 {
		t.Fatalf("environment mismatch counter moved by %d, want 0", d)
	}
}

func TestNamespace_NilResource_NoPanicReachesNext(t *testing.T) {
	next := &recordingSink{}
	ns := NewNamespace(next, NamespaceConfig{Value: "team-a"}, nil)

	batch := &otherlodepb.DeltaBatch{}
	if err := ns.AcceptDeltaBatch(context.Background(), batch); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(next.deltaBatches) != 1 || next.deltaBatches[0] != batch {
		t.Fatalf("next did not receive the same batch pointer")
	}
}

func TestNamespace_NextError_Returned(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	ns := NewNamespace(&recordingSink{err: wantErr}, NamespaceConfig{Value: "team-a"}, nil)

	for name, accept := range map[string]func() error{
		"deltas": func() error {
			return ns.AcceptDeltaBatch(context.Background(), &otherlodepb.DeltaBatch{Resource: resourceWithNamespace(nil)})
		},
		"manifest": func() error {
			return ns.AcceptManifest(context.Background(), &otherlodepb.ProbeManifest{Resource: resourceWithNamespace(nil)})
		},
		"static_baseline": func() error {
			return ns.AcceptStaticBaseline(context.Background(), &otherlodepb.StaticBaseline{Resource: resourceWithNamespace(nil)})
		},
	} {
		if err := accept(); !errors.Is(err, wantErr) {
			t.Errorf("%s error = %v, want %v", name, err, wantErr)
		}
	}
}
