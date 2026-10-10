//go:build darwin || linux

package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInventoryStateSamplerImmutableScopeAndClosedWriter(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	sampler, err := w.InventoryStateBudgetSampler()
	if err != nil {
		w.Close()
		t.Fatal(err)
	}
	copy := sampler
	another, err := w.InventoryStateBudgetSampler()
	if err != nil || !sampler.SameScope(copy) || sampler.SameScope(another) || sampler.SameScope(InventoryStateBudgetSampler{}) {
		w.Close()
		t.Fatal("capability identities were not distinct", err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	// The sampler cannot retain the writer connection or its lock.
	reopened, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal("sampler retained closed writer authority", err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
	observed, err := sampler.Observe(ctx, MaxInventoryStateBytes)
	if err != nil || !observed.Available || observed.Status != "below_limit" || observed.TotalBytes == nil || *observed.TotalBytes <= 0 {
		t.Fatal("closed writer sampler unavailable", observed, err)
	}
	if observed.HardLimitEnforced || observed.NamespaceAuthenticated || observed.PhysicalAllocationVerified || observed.OtherStoresIncluded {
		t.Fatal("metadata capability overstated authority", observed)
	}
}

func TestInventoryStateSamplerCopiesPathAndPreservesOldUnavailable(t *testing.T) {
	s := stateBudgetNamedFixture(t, 41, -1)
	original := s.path
	sampler, err := s.InventoryStateBudgetSampler()
	if err != nil {
		t.Fatal(err)
	}
	s.path = filepath.Join(filepath.Dir(original), "different-uncreated-name")
	r, err := sampler.Observe(context.Background(), MinInventoryStateBytes)
	if err != nil || r.TotalBytes == nil || *r.TotalBytes != 41 || r.WALBytes == nil || *r.WALBytes != 0 {
		t.Fatal("sampler followed mutable Store", r, err)
	}
	if err = os.WriteFile(original+"-wal", []byte("generated"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = sampler.Observe(context.Background(), MinInventoryStateBytes)
	if err != nil || r.TotalBytes == nil || *r.TotalBytes != 50 {
		t.Fatal("sampler reused a historical length", r, err)
	}
	var unavailable *Store
	old, err := unavailable.InventoryStateBudget(context.Background(), MinInventoryStateBytes)
	if err != nil || old.Available || old.Status != "unavailable" || old.Reason != "inventory_state_unavailable" {
		t.Fatal("old nil-store report changed", old, err)
	}
	if _, err = unavailable.InventoryStateBudgetSampler(); !errors.Is(err, ErrInventoryStateBudgetInput) {
		t.Fatal("nil sampler construction accepted", err)
	}
}

func TestInventoryStateSamplerInvalidAndCanceledNoMetadata(t *testing.T) {
	for _, path := range []string{"", "relative", "/a/../b", "/nul\x00name", "/" + strings.Repeat("x", 4096)} {
		if _, err := (&Store{path: path}).InventoryStateBudgetSampler(); !errors.Is(err, ErrInventoryStateBudgetInput) {
			t.Fatal("invalid sampler path accepted", err)
		}
	}
	if _, err := (InventoryStateBudgetSampler{}).Observe(context.Background(), MinInventoryStateBytes); !errors.Is(err, ErrInventoryStateBudgetInput) {
		t.Fatal("zero sampler accepted", err)
	}
	s := stateBudgetNamedFixture(t, 17, -1)
	sampler, err := s.InventoryStateBudgetSampler()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = sampler.Observe(ctx, MinInventoryStateBytes); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled immutable observation accepted", err)
	}
	if _, err = sampler.Observe(nil, MinInventoryStateBytes); !errors.Is(err, ErrInventoryStateBudgetInput) {
		t.Fatal("nil observation context accepted", err)
	}
}
