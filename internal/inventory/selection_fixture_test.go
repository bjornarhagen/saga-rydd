package inventory

import (
	"context"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Fresh production fixtures finish saved reconciliation after source queue EOF.
// Exact file selections must retain their refusal while that work is pending.
// This helper does not call the scanner or elide any lifecycle evidence.
func finishSelectionFixtureMaintenance(t *testing.T, ctx context.Context, store *state.Store, scanner *Scanner) {
	t.Helper()
	before, err := store.Summary(ctx)
	if err != nil || before.PendingJobs != 0 || before.RunningJobs != 0 {
		t.Fatal("fixture maintenance started before source queue EOF", before, err)
	}
	metrics := scanner.Metrics()
	for step := 0; step < 1024; step++ {
		worked, err := store.RetireSubtrees(ctx)
		if err != nil {
			t.Fatal("fixture saved reconciliation failed", err)
		}
		if worked {
			continue
		}
		pending, err := store.HasSubtreeRetirement(ctx)
		if err != nil || pending {
			t.Fatal("fixture selection retained unfinished reconciliation", pending, err)
		}
		after, err := store.Summary(ctx)
		if err != nil || before.Entries != after.Entries || after.PendingJobs != 0 || after.RunningJobs != 0 || metrics != scanner.Metrics() {
			t.Fatal("fresh fixture maintenance changed source observations or called the scanner", before, after, err)
		}
		return
	}
	t.Fatal("fixture saved reconciliation exceeded its bounded setup steps")
}
