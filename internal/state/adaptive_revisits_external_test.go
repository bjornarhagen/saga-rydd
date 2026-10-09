package state_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// A native scanner supplies real metadata for >128 names and a deep child.
// The clock is a library scheduling input; no host clock changes are made.
func TestAdaptiveRevisitGeneratedNativeWideDeepEpochs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base := t.TempDir()
	root := filepath.Join(base, "source")
	private := filepath.Join(base, "private")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	deep := filepath.Join(root, "deep")
	if err := os.Mkdir(deep, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 131; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), []byte("generated"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(deep, "file")
	if err := os.WriteFile(file, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := state.OpenWriter(ctx, private)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	roots, err := s.ResolveFairInventoryRoots(ctx, []string{root})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := state.AdaptiveRevisitScopeDigest([]string{root}, nil, []string{private})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	scope, err := s.ConfigureAdaptiveRevisits(ctx, roots, true, digest, now)
	if err != nil {
		t.Fatal(err)
	}
	ids := scope.RootIDs()
	if len(ids) != 1 {
		t.Fatal(ids)
	}
	rootID := ids[0]
	first, err := s.FinalizeAdaptiveRevisit(ctx, scope, rootID, now)
	if err != nil || !first.Scheduled || !first.Initialized {
		t.Fatal(first, err)
	}
	var result state.AdaptiveRevisitResult
	for pass := 0; pass < 5; pass++ {
		if pass == 3 {
			if err = os.WriteFile(file, []byte("edited longer"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if pass == 4 {
			if err = os.Remove(filepath.Join(root, "file-000")); err != nil {
				t.Fatal(err)
			}
		}
		scanner, e := inventory.New([]string{root}, nil, []string{private}, inventory.WithEntryRate(100000))
		if e != nil {
			t.Fatal(e)
		}
		chunks, partial := 0, false
		for {
			j, e := s.ClaimJob(ctx, []string{state.ScanKind}, now, time.Hour)
			if e != nil {
				scanner.Close()
				t.Fatal(e)
			}
			if j == nil {
				break
			}
			batch, e := scanner.Next(ctx, *j)
			if e != nil || batch.Fault != "" {
				scanner.Close()
				t.Fatal("generated source failure", e, batch.Fault)
			}
			if !batch.Complete {
				partial = true
			}
			chunks++
			if e = s.CommitAdaptiveScan(ctx, scope, *j, batch, now); e != nil {
				scanner.Close()
				t.Fatal(e)
			}
		}
		scanner.Close()
		if !partial || chunks < 3 {
			t.Fatal("wide fixture did not exercise continuation", chunks, partial)
		}
		for turn := 0; turn < 1000; turn++ {
			step, e := s.RetireInventoryForRoot(ctx, rootID)
			if e != nil {
				t.Fatal(e)
			}
			if !step.Remaining {
				break
			}
			if !step.Worked || turn == 999 {
				t.Fatal("generated maintenance did not drain", step)
			}
		}
		result, err = s.FinalizeAdaptiveRevisit(ctx, scope, rootID, now)
		if err != nil || !result.Scheduled {
			t.Fatal(result, err)
		}
		switch pass {
		case 0:
			if !result.Unknown || !result.Changed {
				t.Fatal("first scope was treated as learned", result)
			}
		case 1:
			if result.Unknown || result.Changed || result.UnchangedStreak != 1 {
				t.Fatal("real unchanged metadata did not learn", result)
			}
		case 2:
			if result.Unknown || result.Changed || result.Interval != state.AdaptiveStableRevisitInterval {
				t.Fatal(result)
			}
		case 3, 4:
			if result.Unknown || !result.Changed || result.Interval != state.InventoryRevisitInterval {
				t.Fatal("real deep edit/removal did not reset", result)
			}
		}
		now = result.Due
	}
}
