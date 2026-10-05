package state

import (
	"context"
	"errors"
	"testing"
)

func TestPrepareLiveSelection(t *testing.T) {
	s, saved := checkFixture(t)
	ctx := context.Background()
	check, targets, err := s.PrepareLiveSelection(ctx, saved)
	if err != nil || check.Status != "matches_saved_inventory" || len(targets) != 1 || len(targets[0].Ancestors) != 2 || string(targets[0].Ancestors[0].Path) != "." || string(targets[0].Ancestors[1].Path) != "a" {
		t.Fatal(check, targets, err)
	}
	if _, err = s.db.Exec("UPDATE entries SET ctime_ns=0 WHERE path=X'61'"); err != nil {
		t.Fatal(err)
	}
	check, targets, err = s.PrepareLiveSelection(ctx, saved)
	if err != nil || check.Status != "unverifiable" || len(targets) != 0 || !hasIssue(check, "ancestor_evidence_unknown") {
		t.Fatal(check, targets, err)
	}
	if _, err = s.db.Exec("UPDATE roots SET enabled=0"); err != nil {
		t.Fatal(err)
	}
	check, targets, err = s.PrepareLiveSelection(ctx, saved)
	if err != nil || check.Status != "changed" || len(targets) != 0 {
		t.Fatal(check, targets, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err = s.PrepareLiveSelection(canceled, saved); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err = s.PrepareLiveSelection(ctx, SelectionSnapshot{}); !errors.Is(err, ErrFindingSelection) {
		t.Fatal(err)
	}
}
