package cli

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var errReviewSelection = errors.New("select 1–20 unique row numbers shown on this page")

// reviewPage keeps numbered choices bound to the evidence displayed before
// input. The discovery cursor describes a later page, not a frozen inventory.
type reviewPage struct {
	Selection  state.SelectionSnapshot
	Evidence   state.FindingReport
	NextCursor string
}

func loadReviewPage(ctx context.Context, inventoryBase, cursor string, minAge int) (reviewPage, error) {
	s, err := state.OpenReader(ctx, inventoryBase)
	if err != nil {
		return reviewPage{}, err
	}
	defer s.Close()
	discovery, err := s.NodeModulesFindings(ctx, cursor, minAge)
	if err != nil {
		return reviewPage{}, err
	}
	p := reviewPage{Evidence: discovery, NextCursor: discovery.NextCursor}
	if len(discovery.Findings) == 0 {
		return p, nil
	}
	ids := make([]string, len(discovery.Findings))
	for i, finding := range discovery.Findings {
		ids[i] = finding.ID
	}
	// Discovery combines separate measurement snapshots. Freeze the exact rows
	// and all bindings in one inventory snapshot before any numbered display.
	p.Selection, err = s.SnapshotSelection(ctx, ids, minAge)
	if err != nil {
		return reviewPage{}, err
	}
	p.Evidence = p.Selection.Evidence
	return p, nil
}

func selectReviewPage(page reviewPage, numbers []int) (state.SelectionSnapshot, error) {
	full := page.Selection
	if len(numbers) < 1 || len(numbers) > state.PreviewTargetLimit || len(full.Targets) != len(full.Evidence.Findings) ||
		len(full.Targets) > state.PreviewTargetLimit || !reflect.DeepEqual(page.Evidence.Findings, full.Evidence.Findings) {
		return state.SelectionSnapshot{}, errReviewSelection
	}
	selected := make(map[int]bool, len(numbers))
	for _, number := range numbers {
		if number < 1 || number > len(full.Evidence.Findings) || selected[number] {
			return state.SelectionSnapshot{}, errReviewSelection
		}
		selected[number] = true
	}
	r := state.SelectionSnapshot{InventoryID: full.InventoryID, Evidence: full.Evidence}
	r.Evidence.Findings = nil
	r.Evidence.NextCursor = ""
	r.Evidence.PageCoverage = "selected_entries_only"
	r.Evidence.EntryLimit = len(numbers)
	r.Evidence.EntriesExamined = len(numbers)
	r.Evidence.Diagnostics = slices.Clone(full.Evidence.Diagnostics)
	for i := range r.Evidence.Diagnostics {
		r.Evidence.Diagnostics[i].Count = 0
		if r.Evidence.Diagnostics[i].Code == "selected" {
			r.Evidence.Diagnostics[i].Count = len(numbers)
		}
	}
	r.Evidence.Notes = slices.Clone(full.Evidence.Notes)
	for i, note := range r.Evidence.Notes {
		if strings.HasPrefix(note, "Selection diagnostics count only this page,") {
			r.Evidence.Notes[i] = "Selection diagnostics cover only the explicitly selected saved findings. Other saved entries are not counted."
		}
	}
	rootByID := make(map[int64]state.RootBinding, len(full.Roots))
	for _, root := range full.Roots {
		if _, exists := rootByID[root.ID]; exists {
			return state.SelectionSnapshot{}, errReviewSelection
		}
		rootByID[root.ID] = root
	}
	seenRoots := make(map[int64]bool, len(full.Roots))
	for i, finding := range full.Evidence.Findings {
		if !selected[i+1] {
			continue
		}
		if full.Targets[i].FindingID != finding.ID {
			return state.SelectionSnapshot{}, errReviewSelection
		}
		r.Evidence.Findings = append(r.Evidence.Findings, cloneReviewFinding(finding))
		r.Targets = append(r.Targets, full.Targets[i])
		if !seenRoots[finding.RootID] {
			root, exists := rootByID[finding.RootID]
			if !exists {
				return state.SelectionSnapshot{}, errReviewSelection
			}
			root.PathBytes = slices.Clone(root.PathBytes)
			r.Roots = append(r.Roots, root)
			seenRoots[finding.RootID] = true
		}
	}
	return r, nil
}

func cloneReviewFinding(f state.Finding) state.Finding {
	f.PathBytes = slices.Clone(f.PathBytes)
	f.ManifestPathBytes = slices.Clone(f.ManifestPathBytes)
	f.Actions = slices.Clone(f.Actions)
	f.Measurement.PathBytes = slices.Clone(f.Measurement.PathBytes)
	f.Measurement.Notes = slices.Clone(f.Measurement.Notes)
	if f.Measurement.LogicalBytes != nil {
		value := *f.Measurement.LogicalBytes
		f.Measurement.LogicalBytes = &value
	}
	if f.Measurement.AllocatedBytes != nil {
		value := *f.Measurement.AllocatedBytes
		f.Measurement.AllocatedBytes = &value
	}
	if f.Measurement.OldestObservation != nil {
		value := *f.Measurement.OldestObservation
		f.Measurement.OldestObservation = &value
	}
	if f.Measurement.NewestObservation != nil {
		value := *f.Measurement.NewestObservation
		f.Measurement.NewestObservation = &value
	}
	return f
}
