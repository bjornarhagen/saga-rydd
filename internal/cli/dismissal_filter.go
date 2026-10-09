package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Filtering never fills an empty page from later inventory rows. The snapshot's
// cursor, raw counts and measurements retain their original bounded scope.
func filterDismissalPage(ctx context.Context, base, root string, page state.SelectionSnapshot, include bool) (state.SelectionSnapshot, error) {
	selections := make([]state.SelectionSnapshot, len(page.Evidence.Findings))
	for i, finding := range page.Evidence.Findings {
		selection, err := state.SingleDismissalSelection(page, finding.ID)
		if err != nil && !errors.Is(err, state.ErrDismissalSelection) {
			return state.SelectionSnapshot{}, err
		}
		// Unknown identities cannot match a dismissal, but remain reportable.
		// Retain any known bindings for private-storage alias checks even when
		// another part of this finding cannot support an exact dismissal.
		if err != nil && i < len(page.Targets) {
			selection.Targets = []state.TargetBinding{page.Targets[i]}
		}
		selections[i] = selection
	}
	dismissed, err := plans.DismissedFindings(ctx, base, []byte(root), selections)
	if err != nil {
		return state.SelectionSnapshot{}, err
	}
	if len(dismissed) != len(page.Evidence.Findings) {
		return state.SelectionSnapshot{}, errors.New("dismissal results do not match the frozen candidate page")
	}
	matched := []string{}
	visibleFindings := make([]state.Finding, 0, len(page.Evidence.Findings))
	visibleTargets := make([]state.TargetBinding, 0, len(page.Targets))
	targets := make(map[string]state.TargetBinding, len(page.Targets))
	for _, target := range page.Targets {
		targets[target.FindingID] = target
	}
	for i, finding := range page.Evidence.Findings {
		if dismissed[i] {
			matched = append(matched, finding.ID)
			if !include {
				continue
			}
		}
		visibleFindings = append(visibleFindings, finding)
		if target, ok := targets[finding.ID]; ok {
			visibleTargets = append(visibleTargets, target)
		}
	}
	page.Evidence.Findings = visibleFindings
	page.Targets = visibleTargets
	if len(matched) == 0 {
		return page, nil
	}
	page.Evidence.Notes = slices.Clone(page.Evidence.Notes)
	if include {
		page.Evidence.Notes = append(page.Evidence.Notes, "Included dismissed references: "+strings.Join(matched, ", ")+". These exact saved findings are included for inspection; their dismissals remain unchanged.")
		return page, nil
	}
	page.Evidence.Diagnostics = slices.Clone(page.Evidence.Diagnostics)
	selected := false
	for i := range page.Evidence.Diagnostics {
		if page.Evidence.Diagnostics[i].Code == "selected" {
			selected = true
			page.Evidence.Diagnostics[i].Count -= len(matched)
			if page.Evidence.Diagnostics[i].Count < 0 {
				return state.SelectionSnapshot{}, errors.New("dismissed findings exceed this page's eligible count")
			}
		}
	}
	if !selected {
		return state.SelectionSnapshot{}, errors.New("candidate page is missing its selection count")
	}
	page.Evidence.Diagnostics = append(page.Evidence.Diagnostics, state.SelectionDiagnostic{Code: "dismissed", Count: len(matched), Explanation: "Hidden by an active dismissal of this exact saved evidence; no source files changed."})
	page.Evidence.Notes = append(page.Evidence.Notes, fmt.Sprintf("Dismissal filter: %d exact saved findings hidden on this page. Changed observations resurface. Use --include-dismissed to inspect them; no later rows were added to fill this page.", len(matched)))
	return page, nil
}

func loadDismissalReviewPage(ctx context.Context, base, inventoryBase, root, cursor string, minAge int) (reviewPage, error) {
	s, err := state.OpenReader(ctx, inventoryBase)
	if err != nil {
		return reviewPage{}, err
	}
	page, readErr := s.NodeModulesFindingPage(ctx, cursor, minAge)
	closeErr := s.Close()
	if readErr != nil {
		return reviewPage{}, readErr
	}
	if closeErr != nil {
		return reviewPage{}, closeErr
	}
	page, err = filterDismissalPage(ctx, base, root, page, false)
	if err != nil {
		return reviewPage{}, err
	}
	return reviewPage{Selection: page, Evidence: page.Evidence, NextCursor: page.Evidence.NextCursor}, nil
}

func printDismissalNotes(out io.Writer, r state.FindingReport) {
	for _, note := range r.Notes {
		if strings.HasPrefix(note, "Dismissal filter:") || strings.HasPrefix(note, "Included dismissed references:") {
			printWrapped(out, note, "")
		}
	}
}
