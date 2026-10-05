package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type PlanPreview struct {
	Mode                      string              `json:"mode"`
	Executable                bool                `json:"executable"`
	ApprovalAvailable         bool                `json:"approval_available"`
	Activity                  string              `json:"project_activity"`
	FutureAction              string              `json:"proposed_future_action"`
	QuarantineReclaimsSpace   bool                `json:"quarantine_reclaims_space"`
	EstimatedReclaimableBytes *int64              `json:"estimated_reclaimable_bytes"`
	Evidence                  state.FindingReport `json:"evidence"`
	Requirements              []string            `json:"requirements_before_execution"`
	Notes                     []string            `json:"notes"`
}

func plan(ctx context.Context, args []string, paths config.Paths) (PlanPreview, error) {
	r := PlanPreview{}
	f := flag.NewFlagSet("plan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	preview := f.Bool("preview", false, "show a read-only cleanup review preview")
	directory := f.String("directory", "", "exact manual scan root")
	f.StringVar(directory, "d", "", "directory alias")
	days := f.Int("min-age-days", state.FindingAgeDays, "minimum saved candidate age (1–36500)")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	aliases := 0
	f.Visit(func(v *flag.Flag) {
		if v.Name == "directory" || v.Name == "d" {
			aliases++
		}
	})
	if !*preview || aliases > 1 || f.NArg() < 1 || f.NArg() > state.PreviewTargetLimit {
		return r, usageError{errors.New("plan requires --preview and 1–20 finding IDs; put options before IDs")}
	}
	if *days < 1 || *days > state.MaxFindingAgeDays {
		return r, usageError{state.ErrFindingAge}
	}
	if aliases == 1 {
		root, err := directoryPath(*directory)
		if err != nil {
			return r, err
		}
		paths.StateDir = manualState(paths, root)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return r, err
	}
	defer s.Close()
	evidence, err := s.PreviewFindings(ctx, f.Args(), *days)
	if err != nil {
		if errors.Is(err, state.ErrFindingSelection) || errors.Is(err, state.ErrFindingAge) {
			return r, usageError{err}
		}
		return r, err
	}
	r = PlanPreview{
		Mode: "preview", Activity: "unconfirmed", FutureAction: "same_filesystem_quarantine", Evidence: evidence,
		Requirements: []string{
			"Confirm actual project activity, local dependency edits and regeneration requirements with the owner; old timestamps do not establish disuse.",
			"Create and persist an immutable exact-target plan bound to store/root identity and explicit approval. This preview has no approval ID.",
			"Revalidate current target and ancestor identities, scope, exclusions and freshness immediately before an action. Changed or uncertain evidence stops the action.",
			"Validate private quarantine on the same filesystem and descriptor-relative no-follow operations; never fall back to cross-filesystem copying or permanent deletion.",
			"Persist action intent and results with original paths and restore records; recover interrupted work and restore without overwriting existing files.",
		},
		Notes: []string{
			"Read-only preview of explicitly selected saved findings. No live filesystem checks, approval, action record or cleanup has occurred.",
			"Quarantine supports recovery but does not reclaim disk space. Permanent purge is separate, irreversible and requires separate approval; it is not implemented.",
			"Saved paths and IDs may be reused after an inventory rebuild. Selection and measurements use separate snapshots and may change during reporting. This output cannot authorize a future action.",
		},
	}
	return r, nil
}

func printPlanPreview(out io.Writer, r PlanPreview) {
	printWrapped(out, "Saga — Rydd: cleanup plan preview", "")
	printResultBanner(out, "PREVIEW ONLY - NO ACTIONS AVAILABLE")
	printField(out, "Selected targets", len(r.Evidence.Findings))
	printField(out, "Minimum age", fmt.Sprintf("%d days", r.Evidence.MinimumAgeDays))
	printField(out, "Project activity", "Unconfirmed")
	printWrapped(out, "Current contents have not been checked. These sizes are not estimates of space you can free.", "")
	for i, f := range r.Evidence.Findings {
		printFinding(out, i+1, f)
	}
	fmt.Fprintln(out, "\nREVIEW NEXT")
	printWrapped(out, "Confirm whether these projects are still in use. Check for local dependency edits and whether you can reinstall the dependencies. Old timestamps alone do not establish safe deletion.", "  ")
	fmt.Fprintln(out, "\nABOUT THIS PREVIEW")
	printWrapped(out, "No files were moved or deleted. This preview is not saved as an approved plan and cannot authorize cleanup. Saved paths, IDs and measurements can change between reports.", "  ")
	printWrapped(out, "A future quarantine would move files on the same filesystem for recovery. It frees no disk space. Quarantine and restore without overwriting are not yet supported. Permanent deletion would require separate approval and is not yet supported.", "  ")
}
