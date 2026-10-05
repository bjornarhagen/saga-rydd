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
	fmt.Fprintln(out, "Saga — Rydd: cleanup plan preview")
	printResultBanner(out, "PREVIEW ONLY — NO ACTIONS AVAILABLE")
	fmt.Fprintf(out, "\n%d explicitly selected target(s); minimum age %d days.\nProject activity: unconfirmed. Current filesystem state: unverified.\n", len(r.Evidence.Findings), r.Evidence.MinimumAgeDays)
	for i, f := range r.Evidence.Findings {
		m := f.Measurement
		logical, allocated := "unknown", "unknown"
		if m.LogicalBytes != nil {
			logical = humanBytes(*m.LogicalBytes)
		}
		if m.AllocatedBytes != nil {
			allocated = humanBytes(*m.AllocatedBytes)
		}
		fmt.Fprintf(out, "\n%d. %q\n   Reference: %s\n   Saved size: %s logical; %s allocated (%s)\n   Modified: folder %s; package.json %s\n", i+1, string(f.PathBytes), f.ID, logical, allocated, directoryStatusLabel(m.Status), f.DirectoryModifiedAt.Format("2006-01-02"), f.ManifestModifiedAt.Format("2006-01-02"))
		if m.Truncated {
			fmt.Fprintln(out, "   Entry limit reached; size covers only the measured portion.")
		}
	}
	fmt.Fprintln(out, "\nProposed future action: same-filesystem quarantine. This frees no disk space.")
	fmt.Fprintln(out, "\nBefore execution can be supported:")
	for _, requirement := range r.Requirements {
		printWrapped(out, requirement, "  • ")
	}
	for _, note := range r.Notes {
		printWrapped(out, note, "  ")
	}
	fmt.Fprintln(out, "Full saved evidence and caveats: add --json.")
}
