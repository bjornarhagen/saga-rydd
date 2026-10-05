package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
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

type CheckedPlan struct {
	ID    string               `json:"id"`
	Check state.SelectionCheck `json:"check"`
}

func plan(ctx context.Context, args []string, paths config.Paths) (any, error) {
	r := PlanPreview{}
	f := flag.NewFlagSet("plan", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	preview := f.Bool("preview", false, "show a read-only cleanup review preview")
	save := f.Bool("save", false, "save an unapproved selection for later review")
	show := f.String("show", "", "reopen a saved plan ID")
	check := f.String("check", "", "compare a saved plan with its inventory")
	approve := f.String("approve", "", "record 24-hour review consent for an exact plan; cannot execute cleanup")
	revoke := f.String("revoke", "", "revoke review consent for an exact plan")
	projectReview := f.Bool("confirm-project-review", false, "owner reviewed project activity, local dependency edits and reinstall requirements")
	quarantine := f.Bool("confirm-quarantine", false, "accept same-filesystem quarantine without purge; frees no space; review consent only, expires in 24 hours")
	directory := f.String("directory", "", "exact manual scan root")
	f.StringVar(directory, "d", "", "directory alias")
	days := f.Int("min-age-days", state.FindingAgeDays, "minimum saved candidate age (1–36500)")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	aliases, modes := 0, 0
	ageSet := false
	confirmationSet := false
	f.Visit(func(v *flag.Flag) {
		if v.Name == "preview" || v.Name == "save" || v.Name == "show" || v.Name == "check" || v.Name == "approve" || v.Name == "revoke" {
			modes++
		}
		if v.Name == "confirm-project-review" || v.Name == "confirm-quarantine" {
			confirmationSet = true
		}
		if v.Name == "min-age-days" {
			ageSet = true
		}
		if v.Name == "directory" || v.Name == "d" {
			aliases++
		}
	})
	if modes != 1 || aliases > 1 {
		return r, usageError{errors.New("plan accepts exactly one of --preview, --save, --show, --check, --approve or --revoke; put options before IDs")}
	}
	if confirmationSet && *approve == "" {
		return r, usageError{errors.New("review confirmations are accepted only with --approve PLAN_ID")}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if *show != "" {
		if f.NArg() != 0 || aliases != 0 || ageSet || !plans.ValidID(*show) {
			return r, usageError{errors.New("plan --show requires one full plan ID and no directory, age or finding selection")}
		}
		return plans.Show(ctx, paths.StateDir, *show)
	}
	if *revoke != "" {
		if f.NArg() != 0 || aliases != 0 || ageSet || !plans.ValidID(*revoke) {
			return r, usageError{errors.New("plan --revoke requires one full plan ID and no directory, age or finding selection")}
		}
		return plans.Revoke(ctx, paths.StateDir, *revoke)
	}
	selectedPlan := *check
	if *approve != "" {
		selectedPlan = *approve
		if !*projectReview || !*quarantine {
			return r, usageError{plans.ErrConfirmation}
		}
	}
	if selectedPlan != "" {
		if f.NArg() != 0 || ageSet || !plans.ValidID(selectedPlan) {
			return r, usageError{errors.New("plan --check or --approve requires one full plan ID, an optional directory and no age or finding selection")}
		}
	} else if (!*preview && !*save) || f.NArg() < 1 || f.NArg() > state.PreviewTargetLimit {
		return r, usageError{errors.New("plan requires --preview or --save and 1–20 finding IDs; put options before IDs")}
	}
	if *days < 1 || *days > state.MaxFindingAgeDays {
		return r, usageError{state.ErrFindingAge}
	}
	base := paths.StateDir
	if aliases == 1 {
		root, err := directoryPath(*directory)
		if err != nil {
			return r, err
		}
		paths.StateDir = manualState(paths, root)
	}
	var saved plans.Saved
	if *check != "" {
		var err error
		saved, err = plans.Load(ctx, base, *check)
		if err != nil {
			return r, err
		}
	}
	s, err := state.OpenReader(ctx, paths.StateDir)
	if err != nil {
		return r, err
	}
	defer s.Close()
	if *approve != "" {
		return plans.Approve(ctx, base, *approve, s, plans.Confirmations{ProjectReview: *projectReview, Quarantine: *quarantine})
	}
	if *check != "" {
		result, err := s.CheckSelection(ctx, saved.Record.Selection)
		return CheckedPlan{ID: saved.ID, Check: result}, err
	}
	if *save {
		selection, err := s.SnapshotSelection(ctx, f.Args(), *days)
		if err != nil {
			if errors.Is(err, state.ErrFindingSelection) || errors.Is(err, state.ErrFindingAge) {
				err = usageError{err}
			}
			return r, err
		}
		return plans.Save(ctx, base, selection)
	}
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

func printPlan(out io.Writer, result any, paths config.Paths) {
	switch r := result.(type) {
	case CheckedPlan:
		printWrapped(out, "Saga — Rydd: saved selection check", "")
		switch r.Check.Status {
		case "matches_saved_inventory":
			printResultBanner(out, "SAVED OBSERVATIONS MATCH - NO EXECUTION AUTHORIZATION")
		case "changed":
			printResultBanner(out, "SAVED EVIDENCE CHANGED - REVIEW AGAIN")
		default:
			printResultBanner(out, "SAVED EVIDENCE INCOMPLETE - REVIEW REQUIRED")
		}
		fmt.Fprintf(out, "Plan: %s\n", r.ID)
		printField(out, "Selected targets", r.Check.SelectedTargets)
		lastFinding := ""
		for _, issue := range r.Check.Issues {
			if issue.FindingID != "" && issue.FindingID != lastFinding {
				printField(out, "Finding", issue.FindingID)
				lastFinding = issue.FindingID
			}
			printWrapped(out, issue.Message, "  ")
		}
		printWrapped(out, "This compares saved observations only. Current files, project activity and reinstall requirements have not been checked. A match does not establish safe cleanup.", "")
		printWrapped(out, "Review approval is a separate step. Cleanup is not supported. No files were moved or deleted. The saved selection is unchanged.", "")
		fmt.Fprintf(out, "\nReopen this selection:\n  %s plan --show %s\n", commandPrefix(paths), r.ID)
	case PlanPreview:
		printPlanPreview(out, r)
	case plans.Saved:
		printWrapped(out, "Saga — Rydd: saved cleanup selection", "")
		if r.Review == nil {
			printResultBanner(out, "SAVED FOR REVIEW - NOT APPROVED")
		} else {
			printReview(out, *r.Review)
		}
		fmt.Fprintf(out, "Plan: %s\n", r.ID)
		printField(out, "Saved", r.Record.CreatedAt.Format(time.RFC3339))
		printField(out, "Selected targets", len(r.Record.Selection.Evidence.Findings))
		printField(out, "Minimum age", fmt.Sprintf("%d days", r.Record.Selection.Evidence.MinimumAgeDays))
		printField(out, "Project activity at capture", "Unconfirmed")
		printWrapped(out, "This is the saved selection. Scanning cannot add targets to it. Current contents have not been checked.", "")
		for i, f := range r.Record.Selection.Evidence.Findings {
			printFinding(out, i+1, f)
		}
		if r.Review == nil {
			fmt.Fprintln(out, "\nREVIEW NEXT")
			printWrapped(out, "Confirm whether these projects are still in use. Check for local dependency edits and whether you can reinstall the dependencies. Saving does not approve cleanup.", "  ")
		} else {
			fmt.Fprintln(out, "\nABOUT THIS REVIEW")
			if r.Review.Status != "review_approved" {
				printWrapped(out, "To review again, save a new selection. This plan's approval cannot be renewed.", "  ")
			}
		}
		printWrapped(out, "Review consent can be recorded with plan --approve and both owner confirmations. It expires after 24 hours and cannot execute cleanup. A future cleanup command will require renewed approval. No files were moved or deleted. Quarantine would free no disk space; purge is not included.", "  ")
		fmt.Fprintf(out, "\nReopen this selection:\n  %s plan --show %s\n", commandPrefix(paths), r.ID)
		if r.Review != nil && r.Review.Revocation == nil {
			fmt.Fprintf(out, "\nRevoke this review consent:\n  %s plan --revoke %s\n", commandPrefix(paths), r.ID)
		}
	}
}

func printReview(out io.Writer, r plans.Review) {
	switch r.Status {
	case "review_approved":
		printResultBanner(out, "REVIEW CONSENT RECORDED - CLEANUP UNAVAILABLE")
	case "revoked":
		printResultBanner(out, "REVIEW CONSENT REVOKED")
	case "expired":
		printResultBanner(out, "REVIEW CONSENT EXPIRED")
	default:
		printResultBanner(out, "REVIEW CONSENT NOT YET VALID - CHECK CLOCK")
	}
	fmt.Fprintf(out, "Approval: %s\n", r.ID)
	printField(out, "Review recorded", r.Approval.CreatedAt.Format(time.RFC3339))
	printField(out, "Expires", r.Approval.ExpiresAt.Format(time.RFC3339))
	printWrapped(out, "The owner confirmed review of project activity, local dependency edits and reinstall requirements, and accepted same-filesystem quarantine without purge. Rydd has not verified those statements or current files. Review consent is not execution authorization.", "")
	if r.Revocation != nil {
		printField(out, "Revoked", r.Revocation.CreatedAt.Format(time.RFC3339))
	}
}
