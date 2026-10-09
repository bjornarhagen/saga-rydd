package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type planSaver func(context.Context, string, state.SelectionSnapshot) (plans.Saved, error)

// An ID returned with an error is a candidate; it is not a saved-plan result.
type planPublicationCandidate struct {
	Contract          string `json:"contract"`
	CandidatePlanID   string `json:"candidate_plan_id"`
	Publication       string `json:"publication"`
	ReopenCommand     string `json:"reopen_command"`
	CleanupAuthorized bool   `json:"cleanup_authorized"`
}

func planPublicationResult(saved plans.Saved, err error, paths config.Paths) (any, error) {
	if err == nil || !plans.ValidID(saved.ID) {
		return saved, err
	}
	command := commandPrefix(paths) + " plan --show " + saved.ID
	candidate := planPublicationCandidate{Contract: "saved_plan_publication_candidate_v1", CandidatePlanID: saved.ID, Publication: "unconfirmed", ReopenCommand: command}
	return candidate, fmt.Errorf("plan publication or reply did not finish for candidate %s; inspect it with %s before creating another selection. A fresh save creates a new plan: %w", saved.ID, command, err)
}

func printPlanCandidate(out io.Writer, candidate planPublicationCandidate) {
	printResultBanner(out, "PLAN PUBLICATION UNCONFIRMED - NO CLEANUP")
	fmt.Fprintf(out, "Candidate plan: %s\n", candidate.CandidatePlanID)
	printWrapped(out, "This reply does not confirm whether the plan was committed. Inspect this exact ID before creating another selection. Repeating plan --save or review saves a new timestamp and can create another plan. No cleanup is authorized.", "")
	fmt.Fprintf(out, "\nInspect the exact candidate:\n  %s\n", candidate.ReopenCommand)
}

func planMachineFailure(out, errOut io.Writer, candidate planPublicationCandidate, err error) int {
	var failure bytes.Buffer
	code := operationFailure(&failure, errOut, "plan", err)
	var envelope map[string]any
	if decodeErr := json.Unmarshal(failure.Bytes(), &envelope); decodeErr != nil {
		return machineFailure(out, errOut, "plan", "command_failed", "cannot format plan publication failure", 1)
	}
	envelope["plan_candidate"] = candidate
	guard := &reviewOutput{writer: out}
	exitCode := emit(guard, errOut, envelope, code)
	if guard.err != nil {
		fmt.Fprintf(errOut, "Publication is unconfirmed for candidate %s. Inspect it with %s before creating another selection.\n", candidate.CandidatePlanID, candidate.ReopenCommand)
	}
	return exitCode
}

func planReplyMessage(result any, paths config.Paths) string {
	if saved, ok := result.(plans.Saved); ok && plans.ValidID(saved.ID) {
		return fmt.Sprintf("Plan %s is available in saved history. Inspect it with %s plan --show %s before creating another selection", saved.ID, commandPrefix(paths), saved.ID)
	}
	return "Plan reply did not finish; no confirmed plan publication result is available"
}

func printPlanResult(out io.Writer, result any, paths config.Paths) error {
	guard := &reviewOutput{writer: out}
	printPlan(guard, result, paths)
	if guard.err != nil {
		return fmt.Errorf("%s: %w", planReplyMessage(result, paths), guard.err)
	}
	return nil
}
