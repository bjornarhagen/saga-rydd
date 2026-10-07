package cli

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func printHashResult(out io.Writer, result any) error {
	switch result := result.(type) {
	case inventory.HashProposal:
		return printHashProposal(out, result)
	case HashConsentResult:
		return printHashConsentResult(out, result)
	case HashStepReport:
		return printHashStepReport(out, result)
	case inventory.SavedHashKeeperChoice:
		return printHashKeeperChoice(out, result)
	default:
		return errors.New("unknown hash command result")
	}
}

func printHashConsentResult(out io.Writer, result HashConsentResult) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: full-file read consent", "")
	if result.Mode == "approve" {
		printResultBanner(guard, "READ CONSENT SAVED - NO SELECTED FILE CONTENTS READ")
	} else {
		printResultBanner(guard, "READ CONSENT REVOKED - NO SELECTED FILE CONTENTS READ")
	}
	approval := result.ReadConsent.Approval
	fmt.Fprintf(guard, "Store: %s\nSelection: %s\nManual root: %q\n", approval.StoreID, approval.SelectionID, string(approval.SourceLocator.RootPathBytes))
	printHashReadConsent(guard, &result.ReadConsent)
	if result.Mode == "approve" {
		printWrapped(guard, "This records permission for bounded full-file reads of this exact selection. It does not start a read. The fixed expiry and limits cannot be renewed or increased. Each explicit run must pass fresh consent, budget, saved inventory and live checks.", "")
	} else {
		printWrapped(guard, "Revocation blocks future reservations after the writer lock is acquired. It cannot preempt a call already holding that lock or a blocked kernel operation. No source or inventory was read by this command.", "")
	}
	printWrapped(guard, "Read consent never authorizes deletion, quarantine, keeper selection or cleanup.", "")
	if guard.err != nil {
		return fmt.Errorf("write read consent result (saved records may have changed; inspect hash --show or hashes): %w", guard.err)
	}
	return nil
}

func printHashStepReport(out io.Writer, report HashStepReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: one guarded hash step", "")
	printResultBanner(guard, "ONE HASH STEP - NO CLEANUP")
	approval := report.ReadConsent.Approval
	fmt.Fprintf(guard, "Read consent ID: %s\nStore: %s\nSelection: %s\nInventory: %s\n", report.ApprovalID, approval.StoreID, approval.SelectionID, approval.InventoryID)
	result := report.Result
	if result.WorkID == "" {
		printWrapped(guard, "No pending hashing work was selected. This does not prove current files are unchanged or that duplicates exist.", "")
	} else {
		fmt.Fprintf(guard, "Work %s\n  %q\n", result.WorkID, string(result.Progress.PathBytes))
		state := result.Status
		switch result.Status {
		case "pending":
			state = "Partial prefix observed; more work remains"
		case "hash_observed":
			state = "Historical full-file hash observed"
		}
		printField(guard, "Recorded step state", state)
		printField(guard, "Observed prefix", fmt.Sprintf("%d bytes", result.Progress.Offset))
		printField(guard, "Saved prefix", fmt.Sprintf("%d bytes", result.DurableOffset))
		if result.Progress.SHA256 != "" {
			fmt.Fprintf(guard, "Historical SHA-256: %s\n", result.Progress.SHA256)
		}
		printField(guard, "Checked at", result.Progress.CheckedAt.UTC().Format(time.RFC3339Nano))
	}
	printField(guard, "Fixed step ceiling", fmt.Sprintf("%d bytes", report.StepByteLimit))
	printField(guard, "Day reservation cap", fmt.Sprintf("%d bytes", approval.DailyReservedByteLimit))
	printField(guard, "Lifetime reservation cap", fmt.Sprintf("%d bytes", approval.LifetimeReservedByteLimit))
	printField(guard, "Charged reservation", fmt.Sprintf("%d bytes", result.ReservedBytes))
	printField(guard, "Requested bytes", result.Usage.RequestedBytes)
	printField(guard, "Read bytes", result.Usage.ReadBytes)
	printField(guard, "Observed elapsed", result.Usage.Elapsed.String())
	printHashBudget(guard, result.Budget)
	if result.Status == "pending" {
		printWrapped(guard, "To continue, explicitly run the following command with the same global options:", "")
		fmt.Fprintf(guard, "  rydd hash --run %s\n", report.ApprovalID)
	}
	printWrapped(guard, "This command performs one step and exits. It does not loop or retry. Reservations are never refunded. Normal writer opening can also recover a previous interruption as metadata. A saved digest is a historical observation; it does not prove current contents, duplicate files or safe cleanup.", "")
	if guard.err != nil {
		return fmt.Errorf("write hash step result (publication may have completed; inspect hashes before another explicit run): %w", guard.err)
	}
	return nil
}

// Saved consent is historical evidence. A reader must neither evaluate current
// permission nor infer remaining quota from its recorded limits and clocks.
func printHashReadConsent(out io.Writer, consent *inventory.HashReadConsent) {
	if consent == nil {
		return
	}
	printResultBanner(out, "SAVED FULL-FILE READ CONSENT")
	fmt.Fprintf(out, "Read consent ID: %s\n", consent.ID)
	status := "Unknown saved state"
	switch consent.Status {
	case "recorded":
		status = "Consent recorded; current permission not checked"
	case "revoked":
		status = "Revocation recorded"
	case "expired_observed":
		status = "Expiry observed by a writer"
	}
	printField(out, "Saved consent state", status)
	printField(out, "Recorded at", consent.Approval.CreatedAt.UTC().Format(time.RFC3339Nano))
	printField(out, "Recorded expiry", consent.Approval.ExpiresAt.UTC().Format(time.RFC3339Nano))
	for _, field := range []struct {
		label string
		bytes int64
	}{
		{"Step reservation cap", consent.Approval.StepByteLimit},
		{"Day reservation cap", consent.Approval.DailyReservedByteLimit},
		{"Lifetime reservation cap", consent.Approval.LifetimeReservedByteLimit},
		{"Charges before approval", consent.Approval.InitialTotalReservedBytes},
	} {
		printField(out, field.label, fmt.Sprintf("%d bytes", field.bytes))
	}
	printField(out, "Consent clock observation", consent.ClockHighWater.UTC().Format(time.RFC3339Nano))
	if consent.Revocation != nil {
		printField(out, "Recorded revocation", consent.Revocation.RecordedAt.UTC().Format(time.RFC3339Nano))
	}
	if consent.ExpiredObserved {
		printWrapped(out, "A writer recorded that this approval expired. This view does not make a new expiry observation.", "")
	}
	printField(out, "Current read permission", "Not evaluated by this saved-only view")
	printWrapped(out, "These fixed caps cover the whole saved selection. Lifetime charges include earlier, canceled and unknown reservations. Day charges follow the saved reservation day; they do not measure reads per wall-clock day. This view does not calculate remaining quota or time to expiry, and it does not start or recover reads. Read consent does not authorize cleanup.", "")
}
