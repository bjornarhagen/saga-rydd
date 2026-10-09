package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

type HashFreshStepReport struct {
	Mode          string                         `json:"mode"`
	StepByteLimit int64                          `json:"step_byte_limit"`
	BudgetScope   string                         `json:"budget_scope"`
	Result        inventory.HashFreshRunResult   `json:"result"`
	ReadConsent   inventory.HashFreshReadConsent `json:"read_consent"`
}

func hashRunFreshJob(ctx context.Context, paths config.Paths, id string) (HashFreshStepReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashFreshStepReport{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		return HashFreshStepReport{}, hashFreshJobMissing(err, "existing hash storage is required; no storage was initialized")
	}
	c, err := reader.FreshReadApproval(ctx, id)
	var job inventory.SavedFreshJob
	var request *inventory.KeeperChoiceFreshRequest
	if err == nil {
		job, err = reader.FreshJob(ctx, c.Approval.JobID)
	}
	if err == nil {
		request, err = reader.PrepareKeeperChoiceFreshRequest(ctx, job.Record.Request.ChoiceID)
	}
	if err == nil && (request.ID() != job.Record.Request.RequestID || job.ReadConsent == nil || job.ReadConsent.ID != id) {
		err = inventory.ErrHashFreshRunBinding
	}
	closeErr := reader.Close()
	if err != nil {
		if errors.Is(err, inventory.ErrHashFreshReadApprovalMissing) {
			err = missingHashError{err}
		}
		return HashFreshStepReport{}, hashFreshJobMissing(err, "the exact saved fresh job or consent is unavailable")
	}
	if closeErr != nil {
		return HashFreshStepReport{}, closeErr
	}
	// Protect every original proposal identity, including files outside the
	// chosen keeper/copy subset, before configuration or inventory access.
	proposal := request.Proposal()
	cfg, err := loadHashRunConfig(ctx, paths, proposal)
	if err != nil {
		return HashFreshStepReport{}, err
	}
	if err = checkCLIHashReadPacing(ctx, cfg.Scan.ReadBytesPerSecond, freshHashReadMinimum(job)); err != nil {
		return HashFreshStepReport{}, err
	}
	if err = checkCLIHashReadDailyLimit(ctx, cfg.Scan.ReadBytesPerDay, freshHashReadMinimum(job)); err != nil {
		return HashFreshStepReport{}, err
	}
	locator := request.SourceLocator()
	root := string(locator.RootPathBytes)
	if err = checkHashSourceStorage(ctx, manualState(paths, root), proposal); err != nil {
		return HashFreshStepReport{}, hashFreshJobMissing(err, "the frozen manual inventory is unavailable; no replacement was initialized")
	}
	scanner, err := inventory.New([]string{root}, cfg.Excludes, []string{paths.StateDir, paths.ConfigFile})
	if err != nil {
		return HashFreshStepReport{}, err
	}
	defer scanner.Close()
	if err = ctx.Err(); err != nil {
		return HashFreshStepReport{}, err
	}
	// The core owns the exact frozen inventory and recovers only this job.
	// No original reader, checkpoint or caller allowance enters dispatch.
	writer, err := inventory.OpenHashFreshRunWriter(ctx, request, job.ID)
	if err != nil {
		return HashFreshStepReport{}, fmt.Errorf("fresh job %s run writer could not open; inspect hash --show-job %s before another explicit run: %w", job.ID, job.ID, hashFreshJobMissing(err, "existing fresh-job storage became unavailable; no replacement was initialized"))
	}
	run, err := writer.RunFreshConsentedBudgeted(ctx, id, scanner, inventory.HashReadExecutionLimits{RequestedBytesPerSecond: cfg.Scan.ReadBytesPerSecond, DailyReservedByteLimit: cfg.Scan.ReadBytesPerDay})
	if err == nil {
		c, err = writer.FreshReadApproval(ctx, id)
	}
	closeErr = writer.Close()
	report := HashFreshStepReport{Mode: "run", StepByteLimit: inventory.FileHashStepByteLimit, BudgetScope: "whole_fresh_job", Result: run, ReadConsent: c}
	if err != nil {
		if errors.Is(err, inventory.ErrHashDeferred) {
			switch run.Code {
			case "daily_byte_limit", "lifetime_byte_limit", "durable_quantum", "configured_daily_byte_limit":
				err = hashRunDeferredError{code: run.Code, error: err}
			}
		}
		return report, fmt.Errorf("fresh job %s step failed; inspect hash --show-job %s before another explicit run: %w", job.ID, job.ID, err)
	}
	if closeErr != nil {
		return report, fmt.Errorf("fresh job %s writer close failed; inspect hash --show-job %s: %w", job.ID, job.ID, closeErr)
	}
	if err = ctx.Err(); err != nil {
		return report, fmt.Errorf("fresh job %s step reply was canceled; inspect hash --show-job %s: %w", job.ID, job.ID, err)
	}
	return report, nil
}

func printHashFreshStepReport(out io.Writer, report HashFreshStepReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: one guarded fresh-job hash step", "")
	printResultBanner(guard, "ONE FRESH HASH STEP - NO CLEANUP")
	r := report.Result
	printField(guard, "Recorded step state", r.Status)
	if r.Code != "" {
		printField(guard, "Recorded reason", r.Code)
	}
	if r.Ordinal != 0 {
		printField(guard, "Fresh work ordinal", r.Ordinal)
		printField(guard, "Historical work ID", r.HistoricalWorkID)
		printField(guard, "Selected role", r.Role)
		fmt.Fprintf(guard, "Frozen path: %q\n", string(r.Progress.PathBytes))
		printField(guard, "Observed prefix", fmt.Sprintf("%d bytes", r.Progress.Offset))
		printField(guard, "Saved fresh prefix", fmt.Sprintf("%d bytes", r.DurableOffset))
		if r.Progress.SHA256 != "" {
			fmt.Fprintf(guard, "Historical fresh SHA-256: %s\n", r.Progress.SHA256)
		}
		if !r.Progress.CheckedAt.IsZero() {
			printField(guard, "Checked at", r.Progress.CheckedAt.UTC().Format(time.RFC3339Nano))
		}
	}
	printField(guard, "Fixed step ceiling", fmt.Sprintf("%d bytes", report.StepByteLimit))
	printField(guard, "Fresh charged reservation", fmt.Sprintf("%d bytes", r.ReservedBytes))
	printField(guard, "Fresh requested bytes", r.Usage.RequestedBytes)
	printField(guard, "Fresh read bytes", r.Usage.ReadBytes)
	printField(guard, "Observed elapsed", r.Usage.Elapsed.String())
	printHashReadPacing(guard, r.ReadPacing)
	printHashStoreReadBudget(guard, r.StoreReadBudget, r.ConfiguredDailyReservedByteLimit)
	printHashPacingZeroProgress(guard, r.Code)
	fmt.Fprintf(guard, "Job: %s\nJob key: %s\nRequest: %s\nChoice: %s\nFresh read consent: %s\n", r.JobID, r.JobKey, r.RequestID, r.ChoiceID, r.ApprovalID)
	printHashBudgetScope(guard, r.FreshBudget, "FRESH-JOB RESERVATION BUDGET", "fresh job", "whole exact fresh job")
	printWrapped(guard, "One step runs and exits. Only this exact job can be recovered by an explicit run writer; original work and charges stay separate. Historical digests prove no current equality or safe cleanup.", "")
	printWrapped(guard, "Keep the same global options and private data directory when inspecting saved progress with:", "")
	fmt.Fprintf(guard, "  rydd hash --show-job %s\n", r.JobID)
	if guard.err != nil {
		return fmt.Errorf("write fresh job %s step reply; inspect hash --show-job %s before another explicit run: %w", r.JobID, r.JobID, guard.err)
	}
	return nil
}
