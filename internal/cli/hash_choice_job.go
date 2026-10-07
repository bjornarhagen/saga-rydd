package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

type HashFreshJobKeyResult struct {
	Contract          string `json:"contract"`
	JobKey            string `json:"job_key"`
	Saved             bool   `json:"saved"`
	ApprovalAvailable bool   `json:"approval_available"`
	Executable        bool   `json:"executable"`
}

type HashFreshChoiceJobResult struct {
	Mode string                  `json:"mode"`
	Job  inventory.SavedFreshJob `json:"job"`
}

func hashNewFreshJobKey(ctx context.Context) (HashFreshJobKeyResult, error) {
	if err := ctx.Err(); err != nil {
		return HashFreshJobKeyResult{}, err
	}
	key, err := inventory.NewHashFreshJobKey()
	if err != nil {
		return HashFreshJobKeyResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return HashFreshJobKeyResult{}, err
	}
	return HashFreshJobKeyResult{Contract: "fresh_hash_job_key_v1", JobKey: key}, nil
}

func hashSaveFreshChoiceJob(ctx context.Context, paths config.Paths, choiceID, key string) (HashFreshChoiceJobResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashFreshChoiceJobResult{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		return HashFreshChoiceJobResult{}, hashFreshJobMissing(err, "existing hash storage is required; no storage was initialized")
	}
	request, prepareErr := reader.PrepareKeeperChoiceFreshRequest(ctx, choiceID)
	closeErr := reader.Close()
	if prepareErr != nil {
		return HashFreshChoiceJobResult{}, hashFreshJobMissing(prepareErr, "the exact saved choice is unavailable")
	}
	if closeErr != nil {
		return HashFreshChoiceJobResult{}, closeErr
	}
	// The writer verifies the captured private objects before opening SQLite.
	// Neither reader nor writer loads source/configuration/inventory or recovers
	// interrupted work. The caller's explicit key is never replaced on error.
	writer, err := inventory.OpenHashFreshJobWriter(ctx, request)
	if err != nil {
		return HashFreshChoiceJobResult{}, hashFreshJobMissing(err, "original hash storage became unavailable; no replacement was initialized")
	}
	job, saveErr := writer.SaveFreshJob(ctx, request, key)
	closeErr = writer.Close()
	result := HashFreshChoiceJobResult{Mode: "save", Job: job}
	if saveErr != nil {
		if job.ID != "" {
			return result, fmt.Errorf("fresh job publication or its reply is uncertain; inspect hash --show-job %s or repeat the same choice with --job-key %s: %w", job.ID, key, saveErr)
		}
		return result, saveErr
	}
	if closeErr != nil {
		return result, fmt.Errorf("fresh job %s is saved, but closing its writer failed; inspect hash --show-job %s or repeat --job-key %s: %w", job.ID, job.ID, key, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("fresh job %s is saved, but its reply was canceled; inspect hash --show-job %s or repeat --job-key %s: %w", job.ID, job.ID, key, err)
	}
	return result, nil
}

func hashShowFreshChoiceJob(ctx context.Context, paths config.Paths, id string) (HashFreshChoiceJobResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashFreshChoiceJobResult{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		return HashFreshChoiceJobResult{}, hashFreshJobMissing(err, "existing hash storage is required; no storage was initialized")
	}
	job, readErr := reader.FreshJob(ctx, id)
	closeErr := reader.Close()
	if readErr != nil {
		return HashFreshChoiceJobResult{}, hashFreshJobMissing(readErr, "saved fresh job is unavailable")
	}
	if closeErr != nil {
		return HashFreshChoiceJobResult{}, closeErr
	}
	if err := ctx.Err(); err != nil {
		return HashFreshChoiceJobResult{}, err
	}
	return HashFreshChoiceJobResult{Mode: "show", Job: job}, nil
}

func hashFreshJobMissing(err error, message string) error {
	if errors.Is(err, os.ErrNotExist) {
		return missingHashError{fmt.Errorf("%s: %w", message, err)}
	}
	return err
}

func printHashFreshJobKey(out io.Writer, result HashFreshJobKeyResult) error {
	guard := &reviewOutput{writer: out}
	printResultBanner(guard, "FRESH JOB KEY GENERATED - NOTHING SAVED")
	fmt.Fprintf(guard, "Job key: %s\n", result.JobKey)
	printWrapped(guard, "Keep this key for the exact job you intend to save. Reuse it with the same choice after a failed reply. A different key creates another unapproved job. No storage or source files were opened, and no read consent was recorded.", "")
	if guard.err != nil {
		return fmt.Errorf("write fresh job key; nothing was saved: %w", guard.err)
	}
	return nil
}

func printHashFreshChoiceJob(out io.Writer, result HashFreshChoiceJobResult) error {
	guard := &reviewOutput{writer: out}
	job, record := result.Job, result.Job.Record
	printWrapped(guard, "Saga — Rydd: independent fresh full-file job", "")
	if job.ReadConsent != nil {
		printResultBanner(guard, "SAVED FRESH JOB - PERMISSION NOT EVALUATED")
	} else if result.Mode == "save" {
		printResultBanner(guard, "FRESH JOB SAVED - READ CONSENT UNAVAILABLE")
	} else {
		printResultBanner(guard, "SAVED FRESH JOB - READ CONSENT UNAVAILABLE")
	}
	fmt.Fprintf(guard, "Job: %s\nJob key: %s\nRequest: %s\nChoice: %s\nOriginal store: %s\nOriginal selection: %s\nInventory: %s\n", job.ID, record.JobKey, record.Request.RequestID, record.Request.ChoiceID, record.Request.StoreID, record.Request.SelectionID, record.Request.InventoryID)
	printField(guard, "Job saved at", record.CreatedAt.UTC().Format(time.RFC3339Nano))
	printField(guard, "Job creation status", record.Status)
	printWrapped(guard, "This separate job was created unapproved. Its selected files start with zero fresh progress. Historical digests and old read approvals are context only; no SHA continuation state or allowance was copied into this job.", "")
	for i, work := range job.Work {
		target := record.Request.Targets[i]
		role := "Selected copy for review"
		if target.Role == "keeper" {
			role = "Selected keeper for review"
		}
		printHashPreviewMember(guard, role, target.Observation)
		printField(guard, "Fresh work ordinal", work.Ordinal)
		printField(guard, "Fresh work status", work.Status)
		printField(guard, "Fresh checked offset", fmt.Sprintf("%d bytes", work.CheckedOffset))
	}
	fmt.Fprintln(guard)
	printField(guard, "Fresh reserved bytes", fmt.Sprintf("%d bytes", job.FreshReservedBytes))
	printField(guard, "Fresh requested bytes", fmt.Sprintf("%d bytes", job.FreshRequestedBytes))
	printField(guard, "Fresh read bytes", fmt.Sprintf("%d bytes", job.FreshReadBytes))
	printResultBanner(guard, "ORIGINAL HASHING CONTEXT AT FIRST JOB PUBLICATION")
	printField(guard, "Original selected work", record.OriginalContext.SelectedWork)
	printField(guard, "Original completed observations", record.OriginalContext.CompletedObservations)
	printField(guard, "Original unfinished work", record.OriginalContext.UnfinishedWork)
	printWrapped(guard, "These whole-original-selection charges and consent were captured when this job was first saved. They are not this job's accounting or permission. Exact retries retain the first context.", "")
	printHashBudget(guard, record.OriginalContext.Budget)
	printHashReadConsent(guard, record.OriginalContext.ReadConsent)
	if job.ReadConsent == nil {
		printField(guard, "Fresh read approval", "Unavailable")
	} else {
		printHashFreshReadConsent(guard, job.ReadConsent)
	}
	printField(guard, "Reclaimable space", "Unknown")
	printWrapped(guard, "Only existing saved hash records were accessed. No source files, configuration or inventory were opened. Original hashing work, consent and charges were unchanged. This saved view evaluates no current read permission and grants no cleanup permission. Keep the same global options and private data directory when reopening it with:", "")
	fmt.Fprintf(guard, "  rydd hash --show-job %s\n", job.ID)
	if guard.err != nil {
		return fmt.Errorf("write existing fresh job %s; reopen with hash --show-job %s or repeat the exact choice with --job-key %s: %w", job.ID, job.ID, record.JobKey, guard.err)
	}
	return nil
}
