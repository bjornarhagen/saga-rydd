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

type HashFreshReadConsentResult struct {
	Mode        string                         `json:"mode"`
	ReadConsent inventory.HashFreshReadConsent `json:"read_consent"`
}

func hashFreshReadCommand(ctx context.Context, paths config.Paths, mode, id string, dayCap, totalCap int64) (HashFreshReadConsentResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashFreshReadConsentResult{}, err
	}
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		return HashFreshReadConsentResult{}, hashFreshJobMissing(err, "existing hash storage is required; no storage was initialized")
	}
	var consent inventory.HashFreshReadConsent
	var job inventory.SavedFreshJob
	if mode == "approve" {
		job, err = reader.FreshJob(ctx, id)
	} else {
		consent, err = reader.FreshReadApproval(ctx, id)
		if err == nil && mode == "revoke" {
			job, err = reader.FreshJob(ctx, consent.Approval.JobID)
		}
	}
	var request *inventory.KeeperChoiceFreshRequest
	if err == nil && mode != "show" {
		request, err = reader.PrepareKeeperChoiceFreshRequest(ctx, job.Record.Request.ChoiceID)
		if err == nil && request.ID() != job.Record.Request.RequestID {
			err = inventory.ErrHashFreshReadBinding
		}
	}
	closeErr := reader.Close()
	if err != nil {
		if errors.Is(err, inventory.ErrHashFreshReadApprovalMissing) {
			err = missingHashError{err}
		}
		return HashFreshReadConsentResult{}, hashFreshJobMissing(err, "the exact saved fresh job or read consent is unavailable")
	}
	if closeErr != nil {
		return HashFreshReadConsentResult{}, closeErr
	}
	if err := ctx.Err(); err != nil {
		return HashFreshReadConsentResult{}, err
	}
	if mode == "show" {
		return HashFreshReadConsentResult{Mode: mode, ReadConsent: consent}, nil
	}
	// This existing-only writer guards the opaque request before SQLite opens.
	// It does not load configuration/inventory, recover work or read sources.
	writer, err := inventory.OpenHashFreshJobWriter(ctx, request)
	if err != nil {
		return HashFreshReadConsentResult{}, hashFreshJobMissing(err, "original hash storage became unavailable; no replacement was initialized")
	}
	if mode == "approve" {
		consent, err = writer.ApproveFreshRead(ctx, inventory.HashFreshReadApprovalRequest{
			JobID: job.ID, JobKey: job.Record.JobKey, RequestID: job.Record.Request.RequestID,
			ConfirmFullFileRead: true, DailyReservedByteLimit: dayCap, LifetimeReservedByteLimit: totalCap,
		})
	} else {
		consent, err = writer.RevokeFreshRead(ctx, id)
	}
	closeErr = writer.Close()
	result := HashFreshReadConsentResult{Mode: mode, ReadConsent: consent}
	if err != nil {
		if consent.ID != "" {
			if errors.Is(err, inventory.ErrHashReadExpired) || errors.Is(err, inventory.ErrHashReadRevoked) || errors.Is(err, inventory.ErrHashReadClockRollback) {
				return result, fmt.Errorf("fresh consent %s exists, but this request was refused; inspect hash --show-job-read %s: %w", consent.ID, consent.ID, err)
			}
			return result, fmt.Errorf("fresh consent publication or reply is uncertain; inspect hash --show-job-read %s before repeating the same request: %w", consent.ID, err)
		}
		return result, err
	}
	if closeErr != nil {
		return result, fmt.Errorf("fresh consent %s was saved, but closing its writer failed; inspect hash --show-job-read %s: %w", consent.ID, consent.ID, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("fresh consent %s reply was canceled; inspect hash --show-job-read %s: %w", consent.ID, consent.ID, err)
	}
	return result, nil
}

func printHashFreshReadConsent(out io.Writer, consent *inventory.HashFreshReadConsent) {
	if consent == nil {
		printField(out, "Fresh read consent", "Not recorded")
		return
	}
	a := consent.Approval
	fmt.Fprintf(out, "Fresh read consent: %s\nJob: %s\nJob key: %s\nRequest: %s\n", consent.ID, a.JobID, a.JobKey, a.RequestID)
	printField(out, "Saved consent status", consent.Status)
	printField(out, "Consent saved at", a.CreatedAt.UTC().Format(time.RFC3339Nano))
	printField(out, "Fixed expiry", a.ExpiresAt.UTC().Format(time.RFC3339Nano))
	printField(out, "Fixed step ceiling", fmt.Sprintf("%d bytes", a.StepByteLimit))
	printField(out, "Fresh day reservation cap", fmt.Sprintf("%d bytes", a.DailyReservedByteLimit))
	printField(out, "Fresh lifetime reservation cap", fmt.Sprintf("%d bytes", a.LifetimeReservedByteLimit))
	printField(out, "Fresh initial reserved bytes", fmt.Sprintf("%d bytes", a.InitialTotalReservedBytes))
	printField(out, "Saved clock high-water", consent.ClockHighWater.UTC().Format(time.RFC3339Nano))
	printField(out, "Expiry observed", consent.ExpiredObserved)
	if consent.Revocation != nil {
		printField(out, "Revoked at", consent.Revocation.RecordedAt.UTC().Format(time.RFC3339Nano))
	}
	printField(out, "Current read permission", "Not evaluated")
}

func printHashFreshReadConsentResult(out io.Writer, result HashFreshReadConsentResult) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: independent fresh-job read consent", "")
	switch result.Mode {
	case "approve":
		printResultBanner(guard, "FRESH READ CONSENT SAVED - NO CONTENT READ")
	case "revoke":
		printResultBanner(guard, "FRESH READ CONSENT REVOKED - NO CONTENT READ")
	default:
		printResultBanner(guard, "SAVED FRESH READ CONSENT - PERMISSION NOT EVALUATED")
	}
	printHashFreshReadConsent(guard, &result.ReadConsent)
	fmt.Fprintf(guard, "Frozen manual root: %q\n", string(result.ReadConsent.Approval.SourceLocator.RootPathBytes))
	printWrapped(guard, "This consent binds the exact fresh job, generation key, request and ordered keeper/copy scope. Original approvals and charges grant no fresh allowance. Exact retries keep the first consent, expiry and caps; they cannot renew it. No source files, configuration or inventory were opened, and no work was recovered or started.", "")
	if result.Mode == "revoke" {
		printWrapped(guard, "Revocation blocks later reservations after the writer lock is acquired. It cannot preempt a call that already holds the lock or a blocked kernel operation.", "")
	}
	printWrapped(guard, "Read consent grants no cleanup permission. Keep the same global options and private data directory when reopening this record with:", "")
	fmt.Fprintf(guard, "  rydd hash --show-job-read %s\n", result.ReadConsent.ID)
	if guard.err != nil {
		return fmt.Errorf("write fresh consent %s; inspect hash --show-job-read %s: %w", result.ReadConsent.ID, result.ReadConsent.ID, guard.err)
	}
	return nil
}
