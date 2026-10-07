package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type HashConsentResult struct {
	Mode        string                    `json:"mode"`
	ReadConsent inventory.HashReadConsent `json:"read_consent"`
}

type HashStepReport struct {
	Mode          string                    `json:"mode"`
	ApprovalID    string                    `json:"approval_id"`
	StepByteLimit int64                     `json:"step_byte_limit"`
	BudgetScope   string                    `json:"budget_scope"`
	Result        inventory.HashRunResult   `json:"result"`
	ReadConsent   inventory.HashReadConsent `json:"read_consent"`
}

type hashRunDeferredError struct {
	code string
	error
}

func (e hashRunDeferredError) Unwrap() error { return e.error }

func parseHashReservationCap(value, name string) (int64, error) {
	cap, err := strconv.ParseInt(value, 10, 64)
	if err != nil || cap < 1 || cap > 1<<50 || strconv.FormatInt(cap, 10) != value {
		return 0, usageError{fmt.Errorf("%s requires a canonical positive decimal byte count from 1 to 1125899906842624", name)}
	}
	return cap, nil
}

func hashReadCommand(ctx context.Context, mode, id string, dayCap, totalCap int64, paths config.Paths) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Read an existing exact record before any writer can initialize or migrate
	// hashing storage. Missing storage or IDs must not create it.
	reader, err := inventory.OpenHashReader(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage is required for --%s; it was not initialized: %w", mode, err)}
		}
		return nil, err
	}
	var proposal inventory.HashProposal
	var consent inventory.HashReadConsent
	if mode == "approve" {
		proposal, err = reader.Proposal(ctx, id)
	} else {
		consent, err = reader.Approval(ctx, id)
		if err == nil && mode == "run" {
			proposal, err = reader.Proposal(ctx, consent.Approval.SelectionID)
		}
	}
	closeErr := reader.Close()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, inventory.ErrHashReadApprovalMissing) {
			err = missingHashError{fmt.Errorf("the exact saved hash %s record is unavailable: %w", mode, err)}
		}
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mode == "run" {
		return runConsentedHash(ctx, id, paths, proposal)
	}
	if mode == "approve" && proposal.SourceLocator == nil {
		return nil, inventory.ErrHashReadBinding
	}
	writer, err := inventory.OpenExistingHashSelectionWriter(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage became unavailable: %w", err)}
		}
		return nil, err
	}
	if mode == "approve" {
		consent, err = writer.ApproveRead(ctx, inventory.HashReadApprovalRequest{
			StoreID: proposal.StoreID, SelectionID: proposal.SelectionID, InventoryID: proposal.InventoryID,
			SourceLocator: *proposal.SourceLocator, DailyReservedByteLimit: dayCap, LifetimeReservedByteLimit: totalCap, ConfirmFullFileRead: true,
		})
	} else {
		consent, err = writer.RevokeRead(ctx, id)
	}
	closeErr = writer.Close()
	if err != nil {
		if errors.Is(err, inventory.ErrHashReadLimits) || errors.Is(err, inventory.ErrHashReadConfirmation) {
			err = usageError{err}
		}
		return nil, fmt.Errorf("hash --%s failed (publication may have completed; inspect hash --show or hashes before retrying): %w", mode, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close hash --%s (publication may have completed; inspect saved records): %w", mode, closeErr)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("hash --%s reply canceled (publication may have completed; inspect saved records): %w", mode, err)
	}
	return HashConsentResult{Mode: mode, ReadConsent: consent}, nil
}

func runConsentedHash(ctx context.Context, id string, paths config.Paths, proposal inventory.HashProposal) (HashStepReport, error) {
	locator := proposal.SourceLocator
	if locator == nil || locator.Kind != "manual_inventory_v1" {
		return HashStepReport{}, inventory.ErrHashReadBinding
	}
	root := string(locator.RootPathBytes)
	derived := manualState(paths, root)
	// Configuration uses a held descriptor; SQLite storage receives a metadata
	// preflight before its pathname is opened. Both refuse known selected aliases.
	cfg, err := loadHashRunConfig(ctx, paths, proposal)
	if err != nil {
		return HashStepReport{}, err
	}
	if err = checkHashSourceStorage(ctx, derived, proposal); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("the frozen manual inventory is unavailable; no replacement inventory was initialized: %w", err)}
		}
		return HashStepReport{}, err
	}
	source, err := state.OpenReader(ctx, derived)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("the frozen manual inventory became unavailable; no replacement inventory was initialized: %w", err)}
		}
		return HashStepReport{}, err
	}
	defer source.Close()
	scanner, err := inventory.New([]string{root}, cfg.Excludes, []string{paths.StateDir, paths.ConfigFile})
	if err != nil {
		return HashStepReport{}, err
	}
	defer scanner.Close()
	if err := ctx.Err(); err != nil {
		return HashStepReport{}, err
	}
	// Normal writer opening can recover a previous interruption as metadata.
	// It cannot make a new source read: only the single guarded call below can.
	writer, err := inventory.OpenExistingHashWriter(ctx, paths.StateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err = missingHashError{fmt.Errorf("existing saved hash storage became unavailable: %w", err)}
		}
		return HashStepReport{}, err
	}
	result, err := writer.RunConsented(ctx, id, source, scanner)
	var consent inventory.HashReadConsent
	if err == nil {
		consent, err = writer.Approval(ctx, id)
	}
	closeErr := writer.Close()
	if err != nil {
		if errors.Is(err, inventory.ErrHashDeferred) {
			switch result.Code {
			case "daily_byte_limit", "lifetime_byte_limit", "durable_quantum":
				err = hashRunDeferredError{code: result.Code, error: fmt.Errorf("hash step deferred: %s: %w", result.Code, err)}
			}
		}
		return HashStepReport{}, fmt.Errorf("hash --run failed; read usage or publication may be incomplete; inspect hashes before another explicit run: %w", err)
	}
	if closeErr != nil {
		return HashStepReport{}, fmt.Errorf("close hash --run; publication may have completed; inspect hashes: %w", closeErr)
	}
	if err := ctx.Err(); err != nil {
		return HashStepReport{}, fmt.Errorf("hash step reply canceled; publication may have completed; inspect hashes: %w", err)
	}
	return HashStepReport{Mode: "run", ApprovalID: id, StepByteLimit: inventory.FileHashStepByteLimit, BudgetScope: "whole_saved_selection", Result: result, ReadConsent: consent}, nil
}
