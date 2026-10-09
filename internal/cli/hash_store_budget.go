package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

type HashStoreBudgetReport struct {
	Contract    string                         `json:"contract"`
	Activated   bool                           `json:"activated"`
	SavedBudget *inventory.HashStoreReadBudget `json:"saved_budget"`
}

func checkCLIHashReadDailyLimit(ctx context.Context, limit, minimum int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if limit < 1 || limit > 1<<50 || minimum < 0 || minimum > 64 {
		return inventory.ErrHashReadExecutionLimits
	}
	if minimum > limit {
		return hashRunDeferredError{code: "configured_daily_byte_limit", error: fmt.Errorf("configured daily reservation ceiling cannot fit the next minimum durable quantum; inspect hashes --store-budget before another explicit run: %w", inventory.ErrHashDeferred)}
	}
	return nil
}

func printHashStoreBudgetReport(out io.Writer, r HashStoreBudgetReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved shared read accounting", "")
	if r.SavedBudget == nil {
		printResultBanner(guard, "SHARED READ ACCOUNTING NOT ACTIVATED")
		printWrapped(guard, "This store has no activated shared accounting record. Historical selection and fresh-job reservations may exist. A later explicit configured run must account for them before admitting new reads. This view starts no work and changes no records.", "")
	} else {
		printHashStoreReadBudget(guard, r.SavedBudget, nil)
	}
	if guard.err != nil {
		return fmt.Errorf("write saved shared read accounting: %w", guard.err)
	}
	return nil
}

func printHashStoreReadBudget(out io.Writer, b *inventory.HashStoreReadBudget, configuredLimit *int64) {
	if b == nil {
		return
	}
	printResultBanner(out, "SHARED HASH-STORE RESERVATIONS - SAVED ACCOUNTING")
	printField(out, "Saved reservation day (UTC)", b.Day)
	printField(out, "Saved clock high-water", b.MaxNow.UTC().Format(time.RFC3339Nano))
	printField(out, "Day reserved", humanBytes(b.ReservedBytes))
	printField(out, "Day requested, known", humanBytes(b.RequestedBytes))
	printField(out, "Day read, known", humanBytes(b.ReadBytes))
	printField(out, "Day interrupted charge", humanBytes(b.UnknownReservedBytes))
	printField(out, "Day unsettled charge", humanBytes(b.OutstandingReservedBytes))
	printField(out, "Total reserved", humanBytes(b.TotalReservedBytes))
	printField(out, "Total requested, known", humanBytes(b.TotalRequestedBytes))
	printField(out, "Total read, known", humanBytes(b.TotalReadBytes))
	printField(out, "Total interrupted charge", humanBytes(b.TotalUnknownReservedBytes))
	printField(out, "Total unsettled charge", humanBytes(b.TotalOutstandingReservedBytes))
	printField(out, "Saved projection day (UTC)", b.ProjectedDay)
	printField(out, "Saved projection high-water", b.ProjectedMaxNow.UTC().Format(time.RFC3339Nano))
	printField(out, "Saved projection reserved", humanBytes(b.ProjectedReservedBytes))
	if configuredLimit != nil {
		printField(out, "Configured cap for this run", humanBytes(*configuredLimit))
	}
	printWrapped(out, "These counters cover original work and every fresh job in this private hash store. Every reservation stays charged, including interrupted and unsettled attempts. Known counters omit attempts with unknown usage. The separate projection includes newer saved consent clocks; it samples no current clock. This saved view evaluates no current configured limit, remaining quota or read permission.", "")
	printWrapped(out, "Another explicit run must satisfy the current configured ceiling and its unchanged consent limits. Changing configuration cannot renew consent or refund charges. Other stores, sampling, metadata, configuration and SQLite reads are excluded. These counters do not measure physical I/O or reads per wall-clock day. No cleanup permission follows.", "")
}
