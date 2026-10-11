package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

const savedSampleAccountingContract = "saved_sample_accounting_view_v1"
const samplesReplyLimit = 16 * 1024
const samplesArgumentLimit = 16 * 1024

// SavedSampleAccountingReport projects scalar saved history only. It does not
// expose historical target paths or evaluate source-read or cleanup authority.
type SavedSampleAccountingReport struct {
	Source                     string                            `json:"source"`
	Contract                   string                            `json:"contract"`
	CoreContract               string                            `json:"core_contract"`
	StoreID                    string                            `json:"store_id"`
	BatchID                    string                            `json:"batch_id"`
	InventoryID                string                            `json:"inventory_id"`
	Status                     string                            `json:"status"`
	TargetCount                int                               `json:"target_count"`
	PlannedBytes               int64                             `json:"planned_bytes"`
	Policy                     inventory.SampleReservationPolicy `json:"policy"`
	Attempt                    *SavedSampleAccountingAttempt     `json:"attempt"`
	SavedBudget                *inventory.SampleBudget           `json:"saved_budget"`
	CurrentPermissionEvaluated bool                              `json:"current_permission_evaluated"`
	ProvenanceVerified         bool                              `json:"provenance_verified"`
	ContentVerified            bool                              `json:"content_verified"`
	CurrentStateVerified       bool                              `json:"current_state_verified"`
	DuplicatesVerified         bool                              `json:"duplicates_verified"`
	Executable                 bool                              `json:"executable"`
	PhysicalIOVerified         bool                              `json:"physical_io_verified"`
	SharedHashBudget           bool                              `json:"shared_hash_budget"`
	EstimatedReclaimableBytes  *int64                            `json:"estimated_reclaimable_bytes"`
}

// Nullable usage deliberately has no omitempty: unknown is null, known zero is
// zero. Permanent reserved allowance is distinct from observed usage.
type SavedSampleAccountingAttempt struct {
	ID             string    `json:"id"`
	ReservationDay string    `json:"reservation_day_utc"`
	ReservedAt     time.Time `json:"reserved_at"`
	ReservedBytes  int64     `json:"reserved_bytes"`
	Status         string    `json:"status"`
	Code           string    `json:"code,omitempty"`
	UsageKnown     bool      `json:"usage_known"`
	RequestedBytes *int64    `json:"observed_requested_bytes"`
	ReadBytes      *int64    `json:"observed_read_bytes"`
	ElapsedNS      *int64    `json:"elapsed_ns"`
}

type samplesSelection struct{ base, batchID string }
type samplesReader interface {
	Snapshot(context.Context) (inventory.SampleSnapshot, error)
	Close() error
}
type samplesReaderOpener func(context.Context, string) (samplesReader, error)

func samplesHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := range s {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}

func samplesArguments(args []string) (samplesSelection, error) {
	bad := func() (samplesSelection, error) {
		return samplesSelection{}, usageError{errors.New("samples requires exactly one explicit global --data-dir and one --show 64-character lower-case hexadecimal batch ID; no other selectors")}
	}
	if len(args) > 8 {
		return bad()
	}
	total := 0
	for _, a := range args {
		if len(a) > samplesArgumentLimit-total {
			return bad()
		}
		total += len(a)
	}
	var selection samplesSelection
	baseCount, showCount := 0, 0
	command := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--json" {
			continue
		}
		if !command && a == "samples" {
			command = true
			continue
		}
		name, value, equals := strings.Cut(a, "=")
		isBase := !command && (name == "--data-dir" || name == "-data-dir")
		isShow := command && (name == "--show" || name == "-show")
		if !isBase && !isShow {
			return bad()
		}
		if !equals {
			if i+1 >= len(args) {
				return bad()
			}
			i++
			value = args[i]
		}
		if isBase {
			baseCount++
			selection.base = value
		} else {
			showCount++
			selection.batchID = value
		}
		if baseCount > 1 || showCount > 1 {
			return bad()
		}
	}
	if !command || baseCount != 1 || showCount != 1 || !samplesHex(selection.batchID) || len(selection.base) > 4096 || !filepath.IsAbs(selection.base) || filepath.Clean(selection.base) != selection.base || selection.base == string(filepath.Separator) || strings.ContainsRune(selection.base, 0) {
		return bad()
	}
	return selection, nil
}

// Recognize the new command before the legacy JSON scanner allocates its
// filtered slice. Only string headers are scanned here; the untouched argument
// list is checked against the token and byte limits by samplesArguments.
// Preserve the existing scanner's separated-value protection and JSON mode.
func samplesRawInvocation(args []string) (bool, bool) {
	selected, machine := false, false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			return selected || i+1 < len(args) && args[i+1] == "samples", machine
		}
		if a == "--json" {
			machine = true
			continue
		}
		if !selected && len(a) > 0 && a[0] != '-' {
			if a != "samples" {
				return false, false
			}
			selected = true
		}
		switch a {
		case "--data-dir", "-data-dir", "--root", "--exclude", "--limit", "--cursor", "--directory", "-directory", "-d", "--d", "-s", "--sleep", "--min-age-days", "--min-size-bytes", "--batches", "--show", "-show", "--from", "-from", "--observe", "--check", "--approve", "-approve", "--run", "-run", "--max-day-bytes", "-max-day-bytes", "--max-total-bytes", "-max-total-bytes", "--revoke", "-revoke", "--verify", "--inspect", "--capture", "--compare", "--work", "-work":
			if i+1 < len(args) {
				i++
			}
		}
	}
	return selected, machine
}

// Keep the existing global help shortcut. Values are skipped as the existing
// JSON scanner skips them, so a private path named --help is never a flag.
func samplesGlobalHelp(args []string) bool {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--data-dir" || a == "-data-dir" {
			i++
			continue
		}
		if strings.HasPrefix(a, "--data-dir=") || strings.HasPrefix(a, "-data-dir=") || a == "--json" || a == "--version" || a == "-version" || strings.HasPrefix(a, "--version=") || strings.HasPrefix(a, "-version=") {
			continue
		}
		return a == "--help" || a == "-help" || a == "--h" || a == "-h"
	}
	return false
}

func samplesSavedTime(t time.Time) bool {
	n := t.UnixNano()
	return t.Year() >= 1970 && t.Year() <= 2261 && n >= 0 && time.Unix(0, n).Equal(t)
}

func samplesProjection(s inventory.SampleSnapshot, exactID string) (SavedSampleAccountingReport, error) {
	if s.BatchID == "" || s.BatchID != exactID {
		return SavedSampleAccountingReport{}, inventory.ErrSampleBatchMissing
	}
	bad := func() (SavedSampleAccountingReport, error) {
		return SavedSampleAccountingReport{}, inventory.ErrSampleStoreCorrupt
	}
	if s.Source != "saved_sample_accounting" || s.Contract != inventory.SampleReservationContract || !samplesHex(s.StoreID) || !samplesHex(s.BatchID) || !samplesHex(s.InventoryID) || len(s.Targets) < 1 || len(s.Targets) > inventory.FileSampleTargetLimit || s.PlannedBytes < 1 || s.PlannedBytes > inventory.FileSampleCallByteLimit || s.Policy.Contract != inventory.SampleReservationContract || s.Policy.DailyReservedByteLimit < 1 || s.Policy.DailyReservedByteLimit > 1<<50 || s.Policy.LifetimeReservedByteLimit < 1 || s.Policy.LifetimeReservedByteLimit > 1<<50 || s.Budget == nil || s.ProvenanceVerified || s.ContentVerified || s.CurrentStateVerified || s.DuplicatesVerified || s.Executable || s.EstimatedReclaimableBytes != nil {
		return bad()
	}
	b := *s.Budget
	if !samplesSavedTime(b.MaxNow) || b.Day != b.MaxNow.UTC().Format(time.DateOnly) {
		return bad()
	}
	expected := inventory.SampleBudget{Day: b.Day, MaxNow: b.MaxNow}
	out := SavedSampleAccountingReport{Source: s.Source, Contract: savedSampleAccountingContract, CoreContract: s.Contract, StoreID: s.StoreID, BatchID: s.BatchID, InventoryID: s.InventoryID, Status: s.Status, TargetCount: len(s.Targets), PlannedBytes: s.PlannedBytes, Policy: s.Policy}
	if s.Attempt == nil {
		if s.Status != "ready" {
			return bad()
		}
	} else {
		a := s.Attempt
		if !samplesHex(a.ID) || !samplesSavedTime(a.ReservedAt) || a.ReservationDay != a.ReservedAt.UTC().Format(time.DateOnly) || a.ReservedAt.After(b.MaxNow) || a.ReservedBytes != s.PlannedBytes || a.Status != s.Status {
			return bad()
		}
		p := &SavedSampleAccountingAttempt{ID: a.ID, ReservationDay: a.ReservationDay, ReservedAt: a.ReservedAt, ReservedBytes: a.ReservedBytes, Status: a.Status, Code: a.Code}
		expected.TotalReservedBytes = a.ReservedBytes
		switch a.Status {
		case "reserved":
			if a.Code != "" || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil {
				return bad()
			}
			expected.TotalOutstandingReservedBytes = a.ReservedBytes
		case "interrupted_unknown":
			if a.Code != "interrupted_unknown" || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil {
				return bad()
			}
			expected.TotalUnknownReservedBytes = a.ReservedBytes
		case "settled":
			switch a.Code {
			case "observed", "blocked", "request_canceled", "sample_failed", "saved_inventory_changed", "storage_unavailable", "clock_rollback":
			default:
				return bad()
			}
			if a.RequestedBytes == nil || a.ReadBytes == nil || a.ElapsedNS == nil || *a.RequestedBytes < 0 || *a.RequestedBytes > a.ReservedBytes || *a.ReadBytes < 0 || *a.ReadBytes > *a.RequestedBytes || *a.ElapsedNS < 0 {
				return bad()
			}
			requested, read, elapsed := *a.RequestedBytes, *a.ReadBytes, *a.ElapsedNS
			p.UsageKnown = true
			p.RequestedBytes = &requested
			p.ReadBytes = &read
			p.ElapsedNS = &elapsed
			expected.TotalRequestedBytes = requested
			expected.TotalReadBytes = read
		default:
			return bad()
		}
		if a.ReservationDay == b.Day {
			expected.ReservedBytes = expected.TotalReservedBytes
			expected.RequestedBytes = expected.TotalRequestedBytes
			expected.ReadBytes = expected.TotalReadBytes
			expected.UnknownReservedBytes = expected.TotalUnknownReservedBytes
			expected.OutstandingReservedBytes = expected.TotalOutstandingReservedBytes
		}
		out.Attempt = p
	}
	if b != expected || b.ReservedBytes > s.Policy.DailyReservedByteLimit || b.TotalReservedBytes > s.Policy.LifetimeReservedByteLimit {
		return bad()
	}
	out.SavedBudget = &b
	return out, nil
}

func savedSamples(ctx context.Context, selection samplesSelection, open samplesReaderOpener) (SavedSampleAccountingReport, error) {
	if err := ctx.Err(); err != nil {
		return SavedSampleAccountingReport{}, err
	}
	if open == nil {
		open = func(ctx context.Context, base string) (samplesReader, error) {
			return inventory.OpenSampleReader(ctx, base)
		}
	}
	r, err := open(ctx, selection.base)
	if err != nil {
		if r != nil {
			err = errors.Join(err, r.Close())
		}
		return SavedSampleAccountingReport{}, errors.Join(err, ctx.Err())
	}
	if r == nil {
		return SavedSampleAccountingReport{}, errors.Join(errors.New("saved sample reader unavailable"), ctx.Err())
	}
	// The existing reader already took an opening-validation snapshot. This is
	// the one explicit coherent view snapshot; always close before projection.
	s, readErr := r.Snapshot(ctx)
	err = errors.Join(readErr, r.Close(), ctx.Err())
	if err != nil {
		return SavedSampleAccountingReport{}, err
	}
	report, err := samplesProjection(s, selection.batchID)
	err = errors.Join(err, ctx.Err())
	if err != nil {
		return SavedSampleAccountingReport{}, err
	}
	return report, nil
}

func samplesFailure(err error) (string, string, int) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled", "Saved sample accounting inspection was canceled; no accounting transition or source read was requested.", 1
	}
	var usage usageError
	if errors.As(err, &usage) {
		return "invalid_arguments", "Usage: rydd --data-dir ABSOLUTE_PRIVATE_BASE samples --show BATCH_ID [--json]. Use exactly one explicit base and full lower-case batch ID; no other selectors.", 2
	}
	if errors.Is(err, inventory.ErrSampleStoreCorrupt) {
		return "sample_accounting_invalid", "Saved sample accounting is invalid or incompatible; no partial saved view is returned.", 1
	}
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, inventory.ErrSampleBatchMissing) {
		return "not_found", "The exact existing sample batch is unavailable; no store was initialized.", 1
	}
	return "sample_accounting_unavailable", "The private saved sample accounting view is unavailable; no accounting transition or source read was requested.", 1
}

func samplesReply(report SavedSampleAccountingReport, machine bool) ([]byte, error) {
	var out bytes.Buffer
	var err error
	if machine {
		err = json.NewEncoder(&out).Encode(struct {
			Version int                         `json:"api_version"`
			OK      bool                        `json:"ok"`
			Command string                      `json:"command"`
			Samples SavedSampleAccountingReport `json:"samples"`
		}{APIVersion, true, "samples", report})
	} else {
		err = printSamplesReport(&out, report)
	}
	if err != nil || out.Len() > samplesReplyLimit {
		return nil, errors.Join(inventory.ErrSampleStoreCorrupt, err)
	}
	return out.Bytes(), nil
}

func samplesMachineFailure(ctx context.Context, out, errOut io.Writer, code, message string, exit int) int {
	// An already-ended context has a cancellation failure reply and exit1.
	// A context ended by writing this reply must veto the usage exit code.
	alreadyEnded := ctx.Err() != nil
	var body bytes.Buffer
	err := json.NewEncoder(&body).Encode(struct {
		Version int    `json:"api_version"`
		OK      bool   `json:"ok"`
		Command string `json:"command"`
		Error   struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{Version: APIVersion, Command: "samples", Error: struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message}})
	if err == nil && body.Len() <= 4096 {
		var n int
		n, err = out.Write(body.Bytes())
		if n != body.Len() && err == nil {
			err = io.ErrShortWrite
		}
	} else if err == nil {
		err = inventory.ErrSampleStoreCorrupt
	}
	if err != nil || !alreadyEnded && ctx.Err() != nil {
		fmt.Fprintln(errOut, "Saved sample accounting error reply failed or was canceled; output may be partial.")
		return 1
	}
	if ctx.Err() != nil {
		return 1
	}
	return exit
}

func runSamplesCLI(ctx context.Context, args []string, out, errOut io.Writer, machine bool, open samplesReaderOpener) int {
	selection, err := samplesArguments(args)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err == nil {
		err = ctx.Err()
	}
	var report SavedSampleAccountingReport
	if err == nil {
		report, err = savedSamples(ctx, selection, open)
	}
	var reply []byte
	if err == nil {
		reply, err = samplesReply(report, machine)
	}
	err = errors.Join(err, ctx.Err())
	if err != nil {
		code, message, exit := samplesFailure(err)
		if machine {
			return samplesMachineFailure(ctx, out, errOut, code, message, exit)
		}
		body := []byte(message + "\n")
		n, writeErr := errOut.Write(body)
		if writeErr != nil || n != len(body) || ctx.Err() != nil {
			return 1
		}
		return exit
	}
	n, writeErr := out.Write(reply)
	if n != len(reply) && writeErr == nil {
		writeErr = io.ErrShortWrite
	}
	if errors.Join(writeErr, ctx.Err()) != nil {
		fmt.Fprintln(errOut, "Saved sample accounting reply failed or was canceled; output may be partial. No accounting transition or source read was requested.")
		return 1
	}
	return 0
}

func printSamplesReport(out io.Writer, r SavedSampleAccountingReport) error {
	guard := &reviewOutput{writer: out}
	printWrapped(guard, "Saga — Rydd: saved sample accounting", "")
	printResultBanner(guard, "SAVED SAMPLE ACCOUNTING - NO READ APPROVAL")
	printField(guard, "Saved batch", r.BatchID)
	printField(guard, "Private accounting identity", r.StoreID)
	printField(guard, "Saved inventory identity", r.InventoryID)
	printField(guard, "Saved state", r.Status)
	printField(guard, "Historical target count", r.TargetCount)
	printField(guard, "Planned allowance", humanBytes(r.PlannedBytes))
	printField(guard, "Frozen daily reservation limit", humanBytes(r.Policy.DailyReservedByteLimit))
	printField(guard, "Frozen lifetime reservation limit", humanBytes(r.Policy.LifetimeReservedByteLimit))
	if a := r.Attempt; a == nil {
		printWrapped(guard, "No attempt is recorded. Planned bytes are not a reservation charge or read permission.", "")
	} else {
		printField(guard, "Saved attempt", a.ID)
		printField(guard, "Attempt reservation day (UTC)", a.ReservationDay)
		printField(guard, "Saved reservation time", a.ReservedAt.UTC().Format(time.RFC3339Nano))
		printField(guard, "Permanent attempt allowance", humanBytes(a.ReservedBytes))
		if a.UsageKnown {
			printField(guard, "Observed requested bytes, known", humanBytes(*a.RequestedBytes))
			printField(guard, "Observed returned bytes, known", humanBytes(*a.ReadBytes))
			printField(guard, "Observed elapsed, known", time.Duration(*a.ElapsedNS))
			printWrapped(guard, "Known recorded usage can be zero or positive. It does not verify current contents or prove that an ephemeral sample report was published.", "")
		} else {
			printField(guard, "Observed requested bytes", "Unknown")
			printField(guard, "Observed returned bytes", "Unknown")
			printField(guard, "Observed elapsed", "Unknown")
			if a.Status == "reserved" {
				printWrapped(guard, "An unsettled reservation is saved; whether a process is active is unknown. The full allowance stays charged.", "")
			} else {
				printWrapped(guard, "The full interrupted allowance remains charged. Known aggregate usage omits this unknown attempt.", "")
			}
		}
	}
	if b := r.SavedBudget; b != nil {
		printField(guard, "Saved reservation day (UTC)", b.Day)
		printField(guard, "Saved clock high-water", b.MaxNow.UTC().Format(time.RFC3339Nano))
		printField(guard, "Day reserved", humanBytes(b.ReservedBytes))
		printField(guard, "Day requested, known", humanBytes(b.RequestedBytes))
		printField(guard, "Day returned, known", humanBytes(b.ReadBytes))
		printField(guard, "Day interrupted charge", humanBytes(b.UnknownReservedBytes))
		printField(guard, "Day unsettled charge", humanBytes(b.OutstandingReservedBytes))
		printField(guard, "Total reserved", humanBytes(b.TotalReservedBytes))
		printField(guard, "Total requested, known", humanBytes(b.TotalRequestedBytes))
		printField(guard, "Total returned, known", humanBytes(b.TotalReadBytes))
		printField(guard, "Total interrupted charge", humanBytes(b.TotalUnknownReservedBytes))
		printField(guard, "Total unsettled charge", humanBytes(b.TotalOutstandingReservedBytes))
	}
	printWrapped(guard, "Unknown and unsettled charges are subsets of permanent reserved allowance; do not add them again. The saved day and high-water are not necessarily today and establish no remaining quota or current read permission.", "")
	printWrapped(guard, "Only this private sample store and immutable batch are covered. Creating another store creates another accounting identity and local cap. Hash stores, metadata, configuration and SQLite reads and physical device I/O are excluded.", "")
	printWrapped(guard, "This reads saved accounting only. It samples no admission clock, loads no configuration, opens no inventory or source path, recovers no attempt, and performs no sampling or cleanup. Private SQLite and WAL metadata I/O may occur. No read approval is created.", "")
	return guard.err
}
