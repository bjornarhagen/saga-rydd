package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type samplesCLIFixture struct {
	base, root, sourceDir string
	store                 *inventory.SampleStore
	source                *state.Store
	scanner               *inventory.Scanner
	batch                 inventory.SampleSnapshot
}

// Only generated files are scanned. CreateBatch and the real public sampler
// supply the known-usage fixtures; no hash approval or imported digest is used.
func newSamplesCLIFixture(t *testing.T) *samplesCLIFixture {
	t.Helper()
	canonical := func() string {
		p, e := filepath.EvalSymlinks(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	f := &samplesCLIFixture{base: filepath.Join(canonical(), "private sample ledger"), root: canonical(), sourceDir: filepath.Join(canonical(), "inventory")}
	for _, name := range []string{"generated-one", "generated-two"} {
		if e := os.WriteFile(filepath.Join(f.root, name), []byte("abc"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	var err error
	f.scanner, err = inventory.New([]string{f.root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.scanner.Close)
	f.source, err = state.OpenWriter(context.Background(), f.sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.source.Close() })
	ctx := context.Background()
	if err = f.source.SyncRoots(ctx, []string{f.root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = f.source.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = f.source.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for i := 0; i < 64; i++ {
		job, e := f.source.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if job == nil {
			finished = true
			break
		}
		batch, e := f.scanner.Next(ctx, *job)
		if e != nil || batch.Fault != "" {
			t.Fatal(batch, e)
		}
		if e = f.source.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("bounded generated sample inventory did not finish")
	}
	before, err := f.source.Summary(ctx)
	if err != nil || before.PendingJobs != 0 || before.RunningJobs != 0 {
		t.Fatal(before, err)
	}
	metrics := f.scanner.Metrics()
	drained := false
	for i := 0; i < 512; i++ {
		worked, e := f.source.RetireSubtrees(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if worked {
			continue
		}
		pending, e := f.source.HasSubtreeRetirement(ctx)
		if e != nil || pending {
			t.Fatal(pending, e)
		}
		drained = true
		break
	}
	after, err := f.source.Summary(ctx)
	if !drained || err != nil || before.Entries != after.Entries || after.PendingJobs != 0 || after.RunningJobs != 0 || f.scanner.Metrics() != metrics {
		t.Fatal("saved maintenance changed generated observations", before, after, err)
	}
	candidates, err := f.source.SameSizeCandidates(ctx, 20, "", 1)
	if err != nil || len(candidates.Bands) != 1 || len(candidates.Bands[0].Files) != 2 {
		t.Fatal(candidates, err)
	}
	f.store, err = inventory.OpenSampleWriter(ctx, f.base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.store.Close() })
	f.batch, err = f.store.CreateBatch(ctx, f.source, candidates.InventoryID, candidates.Bands[0].Files, inventory.SampleReservationPolicy{Contract: inventory.SampleReservationContract, DailyReservedByteLimit: 1024, LifetimeReservedByteLimit: 1024})
	if err != nil || f.batch.PlannedBytes != 6 || len(f.batch.Targets) != 2 {
		t.Fatal(f.batch, err)
	}
	if err = os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("invalid [ configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	return f
}

func samplesCLISnapshot(t *testing.T, f *samplesCLIFixture) inventory.SampleSnapshot {
	t.Helper()
	s, e := f.store.Snapshot(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return s
}

// Reserved/unknown are explicit saved-state projection fixtures. Inserting
// these rows is not evidence that a reservation, crash or recovery ran.
func samplesCLIUnknownFixture(t *testing.T, f *samplesCLIFixture, status string) {
	t.Helper()
	b := f.batch.Budget
	if b == nil {
		t.Fatal("missing generated budget")
	}
	outcome, unknown, outstanding := "", int64(0), f.batch.PlannedBytes
	if status == "interrupted_unknown" {
		outcome, unknown, outstanding = status, outstanding, 0
	}
	db, e := sql.Open("sqlite", filepath.Join(f.base, "samples", "samples.sqlite3"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	_, e = tx.Exec("INSERT INTO sample_attempt VALUES(1,?,?,?,?,?,?,?,?,?,?,?)", f.batch.StoreID, f.batch.BatchID, strings.Repeat("e", 64), b.Day, b.MaxNow.UnixNano(), f.batch.PlannedBytes, status, outcome, nil, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	_, e = tx.Exec("UPDATE sample_budget SET reserved_bytes=?,unknown_bytes=?,outstanding_bytes=?,total_reserved_bytes=?,total_unknown_bytes=?,total_outstanding_bytes=? WHERE id=1", f.batch.PlannedBytes, unknown, outstanding, f.batch.PlannedBytes, unknown, outstanding)
	if e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}

type samplesFakeReader struct {
	snapshot          inventory.SampleSnapshot
	readErr, closeErr error
	reads, closes     int
	onRead, onClose   func()
}

func (r *samplesFakeReader) Snapshot(context.Context) (inventory.SampleSnapshot, error) {
	r.reads++
	if r.onRead != nil {
		r.onRead()
	}
	return r.snapshot, r.readErr
}
func (r *samplesFakeReader) Close() error {
	r.closes++
	if r.onClose != nil {
		r.onClose()
	}
	return r.closeErr
}

func samplesProjectionFixture(status string) inventory.SampleSnapshot {
	at := time.Date(2020, 2, 3, 4, 5, 6, 0, time.UTC)
	s := inventory.SampleSnapshot{Source: "saved_sample_accounting", Contract: inventory.SampleReservationContract, StoreID: strings.Repeat("a", 64), BatchID: strings.Repeat("b", 64), InventoryID: strings.Repeat("c", 64), Status: status, Targets: make([]inventory.SavedFileTarget, 1), Policy: inventory.SampleReservationPolicy{Contract: inventory.SampleReservationContract, DailyReservedByteLimit: 1024, LifetimeReservedByteLimit: 1024}, PlannedBytes: 6, Budget: &inventory.SampleBudget{Day: at.Format(time.DateOnly), MaxNow: at}}
	if status == "ready" {
		return s
	}
	s.Attempt = &inventory.SampleAttempt{ID: strings.Repeat("d", 64), ReservationDay: s.Budget.Day, ReservedAt: at, ReservedBytes: 6, Status: status}
	s.Budget.ReservedBytes = 6
	s.Budget.TotalReservedBytes = 6
	switch status {
	case "reserved":
		s.Budget.OutstandingReservedBytes = 6
		s.Budget.TotalOutstandingReservedBytes = 6
	case "interrupted_unknown":
		s.Attempt.Code = status
		s.Budget.UnknownReservedBytes = 6
		s.Budget.TotalUnknownReservedBytes = 6
	case "settled":
		requested, read, elapsed := int64(0), int64(0), int64(0)
		s.Attempt.Code = "sample_failed"
		s.Attempt.RequestedBytes = &requested
		s.Attempt.ReadBytes = &read
		s.Attempt.ElapsedNS = &elapsed
	}
	return s
}

func samplesCLIError(t *testing.T, code int, out, diagnostic, want string, exit int) {
	t.Helper()
	var envelope struct {
		Version int                            `json:"api_version"`
		OK      bool                           `json:"ok"`
		Command string                         `json:"command"`
		Error   struct{ Code, Message string } `json:"error"`
	}
	if e := json.Unmarshal([]byte(out), &envelope); e != nil || code != exit || diagnostic != "" || envelope.Version != 1 || envelope.OK || envelope.Command != "samples" || envelope.Error.Code != want {
		t.Fatal(code, out, diagnostic, e)
	}
	if strings.Contains(out, `"samples":`) || len(envelope.Error.Message) > 4096 {
		t.Fatal("partial or oversized failure", out)
	}
}

func samplesCLIReport(t *testing.T, code int, out, diagnostic string) SavedSampleAccountingReport {
	t.Helper()
	var envelope struct {
		Version int                         `json:"api_version"`
		OK      bool                        `json:"ok"`
		Command string                      `json:"command"`
		Samples SavedSampleAccountingReport `json:"samples"`
	}
	if e := json.Unmarshal([]byte(out), &envelope); e != nil || code != 0 || diagnostic != "" || envelope.Version != 1 || !envelope.OK || envelope.Command != "samples" || len(out) > samplesReplyLimit {
		t.Fatal(code, out, diagnostic, e)
	}
	r := envelope.Samples
	if r.Source != "saved_sample_accounting" || r.Contract != savedSampleAccountingContract || r.CoreContract != inventory.SampleReservationContract || r.CurrentPermissionEvaluated || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.PhysicalIOVerified || r.SharedHashBudget || r.EstimatedReclaimableBytes != nil {
		t.Fatal("saved sample view asserted authority", out)
	}
	return r
}

func TestSamplesCLIArgumentsBeforeStorage(t *testing.T) {
	base, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	base = filepath.Join(base, "absent")
	id := strings.Repeat("a", 64)
	valid := []string{"--data-dir", base, "samples", "--show", id}
	cases := []struct {
		name string
		args []string
	}{
		{"missing_base", []string{"samples", "--show", id}},
		{"relative_base", []string{"--data-dir", "relative", "samples", "--show", id}},
		{"unclean_base", []string{"--data-dir", base + "/../alias", "samples", "--show", id}},
		{"root_base", []string{"--data-dir", "/", "samples", "--show", id}},
		{"nul_base", []string{"--data-dir", base + "\x00", "samples", "--show", id}},
		{"long_base", []string{"--data-dir", "/" + strings.Repeat("x", 4096), "samples", "--show", id}},
		{"empty_base", []string{"--data-dir=", "samples", "--show", id}},
		{"repeat_base", []string{"--data-dir", base, "--data-dir=" + base, "samples", "--show", id}},
		{"mixed_base", []string{"--data-dir", base, "-data-dir=" + base, "samples", "--show", id}},
		{"base_after_command", []string{"samples", "--data-dir", base, "--show", id}},
		{"missing_show", valid[:3]},
		{"short_id", append(append([]string{}, valid[:4]...), "a")},
		{"uppercase_id", append(append([]string{}, valid[:4]...), strings.Repeat("A", 64))},
		{"empty_id", append(append([]string{}, valid[:3]...), "--show=")},
		{"invalid_id", append(append([]string{}, valid[:4]...), strings.Repeat("g", 64))},
		{"repeat_show", append(append([]string{}, valid...), "--show="+id)},
		{"mixed_show", append(append([]string{}, valid...), "-show="+id)},
		{"separator", append(append([]string{}, valid...), "--")},
		{"positional", append(append([]string{}, valid...), "extra")},
		{"cursor", append(append([]string{}, valid...), "--cursor=foreign")},
		{"run", append(append([]string{}, valid...), "--run")},
		{"version", append([]string{"--version"}, valid...)},
		{"version_false", append([]string{"--version=false"}, valid...)},
		{"json_as_show_value", append(append([]string{}, valid[:4]...), "--json")},
		{"unknown_global", append([]string{"--foreign"}, valid...)},
		{"too_many_tokens", append(append([]string{}, valid...), "--json", "--json", "--json", "--json")},
		{"too_many_bytes", append(append([]string{}, valid[:4]...), strings.Repeat("a", samplesArgumentLimit+1))},
	}
	for _, tc := range cases {
		for _, machine := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/machine_%t", tc.name, machine), func(t *testing.T) {
				args := append([]string{}, tc.args...)
				if machine {
					args = append([]string{"--json"}, args...)
				}
				calls := 0
				open := func(context.Context, string) (samplesReader, error) {
					calls++
					return nil, errors.New("unexpected opener")
				}
				var out, diag bytes.Buffer
				code := runSamplesCLI(context.Background(), args, &out, &diag, machine, open)
				if calls != 0 || code != 2 {
					t.Fatal("invalid selection reached storage", calls, code, out.String(), diag.String())
				}
				if machine {
					samplesCLIError(t, code, out.String(), diag.String(), "invalid_arguments", 2)
				}
				out.Reset()
				diag.Reset()
				code = Run(context.Background(), args, &out, &diag)
				if code != 2 {
					t.Fatal("public routing bypassed strict samples selection", code, out.String(), diag.String())
				}
				if machine {
					samplesCLIError(t, code, out.String(), diag.String(), "invalid_arguments", 2)
				}
				if strings.Contains(out.String()+diag.String(), base) || strings.Contains(out.String()+diag.String(), strings.Repeat("a", 128)) {
					t.Fatal("argument diagnostic disclosed raw private input")
				}
			})
		}
	}
	for _, args := range [][]string{{"--data-dir=" + base, "samples", "-show=" + id}, {"-data-dir", base, "samples", "-show", id}, {"--json", "--data-dir", base, "samples", "--show=" + id}, append(append([]string{}, valid...), "--json", "--json", "--json")} {
		if s, err := samplesArguments(args); err != nil || s.base != base || s.batchID != id {
			t.Fatal("supported spelling or exact 8-token bound refused", args, s, err)
		}
	}
	// This is caller-owned malformed argv. Public routing must select the
	// bounded parser before the legacy scanner clones this large token list.
	large := append([]string{}, valid...)
	for i := 0; i < 4096; i++ {
		large = append(large, "--json")
	}
	var out, diag bytes.Buffer
	code := Run(context.Background(), large, &out, &diag)
	samplesCLIError(t, code, out.String(), diag.String(), "invalid_arguments", 2)
	for _, tc := range []struct {
		args              []string
		selected, machine bool
	}{
		{[]string{"samples", "--show", "--json"}, true, false},
		{[]string{"--data-dir", "--json", "samples"}, true, false},
		{[]string{"--json", "samples", "--show", "--json"}, true, true},
		{[]string{"hashes", "--show", "samples", "--json"}, false, false},
		{[]string{"--show", "samples", "--version"}, false, false},
	} {
		if selected, machine := samplesRawInvocation(tc.args); selected != tc.selected || machine != tc.machine {
			t.Fatal("early command routing changed protected values or an old command", tc.args, selected, machine)
		}
	}
	if _, e := os.Lstat(base); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("argument checks initialized missing storage", e)
	}
}

func TestSamplesCLIKnownUnknownAndZeroUsage(t *testing.T) {
	for _, kind := range []string{"ready", "reserved", "interrupted_unknown", "settled_zero", "settled_positive"} {
		t.Run(kind, func(t *testing.T) {
			f := newSamplesCLIFixture(t)
			switch kind {
			case "reserved", "interrupted_unknown":
				samplesCLIUnknownFixture(t, f, kind)
			case "settled_zero", "settled_positive":
				// Keep the scanner open so this exercises an acknowledged public
				// reservation and the real sampler. A missing generated root
				// blocks both selected paths before any source read is issued.
				if kind == "settled_zero" {
					if e := os.Rename(f.root, f.root+".unavailable"); e != nil {
						t.Fatal(e)
					}
				}
				r, e := f.store.RunSamples(context.Background(), f.batch.BatchID, f.source, f.scanner)
				if !r.UsageKnown || !r.UsageDurable || r.ReservedBytes != 6 || e != nil || kind == "settled_positive" && (r.Usage.RequestedBytes != 6 || r.Usage.ReadBytes != 6) || kind == "settled_zero" && (r.Code != "blocked" || r.Report.Status != "blocked" || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0) {
					t.Fatal("fixture did not produce genuine known usage", r, e)
				}
			}
			before := samplesCLISnapshot(t, f)
			var out, diag bytes.Buffer
			code := Run(context.Background(), []string{"--data-dir", f.base, "samples", "--show", f.batch.BatchID, "--json"}, &out, &diag)
			report := samplesCLIReport(t, code, out.String(), diag.String())
			if report.BatchID != before.BatchID || report.TargetCount != 2 || !reflect.DeepEqual(report.SavedBudget, before.Budget) {
				t.Fatal("saved scalar history changed", report, before)
			}
			if kind == "ready" {
				if report.Attempt != nil || !strings.Contains(out.String(), `"attempt":null`) || report.SavedBudget.TotalReservedBytes != 0 {
					t.Fatal(out.String())
				}
			} else {
				a := report.Attempt
				if a == nil || a.ReservedBytes != 6 || report.SavedBudget.TotalReservedBytes != 6 {
					t.Fatal("permanent charge lost", report)
				}
				known := strings.HasPrefix(kind, "settled_")
				if a.UsageKnown != known {
					t.Fatal(a)
				}
				if known {
					if a.RequestedBytes == nil || a.ReadBytes == nil || a.ElapsedNS == nil || *a.RequestedBytes != *before.Attempt.RequestedBytes || *a.ReadBytes != *before.Attempt.ReadBytes || *a.ElapsedNS != *before.Attempt.ElapsedNS {
						t.Fatal("known counters were inferred or lost", a)
					}
				} else {
					for _, key := range []string{"observed_requested_bytes", "observed_read_bytes", "elapsed_ns"} {
						if !strings.Contains(out.String(), `"`+key+`":null`) {
							t.Fatal("unknown usage fabricated zero or omitted field", key, out.String())
						}
					}
				}
			}
			if !reflect.DeepEqual(before, samplesCLISnapshot(t, f)) {
				t.Fatal("saved inspection changed logical records")
			}
		})
	}
}

func TestSamplesCLIOfflineReadOnlyAndExactBatch(t *testing.T) {
	f := newSamplesCLIFixture(t)
	before := samplesCLISnapshot(t, f)
	f.scanner.Close()
	if e := f.source.Close(); e != nil {
		t.Fatal(e)
	}
	for _, p := range []string{f.root, f.sourceDir} {
		if e := os.Rename(p, p+".offline"); e != nil {
			t.Fatal(e)
		}
	}
	// The writer remains open and owns its exclusive lock. Readers must neither
	// acquire that writer lock nor call an inventory/configuration/source API.
	for _, machine := range []bool{false, true} {
		t.Run(fmt.Sprintf("machine_%t", machine), func(t *testing.T) {
			var out, diag bytes.Buffer
			args := []string{"--data-dir", f.base, "samples", "--show", f.batch.BatchID}
			if machine {
				args = append(args, "--json")
			}
			if code := Run(context.Background(), args, &out, &diag); code != 0 || diag.Len() != 0 {
				t.Fatal("offline saved reader failed", code, out.String(), diag.String())
			}
			if !reflect.DeepEqual(before, samplesCLISnapshot(t, f)) {
				t.Fatal("offline view evaluated time or recovered records")
			}
		})
	}
	var out, diag bytes.Buffer
	wrong := strings.Repeat("f", 64)
	if wrong == f.batch.BatchID {
		wrong = strings.Repeat("0", 64)
	}
	code := Run(context.Background(), []string{"--data-dir", f.base, "samples", "--show", wrong, "--json"}, &out, &diag)
	samplesCLIError(t, code, out.String(), diag.String(), "not_found", 1)
	for _, secret := range []string{before.BatchID, before.StoreID, before.InventoryID, f.root, f.sourceDir, f.base} {
		if strings.Contains(out.String()+diag.String(), secret) {
			t.Fatal("wrong exact batch disclosed another batch", secret)
		}
	}
	emptyBase, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	emptyBase = filepath.Join(emptyBase, "empty")
	empty, e := inventory.OpenSampleWriter(context.Background(), emptyBase)
	if e != nil {
		t.Fatal(e)
	}
	defer empty.Close()
	out.Reset()
	diag.Reset()
	code = Run(context.Background(), []string{"--data-dir", emptyBase, "samples", "--show", wrong, "--json"}, &out, &diag)
	samplesCLIError(t, code, out.String(), diag.String(), "not_found", 1)
	if !reflect.DeepEqual(before, samplesCLISnapshot(t, f)) {
		t.Fatal("wrong-batch view changed saved records")
	}
}

func TestSamplesCLIReadCloseAndCancellation(t *testing.T) {
	readCause, closeCause := errors.New("private read failure"), errors.New("private close failure")
	for _, kind := range []string{"success", "read_and_close", "close", "missing_batch_and_close", "cancel_after_read", "cancel_after_close", "open_and_close"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &samplesFakeReader{snapshot: samplesProjectionFixture("ready")}
			var openErr error
			switch kind {
			case "read_and_close":
				r.readErr = readCause
				r.closeErr = closeCause
			case "close":
				r.closeErr = closeCause
			case "missing_batch_and_close":
				r.snapshot.BatchID = ""
				r.closeErr = closeCause
			case "cancel_after_read":
				r.readErr = errors.Join(readCause, os.ErrNotExist)
				r.closeErr = closeCause
				r.onRead = cancel
			case "cancel_after_close":
				r.closeErr = closeCause
				r.onClose = cancel
			case "open_and_close":
				openErr = readCause
				r.closeErr = closeCause
			}
			opens := 0
			open := func(context.Context, string) (samplesReader, error) { opens++; return r, openErr }
			result, err := savedSamples(ctx, samplesSelection{base: "/explicit/private", batchID: strings.Repeat("b", 64)}, open)
			if opens != 1 || r.closes != 1 || r.reads != map[bool]int{true: 0, false: 1}[kind == "open_and_close"] {
				t.Fatal("wrong reader lifecycle", opens, r)
			}
			if kind == "success" {
				if err != nil || result.BatchID == "" {
					t.Fatal(result, err)
				}
				return
			}
			if err == nil || !reflect.DeepEqual(result, SavedSampleAccountingReport{}) {
				t.Fatal("failure returned partial projection", result, err)
			}
			if r.closeErr != nil && !errors.Is(err, closeCause) || (r.readErr != nil || openErr != nil) && !errors.Is(err, readCause) {
				t.Fatal("joined error cause lost", err)
			}
			if strings.HasPrefix(kind, "cancel_") {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				code, _, exit := samplesFailure(err)
				if code != "canceled" || exit != 1 {
					t.Fatal("cancellation lost precedence", code, exit)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	var out, diag bytes.Buffer
	code := runSamplesCLI(ctx, []string{"--data-dir", "/explicit/private", "samples", "--show", strings.Repeat("b", 64), "--json"}, &out, &diag, true, func(context.Context, string) (samplesReader, error) { calls++; return nil, os.ErrNotExist })
	samplesCLIError(t, code, out.String(), diag.String(), "canceled", 1)
	if calls != 0 {
		t.Fatal("pre-canceled query opened storage")
	}
	// Even malformed arguments do not erase an ended context in this new route.
	out.Reset()
	diag.Reset()
	code = runSamplesCLI(ctx, []string{"samples", "--json"}, &out, &diag, true, nil)
	samplesCLIError(t, code, out.String(), diag.String(), "canceled", 1)
	codeString, _, _ := samplesFailure(errors.Join(inventory.ErrSampleStoreCorrupt, os.ErrNotExist, context.DeadlineExceeded))
	if codeString != "canceled" {
		t.Fatal("deadline cause lost")
	}
}

type samplesOutputWriter struct {
	mode   string
	cancel context.CancelFunc
	data   bytes.Buffer
}

func (w *samplesOutputWriter) Write(p []byte) (int, error) {
	switch w.mode {
	case "error":
		return 0, errors.New(strings.Repeat("private writer failure", 1024))
	case "short":
		n := len(p) / 2
		_, _ = w.data.Write(p[:n])
		return n, nil
	default:
		n, err := w.data.Write(p)
		if w.cancel != nil {
			w.cancel()
		}
		return n, err
	}
}

func TestSamplesCLIOutputFailuresAndLateVeto(t *testing.T) {
	f := newSamplesCLIFixture(t)
	before := samplesCLISnapshot(t, f)
	for _, machine := range []bool{false, true} {
		for _, kind := range []string{"error", "short", "cancel_after_write"} {
			t.Run(fmt.Sprintf("%s/machine_%t", kind, machine), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				w := &samplesOutputWriter{mode: kind}
				if kind == "cancel_after_write" {
					w.cancel = cancel
				}
				var diag bytes.Buffer
				args := []string{"--data-dir", f.base, "samples", "--show", f.batch.BatchID}
				if machine {
					args = append(args, "--json")
				}
				code := Run(ctx, args, w, &diag)
				if code != 1 || !strings.Contains(diag.String(), "output may be partial") || w.data.Len() > samplesReplyLimit {
					t.Fatal(code, w.data.String(), diag.String())
				}
				if kind == "cancel_after_write" && (w.data.Len() == 0 || !errors.Is(ctx.Err(), context.Canceled)) {
					t.Fatal("writer did not cause actual post-output cancellation")
				}
				if !reflect.DeepEqual(before, samplesCLISnapshot(t, f)) {
					t.Fatal("failed reply changed logical accounting")
				}
			})
		}
	}
	for _, machine := range []bool{false, true} {
		for _, kind := range []string{"error", "short", "cancel_after_write"} {
			t.Run(fmt.Sprintf("error_reply_%s/machine_%t", kind, machine), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				w := &samplesOutputWriter{mode: kind}
				if kind == "cancel_after_write" {
					w.cancel = cancel
				}
				var out, diag bytes.Buffer
				args := []string{"samples"}
				var code int
				if machine {
					args = append(args, "--json")
					code = Run(ctx, args, w, &diag)
				} else {
					code = Run(ctx, args, &out, w)
				}
				if code != 1 || out.Len() != 0 || w.data.Len() > 4096 || diag.Len() > 4096 || strings.Contains(w.data.String()+diag.String(), "private writer failure") {
					t.Fatal("malformed-argv reply failure was ignored or disclosed a writer cause", code, out.String(), w.data.String(), diag.String())
				}
				if kind == "cancel_after_write" {
					if w.data.Len() == 0 || !errors.Is(ctx.Err(), context.Canceled) {
						t.Fatal("malformed-argv writer did not cause actual cancellation")
					}
					if machine {
						var response struct {
							OK    bool `json:"ok"`
							Error struct {
								Code string `json:"code"`
							} `json:"error"`
						}
						if e := json.Unmarshal(w.data.Bytes(), &response); e != nil || response.OK || response.Error.Code != "invalid_arguments" || !strings.Contains(diag.String(), "was canceled") {
							t.Fatal("late cancellation fixture did not write its complete bounded failure first", w.data.String(), diag.String(), e)
						}
					} else if !strings.Contains(w.data.String(), "Usage: rydd") {
						t.Fatal("human failure was not actually written before cancellation", w.data.String())
					}
				}
				if machine && !strings.Contains(diag.String(), "error reply failed") {
					t.Fatal("machine failure diagnostic omitted reply refusal", diag.String())
				}
				if !reflect.DeepEqual(before, samplesCLISnapshot(t, f)) {
					t.Fatal("malformed-argv output failure changed saved accounting")
				}
			})
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &samplesFakeReader{snapshot: samplesProjectionFixture("ready"), onClose: cancel}
	var out, diag bytes.Buffer
	code := runSamplesCLI(ctx, []string{"--data-dir", "/explicit/private", "samples", "--show", strings.Repeat("b", 64)}, &out, &diag, false, func(context.Context, string) (samplesReader, error) { return r, nil })
	if code != 1 || out.Len() != 0 || !strings.Contains(diag.String(), "canceled") || r.closes != 1 {
		t.Fatal("pre-emit cancellation published success", code, out.String(), diag.String(), r)
	}
}

func TestSamplesCLIProjectionBoundsAndPrivacy(t *testing.T) {
	s := samplesProjectionFixture("settled")
	s.Targets = make([]inventory.SavedFileTarget, inventory.FileSampleTargetLimit)
	secret := "private-terminal-\x1b[31m-path"
	for i := range s.Targets {
		s.Targets[i].File.Path = secret
		s.Targets[i].File.PathBytes = bytes.Repeat([]byte{0xff}, 4096)
		s.Targets[i].Root.PathBytes = []byte(secret)
	}
	s.PlannedBytes = inventory.FileSampleCallByteLimit
	s.Policy.DailyReservedByteLimit = 1 << 50
	s.Policy.LifetimeReservedByteLimit = 1 << 50
	at := time.Date(2261, 12, 31, 23, 59, 59, 999999999, time.UTC)
	s.Budget.Day = at.Format(time.DateOnly)
	s.Budget.MaxNow = at
	s.Attempt.ReservationDay = s.Budget.Day
	s.Attempt.ReservedAt = at
	s.Attempt.ReservedBytes = s.PlannedBytes
	*s.Attempt.RequestedBytes = s.PlannedBytes
	*s.Attempt.ReadBytes = s.PlannedBytes
	*s.Attempt.ElapsedNS = math.MaxInt64
	s.Budget.ReservedBytes = s.PlannedBytes
	s.Budget.RequestedBytes = s.PlannedBytes
	s.Budget.ReadBytes = s.PlannedBytes
	s.Budget.TotalReservedBytes = s.PlannedBytes
	s.Budget.TotalRequestedBytes = s.PlannedBytes
	s.Budget.TotalReadBytes = s.PlannedBytes
	r, err := samplesProjection(s, s.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range []bool{false, true} {
		body, e := samplesReply(r, machine)
		if e != nil || len(body) > samplesReplyLimit || bytes.Contains(body, []byte(secret)) || bytes.Contains(body, []byte("path_bytes")) || bytes.Contains(body, []byte("targets")) || bytes.Contains(body, []byte("sha256")) {
			t.Fatal("bounded projection leaked target material", string(body), e)
		}
		if machine {
			got := samplesCLIReport(t, 0, string(body), "")
			if got.TargetCount != 20 || *got.Attempt.ElapsedNS != math.MaxInt64 || *got.Attempt.RequestedBytes != inventory.FileSampleCallByteLimit || got.Policy.DailyReservedByteLimit != 1<<50 {
				t.Fatal("large exact integer changed", got)
			}
		}
	}
	*s.Attempt.RequestedBytes = 0
	s.Budget.TotalRequestedBytes = 0
	s.Targets[0].File.PathBytes[0] = 'x'
	if *r.Attempt.RequestedBytes != inventory.FileSampleCallByteLimit || r.SavedBudget.TotalRequestedBytes != inventory.FileSampleCallByteLimit {
		t.Fatal("projection retained mutable scalar pointers")
	}
	for _, kind := range []string{"contract", "long_identity", "targets", "planned", "budget", "unknown_usage", "negative_usage", "conservation"} {
		t.Run(kind, func(t *testing.T) {
			bad := samplesProjectionFixture("settled")
			switch kind {
			case "contract":
				bad.Contract = "unsupported"
			case "long_identity":
				bad.StoreID = strings.Repeat("a", 65) + secret
			case "targets":
				bad.Targets = make([]inventory.SavedFileTarget, 21)
			case "planned":
				bad.PlannedBytes = inventory.FileSampleCallByteLimit + 1
			case "budget":
				bad.Budget = nil
			case "unknown_usage":
				bad.Attempt.ReadBytes = nil
			case "negative_usage":
				*bad.Attempt.ElapsedNS = -1
			case "conservation":
				bad.Budget.TotalReservedBytes++
			}
			r := &samplesFakeReader{snapshot: bad}
			var out, diag bytes.Buffer
			code := runSamplesCLI(context.Background(), []string{"--data-dir", "/explicit/private", "samples", "--show", bad.BatchID, "--json"}, &out, &diag, true, func(ctx context.Context, _ string) (samplesReader, error) {
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 5*time.Second {
					t.Fatal("missing bounded operation context")
				}
				return r, nil
			})
			samplesCLIError(t, code, out.String(), diag.String(), "sample_accounting_invalid", 1)
			if r.reads != 1 || r.closes != 1 || strings.Contains(out.String()+diag.String(), secret) {
				t.Fatal("invalid projection leaked or skipped checked close", r)
			}
		})
	}
	parent, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	missing := filepath.Join(parent, "absent-ledger")
	var out, diag bytes.Buffer
	code := Run(context.Background(), []string{"--data-dir", missing, "samples", "--show", strings.Repeat("b", 64), "--json"}, &out, &diag)
	samplesCLIError(t, code, out.String(), diag.String(), "not_found", 1)
	if _, e = os.Lstat(missing); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("saved-only open initialized missing storage", e)
	}
	codeString, message, exit := samplesFailure(errors.Join(inventory.ErrSampleStoreCorrupt, fmt.Errorf("%s", secret)))
	if codeString != "sample_accounting_invalid" || exit != 1 || strings.Contains(message, secret) || len(message) > 4096 {
		t.Fatal(codeString, message, exit)
	}
}

func TestSamplesCLISavedHumanMeaningAndCompatibility(t *testing.T) {
	for _, kind := range []string{"ready", "reserved", "interrupted_unknown", "settled_zero", "settled_positive"} {
		t.Run(kind, func(t *testing.T) {
			status := kind
			if strings.HasPrefix(kind, "settled_") {
				status = "settled"
			}
			s := samplesProjectionFixture(status)
			if kind == "settled_positive" {
				*s.Attempt.RequestedBytes = 6
				*s.Attempt.ReadBytes = 3
				*s.Attempt.ElapsedNS = 100
				s.Budget.RequestedBytes = 6
				s.Budget.ReadBytes = 3
				s.Budget.TotalRequestedBytes = 6
				s.Budget.TotalReadBytes = 3
			}
			// Advance only the saved projection, not the reservation day.
			if s.Attempt != nil {
				s.Budget.MaxNow = s.Budget.MaxNow.Add(24 * time.Hour)
				s.Budget.Day = s.Budget.MaxNow.Format(time.DateOnly)
				s.Budget.ReservedBytes = 0
				s.Budget.RequestedBytes = 0
				s.Budget.ReadBytes = 0
				s.Budget.UnknownReservedBytes = 0
				s.Budget.OutstandingReservedBytes = 0
			}
			r, e := samplesProjection(s, s.BatchID)
			if e != nil {
				t.Fatal(e)
			}
			body, e := samplesReply(r, false)
			if e != nil {
				t.Fatal(e)
			}
			flat := strings.Join(strings.Fields(string(body)), " ")
			for _, phrase := range []string{"SAVED SAMPLE ACCOUNTING - NO READ APPROVAL", "Saved reservation day", "Saved clock high-water", "subsets of permanent reserved", "not necessarily today", "another accounting identity and local cap", "opens no inventory or source path", "No read approval is created"} {
				if !strings.Contains(flat, phrase) {
					t.Fatal("missing saved-only qualification", phrase, flat)
				}
			}
			if kind == "ready" {
				if !strings.Contains(flat, "No attempt is recorded") || !strings.Contains(flat, "not a reservation charge") {
					t.Fatal(flat)
				}
			} else if kind == "reserved" || kind == "interrupted_unknown" {
				if !strings.Contains(flat, "Unknown") || kind == "reserved" && !strings.Contains(flat, "whether a process is active is unknown") || kind == "interrupted_unknown" && !strings.Contains(flat, "full interrupted allowance remains charged") {
					t.Fatal(flat)
				}
			} else if !strings.Contains(flat, "Observed requested bytes, known") || !strings.Contains(flat, "Known recorded usage can be zero or positive") {
				t.Fatal("known zero or positive mislabeled unknown", flat)
			}
			if s.Attempt != nil && (!strings.Contains(flat, "2020-02-03") || !strings.Contains(flat, "2020-02-04")) {
				t.Fatal("saved reservation/projection days conflated", flat)
			}
			for _, forbidden := range []string{"Today:", "Remaining quota:", "Current permission: true", "Active: true", "Reclaimed:"} {
				if strings.Contains(flat, forbidden) {
					t.Fatal("saved view asserted current authority", forbidden)
				}
			}
		})
	}
	c := capabilities()
	if c["api_version"] != 1 || !c["features"].(map[string]bool)["saved_sample_accounting_view"] || !c["features"].(map[string]bool)["candidate_page_size_summaries"] || !strings.Contains(help, "samples --show BATCH_ID") {
		t.Fatal("new entry changed existing accepted capabilities", c)
	}
	for _, args := range [][]string{{"--version"}, {"--version", "--json"}, {"--help", "samples"}, {"--help", "samples", "--json"}} {
		var out, diag bytes.Buffer
		if code := Run(context.Background(), args, &out, &diag); code != 0 || diag.Len() != 0 {
			t.Fatal("legacy version/help shortcut changed", args, code, out.String(), diag.String())
		}
	}
}
