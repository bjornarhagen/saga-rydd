// backgroundtopology runs only new generated native wide/deep profiles.
// Generation, independent oracles and observer children are outside worker SELF.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

var errReceipt = errors.New("generated measurement receipt could not be saved; inspect retained outputs")
var errProfile = errors.New("generated profile refused unexpected evidence; retain its private fixture")

type options struct {
	Binary, Output, Shape string
	Files, Seconds        int
}

func (o options) validate() error {
	if !filepath.IsAbs(o.Binary) || filepath.Clean(o.Binary) != o.Binary || !filepath.IsAbs(o.Output) || filepath.Clean(o.Output) != o.Output || strings.ContainsRune(o.Binary+o.Output, 0) || len(o.Binary) > 4096 || len(o.Output) > 512 {
		return errProfile
	}
	if (o.Shape != "wide" && o.Shape != "deep") || o.Files < 256 || o.Files > 1000000 || o.Files%64 != 0 || o.Seconds < 30 || o.Seconds > 3600 {
		return errProfile
	}
	return nil
}

type statusView struct {
	OK     bool          `json:"ok"`
	State  state.Summary `json:"state"`
	Worker struct {
		State string           `json:"state"`
		Live  *worker.Snapshot `json:"live"`
	} `json:"worker"`
	Dispatch state.DispatchBudget   `json:"dispatch_budget"`
	Metadata state.MetadataBudget   `json:"scanner_metadata_budget"`
	CPU      state.CPUFeedbackState `json:"cpu_feedback"`
}
type savedView struct {
	Files, CompleteDirs, HealthyCompleteDirs, ReadyCaches, Caches, Scratch, Errors, Skips, Running int64
	Dispatch                                                                                       int64
	Jobs                                                                                           []savedJob
}
type savedJob struct {
	ID, Root, Due, Attempts, LeaseUntil, Claimed int64
	Kind, Status, Error, Lease                   string
	Path, Cursor                                 []byte
}

func readSaved(ctx context.Context, dir, primary, healthy string) (savedView, error) {
	var v savedView
	db, err := sql.Open("sqlite", readonlyDSN(filepath.Join(dir, state.Filename)))
	if err != nil {
		return v, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return v, err
	}
	defer tx.Rollback()
	var root, other int64
	if err = tx.QueryRowContext(ctx, "SELECT id FROM roots WHERE path=?", []byte(primary)).Scan(&root); err != nil {
		return v, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT id FROM roots WHERE path=?", []byte(healthy)).Scan(&other); err != nil {
		return v, err
	}
	err = tx.QueryRowContext(ctx, `SELECT
 coalesce((SELECT sum(files) FROM compact_dirs WHERE root_id=?),0),
 (SELECT count(*) FROM directories WHERE root_id=? AND complete=1 AND last_error=''),
 (SELECT count(*) FROM directories WHERE root_id=? AND complete=1 AND last_error=''),
 (SELECT count(*) FROM allocation_cache WHERE ready=1 AND phase='done'),
 (SELECT count(*) FROM allocation_cache),
 EXISTS(SELECT 1 FROM compact_retirement)+EXISTS(SELECT 1 FROM subtree_reconcile)+EXISTS(SELECT 1 FROM subtree_retirement)+EXISTS(SELECT 1 FROM allocation_members)+EXISTS(SELECT 1 FROM allocation_identities),
 (SELECT count(*) FROM directories WHERE last_error!='')+(SELECT count(*) FROM roots WHERE last_error!=''),
 (SELECT count(*) FROM entries WHERE skip_reason!=''),
 (SELECT count(*) FROM jobs WHERE status='running'),
 coalesce((SELECT CASE WHEN typeof(chunks)='integer' AND chunks>=0 THEN chunks ELSE -1 END FROM scan_dispatch WHERE id=1),0)`, root, root, other).Scan(&v.Files, &v.CompleteDirs, &v.HealthyCompleteDirs, &v.ReadyCaches, &v.Caches, &v.Scratch, &v.Errors, &v.Skips, &v.Running, &v.Dispatch)
	if err != nil {
		return v, err
	}
	if v.Dispatch < 0 {
		return v, errProfile
	}
	rows, err := tx.QueryContext(ctx, boundedSavedJobsSQL)
	if err != nil {
		return v, err
	}
	for rows.Next() {
		var j savedJob
		var admitted bool
		if err = rows.Scan(&j.ID, &j.Root, &j.Kind, &j.Path, &j.Status, &j.Due, &j.Cursor, &j.Attempts, &j.Error, &j.Lease, &j.LeaseUntil, &j.Claimed, &admitted); err != nil || !admitted || (j.Root != root && j.Root != other) {
			rows.Close()
			if ctx.Err() != nil {
				return v, ctx.Err()
			}
			return v, errProfile
		}
		v.Jobs = append(v.Jobs, j)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return v, err
	}
	rows.Close()
	if len(v.Jobs) > 129 {
		return v, errProfile
	}
	return v, tx.Commit()
}
func (v savedView) drained(t topology) bool {
	return v.Files == int64(t.files) && v.CompleteDirs == int64(t.levels()) && v.HealthyCompleteDirs == 33 && v.Caches == 2 && v.ReadyCaches == 2 && v.Scratch == 0 && v.Errors == 0 && v.Skips == 0 && v.Running == 0
}
func futureJobs(ctx context.Context, dir, primary, healthy string) error {
	if primary == healthy {
		return errProfile
	}
	db, err := sql.Open("sqlite", readonlyDSN(filepath.Join(dir, state.Filename)))
	if err != nil {
		return err
	}
	defer db.Close()
	// Count every job, including rows with no matching root or directory. Only
	// bounded aggregate scalars enter Go; saved path/error/cursor payloads do not.
	var n, valid, distinctRoots int64
	err = db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(CASE WHEN
 typeof(j.root_id)='integer' AND j.root_id>0
 AND typeof(j.kind)='text' AND j.kind='inventory'
 AND typeof(j.status)='text' AND j.status='pending'
 AND typeof(j.path)='blob' AND j.path=X'2e' AND j.cursor IS NULL
 AND typeof(j.last_error)='text' AND j.last_error=''
 AND typeof(j.lease_token)='text' AND j.lease_token=''
 AND typeof(j.lease_until_ns)='integer' AND j.lease_until_ns=0
 AND typeof(j.attempts)='integer' AND j.attempts=0
 AND typeof(j.inventory_claimed)='integer' AND j.inventory_claimed=0
 AND typeof(j.due_at_ns)='integer' AND j.due_at_ns>=0
 AND typeof(d.checked_at_ns)='integer' AND d.checked_at_ns BETWEEN 0 AND ?
 AND j.due_at_ns=d.checked_at_ns+?
 AND typeof(r.path)='blob' AND r.path IN (?,?)
 THEN 1 ELSE 0 END),0),count(DISTINCT j.root_id)
 FROM jobs j LEFT JOIN directories d ON d.root_id=j.root_id AND d.path=X'2e'
 LEFT JOIN roots r ON r.id=j.root_id`, int64(math.MaxInt64)-int64(24*time.Hour), int64(24*time.Hour), []byte(primary), []byte(healthy)).Scan(&n, &valid, &distinctRoots)
	if err != nil {
		return err
	}
	if n != 2 || valid != 2 || distinctRoots != 2 {
		return errProfile
	}
	return nil
}

type sample struct {
	ElapsedNS                                                                                    int64 `json:"elapsed_ns"`
	PrimaryFiles, PrimaryCompleteDirs, HealthyCompleteDirs, ReadyCaches, ScratchScopes, Dispatch int64
	DBBytes, WALBytes                                                                            int64
	Wait                                                                                         string
	Charges                                                                                      *state.MetadataCharges
	CPUStatus                                                                                    string
	APICounters                                                                                  *inventory.Metrics
	DispatchDay                                                                                  string
}
type result struct {
	Contract                                                                                                                                                        string      `json:"contract"`
	Shape                                                                                                                                                           string      `json:"shape"`
	Files                                                                                                                                                           int         `json:"primary_file_observations_expected"`
	Levels                                                                                                                                                          int         `json:"primary_file_bearing_levels"`
	MaximumSeconds                                                                                                                                                  int         `json:"sampling_window_limit_seconds"`
	StopGraceSeconds                                                                                                                                                int         `json:"graceful_stop_window_seconds"`
	Profile                                                                                                                                                         config.Scan `json:"finite_profile"`
	InitialAvailableOutputBytes                                                                                                                                     *int64      `json:"initial_available_output_filesystem_bytes"`
	GenerationNS, OracleBeforeNS, OracleAfterNS                                                                                                                     int64
	Before                                                                                                                                                          oracle
	Samples                                                                                                                                                         []sample
	Workers                                                                                                                                                         []usage
	Observer                                                                                                                                                        usage
	Generation, OracleBefore, OracleAfter                                                                                                                           *usage
	ControlLatencyNS                                                                                                                                                []int64
	Completed, BodyIdentityOracleUnchanged, PauseNoProgress, SIGKILLProven, RestartPreserved, FairProgress, FairCompletion, ExactCachedTotals, ExactFutureDailyJobs bool
	Outcome                                                                                                                                                         string
	TerminalEvidenceSaved                                                                                                                                           bool `json:"terminal_evidence_saved"`
	ForcedCleanup                                                                                                                                                   bool `json:"forced_direct_child_cancellation_requested"`
	Stage                                                                                                                                                           string
	SourceRevision                                                                                                                                                  *string
	SourceRevisionStatus                                                                                                                                            string
	DeclaredBuildMetadata                                                                                                                                           *buildmetadata.Record
	SourceAuthenticationVerified                                                                                                                                    bool
	HarnessBinarySHA256                                                                                                                                             string
	BinarySHA256, ConfigSHA256                                                                                                                                      string
	ArtifactSHA256                                                                                                                                                  map[string]string
	DefaultProfileAccepted, HourlyTargetAccepted, PhysicalIOVerified, ProviderAccepted, SoakAccepted                                                                bool
	Qualifiers                                                                                                                                                      []string
}
type runner struct {
	o                                      options
	base, state, primary, healthy, runtime string
	workerContext                          context.Context
	env                                    []string
	seq                                    int
	observers                              usage
	latencies                              []int64
	children                               []*child
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.CopyBuffer(h, f, make([]byte, 32768)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func readonlyDSN(path string) string {
	return (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
}

func writeReceipt(path string, data []byte) error {
	if os.WriteFile(path, data, 0600) != nil {
		return errReceipt
	}
	return nil
}
func saveJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return writeReceipt(path, append(b, '\n'))
}
func (r *runner) command(ctx context.Context, args ...string) ([]byte, error) {
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c, err := start(call, r.o.Binary, append([]string{"--data-dir", r.state, "--json"}, args...), r.env, 256<<10)
	if err != nil {
		return nil, err
	}
	err = c.join(call)
	r.seq++
	out, _ := c.out.value()
	diagnostic, _ := c.diagnostic.value()
	receiptErr := errors.Join(writeReceipt(filepath.Join(r.base, fmt.Sprintf("command-%03d.stdout", r.seq)), out), writeReceipt(filepath.Join(r.base, fmt.Sprintf("command-%03d.stderr", r.seq)), diagnostic))
	if !c.usage.Available {
		r.observers.Available = false
	}
	r.observers.UserNS += c.usage.UserNS
	r.observers.SystemNS += c.usage.SystemNS
	r.observers.ElapsedNS += c.usage.ElapsedNS
	if c.usage.RSS > r.observers.RSS {
		r.observers.RSS = c.usage.RSS
	}
	if err != nil || receiptErr != nil {
		return nil, errors.Join(err, receiptErr)
	}
	var envelope struct {
		OK bool `json:"ok"`
	}
	if json.Unmarshal(out, &envelope) != nil || !envelope.OK {
		return nil, errProfile
	}
	return out, nil
}
func (r *runner) status(ctx context.Context) (statusView, error) {
	var s statusView
	b, err := r.command(ctx, "status")
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}
func (r *runner) control(ctx context.Context, name string) error {
	started := time.Now()
	_, err := r.command(ctx, name)
	r.latencies = append(r.latencies, time.Since(started).Nanoseconds())
	return err
}
func (r *runner) launch(ctx context.Context) (*child, error) {
	if r.workerContext != nil {
		ctx = r.workerContext
	}
	c, err := start(ctx, r.o.Binary, []string{"--data-dir", r.state, "daemon", "--experimental-scan"}, r.env, 65536)
	if err == nil {
		r.children = append(r.children, c)
	}
	return c, err
}
func (r *runner) ready(ctx context.Context, c *child, paused bool) error {
	return r.readyWithStatus(ctx, c, paused, r.status)
}
func (r *runner) readyWithStatus(ctx context.Context, c *child, paused bool, statusCall func(context.Context) (statusView, error)) error {
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		select {
		case <-c.done:
			return errChild
		default:
		}
		s, err := statusCall(ctx)
		if err != nil {
			return err
		}
		if s.Worker.Live != nil && s.Worker.Live.PID == c.cmd.Process.Pid && s.Worker.Live.Paused == paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errProfile
}
func (r *runner) pauseRestart(ctx context.Context, c *child, report *result) (*child, error) {
	if err := r.control(ctx, "pause"); err != nil {
		return c, err
	}
	until := time.Now().Add(15 * time.Second)
	var s statusView
	for {
		var err error
		s, err = r.status(ctx)
		if err != nil {
			return c, err
		}
		if s.Worker.Live != nil && s.Worker.Live.Paused && s.Worker.Live.ActiveJob == 0 && s.CPU.Status == "observed" && s.CPU.Window != nil && s.CPU.Window.CPUTimeNS != nil && s.Metadata.TotalCharges != nil && s.Metadata.TotalCharges.OutstandingReserved == 0 {
			break
		}
		if time.Now().After(until) {
			return c, errProfile
		}
		select {
		case <-ctx.Done():
			return c, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	before, err := readSaved(ctx, r.state, r.primary, r.healthy)
	if err != nil {
		return c, err
	}
	select {
	case <-ctx.Done():
		return c, ctx.Err()
	case <-time.After(2 * time.Second):
	}
	after, err := readSaved(ctx, r.state, r.primary, r.healthy)
	if err != nil || !reflect.DeepEqual(before, after) {
		return c, errProfile
	}
	report.PauseNoProgress = true
	if err = c.cmd.Process.Kill(); err != nil {
		return c, err
	}
	<-c.done
	status, ok := c.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return c, errProfile
	}
	report.SIGKILLProven = true
	resumed, err := r.launch(ctx)
	if err != nil {
		return c, err
	}
	if err = r.ready(ctx, resumed, true); err != nil {
		return resumed, err
	}
	after, err = readSaved(ctx, r.state, r.primary, r.healthy)
	if err != nil || !reflect.DeepEqual(before, after) {
		return resumed, errProfile
	}
	current, err := r.status(ctx)
	if err != nil || !reflect.DeepEqual(s.Metadata.TotalCharges, current.Metadata.TotalCharges) || !reflect.DeepEqual(current.CPU, s.CPU) {
		return resumed, errProfile
	}
	configHash, err := hashFile(filepath.Join(r.state, "config.toml"))
	if err != nil || configHash != report.ConfigSHA256 {
		return resumed, errProfile
	}
	report.RestartPreserved = true
	return resumed, r.control(ctx, "resume")
}
func (r *runner) exactReport(ctx context.Context, oracle oracle) error {
	b, err := r.command(ctx, "report", "--directory", r.primary)
	if err != nil {
		return err
	}
	var envelope struct {
		Report state.FileReport `json:"report"`
	}
	if err = json.Unmarshal(b, &envelope); err != nil {
		return err
	}
	d := envelope.Report.Directory
	if d == nil || d.Status != "recorded_complete" || d.AllocatedSizeSource != "cached_reduction" || d.InodeEntriesExamined != 0 || d.Truncated || d.LogicalBytes == nil || *d.LogicalBytes != oracle.Logical || d.AllocatedBytes == nil || *d.AllocatedBytes != oracle.Allocated || d.CompactedFiles != int(oracle.Files) || d.CurrentStateVerified || d.SkippedEntries != 0 || d.ExcludedEntries != 0 {
		return errProfile
	}
	return nil
}
func run(ctx context.Context, o options) (result, error) {
	return runWithCapacity(ctx, o, checkInitialOutputCapacity)
}

func runWithCapacity(ctx context.Context, o options, capacity func(context.Context, string) (*int64, error)) (report result, err error) {
	if err = o.validate(); err != nil {
		return report, err
	}
	report = result{Contract: "generated_background_topology_profile_v1", Shape: o.Shape, Files: o.Files, Levels: (topology{o.Shape, o.Files}).levels(), MaximumSeconds: o.Seconds, StopGraceSeconds: 15, Outcome: "partial", Stage: "generation", SourceRevisionStatus: "unknown", ArtifactSHA256: map[string]string{}, Qualifiers: []string{"Finite generated zero-byte topology with bounded sentinel bodies; not representative content or defaults acceptance.", "Worker SELF CPU excludes generator, independent oracle and observer children; those costs are separate.", "Scanner/API accounting is not physical-I/O accounting. Entered native operations can outlast cancellation.", "Only new owned source roots are used; saved cursors, quotas and daily due times are not reset.", "Healthy source progress precedes primary EOF; healthy EOF precedes full primary cache/scratch drain.", "Concurrent development can affect elapsed time and host scheduling.", "Sampling ends at its declared window; source work can continue until the stop request is received. Full worker resources cover launch through Wait return, including separate stop/cleanup grace.", "Generator/oracle RSS is the parent process lifetime high-water through each phase. Observer RSS is the maximum individual CLI child, not combined simultaneous memory.", "Oracle comparisons request at most 4 MiB total named-database page cache, disable mmap, and use file-backed temporary sorting; temporary disk and helper overhead are separate from worker costs.", "Child cancellation addresses the guarded direct child only. No descendant/process-group cleanup is claimed."}}
	binary, err := os.Lstat(o.Binary)
	if err != nil || !binary.Mode().IsRegular() || binary.Mode().Perm()&0111 == 0 {
		return report, errProfile
	}
	if err = os.Mkdir(o.Output, 0700); err != nil {
		return report, err
	}
	base, err := filepath.EvalSymlinks(o.Output)
	if err != nil {
		return report, err
	}
	if len(base) > 512 {
		return report, errProfile
	}
	report.InitialAvailableOutputBytes, err = capacity(ctx, base)
	if err != nil {
		report.Stage = "capacity_preflight"
		report.Outcome = "capacity_unavailable_no_generation"
		if errors.Is(err, errCapacity) {
			report.Outcome = "capacity_refused_no_generation"
		}
		err = errors.Join(err, saveJSON(filepath.Join(base, "aggregate.json"), report))
		return report, err
	}
	r := &runner{o: o, observers: usage{Available: true, RSSScope: "maximum_individual_observer_child"}, base: base, state: filepath.Join(base, "state"), primary: filepath.Join(base, "node_modules"), healthy: filepath.Join(base, "healthy")}
	workerLife, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	r.workerContext = workerLife
	tmpBase := "/tmp"
	if runtime.GOOS == "darwin" {
		tmpBase = "/private/tmp"
	}
	r.runtime, err = os.MkdirTemp(tmpBase, "rydd-topology-")
	if err != nil {
		return report, err
	}
	r.runtime, err = filepath.EvalSymlinks(r.runtime)
	if err != nil {
		return report, err
	}
	r.env = append([]string(nil), os.Environ()...)
	for i := 0; i < len(r.env); i++ {
		if strings.HasPrefix(r.env[i], "RYDD_RUNTIME_DIR=") {
			r.env = append(r.env[:i], r.env[i+1:]...)
			i--
		}
	}
	r.env = append(r.env, "RYDD_RUNTIME_DIR="+r.runtime)

	stateInitAttempted := false
	defer func() {
		for i, c := range r.children {
			select {
			case <-c.done:
			default:
				err = errors.Join(err, r.control(context.Background(), "stop"))
				select {
				case <-c.done:
				case <-time.After(10 * time.Second):
					report.ForcedCleanup = true
					c.cleanup()
				}
			}
			if c.forced.Load() {
				report.ForcedCleanup = true
			}
			report.Workers = append(report.Workers, c.usage)
			out, _ := c.out.value()
			diagnostic, _ := c.diagnostic.value()
			err = errors.Join(err, writeReceipt(filepath.Join(base, fmt.Sprintf("worker-%d.stdout", i)), out), writeReceipt(filepath.Join(base, fmt.Sprintf("worker-%d.stderr", i)), diagnostic))
		}
		if stateInitAttempted {
			probe, cancelProbe := context.WithTimeout(context.Background(), 5*time.Second)
			if v, e := readSaved(probe, r.state, r.primary, r.healthy); e == nil {
				saveErr := saveJSON(filepath.Join(base, "saved-terminal.json"), v)
				err = errors.Join(err, saveErr)
				report.TerminalEvidenceSaved = saveErr == nil
			} else {
				err = errors.Join(err, e)
			}
			cancelProbe()
		}
		report.Observer = r.observers
		report.ControlLatencyNS = r.latencies
		if err != nil {
			report.Outcome = "profile_failed_partial"
			if errors.Is(err, errCapacity) {
				report.Outcome = "capacity_refused_no_generation"
			}
		}
		if receiptErr := saveJSON(filepath.Join(base, "aggregate.json"), report); receiptErr != nil {
			err = errors.Join(err, receiptErr)
			report.Outcome = "receipt_failed_partial"
		}
	}()
	report.Qualifiers = append(report.Qualifiers, "The initial output-filesystem capacity check is an observation, not a disk reservation. Allocation failures remain partial evidence. Generation and each oracle phase have a separate 1800-second deadline. A finite sample does not establish full million-entry drain, defaults or soak acceptance.")
	ownExe, exeErr := os.Executable()
	if exeErr != nil {
		return report, exeErr
	}
	report.HarnessBinarySHA256, err = hashFile(ownExe)
	if err != nil {
		return report, err
	}
	report.BinarySHA256, err = hashFile(o.Binary)
	if err != nil {
		return report, err
	}
	metadata, metadataErr := r.declaredMetadata(ctx)
	if metadataErr != nil {
		return report, metadataErr
	}
	report.DeclaredBuildMetadata = &metadata
	if metadata.Revision != nil {
		value := strings.Clone(*metadata.Revision)
		report.SourceRevision = &value
		report.SourceRevisionStatus = "declared"
	}
	report.Qualifiers = append(report.Qualifiers, "Supplied-binary build metadata and revision are declared observations, not authenticated source provenance. Missing VCS metadata remains unknown; binary SHA identifies the observed bytes only.")
	t := topology{o.Shape, o.Files}
	healthy := topology{"healthy", 512}
	started := time.Now()
	phaseCPU := selfUsage()
	generationCtx, cancelGeneration := context.WithTimeout(ctx, 1800*time.Second)
	if err = generate(generationCtx, r.primary, t); err != nil {
		cancelGeneration()
		return report, err
	}
	if err = generate(generationCtx, r.healthy, healthy); err != nil {
		cancelGeneration()
		return report, err
	}
	cancelGeneration()
	report.GenerationNS = time.Since(started).Nanoseconds()
	report.Generation = phaseUsage(phaseCPU, report.GenerationNS)
	started = time.Now()
	phaseCPU = selfUsage()
	before := filepath.Join(base, "primary-before.sqlite")
	oracleBeforeCtx, cancelOracleBefore := context.WithTimeout(ctx, 1800*time.Second)
	defer cancelOracleBefore()
	report.Before, err = inspect(oracleBeforeCtx, r.primary, before, t)
	if err != nil {
		return report, err
	}
	healthyBefore := filepath.Join(base, "healthy-before.sqlite")
	if _, err = inspect(oracleBeforeCtx, r.healthy, healthyBefore, healthy); err != nil {
		return report, err
	}
	cancelOracleBefore()
	report.OracleBeforeNS = time.Since(started).Nanoseconds()
	report.OracleBefore = phaseUsage(phaseCPU, report.OracleBeforeNS)
	report.Stage = "setup"
	cfg := config.Default()
	cfg.Roots = []string{r.primary, r.healthy}
	cfg.Scan.CompactInventory = true
	cfg.Scan.AdaptiveRevisits = false
	cfg.Scan.PauseOnBattery = false
	cfg.Scan.WorkSeconds = 1
	cfg.Scan.IntervalSeconds = 1
	cfg.Scan.MetadataPerSecond = 100000
	cfg.Scan.MetadataAttemptsPerDay = 1 << 40
	cfg.Scan.MaxScanChunksPerDay = 100000
	cfg.Scan.APIAttemptsPerSecond = 0
	cfg.Scan.MaxStateBytes = 1 << 30
	report.Profile = cfg.Scan
	if err = config.Create(filepath.Join(r.state, "config.toml"), base, cfg); err != nil {
		return report, err
	}
	report.ConfigSHA256, err = hashFile(filepath.Join(r.state, "config.toml"))
	if err != nil {
		return report, err
	}
	stateInitAttempted = true
	if _, err = r.command(ctx, "state", "init"); err != nil {
		return report, err
	}
	profile, cancel := context.WithTimeout(ctx, time.Duration(o.Seconds)*time.Second)
	defer cancel()
	launched := time.Now()
	c, err := r.launch(profile)
	if err != nil {
		return report, err
	}
	report.Stage = "ready"
	if err = r.ready(profile, c, false); err != nil {
		if !samplingEnded(profile, err) {
			return report, err
		}
		err = nil
	}
	report.Stage = "sampling"
	restart := false
	for len(report.Samples) < maximumTopologySamples && profile.Err() == nil {
		s, e := r.status(profile)
		if e != nil {
			if samplingEnded(profile, e) {
				break
			}
			return report, e
		}
		if s.Worker.Live == nil {
			return report, errChild
		}
		v, e := readSaved(profile, r.state, r.primary, r.healthy)
		if e != nil {
			if samplingEnded(profile, e) {
				break
			}
			return report, e
		}
		if err = spoolSavedEvidence(base, len(report.Samples), v); err != nil {
			return report, err
		}
		report.Samples = append(report.Samples, sample{ElapsedNS: time.Since(launched).Nanoseconds(), PrimaryFiles: v.Files, PrimaryCompleteDirs: v.CompleteDirs, HealthyCompleteDirs: v.HealthyCompleteDirs, ReadyCaches: v.ReadyCaches, ScratchScopes: v.Scratch, Dispatch: v.Dispatch, DBBytes: s.State.DatabaseBytes, WALBytes: s.State.WALBytes, Wait: s.Worker.Live.WaitReason, Charges: s.Metadata.TotalCharges, CPUStatus: s.CPU.Status, APICounters: s.Worker.Live.InventoryMetrics, DispatchDay: s.Dispatch.Day})
		if v.HealthyCompleteDirs > 1 && v.CompleteDirs < int64(t.levels()) {
			report.FairProgress = true
		}
		if v.HealthyCompleteDirs == 33 && !v.drained(t) {
			report.FairCompletion = true
		}
		if !restart && v.Files >= 256 && v.CompleteDirs < int64(t.levels()) {
			report.Stage = "pause_restart"
			c, err = r.pauseRestart(profile, c, &report)
			if err != nil {
				if samplingEnded(profile, err) {
					err = nil
					break
				}
				return report, err
			}
			restart = true
			report.Stage = "sampling"
		}
		if v.drained(t) {
			report.Completed = true
			break
		}
		select {
		case <-profile.Done():
			break
		case <-time.After(5 * time.Second):
		}
		if profile.Err() != nil {
			break
		}
	}
	// The sampling window has ended. Source may progress until the stop request
	// arrives; retained child usage includes the separate graceful-stop window.
	report.Stage = "stop"
	stop, cancelStop := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelStop()
	if err = r.control(stop, "stop"); err != nil {
		return report, err
	}
	if err = c.join(stop); err != nil {
		return report, err
	}
	started = time.Now()
	phaseCPU = selfUsage()
	after := filepath.Join(base, "primary-after.sqlite")
	oracleAfterCtx, cancelOracleAfter := context.WithTimeout(ctx, 1800*time.Second)
	defer cancelOracleAfter()
	if _, err = inspect(oracleAfterCtx, r.primary, after, t); err != nil {
		return report, err
	}
	healthyAfter := filepath.Join(base, "healthy-after.sqlite")
	if _, err = inspect(oracleAfterCtx, r.healthy, healthyAfter, healthy); err != nil {
		return report, err
	}
	if err = sameOracle(oracleAfterCtx, before, after); err != nil {
		return report, err
	}
	if err = sameOracle(oracleAfterCtx, healthyBefore, healthyAfter); err != nil {
		return report, err
	}
	cancelOracleAfter()
	report.OracleAfterNS = time.Since(started).Nanoseconds()
	report.OracleAfter = phaseUsage(phaseCPU, report.OracleAfterNS)
	report.BodyIdentityOracleUnchanged = true
	if report.Completed {
		if !report.PauseNoProgress || !report.SIGKILLProven || !report.RestartPreserved || !report.FairProgress || !report.FairCompletion {
			return report, errProfile
		}
		report.Stage = "report"
		if err = r.exactReport(ctx, report.Before); err != nil {
			return report, err
		}
		report.ExactCachedTotals = true
		report.Stage = "daily_jobs"
		if err = futureJobs(ctx, r.state, r.primary, r.healthy); err != nil {
			return report, err
		}
		report.ExactFutureDailyJobs = true
		report.Outcome = "completed"
	}
	hash, err := hashFile(filepath.Join(r.state, "config.toml"))
	if err != nil || hash != report.ConfigSHA256 {
		return report, errProfile
	}
	for name, path := range map[string]string{"primary_before": before, "primary_after": after, "healthy_before": healthyBefore, "healthy_after": healthyAfter} {
		report.ArtifactSHA256[name], err = hashFile(path)
		if err != nil {
			return report, err
		}
	}
	report.Stage = "finished"
	return report, nil
}
func main() {
	var o options
	flag.StringVar(&o.Binary, "binary", "", "exact native production binary")
	flag.StringVar(&o.Output, "output", "", "new exclusive private fixture directory")
	flag.StringVar(&o.Shape, "shape", "wide", "wide or deep")
	flag.IntVar(&o.Files, "files", 4096, "generated file count, at most 1000000")
	flag.IntVar(&o.Seconds, "seconds", 600, "finite worker sampling window, at most 3600 seconds")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, errProfile)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 9600*time.Second)
	defer cancel()
	r, err := run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	b, _ := json.Marshal(r)
	if _, outputErr := fmt.Println(string(bytes.TrimSpace(b))); outputErr != nil {
		err = errors.Join(err, errReceipt)
	}
	if err != nil {
		os.Exit(1)
	}
}

func selfUsage() *usage {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return nil
	}
	u := usage{Available: true, RSSScope: "helper_process_lifetime_through_phase_end", UserNS: timevalNS(r.Utime), SystemNS: timevalNS(r.Stime), RSS: r.Maxrss}
	if runtime.GOOS == "linux" {
		if u.RSS > math.MaxInt64/1024 {
			return nil
		}
		u.RSS *= 1024
	}
	if u.UserNS < 0 || u.SystemNS < 0 || u.RSS <= 0 {
		return nil
	}
	return &u
}
func phaseUsage(before *usage, elapsed int64) *usage {
	after := selfUsage()
	if before == nil || after == nil || after.UserNS < before.UserNS || after.SystemNS < before.SystemNS {
		return nil
	}
	return &usage{Available: true, RSSScope: "helper_process_lifetime_through_phase_end", UserNS: after.UserNS - before.UserNS, SystemNS: after.SystemNS - before.SystemNS, RSS: after.RSS, ElapsedNS: elapsed}
}

func samplingEnded(ctx context.Context, err error) bool {
	return ctx.Err() != nil && !errors.Is(err, errReceipt) && !errors.Is(err, errChildOutput) && !errors.Is(err, errProfile) && errors.Is(err, ctx.Err())
}
