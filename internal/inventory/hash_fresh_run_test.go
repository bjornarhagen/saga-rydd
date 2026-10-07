package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// Build genuine completed original observations, a saved caller-ordered
// choice, and a separate zero-initialized fresh job. All bytes are generated.
type freshRunFixture struct {
	fresh    *freshReadFixture
	consent  HashFreshReadConsent
	original HashSnapshot
}

func freshRunFiles(t *testing.T, size, count int, keeper string, copies ...string) *freshRunFixture {
	t.Helper()
	data := make([][]byte, count)
	for i := range data {
		data[i] = fullHashContents(size)
	}
	f, originalApproval := hashReadFixture(t, data...)
	originalApproval.DailyReservedByteLimit = 64 << 20
	originalApproval.LifetimeReservedByteLimit = 128 << 20
	originalConsent := hashReadApprove(t, f, originalApproval)
	for i := 0; i < count*(size/int(FileHashStepByteLimit)+2); i++ {
		snapshot := hashStoreSnapshot(t, f.store)
		done := true
		for _, work := range snapshot.Work {
			if work.Status != "complete" {
				done = false
			}
		}
		if done {
			break
		}
		if _, err := f.store.RunConsented(context.Background(), originalConsent.ID, f.source, f.scanner); err != nil {
			t.Fatal("generated original observation failed", err)
		}
	}
	snapshot := hashStoreSnapshot(t, f.store)
	for _, work := range snapshot.Work {
		if work.Status != "complete" || work.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(size))) {
			t.Fatal("fixture lacks independent complete original digest", work)
		}
	}
	proposal, err := f.store.Proposal(context.Background(), originalApproval.SelectionID)
	if err != nil || proposal.SourceLocator == nil {
		t.Fatal(proposal, err)
	}
	if err = f.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err = localfs.EnsurePrivateDir(filepath.Join(f.base, "manual")); err != nil {
		t.Fatal(err)
	}
	derived := filepath.Join(f.base, "manual", proposal.SourceLocator.InventoryKey)
	if err = os.Rename(f.stateDir, derived); err != nil {
		t.Fatal(err)
	}
	f.stateDir = derived
	f.source, err = state.OpenWriter(context.Background(), derived)
	if err != nil {
		t.Fatal(err)
	}
	preview := hashKeeperPreview(t, f.store, proposal.SelectionID, keeper, copies...)
	w := hashChoiceWriter(t, f)
	saved := saveHashChoice(t, w, preview)
	proposal.ReadConsent = cloneHashKeeperChoiceFreshConsent(saved.Record.Evidence.ReadConsent)
	m := &hashChoiceRequestFixture{f: f, saved: saved, consent: originalConsent, proposal: proposal}
	request := hashChoiceJobRequest(t, m)
	w = hashChoiceJobWriter(t, m, request)
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	fresh := &freshReadFixture{m: m, request: request, job: job, approval: HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 64 << 20, LifetimeReservedByteLimit: 128 << 20}}
	consent := approveFreshReadFixture(t, fresh)
	return &freshRunFixture{fresh: fresh, consent: consent, original: hashStoreSnapshot(t, w)}
}

func (f *freshRunFixture) open(t *testing.T) *HashStore {
	t.Helper()
	if err := f.fresh.m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenHashFreshRunWriter(context.Background(), f.fresh.request, f.fresh.job.ID)
	if err != nil {
		t.Fatal("exact generated fresh run writer refused", err)
	}
	f.fresh.m.f.store = w
	w.now = func() time.Time { return f.consent.Approval.CreatedAt }
	return w
}

func (f *freshRunFixture) checkOriginal(t *testing.T, s *HashStore) {
	t.Helper()
	if !reflect.DeepEqual(hashStoreSnapshot(t, s), f.original) {
		t.Fatal("fresh work changed original approval, reservation, progress or clock")
	}
	choice, err := s.KeeperChoice(context.Background(), f.fresh.m.saved.ID)
	if err != nil || !reflect.DeepEqual(choice, f.fresh.m.saved) {
		t.Fatal("fresh work changed archived keeper roles/evidence", choice, err)
	}
}

func requireFreshRunSaved(t *testing.T, f *freshRunFixture, s *HashStore) SavedFreshJob {
	t.Helper()
	job, err := s.FreshJob(context.Background(), f.fresh.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(job.Record, f.fresh.job.Record) || !reflect.DeepEqual(job.Work, f.fresh.job.Work) || job.ApprovalAvailable || job.ProvenanceVerified || job.ContentVerified || job.CurrentStateVerified || job.DuplicatesVerified || job.Executable || job.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh progress overwrote seed/request or claimed verification/cleanup", job)
	}
	if len(job.Progress) != len(job.Work) || job.FreshBudget == nil {
		t.Fatal("initialized exact job lacks genuine progress/budget", job)
	}
	for i, work := range job.Progress {
		selected := job.Record.Request.Targets[i]
		if work.Ordinal != i+1 || work.HistoricalWorkID != job.Work[i].HistoricalWorkID || work.Role != job.Work[i].Role || work.FileID != selected.Target.File.ID || !bytes.Equal(work.PathBytes, selected.Target.File.PathBytes) || work.LogicalBytes != selected.Target.File.Size || work.DurableOffset < 0 || work.DurableOffset > work.LogicalBytes {
			t.Fatal("fresh progress remapped caller roles/frozen target", work)
		}
	}
	if job.FreshReservedBytes != job.FreshBudget.TotalReservedBytes || job.FreshRequestedBytes != job.FreshBudget.TotalRequestedBytes || job.FreshReadBytes != job.FreshBudget.TotalReadBytes {
		t.Fatal("fresh counters differ from validated job budget", job)
	}
	f.checkOriginal(t, s)
	return job
}

func requireFreshRunResult(t *testing.T, f *freshRunFixture, r HashFreshRunResult) {
	t.Helper()
	if r.JobID != f.fresh.job.ID || r.JobKey != f.fresh.job.Record.JobKey || r.RequestID != f.fresh.request.ID() || r.ChoiceID != f.fresh.m.saved.ID || r.ApprovalID != f.consent.ID || r.Ordinal < 1 || r.Ordinal > len(f.fresh.job.Work) {
		t.Fatal("fresh result omitted exact job/request/approval scope", r)
	}
	selected := f.fresh.job.Record.Request.Targets[r.Ordinal-1]
	if r.HistoricalWorkID != selected.Observation.WorkID || r.Role != selected.Role || r.Progress.FileID != selected.Target.File.ID || !bytes.Equal(r.Progress.PathBytes, selected.Target.File.PathBytes) || r.ReservedBytes < 0 || r.ReservedBytes > FileHashStepByteLimit || r.Usage.ReadBytes < 0 || r.Usage.ReadBytes > r.Usage.RequestedBytes || r.Usage.RequestedBytes > r.ReservedBytes {
		t.Fatal("fresh result broadened scope or read unreserved bytes", r)
	}
	requireFullHashClaims(t, r.Progress)
}

func TestHashFreshRunStartsNewSHAAndPreservesHistoricalSubsetAcrossReopen(t *testing.T) {
	f := freshRunFiles(t, int(FileHashStepByteLimit)+65, 4, "3", "2", "1")
	w := f.open(t)
	initial := requireFreshRunSaved(t, f, w)
	if initial.FreshReservedBytes != 0 || initial.FreshReadBytes != 0 {
		t.Fatal("new job imported original charges", initial)
	}
	for _, work := range initial.Progress {
		if work.Status != "pending" || work.Sequence != 0 || work.DurableOffset != 0 || work.SHA256 != "" || work.LatestAttempt != nil {
			t.Fatal("new job imported old SHA/offset/attempt", work)
		}
	}
	for _, ordinal := range []int{1, 2, 3, 1, 2, 3} {
		r, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
		if err != nil {
			t.Fatal(r, err)
		}
		requireFreshRunResult(t, f, r)
		if r.Ordinal != ordinal {
			t.Fatal("fresh fair queue ignored explicit new ordinals", r)
		}
		if r.Progress.Status == "partial" {
			if r.DurableOffset != FileHashStepByteLimit || r.ReservedBytes != FileHashStepByteLimit || r.Usage.ReadBytes != FileHashStepByteLimit || r.Progress.SHA256 != "" {
				t.Fatal("fresh first read reused historical completion", r)
			}
		} else if r.Status == "hash_observed" {
			if r.DurableOffset != FileHashStepByteLimit+65 || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(int(FileHashStepByteLimit)+65))) {
				t.Fatal("fresh suffix did not complete independent SHA", r)
			}
		} else {
			t.Fatal("unexpected fresh run status", r)
		}
		requireFreshRunSaved(t, f, w)
		w = f.open(t)
	}
	complete := requireFreshRunSaved(t, f, w)
	want := int64(3) * (FileHashStepByteLimit + 65)
	if complete.FreshReservedBytes != want || complete.FreshReadBytes != want || complete.FreshRequestedBytes != want || complete.FreshBudget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fresh reads reused old accounting or lost tail charges", complete)
	}
	for _, work := range complete.Progress {
		if work.Status != "complete" || work.Sequence != 2 || work.DurableOffset != work.LogicalBytes || work.CheckedAt.IsZero() || work.SHA256 == "" {
			t.Fatal("fresh completed observation not durable", work)
		}
	}
	reader, err := OpenHashReader(context.Background(), f.fresh.m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.now = func() time.Time { return f.consent.Approval.ExpiresAt.Add(time.Hour) }
	if got, e := reader.FreshJob(context.Background(), f.fresh.job.ID); e != nil || !reflect.DeepEqual(got, complete) {
		t.Fatal("saved reader evaluated expiry or lost sequential observations", got, e)
	}
}

func freshRunChangeLimits(t *testing.T, f *freshRunFixture, day, total int64, clock time.Time) {
	t.Helper()
	// Create a separate explicit job: an existing approval is never modified.
	w := f.fresh.m.f.store
	job := saveFreshJobFixture(t, w, f.fresh.request, hashChoiceJobKey(2))
	f.fresh.job = job
	f.fresh.approval = HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: f.fresh.request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: day, LifetimeReservedByteLimit: total}
	w.now = func() time.Time { return clock }
	f.consent = approveFreshReadFixture(t, f.fresh)
}

func TestHashFreshRunCapsQuantumAndUTCRolloverUseOwnFreshBudget(t *testing.T) {
	for _, cap := range []int64{1, 63} {
		t.Run(fmt.Sprint(cap), func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			freshRunChangeLimits(t, f, cap, cap, hashChoiceClock())
			w := f.open(t)
			before := requireFreshRunSaved(t, f, w)
			r, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
			if !errors.Is(err, ErrHashDeferred) || r.Code != "durable_quantum" || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("nonadvancing fresh quantum read or changed error code", r, err)
			}
			after := requireFreshRunSaved(t, f, w)
			if !reflect.DeepEqual(after.Progress, before.Progress) || after.FreshReservedBytes != 0 {
				t.Fatal("deferred fresh quantum advanced/charged work", after)
			}
		})
	}
	t.Run("rollover_fairness_lifetime", func(t *testing.T) {
		f := freshRunFiles(t, 129, 2, "2", "1")
		at := time.Date(2026, 10, 7, 23, 59, 0, 0, time.UTC)
		freshRunChangeLimits(t, f, 64, 128, at)
		w := f.open(t)
		first, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
		if err != nil || first.Ordinal != 1 || first.DurableOffset != 64 || first.ReservedBytes != 64 || first.Usage.ReadBytes != 64 {
			t.Fatal("fresh first day quantum failed", first, err)
		}
		if r, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashDeferred) || r.Code != "daily_byte_limit" || r.ReservedBytes != 0 {
			t.Fatal("fresh daily cap ignored", r, e)
		}
		w = f.open(t)
		w.now = func() time.Time { return at.Add(2 * time.Minute) }
		second, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
		if err != nil || second.Ordinal != 2 || second.DurableOffset != 64 || second.FreshBudget.ReservedBytes != 64 || second.FreshBudget.TotalReservedBytes != 128 {
			t.Fatal("UTC rollover lost lifetime charge/fair queue", second, err)
		}
		if r, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashDeferred) || r.Code != "lifetime_byte_limit" || r.ReservedBytes != 0 {
			t.Fatal("fresh lifetime cap was renewed by day rollover", r, e)
		}
		requireFreshRunSaved(t, f, w)
	})
	t.Run("terminal_unaligned", func(t *testing.T) {
		f := freshRunFiles(t, 65, 2, "2", "1")
		freshRunChangeLimits(t, f, 65, 65, hashChoiceClock())
		w := f.open(t)
		r, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
		if err != nil || r.Status != "hash_observed" || r.ReservedBytes != 65 || r.DurableOffset != 65 || r.Progress.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(65))) {
			t.Fatal("fresh unaligned final tail lost exact digest/charge", r, err)
		}
		requireFreshRunSaved(t, f, w)
	})
}

func TestHashFreshRunRejectsOriginalCrossJobAndNonRunWritersBeforeRead(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	metadata := f.fresh.m.f.store
	otherJob := saveFreshJobFixture(t, metadata, f.fresh.request, hashChoiceJobKey(2))
	otherReq := f.fresh.approval
	otherReq.JobID = otherJob.ID
	otherReq.JobKey = otherJob.Record.JobKey
	otherConsent, err := metadata.ApproveFreshRead(context.Background(), otherReq)
	if err != nil {
		t.Fatal(err)
	}
	if got, e := metadata.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashFreshRunBinding) || got.ReservedBytes != 0 {
		t.Fatal("metadata-only writer dispatched fresh reads", got, e)
	}
	w := f.open(t)
	before := requireFreshRunSaved(t, f, w)
	for _, id := range []string{f.fresh.m.consent.ID, otherConsent.ID, "hash-job-read-v1-" + strings.Repeat("0", 64)} {
		opened := false
		r, e := w.runFreshConsented(context.Background(), id, f.fresh.m.f.scanner, hashFreshRunHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
		if e == nil || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
			t.Fatal("wrong approval reached source or reserved fresh bytes", r, e)
		}
	}
	after := requireFreshRunSaved(t, f, w)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("refused cross-scope invocation changed selected job")
	}
	other, err := w.FreshJob(context.Background(), otherJob.ID)
	if err != nil || len(other.Progress) != 0 || other.FreshReservedBytes != 0 {
		t.Fatal("opening one job initialized unrelated progress", other, err)
	}
}

func TestHashFreshRunCancellationRetainsChargeWithoutCheckedProgressOrDigest(t *testing.T) {
	for _, phase := range []string{"before_reserve", "after_reserve", "after_read", "before_settle", "after_settle"} {
		t.Run(phase, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashFreshRunHooks{}
			switch phase {
			case "before_reserve":
				hooks.beforeReserveCommit = cancel
			case "after_reserve":
				hooks.afterReserve = cancel
			case "after_read":
				hooks.file.afterRead = func(int) { cancel() }
			case "before_settle":
				hooks.beforeSettleCommit = cancel
			case "after_settle":
				hooks.afterSettleCommit = cancel
			}
			r, err := w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner, hooks)
			if !errors.Is(err, context.Canceled) || r.Progress.SHA256 != "" {
				t.Fatal("canceled fresh call published positive digest", r, err)
			}
			wantCharge, wantRead := int64(65), int64(65)
			if phase == "before_reserve" {
				wantCharge, wantRead = 0, 0
			} else if phase == "after_reserve" {
				wantRead = 0
			}
			if r.ReservedBytes != wantCharge || r.Usage.ReadBytes != wantRead {
				t.Fatal("cancellation lost committed charge/actual usage", r)
			}
			job := requireFreshRunSaved(t, f, w)
			work := job.Progress[0]
			if phase == "after_settle" {
				if work.Status != "complete" || work.DurableOffset != 65 || work.SHA256 == "" {
					t.Fatal("late cancellation erased already committed historical observation", work)
				}
			} else if work.DurableOffset != 0 || work.SHA256 != "" {
				t.Fatal("cancel committed tentative fresh SHA/prefix", work)
			}
			if job.FreshReservedBytes != wantCharge || job.FreshReadBytes != wantRead || job.FreshBudget.TotalUnknownReservedBytes != 0 {
				t.Fatal("known cancellation invented unknown usage or refunded charge", job)
			}
			if wantCharge != 0 && (work.LatestAttempt == nil || work.LatestAttempt.Status != "settled" || work.LatestAttempt.ReadBytes == nil || *work.LatestAttempt.ReadBytes != wantRead) {
				t.Fatal("known cancellation was not durably settled", work)
			}
		})
	}
}

func TestHashFreshRunChangedInventoryAndLiveEvidenceNeverReadReplacement(t *testing.T) {
	for _, kind := range []string{"saved_file", "saved_parent", "saved_after_read", "restored_mtime", "symlink", "protected", "excluded"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			path := string(f.fresh.job.Record.Request.Targets[0].Target.File.PathBytes)
			hooks := hashFreshRunHooks{}
			db, err := sql.Open("sqlite", filepath.Join(f.fresh.m.f.stateDir, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mutate := func() {
				if _, e := db.Exec("UPDATE entries SET ctime_ns=ctime_ns+1 WHERE kind='file'"); e != nil {
					t.Fatal(e)
				}
			}
			switch kind {
			case "saved_file":
				mutate()
			case "saved_parent":
				if _, err = db.Exec("UPDATE entries SET generation=generation+1 WHERE kind='directory' AND path=x'70726f6a656374'"); err != nil {
					t.Fatal(err)
				}
			case "saved_after_read":
				hooks.beforeFinalInventoryCheck = mutate
			case "restored_mtime":
				st, e := os.Stat(path)
				if e != nil {
					t.Fatal(e)
				}
				body := fullHashContents(65)
				body[0] ^= 0xff
				if e = os.WriteFile(path, body, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Chtimes(path, st.ModTime(), st.ModTime()); e != nil {
					t.Fatal(e)
				}
			case "symlink":
				if err = os.Rename(path, path+".bound"); err != nil {
					t.Fatal(err)
				}
				if err = os.Symlink(path+".bound", path); err != nil {
					t.Fatal(err)
				}
			case "protected":
				f.fresh.m.f.scanner.protectedIDs[f.fresh.job.Record.Request.Targets[0].Target.File.Device+":"+f.fresh.job.Record.Request.Targets[0].Target.File.Inode] = true
			case "excluded":
				f.fresh.m.f.scanner.Close()
				f.fresh.m.f.scanner, err = New(nil, []string{path}, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
			if err == nil || r.Progress.SHA256 != "" || r.DurableOffset != 0 {
				t.Fatal("changed evidence produced fresh observed contents", r, err)
			}
			wantRead := int64(0)
			if kind == "saved_after_read" {
				wantRead = 65
			}
			if r.Usage.ReadBytes != wantRead {
				t.Fatal("refusal read replacement or missed final-snapshot fixture", kind, r)
			}
			job := requireFreshRunSaved(t, f, w)
			if job.Progress[0].SHA256 != "" || job.Progress[0].DurableOffset != 0 {
				t.Fatal("changed evidence published partial fresh progress", job)
			}
		})
	}
}

func TestHashFreshRunOldOrOtherJobCheckpointCannotBeImported(t *testing.T) {
	for _, kind := range []string{"original", "other_job"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			var checkpoint []byte
			if kind == "original" {
				if err := f.fresh.m.f.store.db.QueryRow("SELECT checkpoint FROM hash_work WHERE id=2").Scan(&checkpoint); err != nil {
					t.Fatal(err)
				}
			} else {
				metadata := f.fresh.m.f.store
				other := saveFreshJobFixture(t, metadata, f.fresh.request, hashChoiceJobKey(2))
				req := f.fresh.approval
				req.JobID = other.ID
				req.JobKey = other.Record.JobKey
				if _, err := metadata.ApproveFreshRead(context.Background(), req); err != nil {
					t.Fatal(err)
				}
				if err := metadata.Close(); err != nil {
					t.Fatal(err)
				}
				w, err := OpenHashFreshRunWriter(context.Background(), f.fresh.request, other.ID)
				if err != nil {
					t.Fatal(err)
				}
				f.fresh.m.f.store = w
				if err = w.db.QueryRow("SELECT checkpoint FROM hash_fresh_progress WHERE job_id=? AND ordinal=1", other.ID).Scan(&checkpoint); err != nil {
					t.Fatal(err)
				}
			}
			w := f.open(t)
			if _, err := w.db.Exec("UPDATE hash_fresh_progress SET checkpoint=? WHERE job_id=? AND ordinal=1", checkpoint, f.fresh.job.ID); err != nil {
				t.Fatal(err)
			}
			opened := false
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
			if !errors.Is(err, ErrHashFreshProgressCorrupt) || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("old/cross-job checkpoint imported SHA state", kind, r, err)
			}
			if job, e := w.FreshJob(context.Background(), f.fresh.job.ID); !errors.Is(e, ErrHashFreshProgressCorrupt) || job.ID != "" {
				t.Fatal("corrupt fresh progress returned partial saved job", job, e)
			}
			f.checkOriginal(t, w)
		})
	}
}

func TestHashFreshRunExactJobRecoveryPreservesOriginalAndUnrelatedUnknownWork(t *testing.T) {
	// The fourth original work item remains pending until a genuine read loses
	// settlement. Its charged/null attempt must never be recovered by a fresh
	// writer, even when that writer explicitly recovers one fresh job.
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	if err := m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := localfs.EnsurePrivateDir(filepath.Join(m.f.base, "manual")); err != nil {
		t.Fatal(err)
	}
	derived := filepath.Join(m.f.base, "manual", request.SourceLocator().InventoryKey)
	if err := os.Rename(m.f.stateDir, derived); err != nil {
		t.Fatal(err)
	}
	m.f.stateDir = derived
	var err error
	m.f.source, err = state.OpenWriter(context.Background(), derived)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.store, err = OpenExistingHashWriter(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.CreatedAt.Add(time.Hour) }
	if _, err = m.f.store.db.Exec("CREATE TRIGGER fail_fresh_run_original_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'generated lost settlement'); END"); err != nil {
		t.Fatal(err)
	}
	originalResult, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if !errors.Is(err, ErrHashRecoveryRequired) || originalResult.Usage.ReadBytes != 65 {
		t.Fatal("fixture lacks genuine unsettled original read", originalResult, err)
	}
	if _, err = m.f.store.db.Exec("DROP TRIGGER fail_fresh_run_original_fixture"); err != nil {
		t.Fatal(err)
	}
	original := hashStoreSnapshot(t, m.f.store)
	if original.Work[3].Status != "running" || original.Work[3].LatestAttempt == nil || original.Work[3].LatestAttempt.ReadBytes != nil || original.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture original was already recovered", original)
	}
	metadata := hashChoiceJobWriter(t, m, request)
	jobOne := saveFreshJobFixture(t, metadata, request, hashChoiceJobKey(1))
	one := &freshReadFixture{m: m, request: request, job: jobOne, approval: HashFreshReadApprovalRequest{JobID: jobOne.ID, JobKey: jobOne.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192}}
	oneConsent := approveFreshReadFixture(t, one)
	jobTwo := saveFreshJobFixture(t, metadata, request, hashChoiceJobKey(2))
	two := *one
	two.job = jobTwo
	two.approval.JobID = jobTwo.ID
	two.approval.JobKey = jobTwo.Record.JobKey
	twoConsent := approveFreshReadFixture(t, &two)
	fOne := &freshRunFixture{fresh: one, consent: oneConsent, original: original}
	fTwo := &freshRunFixture{fresh: &two, consent: twoConsent, original: original}
	for _, f := range []*freshRunFixture{fTwo, fOne} {
		w := f.open(t)
		r, e := w.runFreshConsented(context.Background(), f.consent.ID, m.f.scanner, hashFreshRunHooks{settleCommit: func(*sql.Tx) error { return errors.New("generated missing settlement acknowledgment") }})
		if !errors.Is(e, ErrHashRecoveryRequired) || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
			t.Fatal("fixture did not retain charged uncertain fresh attempt", r, e)
		}
		job := requireFreshRunSaved(t, f, w)
		if job.Progress[0].Status != "running" || job.Progress[0].LatestAttempt == nil || job.Progress[0].LatestAttempt.ReadBytes != nil || job.FreshReadBytes != 0 || job.FreshReservedBytes != 65 || job.FreshBudget.TotalUnknownReservedBytes != 0 {
			t.Fatal("unsettled fresh usage fabricated or already recovered", job)
		}
	}
	if err = m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err = OpenHashFreshJobWriter(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store = metadata
	metadata.now = hashChoiceClock
	for _, f := range []*freshRunFixture{fOne, fTwo} {
		job := requireFreshRunSaved(t, f, metadata)
		if job.Progress[0].Status != "running" || job.FreshBudget.TotalUnknownReservedBytes != 0 {
			t.Fatal("nonrecovering consent writer recovered fresh work", job)
		}
	}
	if _, err = metadata.RevokeFreshRead(context.Background(), oneConsent.ID); err != nil {
		t.Fatal(err)
	}
	if err = m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.scanner.Close()
	for _, path := range []string{m.f.root, m.f.stateDir} {
		if err = os.Rename(path, path+".offline"); err != nil {
			t.Fatal(err)
		}
	}
	scanner, err := New(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer scanner.Close()
	w := fOne.open(t) // Offline recovery works even though this consent is revoked.
	oneRecovered := requireFreshRunSaved(t, fOne, w)
	if oneRecovered.Progress[0].Status != "pending" || oneRecovered.Progress[0].Sequence != 1 || oneRecovered.Progress[0].DurableOffset != 0 || oneRecovered.Progress[0].LatestAttempt.Status != "interrupted_unknown" || oneRecovered.Progress[0].LatestAttempt.ReadBytes != nil || oneRecovered.FreshReservedBytes != 65 || oneRecovered.FreshReadBytes != 0 || oneRecovered.FreshBudget.TotalUnknownReservedBytes != 65 {
		t.Fatal("exact recovery refunded charge/imported tentative SHA/zero usage", oneRecovered)
	}
	twoStillRunning := requireFreshRunSaved(t, fTwo, w)
	if twoStillRunning.Progress[0].Status != "running" || twoStillRunning.FreshBudget.TotalUnknownReservedBytes != 0 {
		t.Fatal("exact job recovery changed unrelated job", twoStillRunning)
	}
	if r, e := w.RunFreshConsented(context.Background(), oneConsent.ID, scanner); !errors.Is(e, ErrHashReadRevoked) || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
		t.Fatal("recovery treated revoked consent as new source permission", r, e)
	}
	w = fTwo.open(t)
	twoRecovered := requireFreshRunSaved(t, fTwo, w)
	if twoRecovered.Progress[0].LatestAttempt.Status != "interrupted_unknown" || twoRecovered.FreshBudget.TotalUnknownReservedBytes != 65 || twoRecovered.FreshReservedBytes != 65 || twoRecovered.FreshReadBytes != 0 {
		t.Fatal("second explicit recovery lost independent unknown charge", twoRecovered)
	}
	if got := requireFreshRunSaved(t, fOne, w); !reflect.DeepEqual(got, oneRecovered) {
		t.Fatal("second job recovery changed first recovered/revoked job", got, oneRecovered)
	}
}

func TestHashFreshRunReservationAndSettlementUncertaintyRequireExactRecovery(t *testing.T) {
	for _, phase := range []string{"reserve_uncommitted", "reserve_committed", "settle_uncommitted", "settle_committed"} {
		t.Run(phase, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			fail := func(*sql.Tx) error { return errors.New("generated missing commit acknowledgment") }
			lost := func(tx *sql.Tx) error {
				if err := tx.Commit(); err != nil {
					return err
				}
				return errors.New("generated lost committed reply")
			}
			hooks := hashFreshRunHooks{}
			switch phase {
			case "reserve_uncommitted":
				hooks.reserveCommit = fail
			case "reserve_committed":
				hooks.reserveCommit = lost
			case "settle_uncommitted":
				hooks.settleCommit = fail
			case "settle_committed":
				hooks.settleCommit = lost
			}
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
			if !errors.Is(err, ErrHashRecoveryRequired) || r.Progress.SHA256 != "" {
				t.Fatal("uncertain publication returned success/live digest", r, err)
			}
			if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
				t.Fatal("poisoned fresh writer accepted another invocation", e)
			}
			w = f.open(t)
			job := requireFreshRunSaved(t, f, w)
			work := job.Progress[0]
			switch phase {
			case "reserve_uncommitted":
				if job.FreshReservedBytes != 0 || work.Sequence != 0 || work.LatestAttempt != nil || work.DurableOffset != 0 {
					t.Fatal("uncommitted reservation survived exact recovery", job)
				}
			case "reserve_committed", "settle_uncommitted":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 0 || job.FreshBudget.TotalUnknownReservedBytes != 65 || work.Sequence != 1 || work.DurableOffset != 0 || work.LatestAttempt.Status != "interrupted_unknown" || work.LatestAttempt.ReadBytes != nil {
					t.Fatal("uncertain attempt refunded/fabricated usage or imported prefix", job)
				}
			case "settle_committed":
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 65 || job.FreshBudget.TotalUnknownReservedBytes != 0 || work.Status != "complete" || work.DurableOffset != 65 || work.LatestAttempt.Status != "settled" || work.SHA256 != fmt.Sprintf("%x", sha256.Sum256(fullHashContents(65))) {
					t.Fatal("committed settlement was repeated/refunded/marked unknown", job)
				}
			}
		})
	}
}

func TestHashFreshRunConstructorDefersMigrationAndCancellationPreservesSavedState(t *testing.T) {
	for _, phase := range []string{"missing_consent", "before_init", "after_init", "init_uncommitted", "init_committed"} {
		t.Run(phase, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			job := f.fresh.job
			if phase == "missing_consent" {
				job = saveFreshJobFixture(t, f.fresh.m.f.store, f.fresh.request, hashChoiceJobKey(2))
			}
			if err := f.fresh.m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashFreshRunOpenHooks{}
			want := error(context.Canceled)
			switch phase {
			case "missing_consent":
				want = ErrHashFreshReadApprovalMissing
			case "before_init":
				hooks.beforeInitCommit = cancel
			case "after_init":
				hooks.afterInitCommit = cancel
			case "init_uncommitted":
				want = ErrHashRecoveryRequired
				hooks.commit = func(*sql.Tx) error { return errors.New("generated unavailable init acknowledgment") }
			case "init_committed":
				want = ErrHashRecoveryRequired
				hooks.commit = func(tx *sql.Tx) error {
					if err := tx.Commit(); err != nil {
						return err
					}
					return errors.New("generated lost committed init reply")
				}
			}
			w, err := openHashFreshRunWriter(ctx, f.fresh.request, job.ID, hooks)
			if !errors.Is(err, want) || w != nil {
				t.Fatal("refused/canceled/uncertain constructor exposed live writer", w, err)
			}
			reader, e := OpenHashReader(context.Background(), f.fresh.m.f.base)
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			saved, e := reader.FreshJob(context.Background(), job.ID)
			if e != nil {
				t.Fatal(e)
			}
			published := phase == "after_init" || phase == "init_committed"
			if published {
				if hashChoiceSchemaVersion(t, reader.db) != 6 || len(saved.Progress) != 2 || saved.FreshBudget == nil || saved.FreshReservedBytes != 0 {
					t.Fatal("committed init reply loss hid genuine zero progress", saved)
				}
			} else if hashChoiceSchemaVersion(t, reader.db) != 5 || len(saved.Progress) != 0 || saved.FreshBudget != nil || hashChoiceTableExists(t, reader.db, "hash_fresh_progress") {
				t.Fatal("refusal/uncommitted init leaked schema6 rows", saved)
			}
			f.checkOriginal(t, reader)
		})
	}
}

func TestHashFreshRunPrivateInventoryAndLockAliasesRefuseBeforeSourceReads(t *testing.T) {
	for _, kind := range []string{"selected_inventory", "unselected_inventory", "selected_lock", "unselected_lock"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 4, "3", "2", "1")
			index := 2
			if strings.HasPrefix(kind, "unselected_") {
				index = 3
			}
			path := string(f.fresh.m.proposal.Targets[index].File.PathBytes)
			if strings.HasSuffix(kind, "_lock") {
				if err := f.fresh.m.f.store.Close(); err != nil {
					t.Fatal(err)
				}
				lock := filepath.Join(f.fresh.m.f.base, "hashes", "writer.lock")
				if err := os.Rename(lock, lock+".bound"); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path, lock); err != nil {
					t.Fatal(err)
				}
				if w, err := OpenHashFreshRunWriter(context.Background(), f.fresh.request, f.fresh.job.ID); !errors.Is(err, ErrHashFreshJobEvidence) || w != nil {
					if w != nil {
						_ = w.Close()
					}
					t.Fatal("request-aware constructor missed pre-open frozen lock alias guard", err)
				}
				if body, e := os.ReadFile(lock); e != nil || !bytes.Equal(body, fullHashContents(65)) {
					t.Fatal("constructor modified bound alias bytes", e)
				}
				return
			}
			w := f.open(t)
			if err := f.fresh.m.f.source.Close(); err != nil {
				t.Fatal(err)
			}
			dbPath := filepath.Join(f.fresh.m.f.stateDir, state.Filename)
			if err := os.Rename(dbPath, dbPath+".bound"); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(path, dbPath); err != nil {
				t.Fatal(err)
			}
			opened := false
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
			if err == nil || !strings.Contains(err.Error(), "inventory storage aliases frozen") || opened || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("selected/unselected inventory alias missed pre-SQL identity guard", r, err)
			}
			job := requireFreshRunSaved(t, f, w)
			if job.FreshReservedBytes != 0 || job.Progress[0].LatestAttempt != nil || job.Progress[0].Sequence != 0 {
				t.Fatal("source-storage alias refusal fabricated an attempt", job)
			}
			if body, e := os.ReadFile(dbPath); e != nil || !bytes.Equal(body, fullHashContents(65)) {
				t.Fatal("inventory alias refusal modified bound source bytes", e)
			}
		})
	}
}

func TestHashFreshRunCorruptAggregateAccountingAndBoundsRefuseWholeJob(t *testing.T) {
	for _, kind := range []string{"combined_known_bytes", "combined_day_bytes", "complete_unknown_attempt", "missing_budget", "checkpoint_oversize", "queue_scalar", "invalidated_no_attempt", "swapped_initial_order", "future_budget_clock", "prior_budget_clock", "excess_day_charge", "excess_total_charge"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			if strings.HasPrefix(kind, "combined_") || kind == "complete_unknown_attempt" {
				for range 2 {
					if _, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); err != nil {
						t.Fatal(err)
					}
				}
				job := requireFreshRunSaved(t, f, w)
				if job.FreshReadBytes != 130 || job.Progress[0].DurableOffset != 65 || job.Progress[1].DurableOffset != 65 {
					t.Fatal("fixture has no combined genuine progress", job)
				}
			}
			for _, trigger := range []string{"hash_fresh_budget_monotonic", "hash_fresh_budget_no_delete", "hash_fresh_run_state_monotonic", "hash_fresh_progress_monotonic"} {
				if _, err := w.db.Exec("DROP TRIGGER " + trigger); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "combined_known_bytes":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET total_requested_bytes=65,total_read_bytes=65,requested_bytes=65,read_bytes=65 WHERE job_id=?", f.fresh.job.ID)
			case "combined_day_bytes":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET reserved_bytes=65,requested_bytes=65,read_bytes=65 WHERE job_id=?", f.fresh.job.ID)
			case "complete_unknown_attempt":
				_, err = w.db.Exec("UPDATE hash_fresh_attempt SET status='interrupted_unknown',from_offset=65,grant_bytes=0,requested_bytes=NULL,read_bytes=NULL,elapsed_ns=NULL WHERE job_id=? AND ordinal=1", f.fresh.job.ID)
			case "missing_budget":
				_, err = w.db.Exec("DELETE FROM hash_fresh_budget WHERE job_id=?", f.fresh.job.ID)
			case "checkpoint_oversize":
				_, err = w.db.Exec("UPDATE hash_fresh_progress SET checkpoint=? WHERE job_id=? AND ordinal=1", bytes.Repeat([]byte("x"), 16385), f.fresh.job.ID)
			case "queue_scalar":
				_, err = w.db.Exec("UPDATE hash_fresh_run_state SET next_order='invalid' WHERE job_id=?", f.fresh.job.ID)
			case "invalidated_no_attempt":
				_, err = w.db.Exec("UPDATE hash_fresh_progress SET status='invalidated',error_code='fixture' WHERE job_id=? AND ordinal=1", f.fresh.job.ID)
			case "swapped_initial_order":
				if _, err = w.db.Exec("UPDATE hash_fresh_progress SET ready_order=ready_order+2 WHERE job_id=?", f.fresh.job.ID); err == nil {
					_, err = w.db.Exec("UPDATE hash_fresh_progress SET ready_order=3-ordinal WHERE job_id=?", f.fresh.job.ID)
				}
			case "future_budget_clock":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET max_now_ns=? WHERE job_id=?", f.consent.ClockHighWater.Add(time.Hour).UnixNano(), f.fresh.job.ID)
			case "prior_budget_clock":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET max_now_ns=? WHERE job_id=?", f.consent.Approval.CreatedAt.Add(-time.Nanosecond).UnixNano(), f.fresh.job.ID)
			case "excess_day_charge":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET reserved_bytes=?,total_reserved_bytes=? WHERE job_id=?", f.consent.Approval.DailyReservedByteLimit+1, f.consent.Approval.DailyReservedByteLimit+1, f.fresh.job.ID)
			case "excess_total_charge":
				_, err = w.db.Exec("UPDATE hash_fresh_budget SET total_reserved_bytes=? WHERE job_id=?", f.consent.Approval.LifetimeReservedByteLimit+1, f.fresh.job.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			if job, e := w.FreshJob(context.Background(), f.fresh.job.ID); !errors.Is(e, ErrHashFreshProgressCorrupt) || job.ID != "" {
				t.Fatal("corrupt bounded accounting returned partial saved progress", kind, job, e)
			}
			opened := false
			r, e := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{file: fileHashHooks{afterOpen: func() { opened = true }}})
			if !errors.Is(e, ErrHashFreshProgressCorrupt) || opened || r.ReservedBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("corrupt aggregate bytes or schema reached fresh source", kind, r, e)
			}
			f.checkOriginal(t, w)
		})
	}
}

func TestHashFreshRunExhaustedOuterDeadlinePreservesUnknownChargeAndRequiresRecovery(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	// Keep the production-sized window for instrumented preparation. Exhaust
	// that same deadline only after the fixture has observed a source read.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	read := false
	r, err := w.runFreshConsented(ctx, f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{file: fileHashHooks{afterRead: func(int) { read = true; <-ctx.Done() }}})
	if !read || err == nil || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
		t.Fatal("deadline fixture did not preserve charged tentative read without digest", r, err)
	}
	if overdue := time.Since(deadline); overdue > time.Second {
		t.Fatal("settlement started a new two-second window after exhausting original deadline", overdue)
	}
	if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
		t.Fatal("exhausted settlement deadline left dispatchable writer", e)
	}
	before := requireFreshRunSaved(t, f, w)
	if before.Progress[0].Status != "running" || before.Progress[0].LatestAttempt.ReadBytes != nil || before.FreshReadBytes != 0 || before.FreshBudget.TotalUnknownReservedBytes != 0 {
		t.Fatal("unsaved deadline usage was fabricated or prematurely recovered", before)
	}
	w = f.open(t)
	after := requireFreshRunSaved(t, f, w)
	if after.Progress[0].Status != "pending" || after.Progress[0].DurableOffset != 0 || after.Progress[0].LatestAttempt.Status != "interrupted_unknown" || after.Progress[0].LatestAttempt.ReadBytes != nil || after.FreshReservedBytes != 65 || after.FreshReadBytes != 0 || after.FreshBudget.TotalUnknownReservedBytes != 65 {
		t.Fatal("deadline recovery refunded charge/imported tentative SHA/known zero", after)
	}
}

func TestHashFreshRunExpiryOrClockRollbackDuringReadVetoesNewDigest(t *testing.T) {
	for _, kind := range []string{"expiry", "rollback"} {
		for _, phase := range []string{"during_read", "before_settle"} {
			t.Run(kind+"/"+phase, func(t *testing.T) {
				f := freshRunFiles(t, 65, 2, "2", "1")
				w := f.open(t)
				at := f.consent.Approval.ExpiresAt
				want := ErrHashReadExpired
				if kind == "rollback" {
					at = f.consent.Approval.CreatedAt.Add(-time.Hour)
					want = ErrHashReadClockRollback
				}
				hooks := hashFreshRunHooks{}
				changeClock := func() { w.now = func() time.Time { return at } }
				if phase == "during_read" {
					hooks.file.afterRead = func(int) { changeClock() }
				} else {
					hooks.beforeSettleCommit = changeClock
				}
				r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
				if !errors.Is(err, want) || r.ReservedBytes != 65 || r.Usage.ReadBytes != 65 || r.DurableOffset != 0 || r.Progress.SHA256 != "" {
					t.Fatal("changed consent time published tentative fresh digest", r, err)
				}
				job := requireFreshRunSaved(t, f, w)
				if job.FreshReservedBytes != 65 || job.FreshReadBytes != 65 || job.Progress[0].DurableOffset != 0 || job.Progress[0].SHA256 != "" || job.FreshBudget.TotalUnknownReservedBytes != 0 {
					t.Fatal("read-time lifecycle refusal lost known charge/usage or imported prefix", job)
				}
				if _, e := w.RevokeFreshRead(context.Background(), f.consent.ID); e != nil {
					t.Fatal("lifecycle refusal blocked revocation", e)
				}
			})
		}
	}
}

func TestHashFreshRunShortExpiryWindowRefusesBeforeSourceAndDoesNotClaimExpiry(t *testing.T) {
	f := freshRunFiles(t, 65, 2, "2", "1")
	w := f.open(t)
	before := requireFreshRunSaved(t, f, w)
	// An absent inventory makes ordering observable: admission must refuse
	// before opening the exact derived inventory or checking live sources.
	if err := f.fresh.m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.fresh.m.f.stateDir, f.fresh.m.f.stateDir+".offline"); err != nil {
		t.Fatal(err)
	}
	near := f.consent.Approval.ExpiresAt.Add(-time.Second)
	w.now = func() time.Time { return near }
	opened := false
	hooks := hashFreshRunHooks{beforeInventoryCheck: func() { opened = true }, file: fileHashHooks{afterOpen: func() { opened = true }}}
	r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
	if !errors.Is(err, ErrHashFreshReadWindow) || r.Status != "refused" || r.Code != "fresh_read_window_too_short" || opened || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
		t.Fatal("still-valid short expiry window reached source/reservation or claimed actual expiry", r, err)
	}
	after := requireFreshRunSaved(t, f, w)
	if !reflect.DeepEqual(after.Progress, before.Progress) || !reflect.DeepEqual(after.FreshBudget, before.FreshBudget) || after.ReadConsent == nil || after.ReadConsent.ExpiredObserved || after.ReadConsent.Status != "recorded" || !after.ReadConsent.ClockHighWater.Equal(near) {
		t.Fatal("short window changed work/charges or persisted false expiry", after)
	}
	reader, e := OpenHashReader(context.Background(), f.fresh.m.f.base)
	if e != nil {
		t.Fatal(e)
	}
	reader.now = func() time.Time { return f.consent.Approval.ExpiresAt.Add(time.Hour) }
	if saved, e := reader.FreshJob(context.Background(), f.fresh.job.ID); e != nil || !reflect.DeepEqual(saved, after) {
		t.Fatal("saved near-expiry reader evaluated clock or lost exact observation", saved, e)
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	w = f.open(t) // The constructor itself performs no permission evaluation.
	w.now = func() time.Time { return near.Add(-time.Second) }
	r, err = w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
	if !errors.Is(err, ErrHashReadClockRollback) || r.Status != "refused" || r.Code != "clock_rollback" || opened || r.ReservedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
		t.Fatal("short-window reopen rewound clock or dispatched source", r, err)
	}
	if saved := requireFreshRunSaved(t, f, w); !reflect.DeepEqual(saved, after) {
		t.Fatal("rollback after short-window refusal changed saved clock/charges", saved, after)
	}
}

func TestHashFreshRunExpiryAtSecondAdmissionLatchesDurablyOrPoisons(t *testing.T) {
	for _, kind := range []string{"expires", "rollback", "expiry_observation_failed"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, 65, 2, "2", "1")
			w := f.open(t)
			before := requireFreshRunSaved(t, f, w)
			if err := f.fresh.m.f.source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(f.fresh.m.f.stateDir, f.fresh.m.f.stateDir+".offline"); err != nil {
				t.Fatal(err)
			}
			if kind == "expiry_observation_failed" {
				if _, err := w.db.Exec("CREATE TRIGGER fail_fresh_expiry_fixture BEFORE UPDATE ON hash_fresh_read_observation WHEN NEW.expired=1 BEGIN SELECT RAISE(ABORT,'generated expiry publication refusal'); END"); err != nil {
					t.Fatal(err)
				}
			}
			clockCalls := 0
			w.now = func() time.Time {
				clockCalls++
				if clockCalls == 1 {
					return f.consent.Approval.CreatedAt
				}
				if kind == "rollback" {
					return f.consent.Approval.CreatedAt.Add(-time.Second)
				}
				return f.consent.Approval.ExpiresAt
			}
			opened := false
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hashFreshRunHooks{beforeInventoryCheck: func() { opened = true }, file: fileHashHooks{afterOpen: func() { opened = true }}})
			want := ErrHashReadExpired
			code := "read_consent_expired"
			status := "refused"
			if kind == "rollback" {
				want = ErrHashReadClockRollback
				code = "clock_rollback"
			}
			if kind == "expiry_observation_failed" {
				want = ErrHashRecoveryRequired
				code = "publication_uncertain"
				status = "recovery_required"
			}
			if !errors.Is(err, want) || r.Status != status || r.Code != code || clockCalls < 2 || opened || r.ReservedBytes != 0 || r.Usage.RequestedBytes != 0 || r.Usage.ReadBytes != 0 || r.Progress.SHA256 != "" {
				t.Fatal("second admission expiry/rollback used wrong refusal or opened source", r, err, clockCalls)
			}
			after := requireFreshRunSaved(t, f, w)
			if !reflect.DeepEqual(after.Progress, before.Progress) || !reflect.DeepEqual(after.FreshBudget, before.FreshBudget) || after.ReadConsent == nil {
				t.Fatal("second admission changed fresh work or accounting", after)
			}
			if kind == "expires" {
				if !after.ReadConsent.ExpiredObserved || after.ReadConsent.Status != "expired_observed" || !after.ReadConsent.ClockHighWater.Equal(f.consent.Approval.ExpiresAt) {
					t.Fatal("actual admission expiry was not durably latched", after.ReadConsent)
				}
				w = f.open(t)
				w.now = func() time.Time { return f.consent.Approval.CreatedAt }
				if got, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashReadExpired) || got.ReservedBytes != 0 || got.Usage.ReadBytes != 0 {
					t.Fatal("reopened admission expiry renewed under rollback", got, e)
				}
			} else if after.ReadConsent.ExpiredObserved || !after.ReadConsent.ClockHighWater.Equal(f.consent.Approval.CreatedAt) {
				t.Fatal("unconfirmed expiry or rollback advanced saved permission record", after.ReadConsent)
			}
			if kind == "expiry_observation_failed" {
				if _, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashRecoveryRequired) {
					t.Fatal("unconfirmed expiry did not poison fresh run writer", e)
				}
				if _, e := w.db.Exec("DROP TRIGGER fail_fresh_expiry_fixture"); e != nil {
					t.Fatal(e)
				}
				w = f.open(t)
				w.now = func() time.Time { return f.consent.Approval.ExpiresAt }
				if got, e := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner); !errors.Is(e, ErrHashReadExpired) || got.ReservedBytes != 0 || got.Usage.ReadBytes != 0 {
					t.Fatal("explicit reopen failed to observe actual expiry without source", got, e)
				}
			}
		})
	}
}
