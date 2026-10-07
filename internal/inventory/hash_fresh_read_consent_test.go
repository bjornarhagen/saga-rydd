package inventory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// All jobs, historical observations and crash targets are generated. Consent
// operations have no source, scanner, configuration or dispatch dependency.
type freshReadFixture struct {
	m        *hashChoiceRequestFixture
	request  *KeeperChoiceFreshRequest
	job      SavedFreshJob
	approval HashFreshReadApprovalRequest
}

func freshReadFixtureFiles(t *testing.T) *freshReadFixture {
	t.Helper()
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	w := hashChoiceJobWriter(t, m, request)
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	approval := HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192}
	return &freshReadFixture{m: m, request: request, job: job, approval: approval}
}

func requireFreshReadConsent(t *testing.T, c HashFreshReadConsent, f *freshReadFixture) {
	t.Helper()
	a := c.Approval
	if !ValidHashFreshReadApprovalID(c.ID) || c.ID == f.m.consent.ID || a.Version != 1 || a.Contract != "explicit_choice_bound_fresh_full_file_hash_read_v1" || a.HashContract != FileHashContract || a.JobID != f.job.ID || a.JobKey != f.job.Record.JobKey || a.RequestID != f.request.ID() || a.StoreID != f.job.Record.Request.StoreID || a.SelectionID != f.job.Record.Request.SelectionID || a.InventoryID != f.job.Record.Request.InventoryID || !reflect.DeepEqual(a.SourceLocator, f.request.SourceLocator()) || !hashStoreDigest(a.RoleTargetScopeDigest) || a.CreatedAt.IsZero() || !a.ExpiresAt.Equal(a.CreatedAt.Add(24*time.Hour)) || a.StepByteLimit != FileHashStepByteLimit || a.InitialTotalReservedBytes != 0 || !a.ConfirmFullFileRead || c.ClockHighWater.Before(a.CreatedAt) || c.CurrentReadPermissionEvaluated || c.ProvenanceVerified || c.ContentVerified || c.CurrentStateVerified || c.DuplicatesVerified || c.Executable || c.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh consent reused original authority, changed exact scope or lost fixed limits", c)
	}
	if c.Status != "recorded" && c.Status != "expired_observed" && c.Status != "revoked" {
		t.Fatal("invalid fresh saved consent status", c)
	}
	body, err := json.Marshal(a)
	if err != nil || len(body) > 16384 || c.ID != fmt.Sprintf("hash-job-read-v1-%x", sha256.Sum256(body)) {
		t.Fatal("fresh approval ID did not bind canonical exact immutable approval", c.ID, err)
	}
}

func approveFreshReadFixture(t *testing.T, f *freshReadFixture) HashFreshReadConsent {
	t.Helper()
	c, err := f.m.f.store.ApproveFreshRead(context.Background(), f.approval)
	if err != nil {
		t.Fatal(err)
	}
	requireFreshReadConsent(t, c, f)
	return c
}

func requireFreshJobSeedUnchanged(t *testing.T, w *HashStore, prior SavedFreshJob) SavedFreshJob {
	t.Helper()
	got, err := w.FreshJob(context.Background(), prior.ID)
	if err != nil || !reflect.DeepEqual(got.Record, prior.Record) || !reflect.DeepEqual(got.Work, prior.Work) || got.FreshReservedBytes != 0 || got.FreshRequestedBytes != 0 || got.FreshReadBytes != 0 || got.ApprovalAvailable || got.ProvenanceVerified || got.ContentVerified || got.CurrentStateVerified || got.DuplicatesVerified || got.Executable || got.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh consent changed immutable seed/request or claimed actual progress/permission", got, err)
	}
	return got
}

func TestHashFreshReadApprovalExactBindingFixedLifetimeAndClonedSavedEvidence(t *testing.T) {
	f := freshReadFixtureFiles(t)
	w := f.m.f.store
	before := hashStoreSnapshot(t, w)
	oldReader, err := OpenHashReader(context.Background(), f.m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer oldReader.Close()
	if hashChoiceSchemaVersion(t, w.db) != 4 || hashChoiceTableExists(t, w.db, "hash_fresh_read_approval") {
		t.Fatal("fixture already published fresh consent")
	}
	c := approveFreshReadFixture(t, f)
	if !c.Approval.CreatedAt.Equal(hashChoiceClock()) || c.Approval.DailyReservedByteLimit != 4096 || c.Approval.LifetimeReservedByteLimit != 8192 || hashChoiceSchemaVersion(t, w.db) != 5 {
		t.Fatal("approval did not atomically publish fixed first limits and schema5", c)
	}
	for _, reader := range []*HashStore{w, oldReader} {
		got, e := reader.FreshReadApproval(context.Background(), c.ID)
		if e != nil || !reflect.DeepEqual(got, c) {
			t.Fatal("saved reader failed to reopen exact whole approval", got, e)
		}
		job := requireFreshJobSeedUnchanged(t, reader, f.job)
		if job.ReadConsent == nil || !reflect.DeepEqual(*job.ReadConsent, c) {
			t.Fatal("saved job lost recoverable fresh approval ID", job)
		}
		if !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) {
			t.Fatal("schema5 consent changed original approval, charge or work")
		}
	}
	changed, err := w.FreshReadApproval(context.Background(), c.ID)
	if err != nil {
		t.Fatal(err)
	}
	changed.Approval.SourceLocator.RootPathBytes[0] = 'x'
	got, e := w.FreshReadApproval(context.Background(), c.ID)
	if e != nil || !reflect.DeepEqual(got, c) {
		t.Fatal("returned consent shared mutable source binding", got, e)
	}
	w.now = func() time.Time { return c.Approval.CreatedAt.Add(time.Hour) }
	retry := approveFreshReadFixture(t, f)
	if retry.ID != c.ID || !reflect.DeepEqual(retry.Approval, c.Approval) || !retry.ClockHighWater.Equal(w.now()) {
		t.Fatal("exact retry renewed or expanded immutable approval", retry)
	}
	for _, limits := range [][2]int64{{2048, 8192}, {4096, 16384}, {8192, 8192}} {
		changed := f.approval
		changed.DailyReservedByteLimit = limits[0]
		changed.LifetimeReservedByteLimit = limits[1]
		if got, e := w.ApproveFreshRead(context.Background(), changed); !errors.Is(e, ErrHashFreshReadApprovalConflict) || got.ID != "" {
			t.Fatal("changed caps renewed fresh approval", got, e)
		}
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("fresh retry/conflict changed original clocks or accounting")
	}
}

func TestHashFreshReadApprovalRequiresExactTupleConfirmationAndBoundedCaps(t *testing.T) {
	f := freshReadFixtureFiles(t)
	w := f.m.f.store
	before := hashStoreSnapshot(t, w)
	second := saveFreshJobFixture(t, w, f.request, hashChoiceJobKey(2))
	for _, kind := range []string{"confirmation", "zero_day", "zero_total", "negative_day", "large_day", "large_total", "old_original_id", "job_key", "request_id", "cross_job"} {
		request := f.approval
		want := ErrHashFreshReadBinding
		switch kind {
		case "confirmation":
			request.ConfirmFullFileRead = false
			want = ErrHashFreshReadConfirmation
		case "zero_day":
			request.DailyReservedByteLimit = 0
			want = ErrHashFreshReadLimits
		case "zero_total":
			request.LifetimeReservedByteLimit = 0
			want = ErrHashFreshReadLimits
		case "negative_day":
			request.DailyReservedByteLimit = -1
			want = ErrHashFreshReadLimits
		case "large_day":
			request.DailyReservedByteLimit = (1 << 50) + 1
			want = ErrHashFreshReadLimits
		case "large_total":
			request.LifetimeReservedByteLimit = (1 << 50) + 1
			want = ErrHashFreshReadLimits
		case "old_original_id":
			request.JobID = f.m.consent.ID
		case "job_key":
			request.JobKey = hashChoiceJobKey(3)
		case "request_id":
			request.RequestID = "hash-choice-request-v1-" + strings.Repeat("0", 64)
		case "cross_job":
			request.JobID = second.ID
		}
		if got, err := w.ApproveFreshRead(context.Background(), request); !errors.Is(err, want) || got.ID != "" {
			t.Fatal("wrong tuple/confirmation/caps reached fresh publication", kind, got, err)
		}
		if hashChoiceSchemaVersion(t, w.db) != 4 || hashChoiceTableExists(t, w.db, "hash_fresh_read_approval") {
			t.Fatal("refused fresh approval leaked deferred migration", kind)
		}
	}
	// A fresh limit below old charges is valid: historical accounting is never
	// imported as this job's zero fresh reservation state.
	f.approval.DailyReservedByteLimit = 1
	f.approval.LifetimeReservedByteLimit = 1
	c := approveFreshReadFixture(t, f)
	if c.Approval.InitialTotalReservedBytes != 0 || before.Budget.TotalReservedBytes < 1 {
		t.Fatal("fresh consent inherited original reservation context", c)
	}
	other := *f
	other.job = second
	other.approval = HashFreshReadApprovalRequest{JobID: second.ID, JobKey: second.Record.JobKey, RequestID: f.request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 1 << 50, LifetimeReservedByteLimit: 1 << 50}
	upper := approveFreshReadFixture(t, &other)
	if upper.Approval.DailyReservedByteLimit != 1<<50 || upper.Approval.LifetimeReservedByteLimit != 1<<50 {
		t.Fatal("inclusive fresh cap boundary changed", upper)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("fresh consent cap checks changed originals")
	}
	for _, id := range []string{f.m.consent.ID, "", c.ID[:len(c.ID)-1], c.ID + "\n", strings.ToUpper(c.ID)} {
		if ValidHashFreshReadApprovalID(id) {
			t.Fatal("old or noncanonical approval ID became fresh authority", id)
		}
		if got, e := w.FreshReadApproval(context.Background(), id); !errors.Is(e, ErrHashFreshReadApprovalMissing) || got.ID != "" {
			t.Fatal("invalid fresh ID resolved original permission", got, e)
		}
		if got, e := w.RevokeFreshRead(context.Background(), id); !errors.Is(e, ErrHashFreshReadApprovalMissing) || got.ID != "" {
			t.Fatal("invalid fresh revocation touched original approval", got, e)
		}
	}
}

func TestHashFreshReadClockExpiryAndRevocationNeverRenew(t *testing.T) {
	for _, kind := range []string{"rollback", "expired"} {
		t.Run(kind, func(t *testing.T) {
			f := freshReadFixtureFiles(t)
			w := f.m.f.store
			before := hashStoreSnapshot(t, w)
			c := approveFreshReadFixture(t, f)
			high := c.Approval.CreatedAt.Add(time.Hour)
			if kind == "expired" {
				high = c.Approval.ExpiresAt
			}
			w.now = func() time.Time { return high }
			observed, err := w.ApproveFreshRead(context.Background(), f.approval)
			if kind == "expired" {
				if !errors.Is(err, ErrHashReadExpired) || !observed.ExpiredObserved || observed.Status != "expired_observed" {
					t.Fatal("expiry was not permanently observed", observed, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			w.now = func() time.Time { return c.Approval.CreatedAt.Add(-time.Hour) }
			late, err := w.ApproveFreshRead(context.Background(), f.approval)
			want := ErrHashReadClockRollback
			if kind == "expired" {
				want = ErrHashReadExpired
			}
			if !errors.Is(err, want) || late.ID != c.ID || !late.ClockHighWater.Equal(high) || !reflect.DeepEqual(late.Approval, c.Approval) {
				t.Fatal("rollback renewed/rewound fresh permission", late, err)
			}
			revoked, err := w.RevokeFreshRead(context.Background(), c.ID)
			if err != nil || revoked.Status != "revoked" || revoked.Revocation == nil || !revoked.Revocation.RecordedAt.Equal(high) || !revoked.ClockHighWater.Equal(high) {
				t.Fatal("rollback/expiry obstructed or rewound revocation", revoked, err)
			}
			retry, err := w.RevokeFreshRead(context.Background(), c.ID)
			if err != nil || !reflect.DeepEqual(retry, revoked) {
				t.Fatal("offline revocation retry changed first record", retry, err)
			}
			if got, e := w.ApproveFreshRead(context.Background(), f.approval); !errors.Is(e, ErrHashReadRevoked) || got.ID != c.ID || !reflect.DeepEqual(got.Approval, c.Approval) {
				t.Fatal("revoked job renewed original approval", got, e)
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("fresh clock observations changed original clocks/caps/history")
			}
		})
	}
}

func TestHashFreshReadJobsHaveIndependentClocksAndSavedReadersDoNotEvaluate(t *testing.T) {
	f := freshReadFixtureFiles(t)
	w := f.m.f.store
	before := hashStoreSnapshot(t, w)
	second := saveFreshJobFixture(t, w, f.request, hashChoiceJobKey(2))
	one := approveFreshReadFixture(t, f)
	w.now = func() time.Time { return one.Approval.CreatedAt.Add(23 * time.Hour) }
	advanced := approveFreshReadFixture(t, f)
	other := *f
	other.job = second
	other.approval = f.approval
	other.approval.JobID = second.ID
	other.approval.JobKey = second.Record.JobKey
	w.now = func() time.Time { return one.Approval.CreatedAt }
	two := approveFreshReadFixture(t, &other)
	if two.ID == one.ID || !two.ClockHighWater.Equal(w.now()) {
		t.Fatal("another job's high-water time rewrote fresh consent", two)
	}
	w.now = func() time.Time { return one.Approval.ExpiresAt.Add(72 * time.Hour) }
	reader, err := OpenHashReader(context.Background(), f.m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	reader.now = w.now
	for _, prior := range []HashFreshReadConsent{advanced, two} {
		got, e := reader.FreshReadApproval(context.Background(), prior.ID)
		if e != nil || !reflect.DeepEqual(got, prior) || got.ExpiredObserved || got.Status != "recorded" || got.CurrentReadPermissionEvaluated {
			t.Fatal("saved reader evaluated clock/current read permission", got, e)
		}
	}
	w.now = func() time.Time { return one.Approval.CreatedAt.Add(-time.Hour) }
	if _, err = w.RevokeFreshRead(context.Background(), one.ID); err != nil {
		t.Fatal(err)
	}
	if got, e := w.FreshReadApproval(context.Background(), two.ID); e != nil || !reflect.DeepEqual(got, two) {
		t.Fatal("one job's revocation changed another approval/clock", got, e)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("fresh job clocks changed original consent/accounting")
	}
}

func TestHashFreshReadFirstApprovalRefusesClockBeforeJobAndDefersMigration(t *testing.T) {
	f := freshReadFixtureFiles(t)
	w := f.m.f.store
	before := hashStoreSnapshot(t, w)
	w.now = func() time.Time { return f.job.Record.CreatedAt.Add(-time.Nanosecond) }
	if got, err := w.ApproveFreshRead(context.Background(), f.approval); !errors.Is(err, ErrHashReadClockRollback) || got.ID != "" {
		t.Fatal("approval predating its immutable job was published", got, err)
	}
	if hashChoiceSchemaVersion(t, w.db) != 4 || hashChoiceTableExists(t, w.db, "hash_fresh_read_approval") {
		t.Fatal("clock refusal published deferred migration")
	}
	requireFreshJobSeedUnchanged(t, w, f.job)
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("first fresh clock refusal changed original state")
	}
	w.now = func() time.Time { return f.job.Record.CreatedAt }
	approveFreshReadFixture(t, f)
}

func TestHashFreshReadConsentOfflineDoesNotRecoverReservedOriginalAttempt(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	if err := m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	m.f.store, err = OpenExistingHashWriter(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.CreatedAt.Add(time.Hour) }
	if _, err = m.f.store.db.Exec("CREATE TRIGGER fail_fresh_read_original_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	run, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if !errors.Is(err, ErrHashRecoveryRequired) || run.Usage.ReadBytes != 65 {
		t.Fatal("fixture did not leave genuine original reservation", run, err)
	}
	if _, err = m.f.store.db.Exec("DROP TRIGGER fail_fresh_read_original_fixture"); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, m.f.store)
	a := before.Work[3].LatestAttempt
	if before.Work[3].Status != "running" || a == nil || a.Status != "reserved" || a.ReadBytes != nil || a.RequestedBytes != nil || a.ElapsedNS != nil || before.Budget.TotalReservedBytes != 260 || before.Budget.TotalReadBytes != 195 || before.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture lost unsettled charge/null usage", before)
	}
	if err = m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.scanner.Close()
	// Rename rather than delete: frozen device/inode identities remain allocated.
	for _, path := range []string{m.f.root, m.f.stateDir} {
		if err = os.Rename(path, path+".offline"); err != nil {
			t.Fatal(err)
		}
	}
	configPath := filepath.Join(m.f.base, "config.toml")
	config := []byte("not [ valid GENERATED configuration")
	if err = os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	w := hashChoiceJobWriter(t, m, request)
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	f := &freshReadFixture{m: m, request: request, job: job, approval: HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 1, LifetimeReservedByteLimit: 1}}
	c := approveFreshReadFixture(t, f)
	if c.Approval.InitialTotalReservedBytes != 0 {
		t.Fatal("old reservation was imported into fresh consent", c)
	}
	if _, err = w.RevokeFreshRead(context.Background(), c.ID); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("fresh approval/revocation recovered, refunded, or changed original work/clock")
	}
	requireFreshJobSeedUnchanged(t, w, job)
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenHashReader(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := reader.FreshReadApproval(context.Background(), c.ID)
	if err != nil || got.Status != "revoked" || got.CurrentReadPermissionEvaluated {
		t.Fatal("offline saved reader lost bounded consent", got, err)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) {
		t.Fatal("saved consent reader recovered original attempt")
	}
	for _, path := range []string{m.f.root, m.f.stateDir} {
		if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("offline consent initialized missing source/inventory", err)
		}
	}
	if body, e := os.ReadFile(configPath); e != nil || !bytes.Equal(body, config) {
		t.Fatal("offline consent changed malformed configuration", e)
	}
}

func TestHashFreshReadApprovalCancellationAndUncertainPublication(t *testing.T) {
	for _, kind := range []string{"before_call", "before_commit", "after_commit", "commit_failed", "committed_lost_reply"} {
		t.Run(kind, func(t *testing.T) {
			f := freshReadFixtureFiles(t)
			w := f.m.f.store
			before := hashStoreSnapshot(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashFreshReadHooks{}
			published := kind == "after_commit" || kind == "committed_lost_reply"
			uncertain := kind == "commit_failed" || kind == "committed_lost_reply"
			switch kind {
			case "before_call":
				cancel()
			case "before_commit":
				hooks.beforeApprovalCommit = cancel
			case "after_commit":
				hooks.afterApprovalCommit = cancel
			case "commit_failed":
				hooks.commit = func(*sql.Tx) error { return errors.New("generated unavailable commit acknowledgment") }
			case "committed_lost_reply":
				hooks.commit = func(tx *sql.Tx) error {
					if err := tx.Commit(); err != nil {
						return err
					}
					return errors.New("generated lost committed acknowledgment")
				}
			}
			candidate, err := w.approveFreshRead(ctx, f.approval, hooks)
			want := error(context.Canceled)
			if uncertain {
				want = ErrHashRecoveryRequired
			}
			if !errors.Is(err, want) {
				t.Fatal("wrong publication failure", candidate, err)
			}
			if uncertain || published {
				requireFreshReadConsent(t, candidate, f)
			} else if candidate.ID != "" {
				t.Fatal("canceled unpublished approval returned publication evidence", candidate)
			}
			if uncertain {
				if got, e := w.ApproveFreshRead(context.Background(), f.approval); !errors.Is(e, ErrHashRecoveryRequired) || got.ID != "" {
					t.Fatal("uncertain writer accepted another fresh publication", got, e)
				}
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("failed approval changed original work/accounting")
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			reader, err := OpenHashReader(context.Background(), f.m.f.base)
			if err != nil {
				t.Fatal(err)
			}
			if published {
				got, e := reader.FreshReadApproval(context.Background(), candidate.ID)
				if e != nil || !reflect.DeepEqual(got, candidate) || hashChoiceSchemaVersion(t, reader.db) != 5 {
					t.Fatal("committed lost reply did not preserve exact approval", got, e)
				}
			} else if hashChoiceSchemaVersion(t, reader.db) != 4 || hashChoiceTableExists(t, reader.db, "hash_fresh_read_approval") {
				t.Fatal("unpublished approval leaked schema5 migration")
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = OpenHashFreshJobWriter(context.Background(), f.request)
			if err != nil {
				t.Fatal(err)
			}
			f.m.f.store = w
			w.now = hashChoiceClock
			retry := approveFreshReadFixture(t, f)
			if (uncertain || published) && retry.ID != candidate.ID {
				t.Fatal("explicit exact retry changed first approval candidate", retry, candidate)
			}
			requireFreshJobSeedUnchanged(t, w, f.job)
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("approval reopen/retry recovered original state")
			}
		})
	}
}

func TestHashFreshReadRevocationCancellationDoesNotHideSavedRevocation(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			f := freshReadFixtureFiles(t)
			w := f.m.f.store
			c := approveFreshReadFixture(t, f)
			before := hashStoreSnapshot(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashFreshReadHooks{}
			if stage == "before_commit" {
				hooks.beforeRevocationCommit = cancel
			} else {
				hooks.afterRevocationCommit = cancel
			}
			got, err := w.revokeFreshRead(ctx, c.ID, hooks)
			if !errors.Is(err, context.Canceled) {
				t.Fatal("revocation cancellation was ignored", got, err)
			}
			if stage == "before_commit" && got.ID != "" {
				t.Fatal("unpublished revocation claimed saved consent", got)
			}
			if stage == "after_commit" && (got.ID != c.ID || got.Status != "revoked" || got.Revocation == nil) {
				t.Fatal("saved revocation was hidden by canceled reply", got)
			}
			saved, e := w.FreshReadApproval(context.Background(), c.ID)
			if e != nil {
				t.Fatal(e)
			}
			if stage == "before_commit" && !reflect.DeepEqual(saved, c) {
				t.Fatal("canceled revocation published observation or revoked consent", saved)
			}
			if stage == "after_commit" && !reflect.DeepEqual(saved, got) {
				t.Fatal("revoked lost reply changed publication evidence", saved, got)
			}
			if _, e = w.RevokeFreshRead(context.Background(), c.ID); e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("revocation cancellation changed original work")
			}
		})
	}
}

func dropFreshReadFixtureGuards(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'hash_fresh_read_%'")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if strings.ContainsAny(name, "\"\x00") {
			t.Fatal("unsafe fixture trigger name")
		}
		if _, err = db.Exec(`DROP TRIGGER "` + name + `"`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
}

func TestHashFreshReadCorruptionCannotReturnPartialConsentOrJob(t *testing.T) {
	if HashFreshReadApprovalMaxRecordBytes != 16384 || HashFreshReadRevocationMaxRecordBytes != 4096 {
		t.Fatal("fresh consent lost explicit bounded payload limits")
	}
	for _, kind := range []string{"approval_oversize", "approval_noncanonical", "approval_store", "approval_job", "approval_scope", "approval_confirmation", "clock_missing", "clock_before_created", "clock_expiry_mismatch", "revocation_oversize", "revocation_wrong_approval"} {
		t.Run(kind, func(t *testing.T) {
			f := freshReadFixtureFiles(t)
			w := f.m.f.store
			c := approveFreshReadFixture(t, f)
			if strings.HasPrefix(kind, "revocation_") {
				var err error
				c, err = w.RevokeFreshRead(context.Background(), c.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			dropFreshReadFixtureGuards(t, w.db)
			id := c.ID
			var err error
			if strings.HasPrefix(kind, "approval_") {
				a := c.Approval
				switch kind {
				case "approval_store":
					a.StoreID = strings.Repeat("0", 64)
				case "approval_job":
					a.JobID = "hash-choice-job-v1-" + strings.Repeat("0", 64)
				case "approval_scope":
					a.RoleTargetScopeDigest = strings.Repeat("0", 64)
				case "approval_confirmation":
					a.ConfirmFullFileRead = false
				}
				payload, e := json.Marshal(a)
				if e != nil {
					t.Fatal(e)
				}
				if kind == "approval_oversize" {
					payload = bytes.Repeat([]byte("x"), 16385)
				}
				if kind == "approval_noncanonical" {
					payload = append(payload, ' ')
				}
				// Update the digest and observation ID as well: binding/canonical
				// validation, not a stale checksum, must reject the forged record.
				id = fmt.Sprintf("hash-job-read-v1-%x", sha256.Sum256(payload))
				if _, err = w.db.Exec("UPDATE hash_fresh_read_approval SET approval_id=?,payload=? WHERE job_id=?", id, payload, f.job.ID); err != nil {
					t.Fatal(err)
				}
				if _, err = w.db.Exec("UPDATE hash_fresh_read_observation SET approval_id=? WHERE job_id=?", id, f.job.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				switch kind {
				case "clock_missing":
					_, err = w.db.Exec("DELETE FROM hash_fresh_read_observation WHERE job_id=?", f.job.ID)
				case "clock_before_created":
					_, err = w.db.Exec("UPDATE hash_fresh_read_observation SET max_now_ns=? WHERE job_id=?", c.Approval.CreatedAt.Add(-time.Nanosecond).UnixNano(), f.job.ID)
				case "clock_expiry_mismatch":
					_, err = w.db.Exec("UPDATE hash_fresh_read_observation SET max_now_ns=?,expired=0 WHERE job_id=?", c.Approval.ExpiresAt.UnixNano(), f.job.ID)
				case "revocation_oversize", "revocation_wrong_approval":
					r := *c.Revocation
					r.ID = ""
					if kind == "revocation_wrong_approval" {
						r.ApprovalID = "hash-job-read-v1-" + strings.Repeat("0", 64)
					}
					payload, e := json.Marshal(r)
					if e != nil {
						t.Fatal(e)
					}
					if kind == "revocation_oversize" {
						payload = bytes.Repeat([]byte("x"), 4097)
					}
					_, err = w.db.Exec("UPDATE hash_fresh_read_revocation SET revocation_id=?,payload=? WHERE job_id=?", fmt.Sprintf("%x", sha256.Sum256(payload)), payload, f.job.ID)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if got, e := w.FreshReadApproval(context.Background(), id); !errors.Is(e, ErrHashFreshReadCorrupt) || got.ID != "" {
				t.Fatal("corrupt consent returned partial authority", kind, got, e)
			}
			if got, e := w.FreshJob(context.Background(), f.job.ID); !errors.Is(e, ErrHashFreshReadCorrupt) || got.ID != "" {
				t.Fatal("corrupt consent returned positive partial job", kind, got, e)
			}
			if got, e := w.ApproveFreshRead(context.Background(), f.approval); !errors.Is(e, ErrHashFreshReadCorrupt) || got.ID != "" {
				t.Fatal("approval retried corrupt lifecycle", kind, got, e)
			}
		})
	}
}

func TestHashFreshReadApprovalCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_FRESH_READ_CRASH_BASE")
	if base == "" {
		return
	}
	reader, err := OpenHashReader(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	request, err := reader.PrepareKeeperChoiceFreshRequest(context.Background(), os.Getenv("RYDD_FRESH_READ_CRASH_CHOICE"))
	if err != nil {
		t.Fatal(err)
	}
	job, err := reader.FreshJob(context.Background(), os.Getenv("RYDD_FRESH_READ_CRASH_JOB"))
	closeErr := reader.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	w, err := OpenHashFreshJobWriter(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.now = hashChoiceClock
	stop := func() {
		if _, e := fmt.Fprintln(os.Stdout, "READY"); e != nil {
			t.Fatal(e)
		}
		var b [1]byte
		if _, e := os.Stdin.Read(b[:]); e != nil {
			t.Fatal(e)
		}
		t.Fatal("fresh consent crash helper resumed")
	}
	hooks := hashFreshReadHooks{}
	switch os.Getenv("RYDD_FRESH_READ_CRASH_STAGE") {
	case "before_commit":
		hooks.beforeApprovalCommit = stop
	case "after_commit":
		hooks.afterApprovalCommit = stop
	default:
		t.Fatal("unknown fresh consent process boundary")
	}
	req := HashFreshReadApprovalRequest{JobID: job.ID, JobKey: job.Record.JobKey, RequestID: request.ID(), ConfirmFullFileRead: true, DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192}
	c, err := w.approveFreshRead(context.Background(), req, hooks)
	t.Fatal("fresh consent process seam not reached", c, err)
}

func killFreshReadAt(t *testing.T, f *freshReadFixture, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashFreshReadApprovalCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_FRESH_READ_CRASH_BASE="+f.m.f.base, "RYDD_FRESH_READ_CRASH_CHOICE="+f.m.saved.ID, "RYDD_FRESH_READ_CRASH_JOB="+f.job.ID, "RYDD_FRESH_READ_CRASH_STAGE="+stage)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	ready := make(chan bool, 1)
	go func() { s := bufio.NewScanner(stdout); ready <- s.Scan() && s.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("fresh consent child failed before boundary", stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("fresh consent child timed out", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("fresh consent child survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("fresh consent child did not die by SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashFreshReadProcessDeathAtomicMigrationAndExactApprovalRetry(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			f := freshReadFixtureFiles(t)
			before := hashStoreSnapshot(t, f.m.f.store)
			if err := f.m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := f.m.f.source.Close(); err != nil {
				t.Fatal(err)
			}
			f.m.f.scanner.Close()
			for _, path := range []string{f.m.f.root, f.m.f.stateDir} {
				if err := os.Rename(path, path+".offline"); err != nil {
					t.Fatal(err)
				}
			}
			killFreshReadAt(t, f, stage)
			reader, err := OpenHashReader(context.Background(), f.m.f.base)
			if err != nil {
				t.Fatal(err)
			}
			var published HashFreshReadConsent
			if stage == "before_commit" {
				if hashChoiceSchemaVersion(t, reader.db) != 4 {
					t.Fatal("crash exposed uncommitted fresh schema migration")
				}
				for _, table := range []string{"hash_fresh_read_approval", "hash_fresh_read_revocation", "hash_fresh_read_observation"} {
					if hashChoiceTableExists(t, reader.db, table) {
						t.Fatal("crash leaked consent table", table)
					}
				}
				job := requireFreshJobSeedUnchanged(t, reader, f.job)
				if job.ReadConsent != nil {
					t.Fatal("crash returned uncommitted consent", job)
				}
			} else {
				var id string
				if err = reader.db.QueryRow("SELECT approval_id FROM hash_fresh_read_approval WHERE job_id=?", f.job.ID).Scan(&id); err != nil {
					t.Fatal(err)
				}
				published, err = reader.FreshReadApproval(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				requireFreshReadConsent(t, published, f)
				if hashChoiceSchemaVersion(t, reader.db) != 5 || !published.Approval.CreatedAt.Equal(hashChoiceClock()) {
					t.Fatal("lost reply changed committed approval", published)
				}
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) {
				t.Fatal("process publication recovered original work")
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			w, err := OpenHashFreshJobWriter(context.Background(), f.request)
			if err != nil {
				t.Fatal(err)
			}
			f.m.f.store = w
			w.now = hashChoiceClock
			retry := approveFreshReadFixture(t, f)
			if stage == "after_commit" && !reflect.DeepEqual(retry, published) {
				t.Fatal("exact manual approval retry replaced committed lost reply", retry, published)
			}
			var approvals, observations, revocations int
			if err = w.db.QueryRow("SELECT (SELECT count(*) FROM hash_fresh_read_approval),(SELECT count(*) FROM hash_fresh_read_observation),(SELECT count(*) FROM hash_fresh_read_revocation)").Scan(&approvals, &observations, &revocations); err != nil {
				t.Fatal(err)
			}
			if approvals != 1 || observations != 1 || revocations != 0 || !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("crash/retry duplicated consent or changed originals", approvals, observations, revocations)
			}
			requireFreshJobSeedUnchanged(t, w, f.job)
		})
	}
}
