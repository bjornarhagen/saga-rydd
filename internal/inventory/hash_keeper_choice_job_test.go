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
	"sync"
	"syscall"
	"testing"
	"time"
)

// Jobs, inputs and crash boundaries in this file are generated. Job operations
// never perform fixture content reads, source traversal or original recovery.
func hashChoiceJobKey(n int) string {
	return fmt.Sprintf("hash-job-key-v1-%064x", n)
}

func hashChoiceJobRequest(t *testing.T, m *hashChoiceRequestFixture) *KeeperChoiceFreshRequest {
	t.Helper()
	request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = requireHashChoiceFreshRequest(t, request, m)
	return request
}

func hashChoiceJobWriter(t *testing.T, m *hashChoiceRequestFixture, request *KeeperChoiceFreshRequest) *HashStore {
	t.Helper()
	if err := m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenHashFreshJobWriter(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	w.now = hashChoiceClock
	m.f.store = w
	return w
}

func requireFreshJob(t *testing.T, job SavedFreshJob, request *KeeperChoiceFreshRequest, key string) {
	t.Helper()
	r := job.Record
	if !ValidHashFreshJobID(job.ID) || job.ID == request.ID() || r.Version != 1 || r.Contract != "choice_bound_fresh_full_hash_job_v1" || r.Status != "unapproved" || r.CreatedAt.IsZero() || r.JobKey != key || !reflect.DeepEqual(r.Request, request.Report()) || job.ApprovalAvailable || job.ProvenanceVerified || job.ContentVerified || job.CurrentStateVerified || job.DuplicatesVerified || job.Executable || job.EstimatedReclaimableBytes != nil || job.FreshReservedBytes != 0 || job.FreshRequestedBytes != 0 || job.FreshReadBytes != 0 {
		t.Fatal("new generation reused progress, consent or action authority", job)
	}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > 2<<20 || job.ID != fmt.Sprintf("hash-choice-job-v1-%x", sha256.Sum256(payload)) {
		t.Fatal("job ID did not bind canonical immutable first-publication evidence", job.ID, err)
	}
	if len(job.Work) != len(r.Request.Targets) {
		t.Fatal("fresh job work expanded/dropped exact selected scope", job)
	}
	for i, work := range job.Work {
		selected := r.Request.Targets[i]
		target, err := json.Marshal(selected.Target)
		if err != nil {
			t.Fatal(err)
		}
		if work.Ordinal != i+1 || work.HistoricalWorkID != selected.Observation.WorkID || work.Role != selected.Role || work.TargetDigest != fmt.Sprintf("%x", sha256.Sum256(target)) || work.Status != "pending" || work.Sequence != 0 || work.CheckedOffset != 0 {
			t.Fatal("fresh work reused historical ordinals or omitted zero/bound role state", work, selected)
		}
	}
	origin := r.OriginalContext
	if origin.StoreID != r.Request.StoreID || origin.SelectionID != r.Request.SelectionID || origin.InventoryID != r.Request.InventoryID || origin.BudgetScope != "whole_original_selection" || origin.Source == "" || origin.Contract != FileHashContract {
		t.Fatal("job origin context was not labelled/bound separately from fresh work", origin)
	}
	for _, forbidden := range []string{`"checkpoint"`, `"hash_state"`, `"sha\u0003`} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatal("job imported a completed SHA continuation", forbidden)
		}
	}
}

func saveFreshJobFixture(t *testing.T, w *HashStore, request *KeeperChoiceFreshRequest, key string) SavedFreshJob {
	t.Helper()
	job, err := w.SaveFreshJob(context.Background(), request, key)
	if err != nil {
		t.Fatal(err)
	}
	requireFreshJob(t, job, request, key)
	return job
}

func hashFreshJobRows(t *testing.T, db *sql.DB) (int, int) {
	t.Helper()
	var jobs, work int
	if err := db.QueryRow("SELECT (SELECT count(*) FROM hash_fresh_job),(SELECT count(*) FROM hash_fresh_work)").Scan(&jobs, &work); err != nil {
		t.Fatal(err)
	}
	return jobs, work
}

func TestHashKeeperChoiceFreshJobAtomicGenerationAndOldReaderCompatibility(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	before := hashStoreSnapshot(t, m.f.store)
	beforeGroups, err := m.f.store.Groups(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	oldReader, err := OpenHashReader(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer oldReader.Close()
	w := hashChoiceJobWriter(t, m, request)
	if hashChoiceSchemaVersion(t, w.db) != 3 || hashChoiceTableExists(t, w.db, "hash_fresh_job") {
		t.Fatal("opening request-aware writer published schema/work")
	}
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	if !job.Record.CreatedAt.Equal(hashChoiceClock()) || hashChoiceSchemaVersion(t, w.db) != 4 {
		t.Fatal("first job did not atomically publish generation and schema4", job)
	}
	jobs, works := hashFreshJobRows(t, w.db)
	if jobs != 1 || works != 3 {
		t.Fatal("job publication omitted or expanded pending work", jobs, works)
	}
	if origin := job.Record.OriginalContext; origin.SelectedWork != 4 || origin.CompletedObservations != 3 || origin.UnfinishedWork != 1 || !reflect.DeepEqual(origin.Budget, before.Budget) || !reflect.DeepEqual(origin.ReadConsent, before.ReadConsent) {
		t.Fatal("first publication lost qualified current original context", origin, before)
	}
	for _, reader := range []*HashStore{w, oldReader} {
		if got := hashStoreSnapshot(t, reader); !reflect.DeepEqual(got, before) {
			t.Fatal("schema4 reader mixed new work/charge into original report", got)
		}
		groups, e := reader.Groups(context.Background())
		if e != nil || !reflect.DeepEqual(groups, beforeGroups) {
			t.Fatal("schema4 changed saved original grouping", groups, e)
		}
		choice, e := reader.KeeperChoice(context.Background(), m.saved.ID)
		if e != nil || !reflect.DeepEqual(choice, m.saved) {
			t.Fatal("schema4 refreshed archived choice context", choice, e)
		}
		got, e := reader.FreshJob(context.Background(), job.ID)
		if e != nil || !reflect.DeepEqual(got, job) {
			t.Fatal("existing reader could not reopen whole committed job", got, e)
		}
	}
	// Returned role, target and consent branches cannot mutate a stored job.
	changed := job
	changed.Record.Request.Targets[0].Target.File.PathBytes[0] = 'x'
	changed.Record.Request.Targets[0].Target.Ancestors[0].Path[0] = 'y'
	changed.Record.Request.HistoricalChoice.Record.Evidence.Copies[0].PathBytes[0] = 'z'
	changed.Record.OriginalContext.ReadConsent.Approval.SourceLocator.RootPathBytes[0] = 'q'
	changed.Work[0].Role = "copy"
	got, e := oldReader.FreshJob(context.Background(), job.ID)
	if e != nil {
		t.Fatal(e)
	}
	requireFreshJob(t, got, request, hashChoiceJobKey(1))
	if got.Work[0].Role != "keeper" || !bytes.Equal(got.Record.Request.Targets[0].Target.File.PathBytes, m.proposal.Targets[2].File.PathBytes) {
		t.Fatal("caller changed stored immutable generation", got)
	}
}

func TestHashKeeperChoiceFreshJobRetryKeepsFirstContextAcrossOriginalProgress(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	w := hashChoiceJobWriter(t, m, request)
	first := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	m.f.store, err = OpenExistingHashWriter(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.CreatedAt.Add(time.Hour) }
	run, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if err != nil || run.WorkID != "4" {
		t.Fatal("fixture did not advance genuine original work", run, err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.ExpiresAt.Add(time.Hour) }
	if _, err = m.f.store.RevokeRead(context.Background(), m.consent.ID); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, m.f.store)
	if before.ReadConsent.Status != "revoked" || before.Budget.TotalReservedBytes == first.Record.OriginalContext.Budget.TotalReservedBytes {
		t.Fatal("fixture did not advance original permission/charges", before)
	}
	w = hashChoiceJobWriter(t, m, request)
	w.now = func() time.Time { return hashChoiceClock().Add(72 * time.Hour) }
	retry := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	if !reflect.DeepEqual(retry, first) {
		t.Fatal("exact key retry renewed job time or first-publication context", retry, first)
	}
	other := saveFreshJobFixture(t, w, request, hashChoiceJobKey(2))
	if other.ID == first.ID || !other.Record.CreatedAt.Equal(w.now()) || !reflect.DeepEqual(other.Record.OriginalContext.Budget, before.Budget) || !reflect.DeepEqual(other.Record.OriginalContext.ReadConsent, before.ReadConsent) {
		t.Fatal("new explicit key failed to identify separate zero generation/current original context", other)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("generation publication renewed old approval or altered charges")
	}
}

func TestHashKeeperChoiceFreshJobReservedOriginalWorkAndOfflineStorageAreUntouched(t *testing.T) {
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
	if _, err = m.f.store.db.Exec("CREATE TRIGGER fail_fresh_job_original_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	run, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if !errors.Is(err, ErrHashRecoveryRequired) || run.Usage.ReadBytes != 65 {
		t.Fatal("fixture did not leave genuine original reservation", run, err)
	}
	if _, err = m.f.store.db.Exec("DROP TRIGGER fail_fresh_job_original_fixture"); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, m.f.store)
	a := before.Work[3].LatestAttempt
	if before.Work[3].Status != "running" || a == nil || a.Status != "reserved" || a.ReadBytes != nil || a.RequestedBytes != nil || a.ElapsedNS != nil || before.Budget.TotalReservedBytes != 260 || before.Budget.TotalReadBytes != 195 || before.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture lost charged unsettled/null usage", before)
	}
	if err = m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.scanner.Close()
	if err = os.RemoveAll(m.f.root); err != nil {
		t.Fatal(err)
	}
	if err = os.RemoveAll(m.f.stateDir); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(m.f.base, "config.toml")
	config := []byte("not [ valid GENERATED configuration")
	if err = os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	w := hashChoiceJobWriter(t, m, request)
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("request-aware writer recovered or refunded old work")
	}
	job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	if !reflect.DeepEqual(job.Record.OriginalContext.Budget, before.Budget) || !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("job publication hid old unsettled charge or changed original work", job)
	}
	if got, e := os.ReadFile(configPath); e != nil || !bytes.Equal(got, config) {
		t.Fatal("saved-only publication read/changed malformed configuration", e)
	}
	for _, path := range []string{m.f.root, m.f.stateDir} {
		if _, e := os.Lstat(path); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("job initialized missing source/inventory", path, e)
		}
	}
}

func TestHashKeeperChoiceFreshJobExplicitKeysCapacityAndStoreWideConflicts(t *testing.T) {
	if HashFreshJobContract != "choice_bound_fresh_full_hash_job_v1" || HashFreshJobLimit != 128 || HashFreshJobMaxRecordBytes != 2<<20 {
		t.Fatal("fresh generation lost declared storage bounds")
	}
	firstKey, err := NewHashFreshJobKey()
	if err != nil || !ValidHashFreshJobKey(firstKey) {
		t.Fatal("explicit key generator returned unusable key", firstKey, err)
	}
	secondKey, err := NewHashFreshJobKey()
	if err != nil || !ValidHashFreshJobKey(secondKey) || firstKey == secondKey {
		t.Fatal("new generation key was reused", secondKey, err)
	}
	var absent *HashStore
	for _, key := range []string{"", hashChoiceJobKey(1)[:len(hashChoiceJobKey(1))-1], hashChoiceJobKey(1) + "a", hashChoiceJobKey(1) + "\n", strings.ToUpper(hashChoiceJobKey(1)), "hash-choice-job-v1-" + strings.Repeat("a", 64)} {
		if ValidHashFreshJobKey(key) {
			t.Fatal("noncanonical/wrong-contract generation key accepted", key)
		}
		if job, e := absent.SaveFreshJob(context.Background(), nil, key); !errors.Is(e, ErrHashFreshJobKey) || job.ID != "" {
			t.Fatal("invalid key reached storage or implied publication", job, e)
		}
	}
	m := hashChoiceRequestFiles(t, 3, 3, "3", "2", "1")
	request := hashChoiceJobRequest(t, m)
	otherChoice := saveHashChoice(t, m.f.store, hashKeeperPreview(t, m.f.store, m.proposal.SelectionID, "3", "1", "2"))
	otherRequest, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), otherChoice.ID)
	if err != nil {
		t.Fatal(err)
	}
	w := hashChoiceJobWriter(t, m, request)
	if job, e := w.SaveFreshJob(context.Background(), otherRequest, hashChoiceJobKey(1)); !errors.Is(e, ErrHashFreshJobEvidence) || job.ID != "" {
		t.Fatal("request-bound writer accepted an opaque different request", job, e)
	}
	if hashChoiceSchemaVersion(t, w.db) != 3 || hashChoiceTableExists(t, w.db, "hash_fresh_job") {
		t.Fatal("request conflict initialized generation schema")
	}
	first := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
	for n := 2; n <= 128; n++ {
		w.now = func() time.Time { return hashChoiceClock().Add(time.Duration(n) * time.Second) }
		_ = saveFreshJobFixture(t, w, request, hashChoiceJobKey(n))
	}
	before := hashStoreSnapshot(t, w)
	jobs, works := hashFreshJobRows(t, w.db)
	if jobs != 128 || works != 128*3 {
		t.Fatal("capacity fixture lost whole job/work rows", jobs, works)
	}
	if retry := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1)); !reflect.DeepEqual(retry, first) {
		t.Fatal("capacity refused exact retry or refreshed first immutable context", retry)
	}
	if job, e := w.SaveFreshJob(context.Background(), request, hashChoiceJobKey(129)); !errors.Is(e, ErrHashFreshJobCapacity) || job.ID != "" {
		t.Fatal("capacity created, evicted or partly published another generation", job, e)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = OpenHashFreshJobWriter(context.Background(), otherRequest)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store = w
	w.now = hashChoiceClock
	if job, e := w.SaveFreshJob(context.Background(), otherRequest, hashChoiceJobKey(1)); !errors.Is(e, ErrHashFreshJobConflict) || job.ID != "" {
		t.Fatal("store-wide key resolved to a different request or only capacity error", job, e)
	}
	if jobs, works = hashFreshJobRows(t, w.db); jobs != 128 || works != 384 || !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
		t.Fatal("conflict/capacity changed generation count or original state", jobs, works)
	}
}

func TestHashKeeperChoiceFreshJobExistingOnlyAndBeforeSQLiteIdentityGuard(t *testing.T) {
	for _, request := range []*KeeperChoiceFreshRequest{nil, {}} {
		if w, e := OpenHashFreshJobWriter(context.Background(), request); w != nil || !errors.Is(e, ErrHashFreshJobEvidence) {
			t.Fatal("empty opaque request opened storage", w, e)
		}
	}
	for _, kind := range []string{"missing_directory", "missing_database", "schema_zero", "schema_one", "schema_two", "replaced_database", "selected_alias", "selected_lock_alias"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
			request := hashChoiceJobRequest(t, m)
			if err := m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(m.f.base, "hashes")
			path := filepath.Join(dir, hashStoreFilename)
			switch kind {
			case "missing_directory":
				if err := os.Rename(dir, dir+"-offline"); err != nil {
					t.Fatal(err)
				}
			case "missing_database":
				if err := os.Rename(path, path+"-offline"); err != nil {
					t.Fatal(err)
				}
			case "schema_zero":
				if err := os.Truncate(path, 0); err != nil {
					t.Fatal(err)
				}
			case "schema_one", "schema_two":
				version := 1
				if kind == "schema_two" {
					version = 2
				}
				hashChoiceDowngrade(t, m.f, version)
			case "replaced_database", "selected_alias":
				body, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if kind == "selected_alias" {
					if err = os.Rename(string(m.saved.Record.Evidence.Keeper.PathBytes), path); err != nil {
						t.Fatal(err)
					}
				} else if err = os.WriteFile(path, body, 0600); err != nil {
					t.Fatal(err)
				}
			case "selected_lock_alias":
				if err := os.Rename(string(m.saved.Record.Evidence.Keeper.PathBytes), filepath.Join(dir, "writer.lock")); err != nil {
					t.Fatal(err)
				}
			}
			w, err := OpenHashFreshJobWriter(context.Background(), request)
			if w != nil || err == nil {
				if w != nil {
					_ = w.Close()
				}
				t.Fatal("request-aware writer initialized, migrated or opened different physical storage", kind, err)
			}
			if kind == "replaced_database" || kind == "selected_alias" {
				if !strings.Contains(err.Error(), "identity changed") {
					t.Fatal("replacement reached SQLite interpretation before its captured identity guard", kind, err)
				}
			}
			if kind == "selected_lock_alias" {
				if !errors.Is(err, ErrHashFreshJobEvidence) {
					t.Fatal("known original alias reached lock opening instead of the evidence guard", err)
				}
				body, e := os.ReadFile(filepath.Join(dir, "writer.lock"))
				if e != nil || !bytes.Equal(body, m.f.data[1]) {
					t.Fatal("refused constructor changed original file bytes through its lock slot", e)
				}
			}
			if kind == "missing_directory" || kind == "missing_database" {
				missing := path
				if kind == "missing_directory" {
					missing = dir
				}
				if _, e := os.Lstat(missing); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("refusal recreated missing private name", missing, e)
				}
			}
			if strings.HasPrefix(kind, "schema_") {
				db := hashChoiceOpenDB(t, m.f.base)
				version := 0
				if kind == "schema_one" {
					version = 1
				}
				if kind == "schema_two" {
					version = 2
				}
				if hashChoiceSchemaVersion(t, db) != version || hashChoiceTableExists(t, db, "hash_fresh_job") {
					t.Fatal("existing-only writer changed refused schema", kind)
				}
			}
		})
	}
}

func TestHashKeeperChoiceFreshJobMissingLockDoesNotCreateOrChangeState(t *testing.T) {
	m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
	request := hashChoiceJobRequest(t, m)
	before := hashStoreSnapshot(t, m.f.store)
	if err := m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(m.f.base, "hashes", hashStoreFilename)
	beforeBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(m.f.base, "hashes", "writer.lock")
	if err = os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if w, e := OpenHashFreshJobWriter(context.Background(), request); w != nil || !errors.Is(e, os.ErrNotExist) {
		if w != nil {
			_ = w.Close()
		}
		t.Fatal("request-aware constructor created a missing lock or opened a writer", w, e)
	}
	if _, err = os.Lstat(lock); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused constructor recreated missing lock", err)
	}
	if afterBytes, e := os.ReadFile(path); e != nil || !bytes.Equal(afterBytes, beforeBytes) {
		t.Fatal("missing-lock constructor changed original database bytes", e)
	}
	reader, err := OpenHashReader(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) || hashChoiceSchemaVersion(t, reader.db) != 3 || hashChoiceTableExists(t, reader.db, "hash_fresh_job") {
		t.Fatal("missing-lock refusal changed original consent, charges, work or schema")
	}
	choice, err := reader.KeeperChoice(context.Background(), m.saved.ID)
	if err != nil || !reflect.DeepEqual(choice, m.saved) {
		t.Fatal("missing-lock refusal changed archived choice", choice, err)
	}
}

func TestHashKeeperChoiceFreshJobCancellationUncertaintyAndConcurrentRetry(t *testing.T) {
	for _, phase := range []string{"already_canceled", "before_commit", "after_commit", "uncertain_rollback", "uncertain_committed", "concurrent_retry"} {
		t.Run(phase, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
			request := hashChoiceJobRequest(t, m)
			before := hashStoreSnapshot(t, m.f.store)
			w := hashChoiceJobWriter(t, m, request)
			key := hashChoiceJobKey(1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashFreshJobHooks{}
			if phase == "already_canceled" {
				cancel()
			}
			if phase == "before_commit" {
				hooks.beforeCommit = cancel
			}
			if phase == "after_commit" {
				hooks.afterCommit = cancel
			}
			if strings.HasPrefix(phase, "uncertain_") {
				hooks.commit = func(tx *sql.Tx) error {
					if phase == "uncertain_committed" {
						if e := tx.Commit(); e != nil {
							t.Fatal(e)
						}
					}
					return errors.New("generated lost job commit reply")
				}
			}
			if phase == "concurrent_retry" {
				const n = 4
				jobs := make(chan SavedFreshJob, n)
				failures := make(chan error, n)
				var group sync.WaitGroup
				for range n {
					group.Go(func() { job, e := w.SaveFreshJob(context.Background(), request, key); jobs <- job; failures <- e })
				}
				group.Wait()
				close(jobs)
				close(failures)
				for e := range failures {
					if e != nil {
						t.Fatal(e)
					}
				}
				var first SavedFreshJob
				for job := range jobs {
					requireFreshJob(t, job, request, key)
					if first.ID == "" {
						first = job
					} else if !reflect.DeepEqual(first, job) {
						t.Fatal("concurrent exact retries published different generations", job)
					}
				}
				count, work := hashFreshJobRows(t, w.db)
				if count != 1 || work != 2 {
					t.Fatal("concurrent retries used extra capacity/partial rows", count, work)
				}
				return
			}
			candidate, err := w.saveFreshJob(ctx, request, key, hooks)
			if phase == "already_canceled" || phase == "before_commit" {
				if !errors.Is(err, context.Canceled) || candidate.ID != "" || hashChoiceSchemaVersion(t, w.db) != 3 || hashChoiceTableExists(t, w.db, "hash_fresh_job") {
					t.Fatal("canceled first publication exposed schema/job/work", candidate, err)
				}
				if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
					t.Fatal("canceled publication changed original records")
				}
				return
			}
			requireFreshJob(t, candidate, request, key)
			if phase == "after_commit" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("committed canceled reply reported success", candidate, err)
				}
			} else {
				if !errors.Is(err, ErrHashRecoveryRequired) {
					t.Fatal("uncertain publication lost candidate ID/key", candidate, err)
				}
				if job, e := w.SaveFreshJob(context.Background(), request, key); !errors.Is(e, ErrHashRecoveryRequired) || job.ID != "" {
					t.Fatal("uncertain writer remained writable", job, e)
				}
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = OpenHashFreshJobWriter(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			m.f.store = w
			w.now = hashChoiceClock
			got, showErr := w.FreshJob(context.Background(), candidate.ID)
			if phase == "uncertain_rollback" {
				if !errors.Is(showErr, os.ErrNotExist) || got.ID != "" || hashChoiceSchemaVersion(t, w.db) != 3 || hashChoiceTableExists(t, w.db, "hash_fresh_job") {
					t.Fatal("rolled-back uncertain publication was visible", got, showErr)
				}
			} else if showErr != nil || !reflect.DeepEqual(got, candidate) {
				t.Fatal("committed lost reply could not reopen exact candidate", got, showErr)
			}
			if retry := saveFreshJobFixture(t, w, request, key); !reflect.DeepEqual(retry, candidate) {
				t.Fatal("explicit exact-key retry replaced publication context/identity", retry, candidate)
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("lost reply/retry changed original work, consent or charge")
			}
		})
	}
}

func dropFreshJobFixtureGuards(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('hash_fresh_job','hash_fresh_work')")
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
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if _, err = db.Exec(`DROP TRIGGER "` + strings.ReplaceAll(name, `"`, `""`) + `"`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec("PRAGMA ignore_check_constraints=ON; PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
}

func TestHashKeeperChoiceFreshJobCorruptionRefusesWholeRecordAndRows(t *testing.T) {
	for _, kind := range []string{"payload_checksum", "oversized", "noncanonical", "version", "request_binding", "role", "target_root", "authority", "job_key", "work_missing", "work_extra", "work_swapped", "work_digest", "work_nonzero"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 3, 3, "3", "2", "1")
			request := hashChoiceJobRequest(t, m)
			w := hashChoiceJobWriter(t, m, request)
			job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
			before := hashStoreSnapshot(t, w)
			dropFreshJobFixtureGuards(t, w.db)
			id := job.ID
			if strings.HasPrefix(kind, "work_") {
				var query string
				switch kind {
				case "work_missing":
					query = "DELETE FROM hash_fresh_work WHERE job_id=? AND ordinal=3"
				case "work_extra":
					query = "INSERT INTO hash_fresh_work SELECT job_id,4,historical_work_id,role,target_digest,status,sequence,checked_offset FROM hash_fresh_work WHERE job_id=? AND ordinal=3"
				case "work_swapped":
					query = "UPDATE hash_fresh_work SET historical_work_id='2' WHERE job_id=? AND ordinal=1"
				case "work_digest":
					query = "UPDATE hash_fresh_work SET target_digest='" + strings.Repeat("0", 64) + "' WHERE job_id=? AND ordinal=1"
				case "work_nonzero":
					query = "UPDATE hash_fresh_work SET sequence=1,checked_offset=1 WHERE job_id=? AND ordinal=1"
				}
				if _, err := w.db.Exec(query, id); err != nil {
					t.Fatal(err)
				}
			} else {
				r := job.Record
				switch kind {
				case "version":
					r.Version++
				case "request_binding":
					r.Request.RequestID = "hash-choice-request-v1-" + strings.Repeat("0", 64)
				case "role":
					r.Request.Targets[0].Role = "copy"
				case "target_root":
					for i := range r.Request.Targets {
						r.Request.Targets[i].Target.Root.Revision++
					}
				case "authority":
					r.Request.Executable = true
				}
				if kind == "role" || kind == "target_root" || kind == "authority" {
					var err error
					r.Request.RequestID, err = hashKeeperChoiceFreshRequestID(r.Request)
					if err != nil {
						t.Fatal(err)
					}
				}
				payload, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "oversized" {
					payload = bytes.Repeat([]byte{'x'}, (2<<20)+1)
				}
				if kind == "noncanonical" {
					payload = append(payload, '\n')
				}
				key := r.JobKey
				if kind == "job_key" {
					key = hashChoiceJobKey(2)
				}
				if kind == "payload_checksum" {
					payload[len(payload)-1] = 'x'
				} else {
					id = fmt.Sprintf("hash-choice-job-v1-%x", sha256.Sum256(payload))
				}
				if _, err = w.db.Exec("UPDATE hash_fresh_job SET id=?,job_key=?,payload=? WHERE id=?", id, key, payload, job.ID); err != nil {
					t.Fatal(err)
				}
				if _, err = w.db.Exec("UPDATE hash_fresh_work SET job_id=? WHERE job_id=?", id, job.ID); err != nil {
					t.Fatal(err)
				}
				if kind == "role" {
					if _, err = w.db.Exec("UPDATE hash_fresh_work SET role='copy' WHERE job_id=? AND ordinal=1", id); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "target_root" {
					for i, target := range r.Request.Targets {
						body, err := json.Marshal(target.Target)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = w.db.Exec("UPDATE hash_fresh_work SET target_digest=? WHERE job_id=? AND ordinal=?", fmt.Sprintf("%x", sha256.Sum256(body)), id, i+1); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			got, err := w.FreshJob(context.Background(), id)
			if !errors.Is(err, ErrHashFreshJobCorrupt) || !reflect.DeepEqual(got, SavedFreshJob{}) {
				t.Fatal("corrupt/rechecksummed job returned partial scope or usable fresh work", kind, got, err)
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("corrupt fresh-job read changed original state")
			}
		})
	}
	var absent *HashStore
	valid := "hash-choice-job-v1-" + strings.Repeat("a", 64)
	if !ValidHashFreshJobID(valid) {
		t.Fatal("canonical fresh job ID rejected")
	}
	for _, id := range []string{"", valid[:len(valid)-1], valid + "a", valid + "\n", strings.ToUpper(valid), hashChoiceJobKey(1)} {
		if ValidHashFreshJobID(id) {
			t.Fatal("noncanonical/wrong-contract job ID accepted", id)
		}
		if got, err := absent.FreshJob(context.Background(), id); !errors.Is(err, ErrHashFreshJobID) || got.ID != "" {
			t.Fatal("invalid job ID reached storage", got, err)
		}
	}
}

func TestHashKeeperChoiceFreshJobRawPathsRetainExactFrozenTargets(t *testing.T) {
	for _, name := range []string{"quote\"雪\nfile", "invalid-\xff-file"} {
		t.Run(fmt.Sprintf("name_%x", []byte(name)), func(t *testing.T) {
			body := fullHashContents(65)
			scanner, targets := sampleFixture(t, body, body)
			root := string(targets[0].Root.PathBytes)
			paths := []string{filepath.Join(root, "project", name), string(targets[1].File.PathBytes)}
			err := os.Rename(string(targets[0].File.PathBytes), paths[0])
			requireSampleFixtureFilename(t, name, err)
			f := hashStoreFixtureFromFiles(t, scanner, captureSampleTargets(t, scanner, root, paths), [][]byte{body, body})
			metadata := hashChoiceMetadataFromFixture(t, f, "2", "1")
			m := &hashChoiceRequestFixture{f: f, saved: metadata.saved, proposal: metadata.request.Proposal()}
			request := hashChoiceJobRequest(t, m)
			w := hashChoiceJobWriter(t, m, request)
			job := saveFreshJobFixture(t, w, request, hashChoiceJobKey(1))
			if !bytes.Equal(job.Record.Request.Targets[1].Target.File.PathBytes, []byte(paths[0])) || !bytes.Equal(job.Record.Request.Targets[1].Observation.PathBytes, []byte(paths[0])) {
				t.Fatal("new job replaced authoritative raw path bytes")
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			f.store, err = OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			got, err := f.store.FreshJob(context.Background(), job.ID)
			if err != nil || !reflect.DeepEqual(got, job) {
				t.Fatal("saved reopening changed raw frozen target evidence", got, err)
			}
		})
	}
}

// The child receives only generated saved storage and exact publication IDs.
// It never opens original sources, inventory, configuration or hash dispatch.
func TestHashKeeperChoiceFreshJobCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_FRESH_JOB_CRASH_BASE")
	if base == "" {
		return
	}
	reader, err := OpenHashReader(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	request, err := reader.PrepareKeeperChoiceFreshRequest(context.Background(), os.Getenv("RYDD_FRESH_JOB_CRASH_CHOICE"))
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
		if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := os.Stdin.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		t.Fatal("fresh job crash helper unexpectedly resumed")
	}
	hooks := hashFreshJobHooks{}
	switch os.Getenv("RYDD_FRESH_JOB_CRASH_STAGE") {
	case "before_commit":
		hooks.beforeCommit = stop
	case "after_commit":
		hooks.afterCommit = stop
	default:
		t.Fatal("unknown fresh job crash boundary")
	}
	job, err := w.saveFreshJob(context.Background(), request, os.Getenv("RYDD_FRESH_JOB_CRASH_KEY"), hooks)
	t.Fatal("fresh job crash seam was not reached", job, err)
}

func killFreshJobAt(t *testing.T, base, choiceID, key, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashKeeperChoiceFreshJobCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_FRESH_JOB_CRASH_BASE="+base, "RYDD_FRESH_JOB_CRASH_CHOICE="+choiceID, "RYDD_FRESH_JOB_CRASH_KEY="+key, "RYDD_FRESH_JOB_CRASH_STAGE="+stage)
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
	go func() { scanner := bufio.NewScanner(stdout); ready <- scanner.Scan() && scanner.Text() == "READY" }()
	select {
	case ok := <-ready:
		if !ok {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatal("fresh job child did not reach exact boundary", stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("fresh job child timed out", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("fresh job child survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("fresh job child did not die by SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashKeeperChoiceFreshJobProcessDeathAtomicPublicationAndExactRetry(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 3, 3, "3", "2", "1")
			request := hashChoiceJobRequest(t, m)
			before := hashStoreSnapshot(t, m.f.store)
			if err := m.f.store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := m.f.source.Close(); err != nil {
				t.Fatal(err)
			}
			m.f.scanner.Close()
			for _, path := range []string{m.f.root, m.f.stateDir} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			key := hashChoiceJobKey(1)
			killFreshJobAt(t, m.f.base, m.saved.ID, key, stage)
			reader, err := OpenHashReader(context.Background(), m.f.base)
			if err != nil {
				t.Fatal(err)
			}
			var committed SavedFreshJob
			if stage == "before_commit" {
				if hashChoiceSchemaVersion(t, reader.db) != 3 || hashChoiceTableExists(t, reader.db, "hash_fresh_job") || hashChoiceTableExists(t, reader.db, "hash_fresh_work") {
					t.Fatal("process death exposed uncommitted migration or work")
				}
			} else {
				var id string
				if err = reader.db.QueryRow("SELECT id FROM hash_fresh_job WHERE job_key=?", key).Scan(&id); err != nil {
					t.Fatal(err)
				}
				committed, err = reader.FreshJob(context.Background(), id)
				if err != nil {
					t.Fatal(err)
				}
				requireFreshJob(t, committed, request, key)
				if !committed.Record.CreatedAt.Equal(hashChoiceClock()) || hashChoiceSchemaVersion(t, reader.db) != 4 {
					t.Fatal("committed process reply lost fixed publication evidence", committed)
				}
				jobs, work := hashFreshJobRows(t, reader.db)
				if jobs != 1 || work != 3 {
					t.Fatal("process death left partial/extra job work", jobs, work)
				}
			}
			if !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) {
				t.Fatal("process publication recovered or changed original hashing state")
			}
			if err = reader.Close(); err != nil {
				t.Fatal(err)
			}
			w, err := OpenHashFreshJobWriter(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			m.f.store = w
			w.now = hashChoiceClock
			retry := saveFreshJobFixture(t, w, request, key)
			if stage == "after_commit" && !reflect.DeepEqual(retry, committed) {
				t.Fatal("manual exact-key retry changed committed lost reply", retry, committed)
			}
			jobs, work := hashFreshJobRows(t, w.db)
			if jobs != 1 || work != 3 || !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
				t.Fatal("post-crash retry replaced key, added partial jobs or changed originals", jobs, work)
			}
		})
	}
}
