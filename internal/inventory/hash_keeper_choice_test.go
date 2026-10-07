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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

// All choices and any content reads in this file use disposable production
// inventories. Choice publication/reopening itself is saved-record-only.
func hashChoiceFixture(t *testing.T, contents ...[]byte) *hashStoreTestFixture {
	t.Helper()
	f := hashStoreFixture(t, contents...)
	for range contents {
		hashStoreRun(t, f, FileHashStepByteLimit, 1<<20)
	}
	return f
}

func hashChoiceClock() time.Time {
	return time.Date(2026, 10, 7, 13, 0, 0, 0, time.UTC)
}

func hashChoiceWriter(t *testing.T, f *hashStoreTestFixture) *HashStore {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenHashChoiceWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	w.now = hashChoiceClock
	f.store = w
	return w
}

func cloneHashChoicePreview(t *testing.T, p HashKeeperPreview) HashKeeperPreview {
	t.Helper()
	body, err := json.Marshal(p)
	var out HashKeeperPreview
	if err != nil || json.Unmarshal(body, &out) != nil {
		t.Fatal("could not clone generated preview", err)
	}
	return out
}

func requireHashChoice(t *testing.T, saved SavedHashKeeperChoice) {
	t.Helper()
	r := saved.Record
	if HashKeeperChoiceContract != "historical_hash_choice_v1" || HashKeeperChoiceLimit != 128 || HashKeeperChoiceMaxRecordBytes != 256<<10 || r.Version != 1 || r.Contract != HashKeeperChoiceContract || r.Status != "historical_unapproved" || r.CreatedAt.IsZero() || !ValidHashKeeperChoiceID(saved.ID) {
		t.Fatal("choice lost its bounded historical contract", saved)
	}
	requireHashKeeperPreviewClaims(t, r.Evidence)
	body, err := json.Marshal(r)
	if err != nil || len(body) > HashKeeperChoiceMaxRecordBytes || saved.ID != fmt.Sprintf("hash-choice-v1-%x", sha256.Sum256(body)) {
		t.Fatal("choice ID did not bind the canonical complete record", saved.ID, err)
	}
	for _, forbidden := range [][]byte{[]byte(`"checkpoint"`), []byte("sha\x03")} {
		if bytes.Contains(body, forbidden) {
			t.Fatal("choice persisted a hashing continuation")
		}
	}
}

func saveHashChoice(t *testing.T, w *HashStore, p HashKeeperPreview) SavedHashKeeperChoice {
	t.Helper()
	saved, err := w.SaveKeeperChoice(context.Background(), p)
	if err != nil {
		t.Fatal("generated choice save failed", err)
	}
	requireHashChoice(t, saved)
	return saved
}

func hashChoiceSchemaVersion(t *testing.T, db *sql.DB) int {
	t.Helper()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func hashChoiceCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM hash_keeper_choice").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func hashChoiceOpenDB(t *testing.T, base string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(base, "hashes", hashStoreFilename))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func hashChoiceDowngrade(t *testing.T, f *hashStoreTestFixture, version int) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	db := hashChoiceOpenDB(t, f.base)
	if _, err := db.Exec("DROP TABLE IF EXISTS hash_keeper_choice"); err != nil {
		t.Fatal(err)
	}
	if version == 1 {
		for _, table := range []string{"hash_read_observation", "hash_read_revocation", "hash_read_approval"} {
			if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version=%d", version)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHashKeeperChoiceImmutableRoundTripAndExplicitOrder(t *testing.T) {
	body := bytes.Repeat([]byte("GENERATED CHOICE SOURCE BODY MUST STAY OUT OF THE RECORD."), 3)
	f := hashChoiceFixture(t, body, body, body)
	before := hashStoreSnapshot(t, f.store)
	p := hashKeeperPreview(t, f.store, before.SelectionID, "3", "2", "1")
	w := hashChoiceWriter(t, f)
	saved := saveHashChoice(t, w, p)
	if !saved.Record.CreatedAt.Equal(hashChoiceClock()) || !reflect.DeepEqual(saved.Record.Evidence, p) || saved.Record.Evidence.Keeper.WorkID != "3" || saved.Record.Evidence.Copies[0].WorkID != "2" || saved.Record.Evidence.Copies[1].WorkID != "1" {
		t.Fatal("choice changed explicit roles or complete save-time evidence", saved)
	}
	var payload []byte
	if err := w.db.QueryRow("SELECT payload FROM hash_keeper_choice WHERE id=?", saved.ID).Scan(&payload); err != nil || bytes.Contains(payload, body) {
		t.Fatal("saved choice lost canonical payload or stored source body", err)
	}
	for _, statement := range []string{"UPDATE hash_keeper_choice SET payload=x'00' WHERE id=?", "DELETE FROM hash_keeper_choice WHERE id=?"} {
		if _, err := w.db.Exec(statement, saved.ID); err == nil {
			t.Fatal("published choice was mutable", statement)
		}
	}
	for i := 0; i < 2; i++ {
		w.now = func() time.Time { return hashChoiceClock().Add(24 * time.Hour) }
		again := saveHashChoice(t, w, p)
		if !reflect.DeepEqual(again, saved) || hashChoiceCount(t, w.db) != 1 {
			t.Fatal("exact retry changed time, ID, payload or row count", again)
		}
	}
	reordered := hashKeeperPreview(t, w, p.SelectionID, "3", "1", "2")
	other := saveHashChoice(t, w, reordered)
	if other.ID == saved.ID || other.Record.Evidence.Copies[0].WorkID != "1" || hashChoiceCount(t, w.db) != 2 {
		t.Fatal("copy order did not identify a distinct explicit choice", other)
	}
	original := saved
	// Mutate a separately returned copy, then reopen the original row.
	saved, err := w.KeeperChoice(context.Background(), original.ID)
	if err != nil {
		t.Fatal(err)
	}
	saved.Record.Evidence.Keeper.PathBytes[0] = 'x'
	saved.Record.Evidence.Copies[0].PathBytes[0] = 'y'
	saved.Record.Evidence.Budget.TotalReadBytes = -1
	got, err := w.KeeperChoice(context.Background(), original.ID)
	if err != nil || !reflect.DeepEqual(got, original) {
		t.Fatal("saved choices shared caller-mutable evidence", got, err)
	}
	if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(before, after) {
		t.Fatal("choice publication changed hashing work, charge or consent", after)
	}
}

func hashChoiceTableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count != 0
}

func hashChoiceOffline(t *testing.T, f *hashStoreTestFixture) {
	t.Helper()
	if err := f.source.Close(); err != nil {
		t.Fatal(err)
	}
	f.scanner.Close()
	for _, path := range []string{f.root, f.stateDir} {
		if err := os.Rename(path, path+"-offline"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.Rename(path+"-offline", path); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestHashKeeperChoiceOfflineRawPathsAndIndependentDigest(t *testing.T) {
	for _, name := range []string{"quote\"雪\nfile", "invalid-\xff-file"} {
		t.Run(fmt.Sprintf("name_%x", []byte(name)), func(t *testing.T) {
			body := fullHashContents(65)
			scanner, targets := sampleFixture(t, body, bytes.Clone(body))
			root := string(targets[0].Root.PathBytes)
			paths := []string{filepath.Join(root, "project", name), string(targets[1].File.PathBytes)}
			err := os.Rename(string(targets[0].File.PathBytes), paths[0])
			requireSampleFixtureFilename(t, name, err)
			targets = captureSampleTargets(t, scanner, root, paths)
			f := hashStoreFixtureFromFiles(t, scanner, targets, [][]byte{body, body})
			for range 2 {
				hashStoreRun(t, f, FileHashStepByteLimit, 1<<20)
			}
			before := hashStoreSnapshot(t, f.store)
			p := hashKeeperPreview(t, f.store, before.SelectionID, "1", "2")
			if p.SHA256 != fmt.Sprintf("%x", sha256.Sum256(body)) || !bytes.Equal(p.Keeper.PathBytes, []byte(paths[0])) {
				t.Fatal("generated observation or raw path changed", p)
			}
			w := hashChoiceWriter(t, f)
			// Source/inventory disappear before publication, not merely before show.
			hashChoiceOffline(t, f)
			saved := saveHashChoice(t, w, p)
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			r.now = func() time.Time { t.Fatal("reopening historical choice consulted wall time"); return time.Time{} }
			got, err := r.KeeperChoice(context.Background(), saved.ID)
			if err != nil || !reflect.DeepEqual(got, saved) || !bytes.Equal(got.Record.Evidence.Keeper.PathBytes, []byte(paths[0])) {
				t.Fatal("offline historical record changed", got, err)
			}
			if after := hashStoreSnapshot(t, r); !reflect.DeepEqual(after, before) {
				t.Fatal("saved-only publication/show changed hashing state", after)
			}
		})
	}
}

func TestHashKeeperChoiceAtomicMigrationAndReaderCompatibility(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			f := hashChoiceFixture(t, []byte("same"), []byte("same"))
			before := hashStoreSnapshot(t, f.store)
			p := hashKeeperPreview(t, f.store, before.SelectionID, "1", "2")
			hashChoiceDowngrade(t, f, version)
			w, err := OpenHashChoiceWriter(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			f.store = w
			w.now = hashChoiceClock
			assertOld := func() {
				t.Helper()
				if hashChoiceSchemaVersion(t, w.db) != version || hashChoiceTableExists(t, w.db, "hash_keeper_choice") || (version == 1 && hashChoiceTableExists(t, w.db, "hash_read_approval")) {
					t.Fatal("opening/refusing/canceling performed a standalone migration")
				}
				if got := hashStoreSnapshot(t, w); !reflect.DeepEqual(got, before) {
					t.Fatal("migration attempt changed historical state", got)
				}
			}
			assertOld()
			r, err := OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			if got, e := r.KeeperChoice(context.Background(), "hash-choice-v1-"+strings.Repeat("0", 64)); !errors.Is(e, os.ErrNotExist) || got.ID != "" {
				t.Fatal("legacy reader initialized a choice", got, e)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			bad := cloneHashChoicePreview(t, p)
			bad.Keeper.Sequence++
			if got, e := w.SaveKeeperChoice(context.Background(), bad); !errors.Is(e, ErrHashKeeperChoiceEvidence) || got.ID != "" {
				t.Fatal(got, e)
			}
			assertOld()
			ctx, cancel := context.WithCancel(context.Background())
			got, err := w.saveKeeperChoice(ctx, p, hashKeeperChoiceHooks{beforeCommit: cancel})
			cancel()
			if !errors.Is(err, context.Canceled) || got.ID != "" {
				t.Fatal("uncommitted cancellation published a choice", got, err)
			}
			assertOld()
			saved := saveHashChoice(t, w, p)
			if hashChoiceSchemaVersion(t, w.db) != 3 || !hashChoiceTableExists(t, w.db, "hash_read_approval") || hashChoiceCount(t, w.db) != 1 {
				t.Fatal("first publication did not atomically migrate both additive schemas")
			}
			if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, before) {
				t.Fatal("successful migration changed work", after)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			r, err = OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err = r.KeeperChoice(context.Background(), saved.ID)
			if err != nil || !reflect.DeepEqual(got, saved) {
				t.Fatal("schema3 reader lost atomic publication", got, err)
			}
		})
	}
}

func TestHashKeeperChoiceDoesNotRecoverReservedAttempts(t *testing.T) {
	f := hashStoreFixture(t, []byte("same"), []byte("same"), fullHashContents(129))
	for range 2 {
		hashStoreRun(t, f, 64, 4096)
	}
	p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "1", "2")
	if _, err := f.store.db.Exec("CREATE TRIGGER fail_choice_fixture_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	r, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 4096)
	if !errors.Is(err, ErrHashRecoveryRequired) || r.Usage.ReadBytes != 64 {
		t.Fatal("fixture did not leave interrupted reservation", r, err)
	}
	if _, err = f.store.db.Exec("DROP TRIGGER fail_choice_fixture_checkpoint"); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, f.store)
	attempt := before.Work[2].LatestAttempt
	if before.Work[2].Status != "running" || attempt == nil || attempt.Status != "reserved" || attempt.RequestedBytes != nil || attempt.ReadBytes != nil || attempt.ElapsedNS != nil || before.Budget.TotalReservedBytes != 72 || before.Budget.TotalReadBytes != 8 || before.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture lost unsettled null usage", before)
	}
	w := hashChoiceWriter(t, f)
	if opened := hashStoreSnapshot(t, w); !reflect.DeepEqual(opened, before) {
		t.Fatal("choice writer recovered an attempt", opened)
	}
	saved := saveHashChoice(t, w, p)
	if saved.Record.Evidence.UnfinishedWork != 1 || !reflect.DeepEqual(saved.Record.Evidence.Budget, before.Budget) {
		t.Fatal("choice lost save-time unsettled charge", saved)
	}
	if _, err = w.RunNext(context.Background(), f.source, f.scanner, 64, 4096); err == nil {
		t.Fatal("choice writer dispatched content reads")
	}
	if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, before) {
		t.Fatal("choice publication/show reconciled or refunded interrupted attempt", after)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	rd, err := OpenHashReader(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	if got, e := rd.KeeperChoice(context.Background(), saved.ID); e != nil || !reflect.DeepEqual(got, saved) {
		t.Fatal(got, e)
	}
	if after := hashStoreSnapshot(t, rd); !reflect.DeepEqual(after, before) {
		t.Fatal("reader recovered an interrupted attempt", after)
	}
}

func TestHashKeeperChoiceCapacityKeepsEarlierRecordsAndExactRetry(t *testing.T) {
	contents := make([][]byte, 20)
	for i := range contents {
		contents[i] = []byte("same")
	}
	f := hashChoiceFixture(t, contents...)
	before := hashStoreSnapshot(t, f.store)
	w := hashChoiceWriter(t, f)
	var first SavedHashKeeperChoice
	var firstPreview HashKeeperPreview
	count := 0
	for keeper := 1; keeper <= 20; keeper++ {
		for copy := 1; copy <= 20; copy++ {
			if keeper == copy {
				continue
			}
			p := hashKeeperPreview(t, w, before.SelectionID, strconv.Itoa(keeper), strconv.Itoa(copy))
			if count == HashKeeperChoiceLimit {
				got, err := w.SaveKeeperChoice(context.Background(), p)
				if !errors.Is(err, ErrHashKeeperChoiceCapacity) || got.ID != "" || hashChoiceCount(t, w.db) != 128 {
					t.Fatal("capacity refusal removed or published a record", got, err)
				}
				again := saveHashChoice(t, w, firstPreview)
				if !reflect.DeepEqual(again, first) || hashChoiceCount(t, w.db) != 128 {
					t.Fatal("full capacity blocked or changed exact retry", again)
				}
				if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, before) {
					t.Fatal("capacity changed hashing state", after)
				}
				return
			}
			saved := saveHashChoice(t, w, p)
			if count == 0 {
				first = saved
				firstPreview = p
			}
			count++
		}
	}
	t.Fatal("fixture did not exercise capacity")
}

func TestHashKeeperChoiceExistingOnlyAndCanonicalIDs(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(root, "missing")
	if s, e := OpenHashChoiceWriter(context.Background(), missing); e == nil || s != nil {
		t.Fatal("choice writer initialized absent storage", s, e)
	}
	if _, e := os.Lstat(missing); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("missing directory was created", e)
	}
	base := filepath.Join(root, "existing")
	if err = localfs.EnsurePrivateDir(base); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "hashes")
	if err = localfs.EnsurePrivateDir(dir); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, hashStoreFilename)
	if s, e := OpenHashChoiceWriter(context.Background(), base); e == nil || s != nil {
		t.Fatal("choice writer initialized absent database", s, e)
	}
	if _, e := os.Lstat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("absent database was created", e)
	}
	if err = os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if s, e := OpenHashChoiceWriter(context.Background(), base); !errors.Is(e, ErrHashStoreCorrupt) || s != nil {
		t.Fatal("choice writer initialized schema zero", s, e)
	}
	db := hashChoiceOpenDB(t, base)
	if hashChoiceSchemaVersion(t, db) != 0 || hashChoiceTableExists(t, db, "hash_meta") {
		t.Fatal("uninitialized database gained product schema")
	}
	var absent *HashStore
	valid := "hash-choice-v1-" + strings.Repeat("a", 64)
	if !ValidHashKeeperChoiceID(valid) {
		t.Fatal("canonical complete ID rejected")
	}
	for _, id := range []string{"", strings.Repeat("a", 64), valid[:len(valid)-1], valid + "\n", strings.ToUpper(valid), "hash-choice-v1-" + strings.Repeat("A", 64)} {
		if ValidHashKeeperChoiceID(id) {
			t.Fatal("noncanonical ID accepted", id)
		}
		if got, e := absent.KeeperChoice(context.Background(), id); !errors.Is(e, ErrHashKeeperChoiceID) || got.ID != "" {
			t.Fatal("invalid ID reached storage", got, e)
		}
	}
}

func TestHashKeeperChoiceCancellationUncertaintyAndConcurrentRetry(t *testing.T) {
	for _, stage := range []string{"already_canceled", "after_commit", "uncertain_rollback", "uncertain_committed", "concurrent_retry"} {
		t.Run(stage, func(t *testing.T) {
			f := hashChoiceFixture(t, []byte("same"), []byte("same"))
			before := hashStoreSnapshot(t, f.store)
			p := hashKeeperPreview(t, f.store, before.SelectionID, "1", "2")
			w := hashChoiceWriter(t, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "already_canceled" {
				cancel()
				got, err := w.SaveKeeperChoice(ctx, p)
				if !errors.Is(err, context.Canceled) || got.ID != "" || hashChoiceTableExists(t, w.db, "hash_keeper_choice") {
					t.Fatal("pre-canceled request published or migrated", got, err)
				}
				return
			}
			if stage == "concurrent_retry" {
				const n = 8
				results := make(chan SavedHashKeeperChoice, n)
				failures := make(chan error, n)
				var workers sync.WaitGroup
				for range n {
					workers.Go(func() { got, err := w.SaveKeeperChoice(context.Background(), p); results <- got; failures <- err })
				}
				workers.Wait()
				close(results)
				close(failures)
				for err := range failures {
					if err != nil {
						t.Fatal(err)
					}
				}
				var first SavedHashKeeperChoice
				for got := range results {
					if first.ID == "" {
						first = got
					}
					if !reflect.DeepEqual(got, first) {
						t.Fatal("serialized retry published multiple records", got)
					}
				}
				if hashChoiceCount(t, w.db) != 1 {
					t.Fatal("concurrent exact retry consumed capacity")
				}
				return
			}
			hooks := hashKeeperChoiceHooks{}
			if stage == "after_commit" {
				hooks.afterCommit = cancel
			} else {
				hooks.commit = func(tx *sql.Tx) error {
					if stage == "uncertain_committed" {
						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
					}
					return errors.New("generated lost commit reply")
				}
			}
			candidate, err := w.saveKeeperChoice(ctx, p, hooks)
			if candidate.ID == "" {
				t.Fatal("publication uncertainty hid recoverable candidate ID", candidate, err)
			}
			requireHashChoice(t, candidate)
			if stage == "after_commit" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("late cancellation was reported as success", err)
				}
			} else {
				if !errors.Is(err, ErrHashRecoveryRequired) {
					t.Fatal("lost commit reply reported definite result", err)
				}
				if got, e := w.SaveKeeperChoice(context.Background(), p); !errors.Is(e, ErrHashRecoveryRequired) || got.ID != "" {
					t.Fatal("uncertain writer remained writable", got, e)
				}
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			w, err = OpenHashChoiceWriter(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			f.store = w
			w.now = hashChoiceClock
			got, showErr := w.KeeperChoice(context.Background(), candidate.ID)
			if stage == "uncertain_rollback" {
				if !errors.Is(showErr, os.ErrNotExist) || got.ID != "" {
					t.Fatal("rolled-back publication became visible", got, showErr)
				}
			} else if showErr != nil || !reflect.DeepEqual(got, candidate) {
				t.Fatal("committed lost reply could not reopen exact candidate", got, showErr)
			}
			again := saveHashChoice(t, w, p)
			if !reflect.DeepEqual(again, candidate) || hashChoiceCount(t, w.db) != 1 {
				t.Fatal("manual exact retry changed recoverable publication", again)
			}
			if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, before) {
				t.Fatal("publication error changed hashing state", after)
			}
		})
	}
}

func TestHashKeeperChoiceCorruptRecordsRefusePartialResults(t *testing.T) {
	for _, kind := range []string{"oversized", "wrong_storage_type", "request_key", "version", "selected_binding", "selected_digest", "keeper_sequence", "copy_sequence", "keeper_checked_at", "copy_checked_at", "authority", "noncanonical"} {
		t.Run(kind, func(t *testing.T) {
			f := hashChoiceFixture(t, []byte("same"), []byte("same"))
			p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "1", "2")
			w := hashChoiceWriter(t, f)
			saved := saveHashChoice(t, w, p)
			for _, name := range []string{"hash_keeper_choice_no_update", "hash_keeper_choice_no_delete"} {
				if _, err := w.db.Exec("DROP TRIGGER " + name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := w.db.Exec("PRAGMA ignore_check_constraints=ON"); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(saved.Record)
			if err != nil {
				t.Fatal(err)
			}
			key, err := hashKeeperChoiceRequestKey(saved.Record.Evidence)
			if err != nil {
				t.Fatal(err)
			}
			id := saved.ID
			switch kind {
			case "oversized":
				payload = bytes.Repeat([]byte{'x'}, HashKeeperChoiceMaxRecordBytes+1)
			case "request_key":
				key = strings.Repeat("0", 64)
			case "version", "selected_binding", "selected_digest", "keeper_sequence", "copy_sequence", "keeper_checked_at", "copy_checked_at", "authority":
				record := saved.Record
				if kind == "version" {
					record.Version++
				}
				if kind == "selected_binding" {
					record.Evidence.Keeper.FileID++
				}
				if kind == "selected_digest" {
					record.Evidence.SHA256 = strings.Repeat("0", 64)
				}
				if kind == "keeper_sequence" {
					record.Evidence.Keeper.Sequence++
				}
				if kind == "copy_sequence" {
					record.Evidence.Copies[0].Sequence++
				}
				if kind == "keeper_checked_at" {
					record.Evidence.Keeper.CheckedAt = record.Evidence.Keeper.CheckedAt.Add(time.Nanosecond)
				}
				if kind == "copy_checked_at" {
					record.Evidence.Copies[0].CheckedAt = record.Evidence.Copies[0].CheckedAt.Add(time.Nanosecond)
				}
				if kind == "authority" {
					record.Evidence.Executable = true
				}
				payload, err = json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				key, err = hashKeeperChoiceRequestKey(record.Evidence)
				if err != nil {
					t.Fatal(err)
				}
				// Rebind fixture checksums, so semantic validation is exercised.
				id = fmt.Sprintf("hash-choice-v1-%x", sha256.Sum256(payload))
			case "noncanonical":
				payload = append(payload, '\n')
				id = fmt.Sprintf("hash-choice-v1-%x", sha256.Sum256(payload))
			}
			var stored any = payload
			if kind == "wrong_storage_type" {
				stored = string(payload)
			}
			if _, err = w.db.Exec("UPDATE hash_keeper_choice SET id=?,request_key=?,payload=? WHERE id=?", id, key, stored, saved.ID); err != nil {
				t.Fatal(err)
			}
			got, err := w.KeeperChoice(context.Background(), id)
			if !errors.Is(err, ErrHashKeeperChoiceCorrupt) || !reflect.DeepEqual(got, SavedHashKeeperChoice{}) {
				t.Fatal("corrupt record yielded partial or authoritative choice", got, err)
			}
		})
	}
}

// The child opens saved storage only. It receives no source or inventory path.
func TestHashKeeperChoiceCrashHelper(t *testing.T) {
	base := os.Getenv("RYDD_HASH_CHOICE_CRASH_BASE")
	if base == "" {
		return
	}
	w, err := OpenHashChoiceWriter(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	w.now = hashChoiceClock
	snapshot := hashStoreSnapshot(t, w)
	p := hashKeeperPreview(t, w, snapshot.SelectionID, "1", "2")
	stop := func() {
		if _, err := fmt.Fprintln(os.Stdout, "READY"); err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		if _, err := os.Stdin.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		t.Fatal("choice crash helper unexpectedly resumed")
	}
	hooks := hashKeeperChoiceHooks{}
	switch os.Getenv("RYDD_HASH_CHOICE_CRASH_STAGE") {
	case "before_commit":
		hooks.beforeCommit = stop
	case "after_commit":
		hooks.afterCommit = stop
	default:
		t.Fatal("unknown generated choice crash seam")
	}
	got, err := w.saveKeeperChoice(context.Background(), p, hooks)
	t.Fatal("choice crash seam was not reached", got, err)
}

func killHashChoiceAt(t *testing.T, base, stage string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHashKeeperChoiceCrashHelper$")
	cmd.Env = append(os.Environ(), "RYDD_HASH_CHOICE_CRASH_BASE="+base, "RYDD_HASH_CHOICE_CRASH_STAGE="+stage)
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
			t.Fatal("choice crash helper did not reach exact seam", stderr.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("choice crash helper timed out", stderr.String())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("choice helper survived SIGKILL")
	}
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("choice helper did not die by SIGKILL", cmd.ProcessState, stderr.String())
	}
}

func TestHashKeeperChoiceProcessDeathAtomicPublicationAndMigration(t *testing.T) {
	for _, stage := range []string{"before_commit", "after_commit"} {
		t.Run(stage, func(t *testing.T) {
			f := hashChoiceFixture(t, fullHashContents(65), fullHashContents(65))
			before := hashStoreSnapshot(t, f.store)
			p := hashKeeperPreview(t, f.store, before.SelectionID, "1", "2")
			record := HashKeeperChoiceRecord{Version: 1, Contract: HashKeeperChoiceContract, CreatedAt: hashChoiceClock(), Status: "historical_unapproved", Evidence: p}
			payload, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			candidate := fmt.Sprintf("hash-choice-v1-%x", sha256.Sum256(payload))
			hashChoiceDowngrade(t, f, 1)
			hashChoiceOffline(t, f)
			killHashChoiceAt(t, f.base, stage)
			r, err := OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got, err := r.KeeperChoice(context.Background(), candidate)
			if stage == "before_commit" {
				if !errors.Is(err, os.ErrNotExist) || got.ID != "" || hashChoiceSchemaVersion(t, r.db) != 1 || hashChoiceTableExists(t, r.db, "hash_keeper_choice") || hashChoiceTableExists(t, r.db, "hash_read_approval") {
					t.Fatal("process death exposed uncommitted schema or record", got, err)
				}
			} else {
				if err != nil || got.ID != candidate || !reflect.DeepEqual(got.Record, record) || hashChoiceSchemaVersion(t, r.db) != 3 || hashChoiceCount(t, r.db) != 1 || !hashChoiceTableExists(t, r.db, "hash_read_approval") {
					t.Fatal("committed lost process reply lost atomic choice", got, err)
				}
				requireHashChoice(t, got)
			}
			if after := hashStoreSnapshot(t, r); !reflect.DeepEqual(after, before) {
				t.Fatal("process publication changed hashing work/charge or read permission", after)
			}
		})
	}
}

func TestHashKeeperChoiceSaveTimeContextAndRetryIgnoreUnrelatedProgress(t *testing.T) {
	f, request := hashReadFixture(t, fullHashContents(65), fullHashContents(65), fullHashContents(129), []byte("unmatched fixture"))
	c := hashReadApprove(t, f, request)
	for i := 0; i < 2; i++ {
		if _, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); err != nil {
			t.Fatal(err)
		}
	}
	p := hashKeeperPreview(t, f.store, request.SelectionID, "2", "1")
	if _, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); err != nil {
		t.Fatal(err)
	}
	advanced := hashStoreSnapshot(t, f.store)
	w := hashChoiceWriter(t, f)
	saved := saveHashChoice(t, w, p)
	if saved.Record.Evidence.CompletedObservations != 3 || saved.Record.Evidence.UnfinishedWork != 1 || !reflect.DeepEqual(saved.Record.Evidence.Budget, advanced.Budget) || !reflect.DeepEqual(saved.Record.Evidence.ReadConsent, advanced.ReadConsent) || !reflect.DeepEqual(saved.Record.Evidence.Keeper, p.Keeper) || !reflect.DeepEqual(saved.Record.Evidence.Copies, p.Copies) {
		t.Fatal("save rejected or silently reused stale unrelated context", saved)
	}
	f.reopen(t)
	if _, err := f.store.RunConsented(context.Background(), c.ID, f.source, f.scanner); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RevokeRead(context.Background(), c.ID); err != nil {
		t.Fatal(err)
	}
	latest := hashStoreSnapshot(t, f.store)
	w = hashChoiceWriter(t, f)
	w.now = func() time.Time { return hashChoiceClock().Add(time.Hour) }
	again := saveHashChoice(t, w, p)
	if !reflect.DeepEqual(again, saved) || hashChoiceCount(t, w.db) != 1 {
		t.Fatal("retry recaptured mutable context instead of original record", again)
	}
	newRoles := hashKeeperPreview(t, w, p.SelectionID, "1", "2")
	second := saveHashChoice(t, w, newRoles)
	if second.ID == saved.ID || second.Record.Evidence.CompletedObservations != 4 || second.Record.Evidence.ReadConsent.Status != "revoked" || !reflect.DeepEqual(second.Record.Evidence.Budget, latest.Budget) || hashChoiceCount(t, w.db) != 2 {
		t.Fatal("new explicit role request did not preserve its own latest context", second)
	}
	if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, latest) {
		t.Fatal("choice publication advanced live work or permission", after)
	}
}

func TestHashKeeperChoiceRejectsForgedOrChangedDisplayedEvidence(t *testing.T) {
	f := hashChoiceFixture(t, []byte("same"), []byte("same"))
	p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "1", "2")
	w := hashChoiceWriter(t, f)
	before := hashStoreSnapshot(t, w)
	mutations := map[string]func(*HashKeeperPreview){
		"store":         func(p *HashKeeperPreview) { p.StoreID = strings.Repeat("0", 64) },
		"selection":     func(p *HashKeeperPreview) { p.SelectionID = strings.Repeat("0", 64) },
		"inventory":     func(p *HashKeeperPreview) { p.InventoryID = strings.Repeat("0", 64) },
		"hash_contract": func(p *HashKeeperPreview) { p.HashContract = "unsupported" },
		"size":          func(p *HashKeeperPreview) { p.LogicalBytes++ },
		"digest":        func(p *HashKeeperPreview) { p.SHA256 = strings.Repeat("0", 64) },
		"keeper_path":   func(p *HashKeeperPreview) { p.Keeper.PathBytes = []byte("/generated/other") },
		"copy_file":     func(p *HashKeeperPreview) { p.Copies[0].FileID++ },
		"sequence":      func(p *HashKeeperPreview) { p.Keeper.Sequence++ },
		"time":          func(p *HashKeeperPreview) { p.Copies[0].CheckedAt = p.Copies[0].CheckedAt.Add(time.Nanosecond) },
		"identity":      func(p *HashKeeperPreview) { p.Keeper.SavedInode = "999999" },
		"alias":         func(p *HashKeeperPreview) { p.Keeper.RepeatedSavedIdentity = true },
		"conflict":      func(p *HashKeeperPreview) { p.Copies[0].SavedIdentityConflict = true },
		"overlap":       func(p *HashKeeperPreview) { p.Copies[0] = p.Keeper },
		"approval":      func(p *HashKeeperPreview) { p.ApprovalAvailable = true },
		"verified":      func(p *HashKeeperPreview) { p.ContentVerified = true },
		"executable":    func(p *HashKeeperPreview) { p.Executable = true },
		"savings":       func(p *HashKeeperPreview) { n := int64(1); p.EstimatedReclaimableBytes = &n },
		"oversized": func(p *HashKeeperPreview) {
			p.Keeper.PathBytes = bytes.Repeat([]byte{'x'}, HashKeeperChoiceMaxRecordBytes+1)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := cloneHashChoicePreview(t, p)
			mutate(&changed)
			saved, err := w.SaveKeeperChoice(context.Background(), changed)
			if err == nil || !reflect.DeepEqual(saved, SavedHashKeeperChoice{}) {
				t.Fatal("invalid expected evidence published a partial choice", saved, err)
			}
			if after := hashStoreSnapshot(t, w); !reflect.DeepEqual(after, before) {
				t.Fatal("refusal changed source-observation state", after)
			}
		})
	}
	valid := saveHashChoice(t, w, p)
	if hashChoiceCount(t, w.db) != 1 || valid.Record.Evidence.Keeper.WorkID != "1" {
		t.Fatal("refused evidence consumed choice capacity")
	}
}
