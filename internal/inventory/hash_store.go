package inventory

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const hashStoreFilename = "hashes.sqlite3"
const hashStoreApplicationID = 0x52594853
const hashStoreCheckpointLimit = 8192
const hashStoreScope = "explicit_file_observation_v1"

var ErrHashStoreCorrupt = errors.New("saved hashing work is invalid or incompatible")
var ErrHashSelectionConflict = errors.New("hash storage already contains a different frozen selection")
var ErrHashInventoryChanged = errors.New("the frozen hash selection differs from current saved inventory")
var ErrHashDeferred = errors.New("hashing is deferred by the reservation budget or durable byte quantum")
var ErrHashRecoveryRequired = errors.New("hash publication is uncertain; close and reopen storage before dispatching work")

// HashStore preserves one finite selected batch separately from inventory.
// Saved evidence supplies no source-read consent or cleanup authority.
type HashStore struct {
	db            *sql.DB
	base          string
	readOnly      bool
	selectionOnly bool
	lock          *localfs.Lock
	mu            sync.Mutex
	closed        atomic.Bool
	poisoned      bool
	now           func() time.Time
	life          context.Context
	cancel        context.CancelFunc
	closeErr      error
	storageIDs    map[string]string
}

type hashSelectionRecord struct {
	Version       int                `json:"version"`
	StoreID       string             `json:"store_id"`
	Scope         string             `json:"scope"`
	InventoryID   string             `json:"inventory_id"`
	Targets       []SavedFileTarget  `json:"targets"`
	SourceLocator *HashSourceLocator `json:"source_locator,omitempty"`
}

// HashBudget measures charged allowances by their UTC reservation day. It is
// not physical I/O or a strict wall-day read meter. Unknown attempts retain the
// full charge; observed counters include only settled, known usage. Unknown
// reservation counters include recovered interrupted attempts, not reservations
// that are still awaiting settlement or recovery.
type HashBudget struct {
	Day                       string    `json:"reservation_day_utc"`
	MaxNow                    time.Time `json:"clock_high_water"`
	ReservedBytes             int64     `json:"reserved_bytes"`
	RequestedBytes            int64     `json:"observed_requested_bytes"`
	ReadBytes                 int64     `json:"observed_read_bytes"`
	UnknownReservedBytes      int64     `json:"unknown_reserved_bytes"`
	TotalReservedBytes        int64     `json:"total_reserved_bytes"`
	TotalRequestedBytes       int64     `json:"total_observed_requested_bytes"`
	TotalReadBytes            int64     `json:"total_observed_read_bytes"`
	TotalUnknownReservedBytes int64     `json:"total_unknown_reserved_bytes"`
}

type HashAttempt struct {
	Status         string `json:"status"`
	ReservationDay string `json:"reservation_day_utc"`
	ReservedBytes  int64  `json:"reserved_bytes"`
	RequestedBytes *int64 `json:"observed_requested_bytes"`
	ReadBytes      *int64 `json:"observed_read_bytes"`
	ElapsedNS      *int64 `json:"observed_elapsed_ns"`
}

type SavedHashWork struct {
	ID            string       `json:"id"`
	FileID        int64        `json:"file_id"`
	PathBytes     []byte       `json:"path_bytes"`
	Status        string       `json:"status"`
	Sequence      int64        `json:"sequence"`
	LogicalBytes  int64        `json:"logical_bytes"`
	DurableOffset int64        `json:"durable_offset"`
	CheckedAt     time.Time    `json:"checked_at"`
	SHA256        string       `json:"sha256,omitempty"`
	Code          string       `json:"code,omitempty"`
	LatestAttempt *HashAttempt `json:"latest_attempt,omitempty"`
}

// Snapshot digests describe saved historical observations, never current files.
type HashSnapshot struct {
	StoreID                   string          `json:"store_id"`
	SelectionID               string          `json:"selection_id,omitempty"`
	InventoryID               string          `json:"inventory_id,omitempty"`
	Source                    string          `json:"source"`
	Contract                  string          `json:"contract"`
	Budget                    *HashBudget     `json:"budget,omitempty"`
	Work                      []SavedHashWork `json:"work"`
	ProvenanceVerified        bool            `json:"provenance_verified"`
	ContentVerified           bool            `json:"content_verified"`
	CurrentStateVerified      bool            `json:"current_state_verified"`
	DuplicatesVerified        bool            `json:"duplicates_verified"`
	Executable                bool            `json:"executable"`
	EstimatedReclaimableBytes *int64          `json:"estimated_reclaimable_bytes"`
}

type hashStoredWork struct {
	id                           int
	status                       string
	sequence, readyOrder, offset int64
	blob                         []byte
	code                         string
	checkpoint                   fullHashCheckpoint
	attempt                      *hashStoredAttempt
}

type hashStoredAttempt struct {
	HashAttempt
	nonce            string
	sequence, offset int64
}

func OpenHashWriter(ctx context.Context, base string) (*HashStore, error) {
	return openHashStore(ctx, base, false)
}
func OpenHashReader(ctx context.Context, base string) (*HashStore, error) {
	return openHashStore(ctx, base, true)
}

// OpenHashSelectionWriter may initialize private storage, but never reconciles
// existing attempts. It cannot dispatch source reads or restore continuations.
func OpenHashSelectionWriter(ctx context.Context, base string) (*HashStore, error) {
	return openHashStoreMode(ctx, base, false, true)
}

func openHashStore(ctx context.Context, base string, readOnly bool) (*HashStore, error) {
	return openHashStoreMode(ctx, base, readOnly, false)
}

func openHashStoreMode(ctx context.Context, base string, readOnly, selectionOnly bool) (*HashStore, error) {
	ctx, cancelOpen := context.WithTimeout(ctx, 5*time.Second)
	defer cancelOpen()
	if !filepath.IsAbs(base) || filepath.Clean(base) != base || len(base) > 4096 || strings.ContainsRune(base, 0) {
		return nil, errors.New("hash storage requires a canonical absolute private data directory")
	}
	dir := filepath.Join(base, "hashes")
	for _, p := range []string{base, dir} {
		if !readOnly {
			if err := localfs.EnsurePrivateDir(p); err != nil {
				return nil, err
			}
		}
		if err := localfs.CheckOwnedDir(p); err != nil {
			return nil, err
		}
		canonical, err := filepath.EvalSymlinks(p)
		if err != nil || canonical != p {
			return nil, errors.New("hash storage must not use directory aliases")
		}
	}
	s := &HashStore{base: base, readOnly: readOnly, selectionOnly: selectionOnly, now: time.Now}
	s.life, s.cancel = context.WithCancel(context.Background())
	fail := func(err error) (*HashStore, error) {
		if s.db != nil {
			_ = s.db.Close()
		}
		if s.lock != nil {
			_ = s.lock.Close()
		}
		s.cancel()
		return nil, err
	}
	if !readOnly {
		var err error
		s.lock, err = localfs.AcquireLock(dir)
		if err != nil {
			return fail(err)
		}
	}
	path := filepath.Join(dir, hashStoreFilename)
	if !readOnly {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			err = f.Close()
		}
		if err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := hashPrivateFile(path + suffix); err != nil && !(suffix != "" && errors.Is(err, os.ErrNotExist)) {
			return fail(err)
		}
	}
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(1000)", "foreign_keys(1)", "cache_size(-1024)", "mmap_size(0)"}}
	if readOnly {
		q.Add("_pragma", "query_only(1)")
	} else {
		q.Set("mode", "rw")
		q.Add("_pragma", "synchronous(FULL)")
		q.Add("_pragma", "wal_autocheckpoint(256)")
	}
	u.RawQuery = q.Encode()
	var err error
	s.db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return fail(err)
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	var app, version int
	if err = s.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return fail(err)
	}
	if err = s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fail(err)
	}
	if !readOnly && app == 0 && version == 0 {
		var tables int
		if err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").Scan(&tables); err != nil {
			return fail(err)
		}
		if tables != 0 {
			return fail(ErrHashStoreCorrupt)
		}
		var journal string
		if err = s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil || journal != "wal" {
			if err == nil {
				err = ErrHashStoreCorrupt
			}
			return fail(err)
		}
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return fail(e)
		}
		defer tx.Rollback()
		if _, e = tx.ExecContext(ctx, hashStoreSchema); e != nil {
			return fail(e)
		}
		token, e := hashStoreToken()
		if e != nil {
			return fail(e)
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO hash_meta VALUES(1,?,0)", token); e != nil {
			return fail(e)
		}
		if e = tx.Commit(); e != nil {
			return fail(e)
		}
	} else if app != hashStoreApplicationID || version != 1 {
		return fail(ErrHashStoreCorrupt)
	}
	if !readOnly && !selectionOnly {
		if err = s.recoverHashWork(ctx); err != nil {
			return fail(err)
		}
	}
	if readOnly {
		tx, e := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			return fail(e)
		}
		_, _, e = s.readHashSnapshot(ctx, tx)
		if e == nil {
			e = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if e != nil {
			return fail(e)
		}
	} else if _, _, err = s.readHashSnapshot(ctx, s.db); err != nil {
		return fail(err)
	}
	s.storageIDs = make(map[string]string)
	for _, p := range []string{base, dir, path} {
		var st unix.Stat_t
		if err = unix.Lstat(p, &st); err != nil {
			return fail(err)
		}
		s.storageIDs[p] = objectID(st)
	}
	return s, nil
}

func (s *HashStore) checkHashStorage(target *SavedFileTarget) error {
	dir := filepath.Join(s.base, "hashes")
	path := filepath.Join(dir, hashStoreFilename)
	privateIDs := map[string]bool{}
	for _, p := range []string{s.base, dir, path, path + "-wal", path + "-shm", path + "-journal"} {
		var err error
		if p == s.base || p == dir {
			err = localfs.CheckOwnedDir(p)
		} else {
			err = hashPrivateFile(p)
		}
		if errors.Is(err, os.ErrNotExist) && p != path && p != s.base && p != dir {
			continue
		}
		if err != nil {
			return err
		}
		var st unix.Stat_t
		if err = unix.Lstat(p, &st); err != nil {
			return err
		}
		id := objectID(st)
		privateIDs[id] = true
		if original, ok := s.storageIDs[p]; ok && original != id {
			return errors.New("hash storage path identity changed")
		}
	}
	if target != nil {
		if privateIDs[target.File.Device+":"+target.File.Inode] {
			return errors.New("hash target aliases private storage")
		}
		for _, a := range target.Ancestors {
			if privateIDs[a.Device+":"+a.Inode] {
				return errors.New("hash ancestor aliases private storage")
			}
		}
	}
	return nil
}

func hashPrivateFile(path string) error {
	if err := localfs.CheckPrivateFile(path); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 {
		return errors.New("hash database files must be privately owned with one link")
	}
	return nil
}

func hashStoreToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func hashStoreDigest(s string) bool {
	if len(s) != 64 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (s *HashStore) acquire(ctx context.Context, write bool) error {
	if s == nil {
		return errors.New("hash storage unavailable")
	}
	for !s.mu.TryLock() {
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if s.closed.Load() {
		s.mu.Unlock()
		return errors.New("hash storage closed")
	}
	if s.db == nil {
		s.mu.Unlock()
		return errors.New("hash storage unavailable")
	}
	if write && (s.readOnly || s.poisoned) {
		s.mu.Unlock()
		if s.poisoned {
			return ErrHashRecoveryRequired
		}
		return errors.New("hash storage is read-only")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *HashStore) Close() error {
	if s == nil {
		return nil
	}
	s.closed.Store(true)
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		s.closeErr = s.db.Close()
		s.db = nil
	}
	if s.lock != nil {
		if err := s.lock.Close(); s.closeErr == nil {
			s.closeErr = err
		}
		s.lock = nil
	}
	return s.closeErr
}

// CreateSelection captures current saved bindings without opening source paths.
// One frozen batch is supported; changed evidence can never replace its targets.
func (s *HashStore) CreateSelection(ctx context.Context, source *state.Store, inventoryID string, expected []state.SameSizeFile) (HashSnapshot, error) {
	return s.createHashSelection(ctx, source, inventoryID, expected, nil)
}

func (s *HashStore) createHashSelection(ctx context.Context, source *state.Store, inventoryID string, expected []state.SameSizeFile, locator *HashSourceLocator) (HashSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, true); err != nil {
		return HashSnapshot{}, err
	}
	defer s.mu.Unlock()
	if source == nil {
		return HashSnapshot{}, errors.New("current source inventory is required")
	}
	if err := s.checkHashStorage(nil); err != nil {
		return HashSnapshot{}, err
	}
	targets, err := source.PrepareFileSampleSelection(ctx, inventoryID, expected)
	if err != nil {
		return HashSnapshot{}, err
	}
	for i := range targets {
		targets[i].File.Path = string(targets[i].File.PathBytes)
		if config.Within(string(targets[i].File.PathBytes), s.base) || config.Within(string(targets[i].Root.PathBytes), s.base) {
			return HashSnapshot{}, errors.New("hash targets must stay outside hash storage")
		}
		if err = validateSampleTarget(targets[i]); err != nil {
			return HashSnapshot{}, err
		}
		if err = s.checkHashStorage(&targets[i]); err != nil {
			return HashSnapshot{}, err
		}
	}
	if err = boundedSampleRequest(targets); err != nil {
		return HashSnapshot{}, err
	}
	if locator != nil {
		for _, target := range targets {
			if !bytes.Equal(target.Root.PathBytes, locator.RootPathBytes) {
				return HashSnapshot{}, ErrHashManualRoot
			}
		}
	}
	current, _, err := s.readHashSnapshot(ctx, s.db)
	if err != nil {
		return HashSnapshot{}, err
	}
	record := hashSelectionRecord{Version: 1, StoreID: current.StoreID, Scope: hashStoreScope, InventoryID: inventoryID, Targets: targets}
	if locator != nil {
		record.Version = 2
		record.SourceLocator = &HashSourceLocator{Kind: locator.Kind, RootPathBytes: bytes.Clone(locator.RootPathBytes), InventoryKey: locator.InventoryKey}
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > state.FileSampleEvidenceLimit {
		if err == nil {
			err = ErrHashStoreCorrupt
		}
		return HashSnapshot{}, err
	}
	selectionID := fmt.Sprintf("%x", sha256.Sum256(payload))
	if current.SelectionID != "" {
		if current.SelectionID != selectionID {
			return HashSnapshot{}, ErrHashSelectionConflict
		}
		return current, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return HashSnapshot{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_selection VALUES(1,?,?)", selectionID, payload); err != nil {
		return HashSnapshot{}, err
	}
	for i, target := range targets {
		digest, e := hashTargetDigest(target)
		if e != nil {
			return HashSnapshot{}, e
		}
		checkpoint := fullHashCheckpoint{digest: sha256.New().(hash.Cloner)}
		blob, e := encodeHashCheckpoint(hashCheckpointBinding{StoreID: record.StoreID, SelectionID: selectionID, WorkID: strconv.Itoa(i + 1), Sequence: 0, TargetDigest: digest}, target, checkpoint)
		if e != nil {
			return HashSnapshot{}, e
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO hash_work(id,status,sequence,ready_order,checked_offset,checkpoint) VALUES(?,'pending',0,?,0,?)", i+1, i+1, blob); err != nil {
			return HashSnapshot{}, err
		}
	}
	if _, err = tx.ExecContext(ctx, "UPDATE hash_meta SET next_order=? WHERE id=1", len(targets)); err != nil {
		return HashSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		return HashSnapshot{}, ErrHashRecoveryRequired
	}
	snapshot, _, err := s.readHashSnapshot(ctx, s.db)
	return snapshot, err
}

type hashQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *HashStore) Snapshot(ctx context.Context) (HashSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashSnapshot{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashSnapshot{}, err
	}
	defer tx.Rollback()
	snapshot, _, err := s.readHashSnapshot(ctx, tx)
	if err != nil {
		return HashSnapshot{}, err
	}
	return snapshot, tx.Commit()
}

func (s *HashStore) readHashSnapshot(ctx context.Context, db hashQuery) (HashSnapshot, *hashSelectionRecord, error) {
	out := HashSnapshot{Source: "saved_hash_observations", Contract: FileHashContract, Work: []SavedHashWork{}}
	var next int64
	var metaValid bool
	if err := db.QueryRowContext(ctx, "SELECT substr(CAST(store_id AS BLOB),1,65),CASE WHEN typeof(next_order)='integer' THEN next_order ELSE -1 END,typeof(store_id)='text' AND typeof(next_order)='integer' FROM hash_meta WHERE id=1").Scan(&out.StoreID, &next, &metaValid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrHashStoreCorrupt
		}
		return out, nil, err
	}
	if !metaValid || !hashStoreDigest(out.StoreID) || next < 0 {
		return out, nil, ErrHashStoreCorrupt
	}
	budget, err := readHashBudget(ctx, db)
	if err != nil {
		return out, nil, err
	}
	out.Budget = budget
	var payload []byte
	err = db.QueryRowContext(ctx, "SELECT substr(CAST(selection_id AS BLOB),1,65),substr(CAST(payload AS BLOB),1,1048577) FROM hash_selection WHERE id=1").Scan(&out.SelectionID, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err = db.QueryRowContext(ctx, "SELECT (SELECT count(*) FROM (SELECT id FROM hash_work LIMIT 21))+(SELECT count(*) FROM (SELECT work_id FROM hash_attempt LIMIT 21))").Scan(&count); err != nil || count != 0 {
			if err == nil {
				err = ErrHashStoreCorrupt
			}
			return out, nil, err
		}
		return out, nil, nil
	}
	if err != nil {
		return out, nil, err
	}
	if len(payload) > state.FileSampleEvidenceLimit || out.SelectionID != fmt.Sprintf("%x", sha256.Sum256(payload)) {
		return out, nil, ErrHashStoreCorrupt
	}
	var record hashSelectionRecord
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&record) != nil || d.Decode(new(any)) != io.EOF || !validHashSelectionSource(record) || record.StoreID != out.StoreID || record.Scope != hashStoreScope || !hashStoreDigest(record.InventoryID) || boundedSampleRequest(record.Targets) != nil {
		return out, nil, ErrHashStoreCorrupt
	}
	canonical, _ := json.Marshal(record)
	if !bytes.Equal(canonical, payload) {
		return out, nil, ErrHashStoreCorrupt
	}
	out.InventoryID = record.InventoryID
	rows, err := db.QueryContext(ctx, "SELECT id FROM hash_work ORDER BY id LIMIT 21")
	if err != nil {
		return out, nil, err
	}
	var ids []int
	for rows.Next() {
		var id int
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return out, nil, err
	}
	if len(ids) != len(record.Targets) {
		return out, nil, ErrHashStoreCorrupt
	}
	seen := map[int64]bool{}
	for i, id := range ids {
		record.Targets[i].File.Path = string(record.Targets[i].File.PathBytes)
		target := record.Targets[i]
		if id != i+1 || seen[target.File.ID] || target.InventoryID != record.InventoryID || validateSampleTarget(target) != nil || config.Within(string(target.File.PathBytes), s.base) || config.Within(string(target.Root.PathBytes), s.base) {
			return out, nil, ErrHashStoreCorrupt
		}
		seen[target.File.ID] = true
		w, e := readHashWork(ctx, db, &record, out.SelectionID, id)
		if e != nil || w.readyOrder > next {
			if e == nil {
				e = ErrHashStoreCorrupt
			}
			return out, nil, e
		}
		saved := SavedHashWork{ID: strconv.Itoa(id), FileID: target.File.ID, PathBytes: bytes.Clone(target.File.PathBytes), Status: w.status, Sequence: w.sequence, LogicalBytes: target.File.Size, DurableOffset: w.offset, CheckedAt: w.checkpoint.checkedAt, Code: w.code}
		if w.status == "complete" {
			saved.SHA256 = w.checkpoint.finalSHA
		}
		if w.attempt != nil {
			if budget == nil || w.attempt.ReservationDay > budget.Day || w.attempt.ReservedBytes > budget.TotalReservedBytes {
				return out, nil, ErrHashStoreCorrupt
			}
			a := w.attempt.HashAttempt
			saved.LatestAttempt = &a
		}
		out.Work = append(out.Work, saved)
	}
	var attempts int
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT work_id FROM hash_attempt LIMIT 21)").Scan(&attempts); err != nil || attempts > len(ids) {
		if err == nil {
			err = ErrHashStoreCorrupt
		}
		return out, nil, err
	}
	return out, &record, nil
}

func readHashWork(ctx context.Context, db hashQuery, record *hashSelectionRecord, selectionID string, id int) (hashStoredWork, error) {
	w := hashStoredWork{id: id}
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT substr(CAST(status AS BLOB),1,21),CASE WHEN typeof(sequence)='integer' THEN sequence ELSE -1 END,CASE WHEN typeof(ready_order)='integer' THEN ready_order ELSE -1 END,CASE WHEN typeof(checked_offset)='integer' THEN checked_offset ELSE -1 END,substr(CAST(checkpoint AS BLOB),1,8193),COALESCE(CAST(substr(CAST(error_code AS BLOB),1,65) AS TEXT),''),typeof(status)='text' AND typeof(error_code)='text' AND typeof(sequence)='integer' AND typeof(ready_order)='integer' AND typeof(checked_offset)='integer' AND typeof(checkpoint)='blob' FROM hash_work WHERE id=?`, id).Scan(&w.status, &w.sequence, &w.readyOrder, &w.offset, &w.blob, &w.code, &valid)
	if err != nil {
		return w, err
	}
	if !valid || w.sequence < 0 || w.readyOrder <= 0 || w.offset < 0 || len(w.blob) > hashStoreCheckpointLimit || len(w.code) > 64 || (w.status != "pending" && w.status != "running" && w.status != "complete" && w.status != "invalidated") {
		return w, ErrHashStoreCorrupt
	}
	target := record.Targets[id-1]
	digest, err := hashTargetDigest(target)
	if err != nil {
		return w, ErrHashStoreCorrupt
	}
	w.checkpoint, err = decodeHashCheckpoint(w.blob, hashCheckpointBinding{StoreID: record.StoreID, SelectionID: selectionID, WorkID: strconv.Itoa(id), Sequence: w.sequence, TargetDigest: digest}, target)
	if err != nil || w.checkpoint.offset != w.offset || (w.status == "complete") != w.checkpoint.complete {
		return w, ErrHashStoreCorrupt
	}
	a := &hashStoredAttempt{}
	var requested, read, elapsed sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(nonce AS BLOB),1,65),CASE WHEN typeof(base_sequence)='integer' THEN base_sequence ELSE -1 END,CASE WHEN typeof(from_offset)='integer' THEN from_offset ELSE -1 END,CASE WHEN typeof(grant_bytes)='integer' THEN grant_bytes ELSE -1 END,substr(CAST(reservation_day AS BLOB),1,11),substr(CAST(status AS BLOB),1,21),CASE WHEN typeof(requested_bytes) IN ('integer','null') THEN requested_bytes ELSE NULL END,CASE WHEN typeof(read_bytes) IN ('integer','null') THEN read_bytes ELSE NULL END,CASE WHEN typeof(elapsed_ns) IN ('integer','null') THEN elapsed_ns ELSE NULL END,typeof(nonce)='text' AND typeof(reservation_day)='text' AND typeof(status)='text' AND typeof(base_sequence)='integer' AND typeof(from_offset)='integer' AND typeof(grant_bytes)='integer' AND typeof(requested_bytes) IN ('integer','null') AND typeof(read_bytes) IN ('integer','null') AND typeof(elapsed_ns) IN ('integer','null') FROM hash_attempt WHERE work_id=?`, id).Scan(&a.nonce, &a.sequence, &a.offset, &a.ReservedBytes, &a.ReservationDay, &a.Status, &requested, &read, &elapsed, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		if w.status == "running" || w.sequence > 0 {
			return w, ErrHashStoreCorrupt
		}
		return w, nil
	}
	if err != nil {
		return w, err
	}
	if !valid || !hashStoreDigest(a.nonce) || a.sequence < 0 || a.sequence == math.MaxInt64 || a.offset < 0 || a.offset > target.File.Size || a.ReservedBytes < 0 || a.ReservedBytes > FileHashStepByteLimit || !hashDay(a.ReservationDay) || (a.Status != "reserved" && a.Status != "settled" && a.Status != "interrupted_unknown") {
		return w, ErrHashStoreCorrupt
	}
	if a.Status == "settled" {
		if !requested.Valid || !read.Valid || !elapsed.Valid || requested.Int64 < 0 || read.Int64 < 0 || read.Int64 > requested.Int64 || requested.Int64 > a.ReservedBytes || elapsed.Int64 < 0 || a.sequence+1 != w.sequence {
			return w, ErrHashStoreCorrupt
		}
		a.RequestedBytes = &requested.Int64
		a.ReadBytes = &read.Int64
		a.ElapsedNS = &elapsed.Int64
	} else if requested.Valid || read.Valid || elapsed.Valid || (a.Status == "interrupted_unknown" && (a.sequence == math.MaxInt64 || a.sequence+1 != w.sequence || a.offset != w.offset)) {
		return w, ErrHashStoreCorrupt
	}
	if (w.status == "running") != (a.Status == "reserved") || (a.Status == "reserved" && (a.sequence != w.sequence || a.offset != w.offset)) {
		return w, ErrHashStoreCorrupt
	}
	w.attempt = a
	return w, nil
}

func hashDay(day string) bool {
	t, err := time.Parse(time.DateOnly, day)
	return err == nil && t.Format(time.DateOnly) == day
}

func readHashBudget(ctx context.Context, db hashQuery) (*HashBudget, error) {
	var b HashBudget
	var now int64
	// Integer gates prevent malformed text/blob counters from being materialized.
	columns := []string{"max_now_ns", "reserved_bytes", "requested_bytes", "read_bytes", "unknown_reserved_bytes", "total_reserved_bytes", "total_requested_bytes", "total_read_bytes", "total_unknown_reserved_bytes"}
	fields := []string{"substr(CAST(day AS BLOB),1,11)"}
	guards := []string{"typeof(day)='text'"}
	for _, column := range columns {
		fields = append(fields, "CASE WHEN typeof("+column+")='integer' THEN "+column+" ELSE -1 END")
		guards = append(guards, "typeof("+column+")='integer'")
	}
	var valid bool
	err := db.QueryRowContext(ctx, "SELECT "+strings.Join(fields, ",")+","+strings.Join(guards, " AND ")+" FROM hash_budget WHERE id=1").Scan(&b.Day, &now, &b.ReservedBytes, &b.RequestedBytes, &b.ReadBytes, &b.UnknownReservedBytes, &b.TotalReservedBytes, &b.TotalRequestedBytes, &b.TotalReadBytes, &b.TotalUnknownReservedBytes, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !valid {
		return nil, ErrHashStoreCorrupt
	}
	if !hashDay(b.Day) || now < 0 || b.ReservedBytes < 0 || b.RequestedBytes < 0 || b.ReadBytes < 0 || b.ReadBytes > b.RequestedBytes || b.RequestedBytes > b.ReservedBytes || b.UnknownReservedBytes < 0 || b.UnknownReservedBytes > b.ReservedBytes || b.RequestedBytes > b.ReservedBytes-b.UnknownReservedBytes || b.TotalReservedBytes < b.ReservedBytes || b.TotalRequestedBytes < b.RequestedBytes || b.TotalRequestedBytes > b.TotalReservedBytes || b.TotalReadBytes < b.ReadBytes || b.TotalReadBytes > b.TotalRequestedBytes || b.TotalUnknownReservedBytes < b.UnknownReservedBytes || b.TotalUnknownReservedBytes > b.TotalReservedBytes || b.TotalRequestedBytes > b.TotalReservedBytes-b.TotalUnknownReservedBytes {
		return nil, ErrHashStoreCorrupt
	}
	b.MaxNow = time.Unix(0, now).UTC()
	if b.MaxNow.Format(time.DateOnly) != b.Day {
		return nil, ErrHashStoreCorrupt
	}
	return &b, nil
}
