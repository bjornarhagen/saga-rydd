package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const SampleReservationContract = "generated_sample_store_reservations_v1"
const sampleBatchContract = "sample_batch_v1"
const sampleStoreFilename = "samples.sqlite3"
const sampleStoreApplicationID = 0x52595350

var ErrSampleStoreCorrupt = errors.New("saved sample accounting is invalid or incompatible")
var ErrSampleBatchConflict = errors.New("sample storage already contains a different immutable batch")
var ErrSampleBatchMissing = errors.New("the exact saved sample batch is unavailable")
var ErrSampleBudget = errors.New("the complete sample grant exceeds its frozen reservation limits")
var ErrSampleOneShot = errors.New("this sample batch already has its only attempt")
var ErrSampleInventoryChanged = errors.New("current saved inventory differs from the frozen sample batch")
var ErrSampleClockRollback = errors.New("sample reservation clock precedes its recorded high-water or is invalid")
var ErrSampleRecoveryRequired = errors.New("sample publication is uncertain; reopen and recover saved accounting before dispatch")

// SampleReservationPolicy restricts one generated-fixture library batch. It is
// neither source-read consent nor a shared hash, user-wide or machine-wide cap.
// Creating another store creates another accounting identity and local budget.
type SampleReservationPolicy struct {
	Contract                  string `json:"contract"`
	DailyReservedByteLimit    int64  `json:"daily_reserved_byte_limit"`
	LifetimeReservedByteLimit int64  `json:"lifetime_reserved_byte_limit"`
}

// SampleBudget charges a permanent allowance to its original UTC reservation
// day. Observed counters include only settled known usage; unknown is the full
// allowance of a recovered attempt, not a measurement of actual source I/O.
type SampleBudget struct {
	Day                           string    `json:"reservation_day_utc"`
	MaxNow                        time.Time `json:"clock_high_water"`
	ReservedBytes                 int64     `json:"reserved_bytes"`
	RequestedBytes                int64     `json:"observed_requested_bytes"`
	ReadBytes                     int64     `json:"observed_read_bytes"`
	UnknownReservedBytes          int64     `json:"unknown_reserved_bytes"`
	OutstandingReservedBytes      int64     `json:"outstanding_reserved_bytes"`
	TotalReservedBytes            int64     `json:"total_reserved_bytes"`
	TotalRequestedBytes           int64     `json:"total_observed_requested_bytes"`
	TotalReadBytes                int64     `json:"total_observed_read_bytes"`
	TotalUnknownReservedBytes     int64     `json:"total_unknown_reserved_bytes"`
	TotalOutstandingReservedBytes int64     `json:"total_outstanding_reserved_bytes"`
}

type SampleAttempt struct {
	ID             string    `json:"id"`
	ReservationDay string    `json:"reservation_day_utc"`
	ReservedAt     time.Time `json:"reserved_at"`
	ReservedBytes  int64     `json:"reserved_bytes"`
	Status         string    `json:"status"`
	Code           string    `json:"code,omitempty"`
	RequestedBytes *int64    `json:"observed_requested_bytes,omitempty"`
	ReadBytes      *int64    `json:"observed_read_bytes,omitempty"`
	ElapsedNS      *int64    `json:"elapsed_ns,omitempty"`
}

// SampleSnapshot contains saved accounting and historical selection evidence.
// It never evaluates present read permission or verifies current source data.
// No sample digest, content or report is persisted in this store.
type SampleSnapshot struct {
	Source                    string                  `json:"source"`
	Contract                  string                  `json:"contract"`
	StoreID                   string                  `json:"store_id"`
	BatchID                   string                  `json:"batch_id,omitempty"`
	InventoryID               string                  `json:"inventory_id,omitempty"`
	Status                    string                  `json:"status"`
	Targets                   []SavedFileTarget       `json:"targets"`
	Policy                    SampleReservationPolicy `json:"policy"`
	PlannedBytes              int64                   `json:"planned_bytes"`
	Attempt                   *SampleAttempt          `json:"attempt,omitempty"`
	Budget                    *SampleBudget           `json:"budget,omitempty"`
	ProvenanceVerified        bool                    `json:"provenance_verified"`
	ContentVerified           bool                    `json:"content_verified"`
	CurrentStateVerified      bool                    `json:"current_state_verified"`
	DuplicatesVerified        bool                    `json:"duplicates_verified"`
	Executable                bool                    `json:"executable"`
	EstimatedReclaimableBytes *int64                  `json:"estimated_reclaimable_bytes"`
}

type sampleBatchRecord struct {
	Version     int                     `json:"version"`
	Contract    string                  `json:"contract"`
	StoreID     string                  `json:"store_id"`
	InventoryID string                  `json:"inventory_id"`
	Targets     []SavedFileTarget       `json:"ordered_targets"`
	Policy      SampleReservationPolicy `json:"policy"`
	Planned     []int64                 `json:"per_target_planned_bytes"`
	Total       int64                   `json:"planned_batch_bytes"`
}

// SampleStore is a one-shot generated-fixture foundation. It has no sampling CLI,
// background dispatcher, original-data trial or production read-consent gate.
// A frozen batch or hash approval never authorizes sampling or cleanup.
type SampleStore struct {
	db         *sql.DB
	base       string
	readOnly   bool
	lock       *localfs.Lock
	mu         sync.Mutex
	closed     atomic.Bool
	poisoned   bool
	now        func() time.Time
	life       context.Context
	cancel     context.CancelFunc
	closeErr   error
	storageIDs map[string]string
}

// OpenSampleWriter may initialize only its own schema. It never recovers an
// attempt, samples the wall clock, accesses inventory or opens source files.
func OpenSampleWriter(ctx context.Context, base string) (*SampleStore, error) {
	return openSampleStore(ctx, base, false)
}

// OpenSampleReader opens existing saved records only. It does not initialize or
// migrate logical schema, recover, sample the clock, reserve or dispatch reads.
func OpenSampleReader(ctx context.Context, base string) (*SampleStore, error) {
	return openSampleStore(ctx, base, true)
}

func openSampleStore(ctx context.Context, base string, readOnly bool) (*SampleStore, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !filepath.IsAbs(base) || filepath.Clean(base) != base || len(base) > 4096 || strings.ContainsRune(base, 0) {
		return nil, errors.New("sample storage requires a canonical absolute private directory")
	}
	dir := filepath.Join(base, "samples")
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
			return nil, errors.New("sample storage must not use directory aliases")
		}
	}
	s := &SampleStore{base: base, readOnly: readOnly, now: time.Now, storageIDs: map[string]string{}}
	s.life, s.cancel = context.WithCancel(context.Background())
	fail := func(err error) (*SampleStore, error) {
		if s.db != nil {
			err = errors.Join(err, s.db.Close())
		}
		if s.lock != nil {
			err = errors.Join(err, s.lock.Close())
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
	path := filepath.Join(dir, sampleStoreFilename)
	if !readOnly {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			err = f.Close()
		}
		if err != nil && !errors.Is(err, os.ErrExist) {
			return fail(err)
		}
	}
	if err := s.checkSampleStorage(nil); err != nil {
		return fail(err)
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
	if app == 0 && version == 0 && !readOnly {
		var count int
		if err = s.db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT name FROM sqlite_schema LIMIT 2)").Scan(&count); err != nil || count != 0 {
			return fail(errors.Join(ErrSampleStoreCorrupt, err))
		}
		var journal string
		if err = s.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil || journal != "wal" {
			return fail(errors.Join(ErrSampleStoreCorrupt, err))
		}
		if e := initializeSampleSchema(ctx, s.db); e != nil {
			return fail(e)
		}
	} else if app != sampleStoreApplicationID || version != 1 {
		return fail(ErrSampleStoreCorrupt)
	}
	var journal string
	if err = s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		return fail(errors.Join(ErrSampleStoreCorrupt, err))
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return fail(err)
	}
	if _, err = s.Snapshot(ctx); err != nil {
		return fail(err)
	}
	return s, nil
}

func initializeSampleSchema(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, sampleStoreSchema); err != nil {
		return err
	}
	token, err := hashStoreToken()
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO sample_meta VALUES(1,?)", token); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SampleStore) checkSampleStorage(target *SavedFileTarget) error {
	dir := filepath.Join(s.base, "samples")
	db := filepath.Join(dir, sampleStoreFilename)
	ids := map[string]bool{}
	for _, p := range []string{s.base, dir, db, db + "-wal", db + "-shm", db + "-journal", filepath.Join(dir, "writer.lock")} {
		isDir := p == s.base || p == dir
		var err error
		if isDir {
			err = localfs.CheckOwnedDir(p)
		} else {
			err = hashPrivateFile(p)
		}
		if errors.Is(err, os.ErrNotExist) && p != db && !isDir && p != filepath.Join(dir, "writer.lock") {
			continue
		}
		if err != nil {
			return err
		}
		var st unix.Stat_t
		if err = unix.Lstat(p, &st); err != nil {
			return err
		}
		if !isDir {
			cap := int64(32 * 1024 * 1024)
			switch p {
			case db:
				cap = 16 * 1024 * 1024
			case db + "-shm":
				cap = 1024 * 1024
			case filepath.Join(dir, "writer.lock"):
				cap = 64
			}
			if st.Size < 0 || st.Size > cap {
				return errors.New("sample storage leaf exceeds its admission limit")
			}
		}
		id := objectID(st)
		ids[id] = true
		if old, ok := s.storageIDs[p]; ok && old != id {
			return errors.New("sample storage path identity changed")
		}
		s.storageIDs[p] = id
	}
	if target != nil {
		if config.Within(string(target.File.PathBytes), s.base) || config.Within(string(target.Root.PathBytes), s.base) || ids[target.File.Device+":"+target.File.Inode] {
			return errors.New("sample target aliases private storage")
		}
		for _, a := range target.Ancestors {
			if ids[a.Device+":"+a.Inode] {
				return errors.New("sample ancestor aliases private storage")
			}
		}
	}
	return nil
}

func (s *SampleStore) acquireSample(ctx context.Context, write bool) error {
	if s == nil {
		return errors.New("sample storage unavailable")
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
	if s.closed.Load() || s.db == nil {
		s.mu.Unlock()
		return errors.New("sample storage closed")
	}
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if write && (s.readOnly || s.lock == nil) {
		s.mu.Unlock()
		return errors.New("sample mutation requires its exclusive writer")
	}
	if write && s.poisoned {
		s.mu.Unlock()
		return ErrSampleRecoveryRequired
	}
	return nil
}

func (s *SampleStore) Close() error {
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
		s.closeErr = errors.Join(s.closeErr, s.db.Close())
		s.db = nil
	}
	if s.lock != nil {
		s.closeErr = errors.Join(s.closeErr, s.lock.Close())
		s.lock = nil
	}
	return s.closeErr
}

func validSamplePolicy(p SampleReservationPolicy) bool {
	return p.Contract == SampleReservationContract && p.DailyReservedByteLimit >= 1 && p.DailyReservedByteLimit <= 1<<50 && p.LifetimeReservedByteLimit >= 1 && p.LifetimeReservedByteLimit <= 1<<50
}

func sampleTargetDigest(t SavedFileTarget) (string, error) {
	if boundedSampleRequest([]SavedFileTarget{t}) != nil || validateSampleTarget(t) != nil || t.File.Size <= 0 || t.File.ObservedAt.UnixNano() <= 0 || !time.Unix(0, t.File.ObservedAt.UnixNano()).Equal(t.File.ObservedAt) || t.File.ModifiedAt.UnixNano() <= 0 || !time.Unix(0, t.File.ModifiedAt.UnixNano()).Equal(t.File.ModifiedAt) {
		return "", ErrSampleStoreCorrupt
	}
	for _, a := range t.Ancestors {
		if a.Size < 0 || a.Allocated < 0 || a.MtimeNS <= 0 {
			return "", ErrSampleStoreCorrupt
		}
	}
	body, err := json.Marshal(cloneHashTarget(t))
	if err != nil || len(body) > fileSampleEvidenceLimit {
		return "", ErrSampleStoreCorrupt
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func validateSampleRecord(r *sampleBatchRecord, storeID string) error {
	if r.Version != 1 || r.Contract != sampleBatchContract || r.StoreID != storeID || !sampleHex(storeID) || !sampleHex(r.InventoryID) || !validSamplePolicy(r.Policy) || boundedSampleRequest(r.Targets) != nil || len(r.Planned) != len(r.Targets) {
		return ErrSampleStoreCorrupt
	}
	var total int64
	for i, t := range r.Targets {
		if t.InventoryID != r.InventoryID {
			return ErrSampleStoreCorrupt
		}
		if _, err := sampleTargetDigest(t); err != nil {
			return err
		}
		if conflictingSampleIdentity(t, r.Targets) != nil {
			return ErrSampleStoreCorrupt
		}
		var grant int64
		for _, window := range fileSampleRanges(t.File.Size) {
			if window.Length < 0 || grant > math.MaxInt64-window.Length {
				return ErrSampleStoreCorrupt
			}
			grant += window.Length
		}
		if grant < 1 || grant > FileSampleFileByteLimit || r.Planned[i] != grant || total > FileSampleCallByteLimit-grant {
			return ErrSampleStoreCorrupt
		}
		total += grant
	}
	if total != r.Total {
		return ErrSampleStoreCorrupt
	}
	return nil
}

// CreateBatch captures saved evidence only. One immutable ordered batch and
// policy are supported; a changed selection cannot replace or renew them.
func (s *SampleStore) CreateBatch(ctx context.Context, source *state.Store, inventoryID string, expected []state.SameSizeFile, policy SampleReservationPolicy) (SampleSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !validSamplePolicy(policy) || source == nil {
		return SampleSnapshot{}, errors.New("sample batch requires exact saved inventory and an explicit frozen sample policy")
	}
	if err := s.acquireSample(ctx, true); err != nil {
		return SampleSnapshot{}, err
	}
	defer s.mu.Unlock()
	if err := s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	targets, err := source.PrepareFileSampleSelection(ctx, inventoryID, expected)
	if err != nil {
		return SampleSnapshot{}, err
	}
	current, _, err := s.readSampleSnapshot(ctx, s.db)
	if err != nil {
		return SampleSnapshot{}, err
	}
	r := sampleBatchRecord{Version: 1, Contract: sampleBatchContract, StoreID: current.StoreID, InventoryID: inventoryID, Policy: policy}
	for _, t := range targets {
		t = cloneHashTarget(t)
		if err = s.checkSampleStorage(&t); err != nil {
			return SampleSnapshot{}, err
		}
		var grant int64
		for _, w := range fileSampleRanges(t.File.Size) {
			grant += w.Length
		}
		r.Targets = append(r.Targets, t)
		r.Planned = append(r.Planned, grant)
		r.Total += grant
	}
	if err = validateSampleRecord(&r, current.StoreID); err != nil {
		return SampleSnapshot{}, err
	}
	payload, err := json.Marshal(r)
	if err != nil || len(payload) > fileSampleEvidenceLimit {
		return SampleSnapshot{}, errors.Join(ErrSampleStoreCorrupt, err)
	}
	hash := sha256.Sum256(payload)
	batchID := hex.EncodeToString(hash[:])
	if current.BatchID != "" {
		if current.BatchID != batchID {
			return SampleSnapshot{}, ErrSampleBatchConflict
		}
		return current, nil
	}
	now := s.now().UTC()
	if !validHashReadClock(now) {
		return SampleSnapshot{}, ErrSampleClockRollback
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SampleSnapshot{}, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO sample_batch VALUES(1,?,?)", batchID, payload); err != nil {
		return SampleSnapshot{}, err
	}
	if err = writeSampleBudget(ctx, tx, sampleBudgetProjection(nil, now)); err != nil {
		return SampleSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return SampleSnapshot{}, err
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		return SampleSnapshot{}, errors.Join(ErrSampleRecoveryRequired, err)
	}
	return s.readSampleSnapshotPublic(ctx)
}

func (s *SampleStore) Snapshot(ctx context.Context) (SampleSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquireSample(ctx, false); err != nil {
		return SampleSnapshot{}, err
	}
	defer s.mu.Unlock()
	return s.readSampleSnapshotPublic(ctx)
}

func (s *SampleStore) readSampleSnapshotPublic(ctx context.Context) (SampleSnapshot, error) {
	if err := s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SampleSnapshot{}, err
	}
	defer tx.Rollback()
	out, _, err := s.readSampleSnapshot(ctx, tx)
	if err != nil {
		return SampleSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return SampleSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return SampleSnapshot{}, err
	}
	if err = s.checkSampleStorage(nil); err != nil {
		return SampleSnapshot{}, err
	}
	return out, nil
}

func (s *SampleStore) readSampleSnapshot(ctx context.Context, db hashQuery) (SampleSnapshot, *sampleBatchRecord, error) {
	out := SampleSnapshot{Source: "saved_sample_accounting", Contract: SampleReservationContract, Status: "empty", Targets: []SavedFileTarget{}}
	var app, version int
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return out, nil, err
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return out, nil, err
	}
	if app != sampleStoreApplicationID || version != 1 {
		return out, nil, ErrSampleStoreCorrupt
	}
	counts := make([]int, 4)
	for i, table := range []string{"sample_meta", "sample_batch", "sample_budget", "sample_attempt"} {
		if err := db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT id FROM "+table+" LIMIT 2)").Scan(&counts[i]); err != nil {
			return out, nil, err
		}
		if counts[i] > 1 {
			return out, nil, ErrSampleStoreCorrupt
		}
	}
	var valid bool
	if counts[0] != 1 {
		return out, nil, ErrSampleStoreCorrupt
	}
	if err := db.QueryRowContext(ctx, "SELECT substr(CAST(store_id AS BLOB),1,65),typeof(id)='integer' AND id=1 AND typeof(store_id)='text' FROM sample_meta").Scan(&out.StoreID, &valid); err != nil {
		return out, nil, err
	}
	if !valid || !sampleHex(out.StoreID) {
		return out, nil, ErrSampleStoreCorrupt
	}
	if counts[1] == 0 {
		if counts[2] != 0 || counts[3] != 0 {
			return out, nil, ErrSampleStoreCorrupt
		}
		return out, nil, nil
	}
	if counts[2] != 1 {
		return out, nil, ErrSampleStoreCorrupt
	}
	var payload []byte
	if err := db.QueryRowContext(ctx, "SELECT substr(CAST(batch_id AS BLOB),1,65),substr(CAST(payload AS BLOB),1,1048577),typeof(id)='integer' AND id=1 AND typeof(batch_id)='text' AND typeof(payload)='blob' FROM sample_batch").Scan(&out.BatchID, &payload, &valid); err != nil {
		return out, nil, err
	}
	digest := sha256.Sum256(payload)
	if !valid || len(payload) > fileSampleEvidenceLimit || out.BatchID != hex.EncodeToString(digest[:]) {
		return out, nil, ErrSampleStoreCorrupt
	}
	var record sampleBatchRecord
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if d.Decode(&record) != nil || d.Decode(new(any)) != io.EOF {
		return out, nil, ErrSampleStoreCorrupt
	}
	canonical, e := json.Marshal(record)
	if e != nil || !bytes.Equal(canonical, payload) || validateSampleRecord(&record, out.StoreID) != nil {
		return out, nil, ErrSampleStoreCorrupt
	}
	out.InventoryID = record.InventoryID
	out.Policy = record.Policy
	out.PlannedBytes = record.Total
	out.Status = "ready"
	for i, t := range record.Targets {
		if e = s.checkSampleStorage(&t); e != nil {
			return out, nil, e
		}
		record.Targets[i] = cloneHashTarget(t)
		out.Targets = append(out.Targets, cloneHashTarget(t))
	}
	if counts[3] == 1 {
		out.Attempt, e = readSampleAttempt(ctx, db, out.StoreID, out.BatchID, record.Total)
		if e != nil {
			return out, nil, e
		}
		out.Status = out.Attempt.Status
	}
	budget, e := readSampleBudget(ctx, db)
	if e != nil {
		return out, nil, e
	}
	expected := sampleBudgetProjection(out.Attempt, budget.MaxNow)
	if !equalSampleBudget(budget, expected) || budget.ReservedBytes > record.Policy.DailyReservedByteLimit || budget.TotalReservedBytes > record.Policy.LifetimeReservedByteLimit || out.Attempt != nil && out.Attempt.ReservedAt.After(budget.MaxNow) {
		return out, nil, ErrSampleStoreCorrupt
	}
	out.Budget = &budget
	return out, &record, nil
}
