package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"hash"
	"math"
	"strconv"
	"strings"
	"time"
)

var ErrHashFreshRunBinding = errors.New("fresh dispatch requires the exact initialized job, its fresh approval and an exact-job run writer")
var ErrHashFreshProgressCorrupt = errors.New("saved fresh progress, accounting or checkpoint differs from its exact job binding")
var ErrHashFreshReadWindow = errors.New("fresh read approval must remain valid beyond the remaining operation deadline; no source bytes were read")

// SavedFreshHashWork is genuine independent saved progress. Ordinals and SHA
// state belong to this fresh job; HistoricalWorkID and Role identify scope.
type SavedFreshHashWork struct {
	Ordinal          int          `json:"ordinal"`
	HistoricalWorkID string       `json:"historical_work_id"`
	Role             string       `json:"role"`
	FileID           int64        `json:"file_id"`
	PathBytes        []byte       `json:"path_bytes"`
	Status           string       `json:"status"`
	Sequence         int64        `json:"sequence"`
	LogicalBytes     int64        `json:"logical_bytes"`
	DurableOffset    int64        `json:"durable_offset"`
	CheckedAt        time.Time    `json:"checked_at"`
	SHA256           string       `json:"sha256,omitempty"`
	Code             string       `json:"code,omitempty"`
	LatestAttempt    *HashAttempt `json:"latest_attempt,omitempty"`
}

type HashFreshRunResult struct {
	StoreReadBudget                  *HashStoreReadBudget       `json:"store_read_budget,omitempty"`
	ConfiguredDailyReservedByteLimit *int64                     `json:"configured_daily_reserved_byte_limit,omitempty"`
	ReadPacing                       *HashReadPacingObservation `json:"read_pacing,omitempty"`
	JobID                            string                     `json:"job_id"`
	JobKey                           string                     `json:"job_key"`
	RequestID                        string                     `json:"request_id"`
	ChoiceID                         string                     `json:"choice_id"`
	ApprovalID                       string                     `json:"approval_id"`
	Status                           string                     `json:"status"`
	Code                             string                     `json:"code,omitempty"`
	Ordinal                          int                        `json:"ordinal,omitempty"`
	HistoricalWorkID                 string                     `json:"historical_work_id,omitempty"`
	Role                             string                     `json:"role,omitempty"`
	Progress                         FullHashProgress           `json:"progress"`
	DurableOffset                    int64                      `json:"durable_offset"`
	ReservedBytes                    int64                      `json:"reserved_bytes"`
	Usage                            FileReadUsage              `json:"usage"`
	FreshBudget                      *HashBudget                `json:"fresh_budget,omitempty"`
}

type hashFreshRunOpenHooks struct {
	beforeInitCommit     func()
	afterInitCommit      func()
	beforeRecoveryCommit func()
	afterRecoveryCommit  func()
	commit               func(*sql.Tx) error
}

type hashFreshRunHooks struct {
	pacing                    *hashReadPacer
	execution                 *HashReadExecutionLimits
	storeBudgetHooks          *hashStoreReadBudgetHooks
	beforeReserveCommit       func()
	afterReserve              func()
	beforeSettleCommit        func()
	afterSettleCommit         func()
	beforeInventoryCheck      func()
	afterInventoryCheck       func()
	beforeFinalInventoryCheck func()
	reserveCommit             func(*sql.Tx) error
	settleCommit              func(*sql.Tx) error
	file                      fileHashHooks
}

func hashFreshProgressFailure(ctx context.Context, err error) error {
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrHashFreshProgressCorrupt
}

func hashFreshProgressCount(ctx context.Context, db hashQuery) error {
	var runs, progress, attempts, budgets, orphan int
	err := db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM (SELECT job_id FROM hash_fresh_run_state LIMIT 129)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_progress LIMIT 2561)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_attempt LIMIT 2561)),(SELECT count(*) FROM (SELECT job_id FROM hash_fresh_budget LIMIT 129))`).Scan(&runs, &progress, &attempts, &budgets)
	if err != nil || runs > HashFreshJobLimit || budgets != runs || progress > runs*FileSampleTargetLimit || progress < runs*2 || attempts > progress {
		return hashFreshProgressFailure(ctx, err)
	}
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM (
 SELECT r.job_id FROM hash_fresh_run_state r LEFT JOIN hash_fresh_read_approval a ON r.job_id=a.job_id WHERE a.job_id IS NULL
 UNION ALL SELECT b.job_id FROM hash_fresh_budget b LEFT JOIN hash_fresh_run_state r ON b.job_id=r.job_id WHERE r.job_id IS NULL
 UNION ALL SELECT p.job_id FROM hash_fresh_progress p LEFT JOIN hash_fresh_run_state r ON p.job_id=r.job_id LEFT JOIN hash_fresh_work w ON p.job_id=w.job_id AND p.ordinal=w.ordinal WHERE r.job_id IS NULL OR w.job_id IS NULL
 UNION ALL SELECT a.job_id FROM hash_fresh_attempt a LEFT JOIN hash_fresh_progress p ON a.job_id=p.job_id AND a.ordinal=p.ordinal WHERE p.job_id IS NULL
 LIMIT 1)`).Scan(&orphan)
	if err != nil || orphan != 0 {
		return hashFreshProgressFailure(ctx, err)
	}
	// Aggregate bounds alone can hide an initialized job with missing rows
	// behind another job's larger queue. Every initialized job has all of its
	// immutable seed rows and exactly one budget, or the saved state refuses.
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM (
 SELECT r.job_id FROM hash_fresh_run_state r
 WHERE (SELECT count(*) FROM hash_fresh_work w WHERE w.job_id=r.job_id) NOT BETWEEN 2 AND 20
 OR (SELECT count(*) FROM hash_fresh_progress p WHERE p.job_id=r.job_id)!=(SELECT count(*) FROM hash_fresh_work w WHERE w.job_id=r.job_id)
 OR NOT EXISTS(SELECT 1 FROM hash_fresh_budget b WHERE b.job_id=r.job_id)
 LIMIT 1)`).Scan(&orphan)
	if err != nil || orphan != 0 {
		return hashFreshProgressFailure(ctx, err)
	}
	return nil
}

func readFreshHashBudget(ctx context.Context, db hashQuery, jobID string) (*HashBudget, error) {
	var budget HashBudget
	var high int64
	columns := []string{"max_now_ns", "reserved_bytes", "requested_bytes", "read_bytes", "unknown_reserved_bytes", "total_reserved_bytes", "total_requested_bytes", "total_read_bytes", "total_unknown_reserved_bytes"}
	fields, guards := []string{"substr(CAST(day AS BLOB),1,11)"}, []string{"typeof(job_id)='text'", "typeof(day)='text'"}
	for _, column := range columns {
		fields = append(fields, "CASE WHEN typeof("+column+")='integer' THEN "+column+" ELSE -1 END")
		guards = append(guards, "typeof("+column+")='integer'")
	}
	var valid bool
	err := db.QueryRowContext(ctx, "SELECT "+strings.Join(fields, ",")+","+strings.Join(guards, " AND ")+" FROM hash_fresh_budget WHERE job_id=?", jobID).Scan(&budget.Day, &high, &budget.ReservedBytes, &budget.RequestedBytes, &budget.ReadBytes, &budget.UnknownReservedBytes, &budget.TotalReservedBytes, &budget.TotalRequestedBytes, &budget.TotalReadBytes, &budget.TotalUnknownReservedBytes, &valid)
	if err != nil {
		return nil, hashFreshProgressFailure(ctx, err)
	}
	budget.MaxNow = time.Unix(0, high).UTC()
	if !valid || !validHashKeeperChoiceBudget(&budget) {
		return nil, ErrHashFreshProgressCorrupt
	}
	return &budget, nil
}

func writeFreshHashBudget(ctx context.Context, tx *sql.Tx, jobID string, b HashBudget) error {
	if !validHashKeeperChoiceBudget(&b) {
		return ErrHashFreshProgressCorrupt
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO hash_fresh_budget VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET day=excluded.day,max_now_ns=excluded.max_now_ns,reserved_bytes=excluded.reserved_bytes,requested_bytes=excluded.requested_bytes,read_bytes=excluded.read_bytes,unknown_reserved_bytes=excluded.unknown_reserved_bytes,total_reserved_bytes=excluded.total_reserved_bytes,total_requested_bytes=excluded.total_requested_bytes,total_read_bytes=excluded.total_read_bytes,total_unknown_reserved_bytes=excluded.total_unknown_reserved_bytes`, jobID, b.Day, b.MaxNow.UnixNano(), b.ReservedBytes, b.RequestedBytes, b.ReadBytes, b.UnknownReservedBytes, b.TotalReservedBytes, b.TotalRequestedBytes, b.TotalReadBytes, b.TotalUnknownReservedBytes)
	return err
}

func readFreshHashWork(ctx context.Context, db hashQuery, job SavedFreshJob, ordinal int) (hashStoredWork, error) {
	w := hashStoredWork{id: ordinal}
	if ordinal < 1 || ordinal > len(job.Record.Request.Targets) {
		return w, ErrHashFreshProgressCorrupt
	}
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT substr(CAST(status AS BLOB),1,12),CASE WHEN typeof(sequence)='integer' THEN sequence ELSE -1 END,CASE WHEN typeof(ready_order)='integer' THEN ready_order ELSE -1 END,CASE WHEN typeof(checked_offset)='integer' THEN checked_offset ELSE -1 END,substr(CAST(checkpoint AS BLOB),1,16385),COALESCE(CAST(substr(CAST(error_code AS BLOB),1,65) AS TEXT),''),typeof(job_id)='text' AND typeof(ordinal)='integer' AND typeof(status)='text' AND typeof(error_code)='text' AND typeof(sequence)='integer' AND typeof(ready_order)='integer' AND typeof(checked_offset)='integer' AND typeof(checkpoint)='blob' FROM hash_fresh_progress WHERE job_id=? AND ordinal=?`, job.ID, ordinal).Scan(&w.status, &w.sequence, &w.readyOrder, &w.offset, &w.blob, &w.code, &valid)
	if err != nil {
		return w, hashFreshProgressFailure(ctx, err)
	}
	if !valid || w.sequence < 0 || w.readyOrder <= 0 || w.offset < 0 || len(w.blob) > HashFreshCheckpointMaxRecordBytes || len(w.code) > 64 || strings.ContainsRune(w.code, 0) || w.status != "pending" && w.status != "running" && w.status != "complete" && w.status != "invalidated" || (w.status == "invalidated") != (w.code != "") {
		return w, ErrHashFreshProgressCorrupt
	}
	target := job.Record.Request.Targets[ordinal-1].Target
	w.checkpoint, err = decodeFreshHashCheckpoint(w.blob, job, ordinal, w.sequence)
	if err != nil || w.checkpoint.offset != w.offset || (w.status == "complete") != w.checkpoint.complete {
		return w, ErrHashFreshProgressCorrupt
	}
	a := new(hashStoredAttempt)
	var requested, read, elapsed sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT substr(CAST(nonce AS BLOB),1,65),CASE WHEN typeof(base_sequence)='integer' THEN base_sequence ELSE -1 END,CASE WHEN typeof(from_offset)='integer' THEN from_offset ELSE -1 END,CASE WHEN typeof(grant_bytes)='integer' THEN grant_bytes ELSE -1 END,substr(CAST(reservation_day AS BLOB),1,11),substr(CAST(status AS BLOB),1,20),CASE WHEN typeof(requested_bytes) IN ('integer','null') THEN requested_bytes ELSE NULL END,CASE WHEN typeof(read_bytes) IN ('integer','null') THEN read_bytes ELSE NULL END,CASE WHEN typeof(elapsed_ns) IN ('integer','null') THEN elapsed_ns ELSE NULL END,typeof(job_id)='text' AND typeof(ordinal)='integer' AND typeof(nonce)='text' AND typeof(reservation_day)='text' AND typeof(status)='text' AND typeof(base_sequence)='integer' AND typeof(from_offset)='integer' AND typeof(grant_bytes)='integer' AND typeof(requested_bytes) IN ('integer','null') AND typeof(read_bytes) IN ('integer','null') AND typeof(elapsed_ns) IN ('integer','null') FROM hash_fresh_attempt WHERE job_id=? AND ordinal=?`, job.ID, ordinal).Scan(&a.nonce, &a.sequence, &a.offset, &a.ReservedBytes, &a.ReservationDay, &a.Status, &requested, &read, &elapsed, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		if w.status != "pending" || w.sequence != 0 || w.offset != 0 || w.checkpoint.baseline || w.readyOrder != int64(ordinal) || w.code != "" {
			return w, ErrHashFreshProgressCorrupt
		}
		return w, ctx.Err()
	}
	if err != nil {
		return w, hashFreshProgressFailure(ctx, err)
	}
	if !valid || !hashStoreDigest(a.nonce) || a.sequence < 0 || a.sequence == math.MaxInt64 || a.offset < 0 || a.offset > target.File.Size || a.ReservedBytes < 0 || a.ReservedBytes > min(FileHashStepByteLimit, target.File.Size-a.offset) || !hashDay(a.ReservationDay) || a.Status != "reserved" && a.Status != "settled" && a.Status != "interrupted_unknown" {
		return w, ErrHashFreshProgressCorrupt
	}
	if a.Status == "settled" {
		if !requested.Valid || !read.Valid || !elapsed.Valid || requested.Int64 < 0 || read.Int64 < 0 || read.Int64 > requested.Int64 || requested.Int64 > a.ReservedBytes || elapsed.Int64 < 0 || a.sequence+1 != w.sequence || w.offset < a.offset || w.offset-a.offset > read.Int64 {
			return w, ErrHashFreshProgressCorrupt
		}
		a.RequestedBytes, a.ReadBytes, a.ElapsedNS = &requested.Int64, &read.Int64, &elapsed.Int64
	} else if requested.Valid || read.Valid || elapsed.Valid || a.Status == "interrupted_unknown" && (a.sequence+1 != w.sequence || a.offset != w.offset) {
		return w, ErrHashFreshProgressCorrupt
	}
	if (w.status == "running") != (a.Status == "reserved") || a.Status == "reserved" && (a.sequence != w.sequence || a.offset != w.offset) {
		return w, ErrHashFreshProgressCorrupt
	}
	if a.Status == "interrupted_unknown" && w.status != "pending" {
		return w, ErrHashFreshProgressCorrupt
	}
	w.attempt = a
	return w, ctx.Err()
}

func (s *HashStore) readFreshProgress(ctx context.Context, db hashQuery, job SavedFreshJob) ([]SavedFreshHashWork, *HashBudget, error) {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, nil, hashFreshProgressFailure(ctx, err)
	}
	if version == 4 || version == 5 {
		return nil, nil, nil
	}
	if version != 6 && version != 7 {
		return nil, nil, ErrHashFreshProgressCorrupt
	}
	if err := hashFreshProgressCount(ctx, db); err != nil {
		return nil, nil, err
	}
	var cursor int64
	var valid bool
	err := db.QueryRowContext(ctx, `SELECT CASE WHEN typeof(next_order)='integer' THEN next_order ELSE -1 END,typeof(job_id)='text' AND typeof(next_order)='integer' FROM hash_fresh_run_state WHERE job_id=?`, job.ID).Scan(&cursor, &valid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ctx.Err()
	}
	if err != nil || !valid || cursor < int64(len(job.Work)) {
		return nil, nil, hashFreshProgressFailure(ctx, err)
	}
	var count int
	err = db.QueryRowContext(ctx, "SELECT count(*) FROM (SELECT ordinal FROM hash_fresh_progress WHERE job_id=? LIMIT 21)", job.ID).Scan(&count)
	if err != nil || count != len(job.Work) {
		return nil, nil, hashFreshProgressFailure(ctx, err)
	}
	budget, err := readFreshHashBudget(ctx, db, job.ID)
	if err != nil {
		return nil, nil, err
	}
	progress := make([]SavedFreshHashWork, 0, count)
	orders := make(map[int64]bool, count)
	var reserved, requested, read, unknown, unsettled int64
	var dayReserved, dayRequested, dayRead, dayUnknown, dayUnsettled int64
	var durableTotal int64
	for i, target := range job.Record.Request.Targets {
		w, err := readFreshHashWork(ctx, db, job, i+1)
		if err != nil || w.readyOrder > cursor || orders[w.readyOrder] {
			return nil, nil, hashFreshProgressFailure(ctx, err)
		}
		orders[w.readyOrder] = true
		if w.offset > budget.TotalReadBytes-durableTotal {
			return nil, nil, ErrHashFreshProgressCorrupt
		}
		durableTotal += w.offset
		saved := SavedFreshHashWork{Ordinal: i + 1, HistoricalWorkID: target.Observation.WorkID, Role: target.Role, FileID: target.Target.File.ID, PathBytes: bytes.Clone(target.Target.File.PathBytes), Status: w.status, Sequence: w.sequence, LogicalBytes: target.Target.File.Size, DurableOffset: w.offset, CheckedAt: w.checkpoint.checkedAt, Code: w.code}
		if w.status == "complete" {
			saved.SHA256 = w.checkpoint.finalSHA
		}
		if w.attempt != nil {
			a := w.attempt.HashAttempt
			if w.readyOrder <= int64(count) || a.ReservationDay > budget.Day || a.ReservedBytes > budget.TotalReservedBytes || a.Status == "settled" && (*a.RequestedBytes > budget.TotalRequestedBytes || *a.ReadBytes > budget.TotalReadBytes) || a.Status == "interrupted_unknown" && a.ReservedBytes > budget.TotalUnknownReservedBytes {
				return nil, nil, ErrHashFreshProgressCorrupt
			}
			reserved += a.ReservedBytes
			if a.ReservationDay == budget.Day {
				dayReserved += a.ReservedBytes
			}
			switch a.Status {
			case "settled":
				requested += *a.RequestedBytes
				read += *a.ReadBytes
				if a.ReservationDay == budget.Day {
					dayRequested += *a.RequestedBytes
					dayRead += *a.ReadBytes
				}
			case "interrupted_unknown":
				unknown += a.ReservedBytes
				if a.ReservationDay == budget.Day {
					dayUnknown += a.ReservedBytes
				}
			case "reserved":
				unsettled += a.ReservedBytes
				if a.ReservationDay == budget.Day {
					dayUnsettled += a.ReservedBytes
				}
			}
			saved.LatestAttempt = &a
		}
		progress = append(progress, saved)
	}
	if reserved > budget.TotalReservedBytes || requested > budget.TotalRequestedBytes || read > budget.TotalReadBytes || unknown > budget.TotalUnknownReservedBytes || unsettled > budget.TotalReservedBytes-budget.TotalRequestedBytes-budget.TotalUnknownReservedBytes || dayReserved > budget.ReservedBytes || dayRequested > budget.RequestedBytes || dayRead > budget.ReadBytes || dayUnknown > budget.UnknownReservedBytes || dayUnsettled > budget.ReservedBytes-budget.RequestedBytes-budget.UnknownReservedBytes {
		return nil, nil, ErrHashFreshProgressCorrupt
	}
	return progress, budget, ctx.Err()
}

func initialFreshHashCheckpoint() (fullHashCheckpoint, error) {
	if _, err := initialHashSHAState(); err != nil {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	digest, ok := sha256.New().(hash.Cloner)
	if !ok {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	if _, err := digest.Clone(); err != nil {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	return fullHashCheckpoint{digest: digest}, nil
}

// OpenHashFreshRunWriter is the only open path that initializes or recovers
// fresh progress. It requires existing consent, validates exact job/request
// scope and changes only that job. No live permission is evaluated here.
func OpenHashFreshRunWriter(ctx context.Context, request *KeeperChoiceFreshRequest, jobID string) (*HashStore, error) {
	return openHashFreshRunWriter(ctx, request, jobID, hashFreshRunOpenHooks{})
}

func openHashFreshRunWriter(ctx context.Context, request *KeeperChoiceFreshRequest, jobID string, hooks hashFreshRunOpenHooks) (*HashStore, error) {
	if !ValidHashFreshJobID(jobID) {
		return nil, ErrHashFreshRunBinding
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := OpenHashFreshJobWriter(ctx, request)
	if err != nil {
		return nil, err
	}
	if err = s.initializeFreshProgress(ctx, jobID, hooks); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("fresh progress initialization: %w", err)
	}
	s.freshRunJobID = jobID
	if err = s.recoverFreshProgress(ctx, jobID, hooks); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func (s *HashStore) captureFreshBaselines(ctx context.Context, db hashQuery, job SavedFreshJob) error {
	snapshot, selection, err := s.readHashSnapshot(ctx, db)
	if err != nil || selection == nil {
		return hashFreshProgressFailure(ctx, err)
	}
	baselines := make([]keeperChoiceMetadataBaseline, len(job.Work))
	for i, target := range job.Record.Request.Targets {
		id, err := strconv.Atoi(target.Observation.WorkID)
		if err != nil || id < 1 || id > len(selection.Targets) {
			return ErrHashFreshProgressCorrupt
		}
		w, err := readHashWork(ctx, db, selection, snapshot.SelectionID, id)
		if err != nil || w.status != "complete" || !w.checkpoint.complete || w.sequence != target.Observation.Sequence || !w.checkpoint.checkedAt.Equal(target.Observation.CheckedAt) || w.checkpoint.finalSHA != job.Record.Request.HistoricalChoice.Record.Evidence.SHA256 {
			return hashFreshProgressFailure(ctx, err)
		}
		baselines[i] = keeperChoiceMetadataBaseline{stamp: w.checkpoint.stamp, volume: w.checkpoint.volume, mount: w.checkpoint.mount}
	}
	s.freshRunBaselines = baselines
	return nil
}

func (s *HashStore) initializeFreshProgress(ctx context.Context, jobID string, hooks hashFreshRunOpenHooks) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	job, err := s.readFreshJob(ctx, tx, jobID)
	if err != nil {
		return err
	}
	if !equalHashFreshJobRequests(job.Record.Request, s.freshJobRequest.report) {
		return ErrHashFreshRunBinding
	}
	if job.ReadConsent == nil {
		return ErrHashFreshReadApprovalMissing
	}
	if err = s.captureFreshBaselines(ctx, tx, job); err != nil {
		return fmt.Errorf("historical metadata baseline: %w", err)
	}
	if len(job.Progress) != 0 {
		return s.freshReadStorage(ctx)
	}
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return hashFreshProgressFailure(ctx, err)
	}
	if version == 5 {
		if _, err = tx.ExecContext(ctx, hashFreshProgressSchema); err != nil {
			return hashFreshProgressFailure(ctx, err)
		}
	} else if version != 6 && version != 7 {
		return ErrHashFreshProgressCorrupt
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_run_state VALUES(?,?)", job.ID, len(job.Work)); err != nil {
		return hashFreshProgressFailure(ctx, err)
	}
	for i := range job.Work {
		checkpoint, err := initialFreshHashCheckpoint()
		if err != nil {
			return err
		}
		blob, err := encodeFreshHashCheckpoint(job, i+1, 0, checkpoint)
		if err != nil {
			return fmt.Errorf("initial fresh checkpoint encoding: %w", err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO hash_fresh_progress VALUES(?,?,'pending',0,?,0,?,'')", job.ID, i+1, i+1, blob); err != nil {
			return hashFreshProgressFailure(ctx, err)
		}
	}
	high := job.ReadConsent.ClockHighWater
	if err = writeFreshHashBudget(ctx, tx, job.ID, HashBudget{Day: high.Format(time.DateOnly), MaxNow: high}); err != nil {
		return hashFreshProgressFailure(ctx, err)
	}
	if _, err = s.readFreshJob(ctx, tx, jobID); err != nil {
		return fmt.Errorf("initial fresh saved-state validation: %w", err)
	}
	return s.commitFreshProgress(ctx, tx, job, hooks.beforeInitCommit, hooks.afterInitCommit, hooks.commit, "initialization")
}

func (s *HashStore) recoverFreshProgress(ctx context.Context, jobID string, hooks hashFreshRunOpenHooks) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	job, err := s.readFreshJob(ctx, tx, jobID)
	if err != nil || job.FreshBudget == nil || len(job.Progress) != len(job.Work) {
		return hashFreshProgressFailure(ctx, err)
	}
	b := *job.FreshBudget
	changed := false
	for i, saved := range job.Progress {
		if saved.Status != "running" {
			continue
		}
		w, err := readFreshHashWork(ctx, tx, job, i+1)
		if err != nil || w.sequence == math.MaxInt64 || w.attempt == nil || w.attempt.Status != "reserved" {
			return hashFreshProgressFailure(ctx, err)
		}
		a := w.attempt
		blob, err := encodeFreshHashCheckpoint(job, i+1, w.sequence+1, w.checkpoint)
		if err != nil {
			return err
		}
		b.TotalUnknownReservedBytes, err = addHashCounter(b.TotalUnknownReservedBytes, a.ReservedBytes)
		if err == nil && b.Day == a.ReservationDay {
			b.UnknownReservedBytes, err = addHashCounter(b.UnknownReservedBytes, a.ReservedBytes)
		}
		if err != nil {
			return ErrHashFreshProgressCorrupt
		}
		result, err := tx.ExecContext(ctx, "UPDATE hash_fresh_attempt SET status='interrupted_unknown' WHERE job_id=? AND ordinal=? AND status='reserved' AND nonce=?", job.ID, i+1, a.nonce)
		if err != nil {
			return hashFreshProgressFailure(ctx, err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return hashFreshProgressFailure(ctx, err)
		}
		result, err = tx.ExecContext(ctx, "UPDATE hash_fresh_progress SET status='pending',sequence=sequence+1,checkpoint=? WHERE job_id=? AND ordinal=? AND status='running' AND sequence=? AND checked_offset=?", blob, job.ID, i+1, w.sequence, w.offset)
		if err != nil {
			return hashFreshProgressFailure(ctx, err)
		}
		if rows, err := result.RowsAffected(); err != nil || rows != 1 {
			return hashFreshProgressFailure(ctx, err)
		}
		changed = true
	}
	if !changed {
		return s.freshReadStorage(ctx)
	}
	if err = writeFreshHashBudget(ctx, tx, job.ID, b); err != nil {
		return hashFreshProgressFailure(ctx, err)
	}
	if _, err = s.readFreshJob(ctx, tx, jobID); err != nil {
		return err
	}
	return s.commitFreshProgress(ctx, tx, job, hooks.beforeRecoveryCommit, hooks.afterRecoveryCommit, hooks.commit, "recovery")
}

func (s *HashStore) commitFreshProgress(ctx context.Context, tx *sql.Tx, job SavedFreshJob, before, after func(), commit func(*sql.Tx) error, operation string, admission ...func() error) error {
	if before != nil {
		before()
	}
	if err := s.freshReadStorage(ctx); err != nil {
		return err
	}
	if len(admission) != 0 && admission[0] != nil {
		if err := admission[0](); err != nil {
			return err
		}
	}
	// Capture the actual version inside the committing snapshot; a known
	// successful commit followed by cancellation is not uncertain publication.
	var publishedVersion int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&publishedVersion); err != nil {
		return err
	}
	var err error
	if commit != nil {
		err = commit(tx)
	} else {
		err = tx.Commit()
	}
	if err != nil {
		s.poisoned = true
		return fmt.Errorf("fresh job %s with key %s %s publication is uncertain; close and reopen exact-job recovery: %w", job.ID, job.Record.JobKey, operation, ErrHashRecoveryRequired)
	}
	s.schemaVersion = publishedVersion
	if after != nil {
		after()
	}
	if err = s.freshReadStorage(ctx); err != nil {
		if canceled := ctx.Err(); canceled != nil {
			return fmt.Errorf("fresh job %s with key %s %s was saved before cancellation; inspect saved state: %w", job.ID, job.Record.JobKey, operation, canceled)
		}
		s.poisoned = true
		return fmt.Errorf("fresh job %s with key %s %s was saved but storage became uncertain; close and inspect: %w", job.ID, job.Record.JobKey, operation, ErrHashRecoveryRequired)
	}
	return nil
}
