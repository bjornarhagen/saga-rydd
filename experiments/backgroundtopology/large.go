package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const minimumOutputCapacity = int64(16 << 30)
const maximumTopologySamples = 721
const savedJobsPerPage = 16
const maximumSavedJobs = 129
const maximumJobPageBytes = 2 << 20

var errCapacity = errors.New("generated topology refused initial output-filesystem capacity below 16GiB; no generation or worker launch")

// This checks one selected output filesystem only. It is neither a space
// reservation nor a claim that subsequent native allocation cannot fail.
func checkInitialOutputCapacity(ctx context.Context, path string) (*int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var s unix.Statfs_t
	if err := unix.Statfs(path, &s); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n, err := outputCapacityBytes(uint64(s.Bsize), uint64(s.Bavail))
	if err != nil {
		return nil, err
	}
	if n < minimumOutputCapacity {
		return &n, errCapacity
	}
	return &n, nil
}
func outputCapacityBytes(blockSize, available uint64) (int64, error) {
	if blockSize == 0 || blockSize > math.MaxInt64 || available > uint64(math.MaxInt64)/blockSize {
		return 0, errProfile
	}
	return int64(blockSize * available), nil
}

// Variable saved job payloads are admitted in SQLite before Go allocation.
// A single read holds at most 130 rows, each with a 64KiB cursor, then refuses
// the 130th row. Only the exact two generated inventory roots are admitted.
const boundedSavedJobsSQL = `SELECT
 CASE WHEN typeof(id)='integer' THEN id END,
 CASE WHEN typeof(root_id)='integer' THEN root_id END,
 CASE WHEN typeof(kind)='text' AND kind='inventory' THEN kind END,
 CASE WHEN typeof(path)='blob' AND length(path) BETWEEN 1 AND 512 THEN path END,
 CASE WHEN typeof(status)='text' AND status IN ('pending','running') THEN status END,
 CASE WHEN typeof(due_at_ns)='integer' THEN due_at_ns END,
 CASE WHEN cursor IS NULL OR (typeof(cursor)='blob' AND length(cursor)<=65536) THEN cursor END,
 CASE WHEN typeof(attempts)='integer' THEN attempts END,
 CASE WHEN typeof(last_error)='text' AND length(CAST(last_error AS BLOB))<=2048 THEN last_error END,
 CASE WHEN typeof(lease_token)='text' AND length(CAST(lease_token AS BLOB))<=128 THEN lease_token END,
 CASE WHEN typeof(lease_until_ns)='integer' THEN lease_until_ns END,
 CASE WHEN typeof(inventory_claimed)='integer' THEN inventory_claimed END,
 typeof(id)='integer' AND id>0 AND typeof(root_id)='integer' AND root_id>0
 AND typeof(kind)='text' AND kind='inventory'
 AND typeof(path)='blob' AND length(path) BETWEEN 1 AND 512
 AND typeof(status)='text' AND status IN ('pending','running')
 AND typeof(last_error)='text' AND length(CAST(last_error AS BLOB))<=2048
 AND typeof(lease_token)='text' AND length(CAST(lease_token AS BLOB))<=128
 AND typeof(due_at_ns)='integer' AND due_at_ns>=0
 AND (cursor IS NULL OR (typeof(cursor)='blob' AND length(cursor)<=65536))
 AND typeof(attempts)='integer' AND attempts>=0
 AND typeof(lease_until_ns)='integer' AND lease_until_ns>=0
 AND typeof(inventory_claimed)='integer' AND inventory_claimed IN (0,1)
 FROM jobs ORDER BY id LIMIT 130`

func spoolSavedEvidence(base string, sampleIndex int, v savedView) error {
	if sampleIndex < 0 || sampleIndex >= maximumTopologySamples || len(v.Jobs) > maximumSavedJobs {
		return errProfile
	}
	jobs := v.Jobs
	v.Jobs = nil
	summary, err := json.Marshal(v)
	if err != nil {
		return errors.Join(errReceipt, err)
	}
	if err = exclusiveEvidenceReceipt(filepath.Join(base, fmt.Sprintf("sample-%03d-summary.json", sampleIndex)), summary); err != nil {
		return err
	}
	for start, page := 0, 0; start < len(jobs); start, page = start+savedJobsPerPage, page+1 {
		end := min(start+savedJobsPerPage, len(jobs))
		body, err := json.Marshal(jobs[start:end])
		if err != nil || len(body) > maximumJobPageBytes {
			return errors.Join(errProfile, err)
		}
		if err = exclusiveEvidenceReceipt(filepath.Join(base, fmt.Sprintf("sample-%03d-jobs-%02d.json", sampleIndex, page)), body); err != nil {
			return err
		}
	}
	return nil
}

func exclusiveEvidenceReceipt(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.Join(errReceipt, err)
	}
	_, writeErr := f.Write(body)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		return errors.Join(errReceipt, writeErr, closeErr)
	}
	return nil
}
