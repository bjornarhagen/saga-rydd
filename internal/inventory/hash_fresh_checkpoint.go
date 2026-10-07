package inventory

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const hashFreshCheckpointVersion = "fresh_job_hash_checkpoint_v1"
const HashFreshCheckpointMaxRecordBytes = 16 << 10

// Fresh wrappers have a distinct domain and bind the full job ID. The inner
// codec retains its original byte contract, with the fresh job digest in its
// selection slot and a fresh ordinal. Original checkpoint blobs are invalid
// fresh input; no original continuation is copied into this wrapper.
type hashFreshCheckpointEnvelope struct {
	Version    string `json:"version"`
	JobID      string `json:"job_id"`
	Checkpoint []byte `json:"checkpoint"`
	Checksum   string `json:"checksum,omitempty"`
}

func freshHashCheckpointBinding(job SavedFreshJob, ordinal int, sequence int64) (hashCheckpointBinding, error) {
	if !ValidHashFreshJobID(job.ID) || ordinal < 1 || ordinal > len(job.Record.Request.Targets) || sequence < 0 {
		return hashCheckpointBinding{}, ErrHashFreshProgressCorrupt
	}
	digest, err := hashTargetDigest(job.Record.Request.Targets[ordinal-1].Target)
	if err != nil {
		return hashCheckpointBinding{}, ErrHashFreshProgressCorrupt
	}
	return hashCheckpointBinding{StoreID: job.Record.Request.StoreID, SelectionID: strings.TrimPrefix(job.ID, hashFreshJobPrefix), WorkID: strconv.Itoa(ordinal), Sequence: sequence, TargetDigest: digest}, nil
}

func hashFreshCheckpointChecksum(envelope hashFreshCheckpointEnvelope) (string, error) {
	envelope.Checksum = ""
	payload, err := json.Marshal(envelope)
	if err != nil || len(payload) > HashFreshCheckpointMaxRecordBytes {
		return "", ErrHashFreshProgressCorrupt
	}
	return fmt.Sprintf("%x", sha256.Sum256(payload)), nil
}

func encodeFreshHashCheckpoint(job SavedFreshJob, ordinal int, sequence int64, checkpoint fullHashCheckpoint) ([]byte, error) {
	binding, err := freshHashCheckpointBinding(job, ordinal, sequence)
	if err != nil {
		return nil, err
	}
	inner, err := encodeHashCheckpoint(binding, job.Record.Request.Targets[ordinal-1].Target, checkpoint)
	if err != nil {
		return nil, ErrHashFreshProgressCorrupt
	}
	envelope := hashFreshCheckpointEnvelope{Version: hashFreshCheckpointVersion, JobID: job.ID, Checkpoint: inner}
	envelope.Checksum, err = hashFreshCheckpointChecksum(envelope)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(envelope)
	if err != nil || len(payload) > HashFreshCheckpointMaxRecordBytes {
		return nil, ErrHashFreshProgressCorrupt
	}
	return payload, nil
}

func decodeFreshHashCheckpoint(payload []byte, job SavedFreshJob, ordinal int, sequence int64) (fullHashCheckpoint, error) {
	if len(payload) == 0 || len(payload) > HashFreshCheckpointMaxRecordBytes {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	var envelope hashFreshCheckpointEnvelope
	if decodeHashFreshReadRecord(payload, &envelope) != nil || envelope.Version != hashFreshCheckpointVersion || envelope.JobID != job.ID || !hashStoreDigest(envelope.Checksum) || len(envelope.Checkpoint) == 0 || len(envelope.Checkpoint) > hashCheckpointByteLimit {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	checksum, err := hashFreshCheckpointChecksum(envelope)
	if err != nil || checksum != envelope.Checksum {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(payload, canonical) {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	binding, err := freshHashCheckpointBinding(job, ordinal, sequence)
	if err != nil {
		return fullHashCheckpoint{}, err
	}
	checkpoint, err := decodeHashCheckpoint(envelope.Checkpoint, binding, job.Record.Request.Targets[ordinal-1].Target)
	if err != nil {
		return fullHashCheckpoint{}, ErrHashFreshProgressCorrupt
	}
	return checkpoint, nil
}
