package inventory

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const (
	hashCheckpointVersion         = "file_hash_checkpoint_v1"
	hashCheckpointCodec           = "go_sha256_108_v1"
	hashCheckpointRuntime         = "go1.27.1"
	hashCheckpointByteLimit       = 8192
	hashSHAStateBytes             = 108
	hashSHAStateBufferStart       = 36
	hashSHAStateCountStart        = 100
	hashSHAByteLimit        int64 = (1 << 61) - 1
)

var errHashCheckpoint = errors.New("hash checkpoint is invalid, incompatible or differs from its exact binding")

// These records are private to the codec and its future owning controller.
// A checksum detects corruption; it cannot authenticate a coherently rewritten
// local store or prove that a SHA prefix came from the selected source file.
// No caller-facing method accepts raw checkpoint, offset, usage or digest data.
type hashCheckpointBinding struct {
	StoreID      string `json:"store_id"`
	SelectionID  string `json:"selection_id"`
	WorkID       string `json:"work_id"`
	Sequence     int64  `json:"sequence"`
	TargetDigest string `json:"target_digest"`
}

// Portable fields preserve exactly the comparisons made by sameInputStamp,
// rather than persisting platform-dependent unix.Stat_t memory or widths.
type hashLiveStamp struct {
	Device    string `json:"device"`
	Inode     string `json:"inode"`
	Mode      uint32 `json:"mode"`
	LinkCount uint64 `json:"link_count"`
	Size      int64  `json:"size"`
	MtimeNS   int64  `json:"mtime_ns"`
	CtimeNS   int64  `json:"ctime_ns"`
}

type hashCheckpointEnvelope struct {
	Version      string                `json:"version"`
	Contract     string                `json:"contract"`
	Algorithm    string                `json:"algorithm"`
	Codec        string                `json:"codec"`
	Runtime      string                `json:"runtime"`
	Platform     string                `json:"platform"`
	Binding      hashCheckpointBinding `json:"binding"`
	Status       string                `json:"status"`
	LogicalBytes int64                 `json:"logical_bytes"`
	Offset       int64                 `json:"offset"`
	CheckedAtNS  int64                 `json:"checked_at_ns"`
	Baseline     bool                  `json:"baseline"`
	Stamp        hashLiveStamp         `json:"stamp"`
	Volume       string                `json:"volume"`
	Mount        string                `json:"mount"`
	State        []byte                `json:"state,omitempty"`
	SHA256       string                `json:"sha256,omitempty"`
	Checksum     string                `json:"checksum,omitempty"`
}

func newHashLiveStamp(st unix.Stat_t) hashLiveStamp {
	m, c := timestamps(&st)
	return hashLiveStamp{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), Mode: uint32(st.Mode), LinkCount: uint64(st.Nlink), Size: st.Size, MtimeNS: m, CtimeNS: c}
}

func (stamp hashLiveStamp) matches(st unix.Stat_t) bool {
	return stamp == newHashLiveStamp(st)
}

func cloneHashTarget(target SavedFileTarget) SavedFileTarget {
	target.Root.PathBytes = bytes.Clone(target.Root.PathBytes)
	target.File.PathBytes = bytes.Clone(target.File.PathBytes)
	target.File.Path = string(target.File.PathBytes)
	ancestors := append(target.Ancestors[:0:0], target.Ancestors...)
	for i := range ancestors {
		ancestors[i].Path = bytes.Clone(ancestors[i].Path)
	}
	target.Ancestors = ancestors
	return target
}

func hashTargetDigest(target SavedFileTarget) (string, error) {
	if err := boundedSampleRequest([]SavedFileTarget{target}); err != nil {
		return "", err
	}
	if err := validateSampleTarget(target); err != nil {
		return "", err
	}
	if target.File.Size > hashSHAByteLimit {
		return "", errHashCheckpoint
	}
	body, err := json.Marshal(cloneHashTarget(target))
	if err != nil || len(body) > fileSampleEvidenceLimit {
		return "", errHashCheckpoint
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

func validHashCheckpointBinding(binding hashCheckpointBinding, target SavedFileTarget) bool {
	ordinal, err := strconv.Atoi(binding.WorkID)
	if !sampleHex(binding.StoreID) || !sampleHex(binding.SelectionID) || !sampleHex(binding.TargetDigest) || binding.Sequence < 0 || err != nil || ordinal < 1 || ordinal > FileSampleTargetLimit || strconv.Itoa(ordinal) != binding.WorkID {
		return false
	}
	digest, err := hashTargetDigest(target)
	return err == nil && digest == binding.TargetDigest
}

func validHashCheckpointMetadata(target SavedFileTarget, checkpoint fullHashCheckpoint) bool {
	if checkpoint.offset < 0 || checkpoint.offset > target.File.Size || checkpoint.offset > hashSHAByteLimit {
		return false
	}
	if !checkpoint.baseline {
		return !checkpoint.complete && checkpoint.offset == 0 && checkpoint.checkedAt.IsZero() && checkpoint.stamp == (hashLiveStamp{}) && checkpoint.volume == "" && checkpoint.mount == "" && checkpoint.finalSHA == ""
	}
	stamp := checkpoint.stamp
	return !checkpoint.checkedAt.IsZero() && checkpoint.checkedAt.UnixNano() > 0 &&
		stamp.Device == target.File.Device && stamp.Inode == target.File.Inode && stamp.Mode&unix.S_IFMT == unix.S_IFREG && stamp.LinkCount > 0 &&
		stamp.Size == target.File.Size && stamp.MtimeNS == target.File.ModifiedAt.UnixNano() && stamp.CtimeNS == target.File.ChangedNS &&
		len(checkpoint.volume) > 0 && len(checkpoint.volume) <= 128 && !strings.ContainsRune(checkpoint.volume, 0) &&
		len(checkpoint.mount) > 0 && len(checkpoint.mount) <= 4096 && !strings.ContainsRune(checkpoint.mount, 0)
}

// The pinned codec encodes magic, eight chaining words, a 64-byte buffer and
// an unsigned byte count. An aligned prefix must contain no buffered bytes.
func validHashSHAState(state []byte, count int64, aligned bool) bool {
	if len(state) != hashSHAStateBytes || string(state[:4]) != "sha\x03" || count < 0 || count > hashSHAByteLimit || binary.BigEndian.Uint64(state[hashSHAStateCountStart:]) != uint64(count) {
		return false
	}
	residue := int(count % sha256.BlockSize)
	if aligned && residue != 0 {
		return false
	}
	return bytes.Equal(state[hashSHAStateBufferStart+residue:hashSHAStateCountStart], make([]byte, sha256.BlockSize-residue))
}

func initialHashSHAState() ([]byte, error) {
	if runtime.Version() != hashCheckpointRuntime || (runtime.GOOS != "darwin" && runtime.GOOS != "linux") {
		return nil, errHashCheckpoint
	}
	marshaler, ok := sha256.New().(encoding.BinaryMarshaler)
	if !ok {
		return nil, errHashCheckpoint
	}
	state, err := marshaler.MarshalBinary()
	if err != nil || !validHashSHAState(state, 0, true) {
		return nil, errHashCheckpoint
	}
	return state, nil
}

func hashCheckpointChecksum(envelope hashCheckpointEnvelope) (string, error) {
	envelope.Checksum = ""
	body, err := json.Marshal(envelope)
	if err != nil || len(body) > hashCheckpointByteLimit {
		return "", errHashCheckpoint
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

// Encode only a checked checkpoint while its owning session/controller gate is
// held. Partial encoding retains the chaining state of complete SHA blocks,
// discards at most 63 checked bytes and zeros the whole buffered region.
// Completed records retain only the final digest; they cannot restore a prefix.
func encodeHashCheckpoint(binding hashCheckpointBinding, target SavedFileTarget, checkpoint fullHashCheckpoint) ([]byte, error) {
	initial, err := initialHashSHAState()
	if err != nil || !validHashCheckpointBinding(binding, target) || !validHashCheckpointMetadata(target, checkpoint) {
		return nil, errHashCheckpoint
	}
	envelope := hashCheckpointEnvelope{Version: hashCheckpointVersion, Contract: FileHashContract, Algorithm: "sha256", Codec: hashCheckpointCodec, Runtime: hashCheckpointRuntime, Platform: runtime.GOOS, Binding: binding, Status: "partial", LogicalBytes: target.File.Size, Offset: checkpoint.offset, Baseline: checkpoint.baseline, Stamp: checkpoint.stamp, Volume: checkpoint.volume, Mount: checkpoint.mount}
	if checkpoint.baseline {
		envelope.CheckedAtNS = checkpoint.checkedAt.UnixNano()
	}
	if checkpoint.complete {
		if checkpoint.offset != target.File.Size {
			return nil, errHashCheckpoint
		}
		envelope.Status, envelope.SHA256 = "complete", checkpoint.finalSHA
		if checkpoint.digest != nil {
			marshaler, ok := checkpoint.digest.(encoding.BinaryMarshaler)
			if !ok {
				return nil, errHashCheckpoint
			}
			state, err := marshaler.MarshalBinary()
			if err != nil || !validHashSHAState(state, checkpoint.offset, false) {
				return nil, errHashCheckpoint
			}
			clear(state)
			envelope.SHA256 = hex.EncodeToString(checkpoint.digest.Sum(nil))
			if checkpoint.finalSHA != "" && checkpoint.finalSHA != envelope.SHA256 {
				return nil, errHashCheckpoint
			}
		}
		if !sampleHex(envelope.SHA256) {
			return nil, errHashCheckpoint
		}
	} else {
		if checkpoint.digest == nil || checkpoint.finalSHA != "" || (checkpoint.baseline && checkpoint.offset >= target.File.Size) {
			return nil, errHashCheckpoint
		}
		marshaler, ok := checkpoint.digest.(encoding.BinaryMarshaler)
		if !ok {
			return nil, errHashCheckpoint
		}
		state, err := marshaler.MarshalBinary()
		if err != nil || !validHashSHAState(state, checkpoint.offset, false) || (!checkpoint.baseline && !bytes.Equal(state, initial)) {
			return nil, errHashCheckpoint
		}
		envelope.Offset -= envelope.Offset % sha256.BlockSize
		clear(state[hashSHAStateBufferStart:hashSHAStateCountStart])
		binary.BigEndian.PutUint64(state[hashSHAStateCountStart:], uint64(envelope.Offset))
		if envelope.Offset == 0 && !bytes.Equal(state, initial) {
			return nil, errHashCheckpoint
		}
		envelope.State = state
	}
	envelope.Checksum, err = hashCheckpointChecksum(envelope)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(envelope)
	if err != nil || len(body) > hashCheckpointByteLimit {
		return nil, errHashCheckpoint
	}
	return body, nil
}

// Decode validates bounded canonical bytes and the exact controller binding
// before touching a SHA decoder. It opens no source path. Final records decode
// as historical digest-only observations and are refused by restoreHashSession.
func decodeHashCheckpoint(body []byte, binding hashCheckpointBinding, target SavedFileTarget) (fullHashCheckpoint, error) {
	var checkpoint fullHashCheckpoint
	initial, err := initialHashSHAState()
	if err != nil || len(body) == 0 || len(body) > hashCheckpointByteLimit || !validHashCheckpointBinding(binding, target) {
		return checkpoint, errHashCheckpoint
	}
	var envelope hashCheckpointEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return checkpoint, errHashCheckpoint
	}
	canonical, err := json.Marshal(envelope)
	if err != nil || !bytes.Equal(body, canonical) || envelope.Version != hashCheckpointVersion || envelope.Contract != FileHashContract || envelope.Algorithm != "sha256" || envelope.Codec != hashCheckpointCodec || envelope.Runtime != hashCheckpointRuntime || envelope.Platform != runtime.GOOS || envelope.Binding != binding || envelope.LogicalBytes != target.File.Size || !sampleHex(envelope.Checksum) {
		return checkpoint, errHashCheckpoint
	}
	checksum, err := hashCheckpointChecksum(envelope)
	if err != nil || checksum != envelope.Checksum {
		return checkpoint, errHashCheckpoint
	}
	checkpoint = fullHashCheckpoint{offset: envelope.Offset, stamp: envelope.Stamp, volume: envelope.Volume, mount: envelope.Mount, baseline: envelope.Baseline}
	if envelope.CheckedAtNS != 0 {
		checkpoint.checkedAt = time.Unix(0, envelope.CheckedAtNS).UTC()
	}
	switch envelope.Status {
	case "complete":
		checkpoint.complete, checkpoint.finalSHA = true, envelope.SHA256
		if len(envelope.State) != 0 || !sampleHex(envelope.SHA256) || envelope.Offset != target.File.Size || !validHashCheckpointMetadata(target, checkpoint) {
			return fullHashCheckpoint{}, errHashCheckpoint
		}
		return checkpoint, nil
	case "partial":
		if envelope.SHA256 != "" || !validHashSHAState(envelope.State, envelope.Offset, true) || (envelope.Offset == 0 && !bytes.Equal(envelope.State, initial)) || !validHashCheckpointMetadata(target, checkpoint) || (envelope.Baseline && envelope.Offset >= target.File.Size) {
			return fullHashCheckpoint{}, errHashCheckpoint
		}
	default:
		return fullHashCheckpoint{}, errHashCheckpoint
	}
	digest := sha256.New()
	unmarshaler, ok := digest.(encoding.BinaryUnmarshaler)
	if !ok || unmarshaler.UnmarshalBinary(envelope.State) != nil {
		return fullHashCheckpoint{}, errHashCheckpoint
	}
	checkpoint.digest, ok = digest.(hash.Cloner)
	if !ok {
		return fullHashCheckpoint{}, errHashCheckpoint
	}
	return checkpoint, nil
}

func restoreHashSession(scanner *Scanner, target SavedFileTarget, checkpoint fullHashCheckpoint) (*FullHashSession, error) {
	if checkpoint.complete || checkpoint.digest == nil || checkpoint.finalSHA != "" || !validHashCheckpointMetadata(target, checkpoint) || checkpoint.offset%sha256.BlockSize != 0 {
		return nil, errHashCheckpoint
	}
	initial, err := initialHashSHAState()
	if err != nil {
		return nil, errHashCheckpoint
	}
	marshaler, ok := checkpoint.digest.(encoding.BinaryMarshaler)
	if !ok {
		return nil, errHashCheckpoint
	}
	state, err := marshaler.MarshalBinary()
	if err != nil || !validHashSHAState(state, checkpoint.offset, true) || (checkpoint.offset == 0 && !bytes.Equal(state, initial)) {
		return nil, errHashCheckpoint
	}
	session, err := scanner.NewFullHashSession(target)
	if err != nil {
		return nil, err
	}
	checkpoint.digest, err = checkpoint.digest.Clone()
	if err != nil {
		return nil, errHashCheckpoint
	}
	session.core.checkpoint = checkpoint
	session.core.publishCheckedProgress()
	return session, nil
}
