package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
)

func checkpointBinding(t *testing.T, target SavedFileTarget) hashCheckpointBinding {
	t.Helper()
	digest, err := hashTargetDigest(target)
	if err != nil {
		t.Fatal(err)
	}
	return hashCheckpointBinding{StoreID: strings.Repeat("a", 64), SelectionID: strings.Repeat("b", 64), WorkID: "1", Sequence: 7, TargetDigest: digest}
}

func checkpointFixture(t *testing.T, checked int64) (*Scanner, SavedFileTarget, *FullHashSession, []byte) {
	t.Helper()
	data := fullHashContents(513)
	s, targets := sampleFixture(t, data)
	session := fullHashSession(t, s, targets[0])
	if checked > 0 {
		p, usage, err := session.Step(context.Background(), checked)
		if err != nil || p.Offset != checked || usage.RequestedBytes != checked {
			t.Fatal("checkpoint fixture did not reach exact checked offset", p, usage, err)
		}
	}
	return s, targets[0], session, data
}

func checkpointSHAState(t *testing.T, blob []byte) []byte {
	t.Helper()
	var wire struct {
		State []byte `json:"state"`
	}
	if err := json.Unmarshal(blob, &wire); err != nil {
		t.Fatal(err)
	}
	return wire.State
}

func TestHashCheckpointAlignedFloorAndIndependentRestore(t *testing.T) {
	for _, checked := range []int64{0, 63, 64, 65, 127, 128} {
		t.Run(fmt.Sprint(checked), func(t *testing.T) {
			s, target, session, data := checkpointFixture(t, checked)
			binding := checkpointBinding(t, target)
			blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			state := checkpointSHAState(t, blob)
			floor := checked / 64 * 64
			if len(state) != 108 || !bytes.Equal(state[36:100], make([]byte, 64)) || binary.BigEndian.Uint64(state[100:108]) != uint64(floor) {
				t.Fatal("partial checkpoint persisted buffered bytes or a nonaligned count", checked, state)
			}
			decoded, err := decodeHashCheckpoint(blob, binding, target)
			if err != nil || decoded.offset != floor || decoded.complete {
				t.Fatal("wrong checkpoint floor or completion", decoded.offset, decoded.complete, err)
			}
			marshaler, ok := decoded.digest.(encoding.BinaryMarshaler)
			if !ok {
				t.Fatal("restored digest has no binary-state inspection")
			}
			roundtrip, err := marshaler.MarshalBinary()
			if err != nil || !bytes.Equal(roundtrip, state) {
				t.Fatal("decoder changed the stored normalized SHA state", roundtrip, err)
			}
			restored, err := restoreHashSession(s, target, decoded)
			if err != nil {
				t.Fatal(err)
			}
			p, usage, err := restored.Step(context.Background(), FileHashStepByteLimit)
			if err != nil || usage.RequestedBytes != int64(len(data))-floor || usage.ReadBytes != usage.RequestedBytes {
				t.Fatal("restored suffix skipped/repeated checkpoint bytes", p, usage, err)
			}
			requireFullHashDigest(t, p, data)
		})
	}
}

func TestHashCheckpointCompletedDigestOnlyCannotResume(t *testing.T) {
	for _, size := range []int{0, 63, 64, 65, 127, 128} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := fullHashContents(size)
			s, targets := sampleFixture(t, data)
			target := targets[0]
			session := fullHashSession(t, s, target)
			p, _, err := session.Step(context.Background(), FileHashStepByteLimit)
			if err != nil {
				t.Fatal(err)
			}
			requireFullHashDigest(t, p, data)
			binding := checkpointBinding(t, target)
			blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
			if err != nil {
				t.Fatal(err)
			}
			var wire struct {
				State  []byte `json:"state"`
				SHA256 string `json:"sha256"`
			}
			if err := json.Unmarshal(blob, &wire); err != nil {
				t.Fatal(err)
			}
			want := sha256.Sum256(data)
			if len(wire.State) != 0 || wire.SHA256 != hex.EncodeToString(want[:]) {
				t.Fatal("completed checkpoint persisted resumable/buffered SHA state", wire)
			}
			decoded, err := decodeHashCheckpoint(blob, binding, target)
			if err != nil || !decoded.complete || decoded.offset != int64(size) {
				t.Fatal(decoded.offset, decoded.complete, err)
			}
			if resumed, err := restoreHashSession(s, target, decoded); err == nil || resumed != nil {
				t.Fatal("completed digest-only record was restored as prefix state", resumed, err)
			}
		})
	}
}

func TestHashCheckpointExactBindingAndTargetRefusals(t *testing.T) {
	_, target, session, _ := checkpointFixture(t, 65)
	binding := checkpointBinding(t, target)
	blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"store", "selection", "work", "sequence", "target_digest"} {
		t.Run(name, func(t *testing.T) {
			changed := binding
			switch name {
			case "store":
				changed.StoreID = strings.Repeat("c", 64)
			case "selection":
				changed.SelectionID = strings.Repeat("c", 64)
			case "work":
				changed.WorkID = "2"
			case "sequence":
				changed.Sequence++
			case "target_digest":
				changed.TargetDigest = strings.Repeat("c", 64)
			}
			if _, err := decodeHashCheckpoint(blob, changed, target); err == nil {
				t.Fatal("checkpoint accepted another exact binding")
			}
		})
	}
	for _, name := range []string{"inventory", "root_revision", "root_path", "file_id", "file_stamp", "ancestor"} {
		t.Run(name, func(t *testing.T) {
			changed := target
			changed.Root.PathBytes = bytes.Clone(target.Root.PathBytes)
			changed.File.PathBytes = bytes.Clone(target.File.PathBytes)
			changed.Ancestors = append(target.Ancestors[:0:0], target.Ancestors...)
			switch name {
			case "inventory":
				changed.InventoryID = strings.Repeat("c", 64)
			case "root_revision":
				changed.Root.Revision++
			case "root_path":
				changed.Root.PathBytes[len(changed.Root.PathBytes)-1] = 'x'
			case "file_id":
				changed.File.ID++
			case "file_stamp":
				changed.File.ChangedNS++
			case "ancestor":
				changed.Ancestors[0].CtimeNS++
			}
			if _, err := decodeHashCheckpoint(blob, binding, changed); err == nil {
				t.Fatal("checkpoint accepted changed source selection evidence")
			}
		})
	}
}

func checkpointEnvelope(t *testing.T, blob []byte) hashCheckpointEnvelope {
	t.Helper()
	var envelope hashCheckpointEnvelope
	if err := json.Unmarshal(blob, &envelope); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func checkpointRechecksum(t *testing.T, envelope hashCheckpointEnvelope) []byte {
	t.Helper()
	checksum, err := hashCheckpointChecksum(envelope)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Checksum = checksum
	blob, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

func TestHashCheckpointChecksummedMalformedEnvelopeRejected(t *testing.T) {
	_, target, session, _ := checkpointFixture(t, 128)
	binding := checkpointBinding(t, target)
	blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	base := checkpointEnvelope(t, blob)
	mutations := []struct {
		name   string
		change func(*hashCheckpointEnvelope)
	}{
		{"version", func(e *hashCheckpointEnvelope) { e.Version = "file_hash_checkpoint_v2" }},
		{"contract", func(e *hashCheckpointEnvelope) { e.Contract += "_other" }},
		{"algorithm", func(e *hashCheckpointEnvelope) { e.Algorithm = "sha224" }},
		{"codec", func(e *hashCheckpointEnvelope) { e.Codec += "_other" }},
		{"runtime", func(e *hashCheckpointEnvelope) { e.Runtime = "go1.27.2" }},
		{"platform", func(e *hashCheckpointEnvelope) {
			if runtime.GOOS == "linux" {
				e.Platform = "darwin"
			} else {
				e.Platform = "linux"
			}
		}},
		{"binding_store", func(e *hashCheckpointEnvelope) { e.Binding.StoreID = strings.Repeat("c", 64) }},
		{"binding_selection", func(e *hashCheckpointEnvelope) { e.Binding.SelectionID = strings.Repeat("c", 64) }},
		{"binding_work", func(e *hashCheckpointEnvelope) { e.Binding.WorkID = "2" }},
		{"binding_sequence", func(e *hashCheckpointEnvelope) { e.Binding.Sequence++ }},
		{"binding_target", func(e *hashCheckpointEnvelope) { e.Binding.TargetDigest = strings.Repeat("c", 64) }},
		{"logical_size", func(e *hashCheckpointEnvelope) { e.LogicalBytes++ }},
		{"negative_offset", func(e *hashCheckpointEnvelope) { e.Offset = -64 }},
		{"offset_count_mismatch", func(e *hashCheckpointEnvelope) { e.Offset = 64 }},
		{"unaligned_count", func(e *hashCheckpointEnvelope) { e.Offset = 129; binary.BigEndian.PutUint64(e.State[100:], 129) }},
		{"count_mismatch", func(e *hashCheckpointEnvelope) { binary.BigEndian.PutUint64(e.State[100:], 64) }},
		{"huge_count", func(e *hashCheckpointEnvelope) { binary.BigEndian.PutUint64(e.State[100:], ^uint64(0)) }},
		{"state_short", func(e *hashCheckpointEnvelope) { e.State = e.State[:107] }},
		{"state_long", func(e *hashCheckpointEnvelope) { e.State = append(e.State, 0) }},
		{"state_magic", func(e *hashCheckpointEnvelope) { e.State[3]++ }},
		{"buffer_first_byte", func(e *hashCheckpointEnvelope) { e.State[36] = 1 }},
		{"buffer_last_byte", func(e *hashCheckpointEnvelope) { e.State[99] = 1 }},
		{"partial_digest", func(e *hashCheckpointEnvelope) { e.SHA256 = strings.Repeat("c", 64) }},
		{"unknown_status", func(e *hashCheckpointEnvelope) { e.Status = "done" }},
		{"missing_baseline", func(e *hashCheckpointEnvelope) { e.Baseline = false }},
		{"zero_check_time", func(e *hashCheckpointEnvelope) { e.CheckedAtNS = 0 }},
		{"negative_check_time", func(e *hashCheckpointEnvelope) { e.CheckedAtNS = -1 }},
		{"stamp_device", func(e *hashCheckpointEnvelope) { e.Stamp.Device += "0" }},
		{"stamp_inode", func(e *hashCheckpointEnvelope) { e.Stamp.Inode += "0" }},
		{"stamp_kind", func(e *hashCheckpointEnvelope) { e.Stamp.Mode = 0 }},
		{"stamp_links", func(e *hashCheckpointEnvelope) { e.Stamp.LinkCount = 0 }},
		{"stamp_size", func(e *hashCheckpointEnvelope) { e.Stamp.Size++ }},
		{"stamp_mtime", func(e *hashCheckpointEnvelope) { e.Stamp.MtimeNS++ }},
		{"stamp_ctime", func(e *hashCheckpointEnvelope) { e.Stamp.CtimeNS++ }},
		{"empty_volume", func(e *hashCheckpointEnvelope) { e.Volume = "" }},
		{"volume_bound", func(e *hashCheckpointEnvelope) { e.Volume = strings.Repeat("x", 129) }},
		{"volume_nul", func(e *hashCheckpointEnvelope) { e.Volume += "\x00" }},
		{"empty_mount", func(e *hashCheckpointEnvelope) { e.Mount = "" }},
		{"mount_bound", func(e *hashCheckpointEnvelope) { e.Mount = strings.Repeat("x", 4097) }},
		{"mount_nul", func(e *hashCheckpointEnvelope) { e.Mount += "\x00" }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			envelope := base
			envelope.State = bytes.Clone(base.State)
			test.change(&envelope)
			if _, err := decodeHashCheckpoint(checkpointRechecksum(t, envelope), binding, target); err == nil {
				t.Fatal("structurally invalid checkpoint accepted despite exact checksum")
			}
		})
	}
}

func TestHashCheckpointCanonicalChecksumAndBounds(t *testing.T) {
	_, target, session, _ := checkpointFixture(t, 65)
	binding := checkpointBinding(t, target)
	blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	envelope := checkpointEnvelope(t, blob)
	badChecksum := envelope
	badChecksum.Checksum = strings.Repeat("c", 64)
	wrongChecksum, err := json.Marshal(badChecksum)
	if err != nil {
		t.Fatal(err)
	}
	indented, err := json.MarshalIndent(envelope, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		"empty":           nil,
		"checksum":        wrongChecksum,
		"whitespace":      indented,
		"trailing_space":  append(bytes.Clone(blob), ' '),
		"trailing_json":   append(bytes.Clone(blob), []byte("{}")...),
		"unknown_field":   append(append(bytes.Clone(blob[:len(blob)-1]), []byte(",\"other\":1")...), '}'),
		"duplicate_field": append(append(bytes.Clone(blob[:len(blob)-1]), []byte(",\"offset\":64")...), '}'),
		"oversized":       bytes.Repeat([]byte{' '}, hashCheckpointByteLimit+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeHashCheckpoint(body, binding, target); err == nil {
				t.Fatal("noncanonical, corrupt or over-limit record accepted")
			}
		})
	}
}

func TestHashCheckpointInitialAndEncoderCountRefusals(t *testing.T) {
	_, target, session, _ := checkpointFixture(t, 0)
	binding := checkpointBinding(t, target)
	blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	initial := checkpointEnvelope(t, blob)
	initial.State[4] ^= 1
	if _, err := decodeHashCheckpoint(checkpointRechecksum(t, initial), binding, target); err == nil {
		t.Fatal("checksummed forged initial chaining words accepted")
	}
	for _, checked := range []int64{63, 64, 65, 127, 128} {
		t.Run(fmt.Sprint(checked), func(t *testing.T) {
			_, target, session, _ := checkpointFixture(t, checked)
			binding := checkpointBinding(t, target)
			marshaler := session.core.checkpoint.digest.(encoding.BinaryMarshaler)
			before, err := marshaler.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			wrong := session.core.checkpoint
			wrong.offset++
			if _, err := encodeHashCheckpoint(binding, target, wrong); err == nil {
				t.Fatal("encoder replaced count before checking actual digest offset")
			}
			if _, err := encodeHashCheckpoint(binding, target, session.core.checkpoint); err != nil {
				t.Fatal(err)
			}
			after, err := marshaler.MarshalBinary()
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("encoding mutated the live checked digest", err)
			}
		})
	}
}

func TestHashCheckpointCompletedEnvelopeRefusals(t *testing.T) {
	s, target, session, _ := checkpointFixture(t, 0)
	if _, _, err := session.Step(context.Background(), FileHashStepByteLimit); err != nil {
		t.Fatal(err)
	}
	binding := checkpointBinding(t, target)
	blob, err := encodeHashCheckpoint(binding, target, session.core.checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	base := checkpointEnvelope(t, blob)
	for _, name := range []string{"state", "short_digest", "uppercase_digest", "missing_digest", "wrong_offset", "no_baseline"} {
		t.Run(name, func(t *testing.T) {
			envelope := base
			switch name {
			case "state":
				envelope.State, err = initialHashSHAState()
				if err != nil {
					t.Fatal(err)
				}
			case "short_digest":
				envelope.SHA256 = envelope.SHA256[:63]
			case "uppercase_digest":
				envelope.SHA256 = strings.ToUpper(envelope.SHA256)
			case "missing_digest":
				envelope.SHA256 = ""
			case "wrong_offset":
				envelope.Offset--
			case "no_baseline":
				envelope.Baseline = false
			}
			if _, err := decodeHashCheckpoint(checkpointRechecksum(t, envelope), binding, target); err == nil {
				t.Fatal("invalid complete record accepted")
			}
		})
	}
	decoded, err := decodeHashCheckpoint(blob, binding, target)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restoreHashSession(s, target, decoded); err == nil {
		t.Fatal("complete record is resumable")
	}
	_, partialTarget, partialSession, _ := checkpointFixture(t, 63)
	falseComplete := partialSession.core.checkpoint
	falseComplete.complete = true
	falseComplete.offset = partialTarget.File.Size
	if _, err := encodeHashCheckpoint(checkpointBinding(t, partialTarget), partialTarget, falseComplete); err == nil {
		t.Fatal("completed encoder accepted a digest whose actual count differs from claimed full size")
	}
}

func TestHashCheckpointBindingCanonicalBounds(t *testing.T) {
	_, target, session, _ := checkpointFixture(t, 65)
	binding := checkpointBinding(t, target)
	for _, name := range []string{"short_store", "uppercase_selection", "work_zero", "work_21", "work_leading_zero", "work_sign", "negative_sequence", "wrong_target_digest"} {
		t.Run(name, func(t *testing.T) {
			changed := binding
			switch name {
			case "short_store":
				changed.StoreID = changed.StoreID[:63]
			case "uppercase_selection":
				changed.SelectionID = strings.ToUpper(changed.SelectionID)
			case "work_zero":
				changed.WorkID = "0"
			case "work_21":
				changed.WorkID = "21"
			case "work_leading_zero":
				changed.WorkID = "01"
			case "work_sign":
				changed.WorkID = "+1"
			case "negative_sequence":
				changed.Sequence = -1
			case "wrong_target_digest":
				changed.TargetDigest = strings.Repeat("c", 64)
			}
			if _, err := encodeHashCheckpoint(changed, target, session.core.checkpoint); err == nil {
				t.Fatal("encoder accepted a noncanonical or mismatched binding")
			}
		})
	}
}
