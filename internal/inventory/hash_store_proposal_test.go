package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func hashProposalFixtureWriter(t *testing.T) *HashStore {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := OpenHashSelectionWriter(context.Background(), filepath.Join(base, "proposal-data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func requireHashProposalClaims(t *testing.T, p HashProposal) {
	t.Helper()
	if p.Source != "saved_hash_selection" || p.Contract != FileHashContract || p.ProvenanceVerified || p.ContentVerified || p.CurrentStateVerified || p.DuplicatesVerified || p.Executable || p.EstimatedReclaimableBytes != nil {
		t.Fatal("proposal claimed current state, content, consent or savings", p)
	}
}

func TestHashManualProposalExactEvidenceAndImmutableRetry(t *testing.T) {
	f := hashStoreFixture(t, []byte("first fixture"), []byte("second fixture"))
	s := hashProposalFixtureWriter(t)
	ctx := context.Background()
	expected := []state.SameSizeFile{f.expected[1], f.expected[0]}
	root := []byte(f.root)
	p, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, expected, root)
	if err != nil {
		t.Fatal(err)
	}
	requireHashProposalClaims(t, p)
	if !hashStoreDigest(p.StoreID) || !hashStoreDigest(p.SelectionID) || p.InventoryID != f.inventoryID || p.SourceLocator == nil || p.SourceLocator.Kind != "manual_inventory_v1" || !bytes.Equal(p.SourceLocator.RootPathBytes, root) || p.SourceLocator.InventoryKey != fmt.Sprintf("%x", sha256.Sum256(root)) {
		t.Fatal("incorrect selection locator", p)
	}
	want, err := f.source.PrepareFileSampleSelection(ctx, f.inventoryID, expected)
	if err != nil || !reflect.DeepEqual(p.Targets, want) {
		t.Fatal("capture differs or order changed", p.Targets, want, err)
	}
	var payload []byte
	if err = s.db.QueryRow("SELECT payload FROM hash_selection WHERE id=1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var record hashSelectionRecord
	if err = json.Unmarshal(payload, &record); err != nil || record.Version != 2 || !validHashSelectionSource(record) {
		t.Fatal("manual record is not version 2", record, err)
	}
	before := hashStoreSnapshot(t, s)
	again, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, expected, root)
	if err != nil || !reflect.DeepEqual(again, p) || !reflect.DeepEqual(before, hashStoreSnapshot(t, s)) {
		t.Fatal("retry replaced selection or work", again, err)
	}
	for _, changed := range [][]state.SameSizeFile{f.expected, expected[:1]} {
		if _, err = s.CreateManualSelection(ctx, f.source, f.inventoryID, changed, root); !errors.Is(err, ErrHashSelectionConflict) {
			t.Fatal("different selection replaced immutable batch", err)
		}
	}
	if _, err = s.CreateSelection(ctx, f.source, f.inventoryID, expected); !errors.Is(err, ErrHashSelectionConflict) {
		t.Fatal("legacy retry silently converted manual selection", err)
	}
	if !reflect.DeepEqual(before, hashStoreSnapshot(t, s)) {
		t.Fatal("conflict modified saved work")
	}
	if other, err := OpenHashSelectionWriter(ctx, s.base); !errors.Is(err, localfs.ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("selection writer is not exclusive", err)
	}
	if _, err = s.RunNext(ctx, f.source, f.scanner, 64, 64); err == nil {
		t.Fatal("selection writer dispatched hashing")
	}
	if !reflect.DeepEqual(before, hashStoreSnapshot(t, s)) {
		t.Fatal("refused dispatch modified saved work")
	}
}

func TestHashManualProposalIndependentCopiesAndOfflineCapture(t *testing.T) {
	f := hashStoreFixture(t, []byte("fixture"))
	s := hashProposalFixtureWriter(t)
	ctx := context.Background()
	// Remove source availability before capture. Saved inventory is enough.
	offline := f.root + "-offline"
	if err := os.Rename(f.root, offline); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(offline, f.root); err != nil {
			t.Error(err)
		}
	}()
	root := []byte(f.root)
	p, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, f.expected, root)
	if err != nil {
		t.Fatal("offline capture tried to resolve source paths", err)
	}
	want, err := s.Proposal(ctx, p.SelectionID)
	if err != nil {
		t.Fatal(err)
	}
	root[0] = 'x'
	p.SourceLocator.RootPathBytes[0] = 'x'
	p.Targets[0].Root.PathBytes[0] = 'x'
	p.Targets[0].File.PathBytes[0] = 'x'
	p.Targets[0].Ancestors[0].Path[0] = 'x'
	p.Targets[0].Ancestors[0].Device = "999"
	f.expected[0].PathBytes[0] = 'x'
	if err = f.source.Close(); err != nil {
		t.Fatal(err)
	}
	f.scanner.Close()
	stateOffline := f.stateDir + "-offline"
	if err = os.Rename(f.stateDir, stateOffline); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Rename(stateOffline, f.stateDir); err != nil {
			t.Error(err)
		}
	}()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenHashReader(ctx, s.base)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	shown, err := r.Proposal(ctx, want.SelectionID)
	if err != nil || !reflect.DeepEqual(shown, want) {
		t.Fatal("offline saved proposal changed or depended on source", shown, err)
	}
	requireHashProposalClaims(t, shown)
	encoded, err := json.Marshal(shown)
	if err != nil || bytes.Contains(encoded, []byte("checkpoint")) || bytes.Contains(encoded, []byte("offset")) {
		t.Fatal("proposal leaked continuation state", string(encoded), err)
	}
}

func TestHashManualProposalLegacySelectionAndIDContract(t *testing.T) {
	f := hashStoreFixture(t, []byte("fixture"))
	ctx := context.Background()
	snapshot := hashStoreSnapshot(t, f.store)
	p, err := f.store.Proposal(ctx, snapshot.SelectionID)
	if err != nil || p.SourceLocator != nil || p.SelectionID != snapshot.SelectionID || p.InventoryID != f.inventoryID || len(p.Targets) != 1 || !bytes.Equal(p.Targets[0].File.PathBytes, f.expected[0].PathBytes) {
		t.Fatal("legacy selection is not reportable", p, err)
	}
	requireHashProposalClaims(t, p)
	for _, invalid := range []string{"", snapshot.SelectionID[:12], strings.ToUpper(snapshot.SelectionID), strings.Repeat("g", 64), snapshot.SelectionID + "\n"} {
		if _, err = f.store.Proposal(ctx, invalid); !errors.Is(err, ErrHashSelectionID) {
			t.Fatal("invalid ID was not rejected", invalid, err)
		}
	}
	other := strings.Repeat("0", 64)
	if other == snapshot.SelectionID {
		other = strings.Repeat("1", 64)
	}
	if _, err = f.store.Proposal(ctx, other); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("wrong full ID was accepted", err)
	}
	if _, err = f.store.CreateManualSelection(ctx, f.source, f.inventoryID, f.expected, []byte(f.root)); !errors.Is(err, ErrHashSelectionConflict) {
		t.Fatal("legacy selection was replaced with version 2", err)
	}
	if !reflect.DeepEqual(snapshot, hashStoreSnapshot(t, f.store)) {
		t.Fatal("proposal changed legacy work")
	}
	empty := hashProposalFixtureWriter(t)
	if _, err = empty.Proposal(ctx, other); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("empty store invented proposal", err)
	}
}

func TestHashManualProposalRawRootAndFileBytes(t *testing.T) {
	for _, name := range []string{"line\nquote\"雪", string([]byte{'r', 0xff, 'w'})} {
		t.Run(fmt.Sprintf("%x", name), func(t *testing.T) {
			parent, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(parent, name)
			requireSampleFixtureFilename(t, name, os.Mkdir(root, 0700))
			path := filepath.Join(root, name)
			requireSampleFixtureFilename(t, name, os.WriteFile(path, []byte("fixture"), 0600))
			scanner, err := New([]string{root}, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(scanner.Close)
			f := hashStoreFixtureFromFiles(t, scanner, captureSampleTargets(t, scanner, root, []string{path}), [][]byte{[]byte("fixture")})
			s := hashProposalFixtureWriter(t)
			p, err := s.CreateManualSelection(context.Background(), f.source, f.inventoryID, f.expected, []byte(root))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(p.SourceLocator.RootPathBytes, []byte(root)) || p.SourceLocator.InventoryKey != fmt.Sprintf("%x", sha256.Sum256([]byte(root))) || !bytes.Equal(p.Targets[0].Root.PathBytes, []byte(root)) || !bytes.Equal(p.Targets[0].File.PathBytes, []byte(path)) || p.Targets[0].File.Path != path {
				t.Fatal("raw source locator or target was lossy", p)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			r, err := OpenHashReader(context.Background(), s.base)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			shown, err := r.Proposal(context.Background(), p.SelectionID)
			if err != nil || !reflect.DeepEqual(shown, p) {
				t.Fatal("raw paths changed on read", shown, err)
			}
		})
	}
}

func TestHashManualProposalRefusesChangedEvidenceAndBounds(t *testing.T) {
	f := hashStoreFixture(t, []byte("fixture"))
	s := hashProposalFixtureWriter(t)
	ctx := context.Background()
	for _, root := range [][]byte{nil, []byte("relative"), []byte(f.root + "/.."), []byte(f.root + "/"), []byte(f.root + "\x00"), []byte("/" + strings.Repeat("x", 4096)), []byte(f.root + "-wrong")} {
		if _, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, f.expected, root); !errors.Is(err, ErrHashManualRoot) {
			t.Fatal("invalid/mismatched root was accepted", fmt.Sprintf("%x", root), err)
		}
	}
	changed := append([]state.SameSizeFile(nil), f.expected...)
	changed[0].Size++
	if _, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, changed, []byte(f.root)); !errors.Is(err, state.ErrFileSampleEvidence) {
		t.Fatal("changed row accepted", err)
	}
	if _, err := s.CreateManualSelection(ctx, f.source, strings.Repeat("0", 64), f.expected, []byte(f.root)); !errors.Is(err, state.ErrFileSampleEvidence) {
		t.Fatal("stale inventory accepted", err)
	}
	for _, rows := range [][]state.SameSizeFile{nil, {f.expected[0], f.expected[0]}, make([]state.SameSizeFile, 21)} {
		if _, err := s.CreateManualSelection(ctx, f.source, f.inventoryID, rows, []byte(f.root)); !errors.Is(err, state.ErrFileSampleSelection) {
			t.Fatal("invalid exact selection accepted", err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.CreateManualSelection(canceled, f.source, f.inventoryID, f.expected, []byte(f.root)); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	if hashStoreSnapshot(t, s).SelectionID != "" {
		t.Fatal("refused proposal was published")
	}
}

func TestHashSelectionSourceRecordValidation(t *testing.T) {
	f := hashStoreFixture(t, []byte("fixture"))
	targets, err := f.source.PrepareFileSampleSelection(context.Background(), f.inventoryID, f.expected)
	if err != nil {
		t.Fatal(err)
	}
	root := []byte(f.root)
	valid := hashSelectionRecord{Version: 2, Targets: targets, SourceLocator: &HashSourceLocator{Kind: "manual_inventory_v1", RootPathBytes: root, InventoryKey: hashManualInventoryKey(root)}}
	for _, tc := range []struct {
		name   string
		change func(*hashSelectionRecord)
	}{
		{"missing_locator", func(r *hashSelectionRecord) { r.SourceLocator = nil }},
		{"legacy_locator", func(r *hashSelectionRecord) { r.Version = 1 }},
		{"unknown_version", func(r *hashSelectionRecord) { r.Version = 3 }},
		{"unknown_kind", func(r *hashSelectionRecord) { r.SourceLocator.Kind = "manual_inventory_v2" }},
		{"wrong_key", func(r *hashSelectionRecord) { r.SourceLocator.InventoryKey = strings.Repeat("0", 64) }},
		{"unclean_root", func(r *hashSelectionRecord) { r.SourceLocator.RootPathBytes = []byte(f.root + "/") }},
		{"oversized_root", func(r *hashSelectionRecord) { r.SourceLocator.RootPathBytes = []byte("/" + strings.Repeat("x", 4096)) }},
		{"different_target_root", func(r *hashSelectionRecord) { r.Targets[0].Root.PathBytes = []byte(f.root + "-other") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := valid
			r.Targets = []SavedFileTarget{cloneHashTarget(valid.Targets[0])}
			locator := *valid.SourceLocator
			locator.RootPathBytes = bytes.Clone(root)
			r.SourceLocator = &locator
			tc.change(&r)
			if validHashSelectionSource(r) {
				t.Fatal("malformed version/locator accepted", r)
			}
		})
	}
	if !validHashSelectionSource(valid) || !validHashManualRoot([]byte("/"+strings.Repeat("x", 4095))) {
		t.Fatal("valid version or boundary root rejected")
	}
	legacy := valid
	legacy.Version = 1
	legacy.SourceLocator = nil
	if !validHashSelectionSource(legacy) {
		t.Fatal("legacy source record rejected")
	}
}

func TestHashSelectionWriterNeverRecoversRunningAttempt(t *testing.T) {
	f := hashStoreFixture(t, fullHashContents(129))
	s := hashProposalFixtureWriter(t)
	p, err := s.CreateManualSelection(context.Background(), f.source, f.inventoryID, f.expected, []byte(f.root))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	w, err := OpenHashWriter(context.Background(), s.base)
	if err != nil {
		t.Fatal(err)
	}
	w.now = f.store.now
	if _, err = w.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	result, err := w.RunNext(context.Background(), f.source, f.scanner, 64, 128)
	if !errors.Is(err, ErrHashRecoveryRequired) || result.Usage.ReadBytes != 64 {
		t.Fatal("fixture did not leave reserved work", result, err)
	}
	before := hashStoreSnapshot(t, w)
	if before.Work[0].Status != "running" || before.Work[0].Sequence != 0 || before.Work[0].LatestAttempt.Status != "reserved" || before.Work[0].LatestAttempt.ReadBytes != nil || before.Budget.TotalReservedBytes != 64 {
		t.Fatal("fixture did not retain old checkpoint and charge", before)
	}
	if _, err = w.db.Exec("DROP TRIGGER fail_checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	selection, err := OpenHashSelectionWriter(context.Background(), s.base)
	if err != nil {
		t.Fatal(err)
	}
	defer selection.Close()
	if !reflect.DeepEqual(before, hashStoreSnapshot(t, selection)) {
		t.Fatal("opening selection writer recovered an attempt")
	}
	shown, err := selection.Proposal(context.Background(), p.SelectionID)
	if err != nil || !reflect.DeepEqual(shown, p) {
		t.Fatal("running proposal not reportable", shown, err)
	}
	again, err := selection.CreateManualSelection(context.Background(), f.source, f.inventoryID, f.expected, []byte(f.root))
	if err != nil || !reflect.DeepEqual(again, p) {
		t.Fatal("manual retry recovered or changed proposal", again, err)
	}
	opened := false
	if _, err = selection.runNext(context.Background(), f.source, f.scanner, 64, 128, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}}); err == nil || opened {
		t.Fatal("selection-only writer resumed source work", err)
	}
	if !reflect.DeepEqual(before, hashStoreSnapshot(t, selection)) {
		t.Fatal("metadata operations modified running attempt or counters")
	}
	if err = selection.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenHashWriter(context.Background(), s.base)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	after := hashStoreSnapshot(t, recovered)
	if after.Work[0].Status != "pending" || after.Work[0].Sequence != 1 || after.Work[0].LatestAttempt.Status != "interrupted_unknown" || after.Budget.TotalUnknownReservedBytes != 64 {
		t.Fatal("normal writer no longer recovers reserved work", after)
	}
}
