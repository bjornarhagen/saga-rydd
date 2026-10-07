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
	"time"
)

// Every file, inventory and historical observation in this file is generated.
// Content reads occur only during fixture setup, never request preparation.
type hashChoiceRequestFixture struct {
	f        *hashStoreTestFixture
	saved    SavedHashKeeperChoice
	consent  HashReadConsent
	proposal HashProposal
}

func hashChoiceRequestFiles(t *testing.T, count, completed int, keeper string, copies ...string) *hashChoiceRequestFixture {
	t.Helper()
	contents := make([][]byte, count)
	for i := range contents {
		contents[i] = fullHashContents(65)
	}
	f, approval := hashReadFixture(t, contents...)
	consent := hashReadApprove(t, f, approval)
	for range completed {
		if _, err := f.store.RunConsented(context.Background(), consent.ID, f.source, f.scanner); err != nil {
			t.Fatal(err)
		}
	}
	proposal, err := f.store.Proposal(context.Background(), approval.SelectionID)
	if err != nil {
		t.Fatal(err)
	}
	preview := hashKeeperPreview(t, f.store, approval.SelectionID, keeper, copies...)
	w := hashChoiceWriter(t, f)
	saved := saveHashChoice(t, w, preview)
	proposal.ReadConsent = cloneHashKeeperChoiceFreshConsent(saved.Record.Evidence.ReadConsent)
	return &hashChoiceRequestFixture{f: f, saved: saved, consent: consent, proposal: proposal}
}

// Inspect private application records directly rather than triggering recovery
// or source observation while checking that a saved-only request changed none.
func hashChoiceRequestSavedState(t *testing.T, f *hashStoreTestFixture) (HashSnapshot, SavedHashKeeperChoice, []byte, int) {
	t.Helper()
	snapshot := hashStoreSnapshot(t, f.store)
	var payload []byte
	var id string
	if err := f.store.db.QueryRow("SELECT id,payload FROM hash_keeper_choice ORDER BY id LIMIT 1").Scan(&id, &payload); err != nil {
		t.Fatal(err)
	}
	saved, err := f.store.KeeperChoice(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot, saved, bytes.Clone(payload), hashChoiceCount(t, f.store.db)
}

func requireHashChoiceFreshRequest(t *testing.T, request *KeeperChoiceFreshRequest, m *hashChoiceRequestFixture) HashKeeperChoiceFreshRequestReport {
	t.Helper()
	if request == nil {
		t.Fatal("saved request preparation returned nil")
	}
	r := request.Report()
	if r.Version != 1 || r.Contract != "choice_bound_fresh_full_hash_request_v1" || r.HashContract != FileHashContract || r.RequestID != request.ID() || !ValidHashKeeperChoiceFreshRequestID(request.ID()) || r.Source != "saved_hash_choice" || r.Status != "unapproved" || r.ChoiceID != m.saved.ID || r.StoreID != m.proposal.StoreID || r.SelectionID != m.proposal.SelectionID || r.InventoryID != m.proposal.InventoryID || r.ApprovalAvailable || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("fresh request lost historical binding or claimed read/action authority", r)
	}
	if !reflect.DeepEqual(r.HistoricalChoice, m.saved) || !reflect.DeepEqual(request.SourceLocator(), *m.proposal.SourceLocator) || !reflect.DeepEqual(r.SourceLocator, *m.proposal.SourceLocator) {
		t.Fatal("fresh request refreshed archived choice or substituted manual locator", r)
	}
	members := append([]SavedHashPreviewMember{m.saved.Record.Evidence.Keeper}, m.saved.Record.Evidence.Copies...)
	if len(r.Targets) != len(members) {
		t.Fatal("fresh request expanded or dropped selected roles", r)
	}
	for i, selected := range r.Targets {
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		ordinal := 0
		if _, err := fmt.Sscanf(members[i].WorkID, "%d", &ordinal); err != nil {
			t.Fatal(err)
		}
		if selected.Role != role || !reflect.DeepEqual(selected.Observation, members[i]) || !reflect.DeepEqual(selected.Target, m.proposal.Targets[ordinal-1]) {
			t.Fatal("fresh request remapped explicit identity/order or omitted full frozen ancestry", selected, members[i])
		}
	}
	payload, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"approval_available":false`, `"provenance_verified":false`, `"content_verified":false`, `"current_state_verified":false`, `"duplicates_verified":false`, `"executable":false`, `"estimated_reclaimable_bytes":null`} {
		if !bytes.Contains(payload, []byte(required)) {
			t.Fatal("request omitted an explicit unavailable-authority qualifier", required)
		}
	}
	for _, forbidden := range []string{`"checkpoint"`, `"hash_state"`, `"sha\u0003`} {
		if bytes.Contains(payload, []byte(forbidden)) {
			t.Fatal("fresh request retained SHA continuation state", forbidden)
		}
	}
	return r
}

func TestHashKeeperChoiceFreshRequestExactRolesAndClonedFrozenEvidence(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	before, choice, payload, count := hashChoiceRequestSavedState(t, m.f)
	request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := requireHashChoiceFreshRequest(t, request, m)
	wantProposal := request.Proposal()
	if !reflect.DeepEqual(wantProposal, m.proposal) {
		t.Fatal("request omitted unselected proposal evidence needed by later alias guards", wantProposal)
	}
	// Mutate every returned nested byte/evidence branch. None may redirect the
	// opaque request, even through a copied public wrapper.
	r := request.Report()
	r.Targets[0].Target.File.PathBytes[0] = 'x'
	r.Targets[0].Target.Root.PathBytes[0] = 'y'
	r.Targets[0].Target.Ancestors[0].Path[0] = 'z'
	r.Targets[0].Target.Ancestors[0].Inode = "999999"
	r.Targets[0].Observation.PathBytes[0] = 'q'
	r.Targets[1].Target.File.PathBytes[0] = 'w'
	r.HistoricalChoice.Record.Evidence.Keeper.PathBytes[0] = 'a'
	r.HistoricalChoice.Record.Evidence.Copies[0].PathBytes[0] = 'b'
	r.HistoricalChoice.Record.Evidence.Budget.TotalReservedBytes = -1
	r.HistoricalChoice.Record.Evidence.ReadConsent.Approval.SourceLocator.RootPathBytes[0] = 'c'
	r.SourceLocator.RootPathBytes[0] = 'd'
	p := request.Proposal()
	p.Targets[3].File.PathBytes[0] = 'e'
	p.Targets[0].Ancestors[0].Path[0] = 'f'
	p.SourceLocator.RootPathBytes[0] = 'g'
	p.ReadConsent.Approval.SourceLocator.RootPathBytes[0] = 'h'
	locator := request.SourceLocator()
	locator.RootPathBytes[0] = 'i'
	locator.InventoryKey = strings.Repeat("0", 64)
	copied := *request
	if !reflect.DeepEqual(copied.Report(), want) || !reflect.DeepEqual(copied.Proposal(), wantProposal) || !reflect.DeepEqual(copied.SourceLocator(), *m.proposal.SourceLocator) {
		t.Fatal("caller mutation escaped an accessor into frozen request state")
	}
	after, afterChoice, afterPayload, afterCount := hashChoiceRequestSavedState(t, m.f)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(choice, afterChoice) || !bytes.Equal(payload, afterPayload) || count != afterCount {
		t.Fatal("request preparation or accessor changed work, consent, charge or saved choice")
	}
}

func TestHashKeeperChoiceFreshRequestIdentityPreservesArchivedContext(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	first, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	original := requireHashChoiceFreshRequest(t, first, m)
	if err = m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.store, err = OpenExistingHashWriter(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.CreatedAt.Add(time.Hour) }
	run, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if err != nil || run.WorkID != "4" || run.Progress.Status != "hash_observed" {
		t.Fatal("fixture did not advance unrelated work and charges", run, err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.ExpiresAt.Add(time.Hour) }
	if _, err = m.f.store.RevokeRead(context.Background(), m.consent.ID); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, m.f.store)
	if before.Budget.TotalReservedBytes == original.HistoricalChoice.Record.Evidence.Budget.TotalReservedBytes || before.ReadConsent.Status != "revoked" {
		t.Fatal("fixture did not change mutable whole-selection context", before)
	}
	second, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil || second.ID() != first.ID() || !reflect.DeepEqual(second.Report(), original) {
		t.Fatal("unrelated progress, expired/revoked old consent or clock created a new request identity/context", second, err)
	}
	if !reflect.DeepEqual(hashStoreSnapshot(t, m.f.store), before) {
		t.Fatal("request renewed old consent or reset accounting")
	}
	// A distinct explicit copy order is a distinct immutable request.
	other := saveHashChoice(t, m.f.store, hashKeeperPreview(t, m.f.store, m.proposal.SelectionID, "3", "1", "2"))
	reordered, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), other.ID)
	if err != nil || reordered.ID() == first.ID() || reordered.Report().Targets[1].Observation.WorkID != "1" {
		t.Fatal("request identity ignored explicit copy order", reordered, err)
	}
	// Saved storage is not the source scope or a future job identity. Moving
	// its disposable private directory must not redefine an exact request.
	if err = m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	moved := filepath.Join(filepath.Dir(m.f.base), "moved-generated-hash-state")
	if err = os.Rename(m.f.base, moved); err != nil {
		t.Fatal(err)
	}
	m.f.base = moved
	m.f.store, err = OpenHashReader(context.Background(), moved)
	if err != nil {
		t.Fatal(err)
	}
	relocated, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil || relocated.ID() != first.ID() || !reflect.DeepEqual(relocated.Report(), original) {
		t.Fatal("private storage location became source scope or a request/job identity", relocated, err)
	}
}

func TestHashKeeperChoiceFreshRequestOfflineMalformedConfigAndNoInitialization(t *testing.T) {
	m := hashChoiceRequestFiles(t, 3, 3, "3", "1")
	if err := m.f.source.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.scanner.Close()
	if err := os.RemoveAll(m.f.root); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(m.f.stateDir); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(m.f.base, "config.toml")
	invalid := []byte("not [ valid TOML; GENERATED ONLY")
	if err := os.WriteFile(configPath, invalid, 0600); err != nil {
		t.Fatal(err)
	}
	before, choice, payload, count := hashChoiceRequestSavedState(t, m.f)
	request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil {
		t.Fatal("saved-only request needed source, inventory or configuration", err)
	}
	_ = requireHashChoiceFreshRequest(t, request, m)
	for _, path := range []string{m.f.root, m.f.stateDir} {
		if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("saved request initialized absent source/inventory", path, err)
		}
	}
	if got, e := os.ReadFile(configPath); e != nil || !bytes.Equal(got, invalid) {
		t.Fatal("request changed malformed configuration", e)
	}
	after, afterChoice, afterPayload, afterCount := hashChoiceRequestSavedState(t, m.f)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(choice, afterChoice) || !bytes.Equal(payload, afterPayload) || count != afterCount {
		t.Fatal("offline request changed historical storage")
	}
	missing := filepath.Join(t.TempDir(), "must-not-exist")
	if r, e := OpenHashReader(context.Background(), missing); e == nil || r != nil {
		t.Fatal("saved request reader initialized absent storage", r, e)
	}
	if _, e := os.Lstat(missing); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("saved reader created missing private directory", e)
	}
}

func TestHashKeeperChoiceFreshRequestReservedAttemptAndWriterHeldRemainUnchanged(t *testing.T) {
	m := hashChoiceRequestFiles(t, 4, 3, "3", "2", "1")
	first, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.f.store.Close(); err != nil {
		t.Fatal(err)
	}
	m.f.store, err = OpenExistingHashWriter(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	m.f.store.now = func() time.Time { return m.consent.Approval.CreatedAt.Add(time.Hour) }
	if _, err = m.f.store.db.Exec("CREATE TRIGGER fail_fresh_request_fixture BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
		t.Fatal(err)
	}
	run, err := m.f.store.RunConsented(context.Background(), m.consent.ID, m.f.source, m.f.scanner)
	if !errors.Is(err, ErrHashRecoveryRequired) || run.Usage.ReadBytes != 65 {
		t.Fatal("fixture did not leave charged unsettled work", run, err)
	}
	if _, err = m.f.store.db.Exec("DROP TRIGGER fail_fresh_request_fixture"); err != nil {
		t.Fatal(err)
	}
	w := hashChoiceWriter(t, m.f) // Holds the writer lock without recovery.
	before := hashStoreSnapshot(t, w)
	a := before.Work[3].LatestAttempt
	if before.Work[3].Status != "running" || a == nil || a.Status != "reserved" || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil || before.Budget.TotalReservedBytes != 4*65 || before.Budget.TotalReadBytes != 3*65 || before.Budget.TotalUnknownReservedBytes != 0 {
		t.Fatal("fixture lost the reserved/null versus interrupted distinction", before)
	}
	reader, err := OpenHashReader(context.Background(), m.f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	request, err := reader.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
	if err != nil || request.ID() != first.ID() {
		t.Fatal("saved-only request waited for writer, recovered work or changed identity", request, err)
	}
	_ = requireHashChoiceFreshRequest(t, request, m)
	if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) || !reflect.DeepEqual(hashStoreSnapshot(t, reader), before) {
		t.Fatal("saved request reconciled, refunded or changed reserved work")
	}
}

func TestHashKeeperChoiceFreshRequestMissingLegacyAndClosedRefuseWholeRequest(t *testing.T) {
	var absent *HashStore
	valid := "hash-choice-v1-" + strings.Repeat("a", 64)
	for _, id := range []string{"", "bad", valid[:len(valid)-1], valid + "\n", strings.ToUpper(valid)} {
		if request, err := absent.PrepareKeeperChoiceFreshRequest(context.Background(), id); request != nil || !errors.Is(err, ErrHashKeeperChoiceID) {
			t.Fatal("invalid choice reached saved storage", request, err)
		}
	}
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("legacy_schema_%d", version), func(t *testing.T) {
			f := hashChoiceFixture(t, []byte("same"), []byte("same"))
			w := hashChoiceWriter(t, f)
			saved := saveHashChoice(t, w, hashKeeperPreview(t, w, hashStoreSnapshot(t, w).SelectionID, "2", "1"))
			hashChoiceDowngrade(t, f, version)
			reader, err := OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			if request, e := reader.PrepareKeeperChoiceFreshRequest(context.Background(), saved.ID); request != nil || !errors.Is(e, os.ErrNotExist) {
				t.Fatal("missing old-schema choice created a request", request, e)
			}
			if hashChoiceSchemaVersion(t, reader.db) != version || hashChoiceTableExists(t, reader.db, "hash_keeper_choice") {
				t.Fatal("saved-only preparation migrated legacy storage")
			}
		})
	}
	t.Run("unsupported_locator", func(t *testing.T) {
		f := hashChoiceFixture(t, []byte("same"), []byte("same"))
		w := hashChoiceWriter(t, f)
		saved := saveHashChoice(t, w, hashKeeperPreview(t, w, hashStoreSnapshot(t, w).SelectionID, "2", "1"))
		before := hashStoreSnapshot(t, w)
		if request, err := w.PrepareKeeperChoiceFreshRequest(context.Background(), saved.ID); request != nil || !errors.Is(err, ErrHashKeeperChoiceFreshRequestLocator) {
			t.Fatal("legacy choice synthesized a new source locator", request, err)
		}
		if !reflect.DeepEqual(hashStoreSnapshot(t, w), before) {
			t.Fatal("unsupported locator refusal changed saved work")
		}
	})
	t.Run("missing_and_closed", func(t *testing.T) {
		m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
		if request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), valid); request != nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatal("missing exact choice resolved to another row", request, err)
		}
		if err := m.f.store.Close(); err != nil {
			t.Fatal(err)
		}
		if request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID); request != nil || err == nil {
			t.Fatal("closed storage returned frozen request", request, err)
		}
	})
}

func TestHashKeeperChoiceFreshRequestCorruptEvidenceAndBoundsRefusePartialRequest(t *testing.T) {
	for _, kind := range []string{"choice_oversized", "choice_checksumming", "forged_selected_sha", "forged_selected_sequence", "authority", "selection_oversized"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
			w := m.f.store
			id := m.saved.ID
			if kind == "selection_oversized" {
				if _, err := w.db.Exec("DROP TRIGGER hash_selection_no_update; PRAGMA ignore_check_constraints=ON"); err != nil {
					t.Fatal(err)
				}
				payload := bytes.Repeat([]byte{'x'}, (1<<20)+1)
				if _, err := w.db.Exec("UPDATE hash_selection SET selection_id=?,payload=? WHERE id=1", fmt.Sprintf("%x", sha256.Sum256(payload)), payload); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := w.db.Exec("DROP TRIGGER hash_keeper_choice_no_update; PRAGMA ignore_check_constraints=ON"); err != nil {
					t.Fatal(err)
				}
				record := m.saved.Record
				if kind == "forged_selected_sha" {
					record.Evidence.SHA256 = strings.Repeat("0", 64)
				}
				if kind == "forged_selected_sequence" {
					record.Evidence.Copies[0].Sequence++
				}
				if kind == "authority" {
					record.Evidence.Executable = true
				}
				payload, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "choice_oversized" {
					payload = bytes.Repeat([]byte{'x'}, HashKeeperChoiceMaxRecordBytes+1)
				}
				key, err := hashKeeperChoiceRequestKey(record.Evidence)
				if err != nil {
					t.Fatal(err)
				}
				if kind != "choice_checksumming" {
					id = fmt.Sprintf("hash-choice-v1-%x", sha256.Sum256(payload))
				} else {
					payload[len(payload)-1] = 'x'
				}
				if _, err = w.db.Exec("UPDATE hash_keeper_choice SET id=?,request_key=?,payload=? WHERE id=?", id, key, payload, m.saved.ID); err != nil {
					t.Fatal(err)
				}
			}
			request, err := w.PrepareKeeperChoiceFreshRequest(context.Background(), id)
			if request != nil || err == nil {
				t.Fatal("corrupt, forged or oversized saved evidence yielded a partial request", request, err)
			}
			if kind != "selection_oversized" && !errors.Is(err, ErrHashKeeperChoiceCorrupt) {
				t.Fatal("choice corruption lost its stable refusal", err)
			}
			if kind == "selection_oversized" && !errors.Is(err, ErrHashStoreCorrupt) {
				t.Fatal("oversized selection was materialized as usable evidence", err)
			}
		})
	}
}

func TestHashKeeperChoiceFreshRequestCancellationAndReplacedStorage(t *testing.T) {
	for _, phase := range []string{"already_canceled", "after_snapshot", "after_commit", "wait_gate", "storage_before", "storage_after_commit"} {
		t.Run(phase, func(t *testing.T) {
			m := hashChoiceRequestFiles(t, 2, 2, "2", "1")
			before := hashStoreSnapshot(t, m.f.store)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashKeeperChoiceFreshRequestHooks{}
			if phase == "already_canceled" {
				cancel()
			}
			if phase == "after_snapshot" {
				hooks.afterSnapshot = cancel
			}
			if phase == "after_commit" {
				hooks.afterCommit = cancel
			}
			if phase == "wait_gate" {
				m.f.store.mu.Lock()
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				request, err := m.f.store.PrepareKeeperChoiceFreshRequest(ctx, m.saved.ID)
				m.f.store.mu.Unlock()
				if request != nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("canceled gate waiter returned a usable request", request, err)
				}
				if !reflect.DeepEqual(hashStoreSnapshot(t, m.f.store), before) {
					t.Fatal("canceled gate waiter changed saved records")
				}
				return
			}
			if strings.HasPrefix(phase, "storage_") {
				path := filepath.Join(m.f.base, "hashes", hashStoreFilename)
				replace := func() {
					if err := os.Rename(path, path+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte("GENERATED private replacement"), 0600); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						_ = os.Remove(path)
						if err := os.Rename(path+"-old", path); err != nil {
							t.Error(err)
						}
					})
				}
				if phase == "storage_before" {
					replace()
				} else {
					hooks.afterCommit = replace
				}
			}
			request, err := m.f.store.prepareKeeperChoiceFreshRequest(ctx, m.saved.ID, hooks)
			if request != nil || err == nil {
				t.Fatal("canceled or replaced-storage request escaped the final veto", request, err)
			}
			if !strings.HasPrefix(phase, "storage_") {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("late preparation cancellation lost context error", err)
				}
				if !reflect.DeepEqual(hashStoreSnapshot(t, m.f.store), before) {
					t.Fatal("canceled request changed saved work or consent")
				}
			}
		})
	}
}

func TestHashKeeperChoiceFreshRequestRawPathsAndMaximumExplicitScope(t *testing.T) {
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
			request, err := f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
			if err != nil {
				t.Fatal(err)
			}
			r := requireHashChoiceFreshRequest(t, request, m)
			if !bytes.Equal(r.Targets[1].Target.File.PathBytes, []byte(paths[0])) || !bytes.Equal(r.Targets[1].Observation.PathBytes, []byte(paths[0])) {
				t.Fatal("fresh request replaced authoritative raw path bytes")
			}
		})
	}
	t.Run("twenty_chosen_targets", func(t *testing.T) {
		copies := make([]string, 19)
		for i := range copies {
			copies[i] = fmt.Sprint(19 - i)
		}
		m := hashChoiceRequestFiles(t, 20, 20, "20", copies...)
		request, err := m.f.store.PrepareKeeperChoiceFreshRequest(context.Background(), m.saved.ID)
		if err != nil {
			t.Fatal("maximum bounded explicit request was rejected", err)
		}
		r := requireHashChoiceFreshRequest(t, request, m)
		if len(r.Targets) != 20 || r.Targets[0].Observation.WorkID != "20" || r.Targets[19].Observation.WorkID != "1" {
			t.Fatal("maximum request reordered or dropped explicit roles", r)
		}
	})
	valid := "hash-choice-request-v1-" + strings.Repeat("a", 64)
	if !ValidHashKeeperChoiceFreshRequestID(valid) {
		t.Fatal("complete canonical request identity rejected")
	}
	for _, id := range []string{"", "hash-choice-v1-" + strings.Repeat("a", 64), valid[:len(valid)-1], valid + "a", valid + "\n", strings.ToUpper(valid)} {
		if ValidHashKeeperChoiceFreshRequestID(id) {
			t.Fatal("noncanonical or wrong-contract request identity accepted", id)
		}
	}
	for _, request := range []*KeeperChoiceFreshRequest{nil, {}} {
		if request.ID() != "" || !reflect.DeepEqual(request.Report(), HashKeeperChoiceFreshRequestReport{}) || !reflect.DeepEqual(request.SourceLocator(), HashSourceLocator{}) || !reflect.DeepEqual(request.Proposal(), HashProposal{}) {
			t.Fatal("empty request wrapper exposed usable binding")
		}
	}
}
