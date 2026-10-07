package inventory

import (
	"bytes"
	"context"
	"database/sql"
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

type hashChoiceMetadataFixture struct {
	f       *hashStoreTestFixture
	saved   SavedHashKeeperChoice
	request *KeeperChoiceMetadataRequest
}

// Build an ordinary production manual selection. Every content read occurs in
// fixture setup, before the saved historical choice and metadata request exist.
func hashChoiceMetadataFromFixture(t *testing.T, f *hashStoreTestFixture, keeper string, copies ...string) *hashChoiceMetadataFixture {
	t.Helper()
	manual := hashProposalFixtureWriter(t)
	p, err := manual.CreateManualSelection(context.Background(), f.source, f.inventoryID, f.expected, []byte(f.root))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manual.Close(); err != nil {
		t.Fatal(err)
	}
	f.base = manual.base
	// The metadata API owns opening the exact derived manual inventory. Move
	// only the disposable inventory, retaining its captured logical evidence.
	if err = f.source.Close(); err != nil {
		t.Fatal(err)
	}
	if err = localfs.EnsurePrivateDir(filepath.Join(f.base, "manual")); err != nil {
		t.Fatal(err)
	}
	derived := filepath.Join(f.base, "manual", p.SourceLocator.InventoryKey)
	if err = os.Rename(f.stateDir, derived); err != nil {
		t.Fatal(err)
	}
	f.stateDir = derived
	f.source, err = state.OpenWriter(context.Background(), derived)
	if err != nil {
		t.Fatal(err)
	}
	f.store, err = OpenHashWriter(context.Background(), f.base)
	if err != nil {
		t.Fatal(err)
	}
	f.store.now = hashChoiceClock
	consent, err := f.store.ApproveRead(context.Background(), HashReadApprovalRequest{
		StoreID: p.StoreID, SelectionID: p.SelectionID, InventoryID: p.InventoryID, SourceLocator: *p.SourceLocator,
		DailyReservedByteLimit: 4096, LifetimeReservedByteLimit: 8192, ConfirmFullFileRead: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	for range f.expected {
		if _, err = f.store.RunConsented(context.Background(), consent.ID, f.source, f.scanner); err != nil {
			t.Fatal(err)
		}
	}
	preview := hashKeeperPreview(t, f.store, p.SelectionID, keeper, copies...)
	w := hashChoiceWriter(t, f)
	saved := saveHashChoice(t, w, preview)
	request, err := w.PrepareKeeperChoiceMetadata(context.Background(), saved.ID)
	if err != nil || request == nil {
		t.Fatal("could not prepare exact generated metadata request", err)
	}
	return &hashChoiceMetadataFixture{f: f, saved: saved, request: request}
}

func hashChoiceMetadataFixtureFiles(t *testing.T, count int) *hashChoiceMetadataFixture {
	t.Helper()
	contents := make([][]byte, count)
	for i := range contents {
		contents[i] = fullHashContents(65)
	}
	f := hashStoreFixture(t, contents...)
	return hashChoiceMetadataFromFixture(t, f, "2", "1")
}

func requireHashChoiceMetadataClaims(t *testing.T, r HashKeeperChoiceMetadataReport) {
	t.Helper()
	if r.Contract != "historical_choice_metadata_check_v1" || r.Source != "live_metadata_with_saved_inventory" || r.SelectedContentRequestedBytes != 0 || r.SelectedContentReadBytes != 0 || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.ApprovalAvailable || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("metadata screen claimed content, current verification or action authority", r)
	}
}

func requireHashChoiceMetadataBlocked(t *testing.T, r HashKeeperChoiceMetadataReport, err error) {
	t.Helper()
	if err != nil {
		t.Fatal("ordinary refusal did not return a qualified blocked report", r, err)
	}
	requireHashChoiceMetadataClaims(t, r)
	if r.Status != "blocked" {
		t.Fatal("changed evidence passed metadata screen", r)
	}
}

func hashChoiceMetadataSourceDB(t *testing.T, m *hashChoiceMetadataFixture) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(m.f.stateDir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestHashKeeperChoiceMetadataExactSubsetAndClonedAccessors(t *testing.T) {
	data := fullHashContents(65)
	scanner, targets := sampleFixture(t, data, data, data, data)
	root := string(targets[0].Root.PathBytes)
	for _, dir := range []string{"selected", "unselected"} {
		if err := os.Mkdir(filepath.Join(root, "project", dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	paths := make([]string, 4)
	for i, target := range targets {
		part := "selected"
		if i == 3 {
			part = "unselected"
		}
		paths[i] = filepath.Join(root, "project", part, fmt.Sprintf("file-%d", i))
		if err := os.Rename(string(target.File.PathBytes), paths[i]); err != nil {
			t.Fatal(err)
		}
	}
	f := hashStoreFixtureFromFiles(t, scanner, captureSampleTargets(t, scanner, root, paths), [][]byte{data, data, data, data})
	m := hashChoiceMetadataFromFixture(t, f, "3", "2", "1")
	wantLocator := m.request.SourceLocator()
	wantProposal := m.request.Proposal()
	locator := m.request.SourceLocator()
	locator.RootPathBytes[0] = 'x'
	locator.InventoryKey = strings.Repeat("0", 64)
	proposal := m.request.Proposal()
	proposal.Targets[0].File.PathBytes[0] = 'x'
	proposal.Targets[0].Root.PathBytes[0] = 'y'
	proposal.Targets[0].Ancestors[0].Path[0] = 'z'
	proposal.Targets[0].Ancestors[0].Inode = "999999"
	proposal.SourceLocator.RootPathBytes[0] = 'q'
	if !reflect.DeepEqual(m.request.SourceLocator(), wantLocator) || !reflect.DeepEqual(m.request.Proposal(), wantProposal) {
		t.Fatal("accessors changed the opaque frozen scope")
	}
	// Only the unselected subdirectory changes. The chosen frozen ancestry is
	// unchanged; a whole-selection live walk would incorrectly touch this file.
	if err := os.Remove(paths[3]); err != nil {
		t.Fatal(err)
	}
	before := hashStoreSnapshot(t, f.store)
	r, err := m.request.Check(context.Background(), f.scanner)
	if err != nil || r.Status != "metadata_matches" || r.InventoryStatus != "matches_saved_inventory" || len(r.Targets) != 3 {
		t.Fatal("explicit selected subset was remapped or expanded", r, err)
	}
	requireHashChoiceMetadataClaims(t, r)
	if r.ChoiceID != m.saved.ID || r.StoreID != m.saved.Record.Evidence.StoreID || r.SelectionID != m.saved.Record.Evidence.SelectionID || r.InventoryID != f.inventoryID || r.CheckedAt.IsZero() {
		t.Fatal("metadata report lost exact saved bindings", r)
	}
	want := []SavedHashPreviewMember{m.saved.Record.Evidence.Keeper, m.saved.Record.Evidence.Copies[0], m.saved.Record.Evidence.Copies[1]}
	for i, target := range r.Targets {
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		if target.Role != role || target.Status != "metadata_matches" || target.MetadataCheckedAt.IsZero() || !reflect.DeepEqual(target.Observation, want[i]) {
			t.Fatal("metadata report changed explicit role/order/observation", target)
		}
		if i > 0 && target.MetadataCheckedAt.Before(r.Targets[i-1].MetadataCheckedAt) {
			t.Fatal("sequential check times were reordered")
		}
	}
	r.Targets[0].Observation.PathBytes[0] = 'x'
	copied := *m.request
	second, err := copied.Check(context.Background(), f.scanner)
	if err != nil || second.Status != "metadata_matches" || len(second.Targets) != 3 || !reflect.DeepEqual(second.Targets[0].Observation, want[0]) {
		t.Fatal("returned report or copied wrapper redirected frozen scope", second, err)
	}
	requireHashChoiceMetadataClaims(t, second)
	if after := hashStoreSnapshot(t, f.store); !reflect.DeepEqual(after, before) {
		t.Fatal("metadata-only check changed work, consent or accounting", after)
	}
	if got, e := f.store.KeeperChoice(context.Background(), m.saved.ID); e != nil || !reflect.DeepEqual(got, m.saved) {
		t.Fatal("metadata check refreshed immutable choice context", got, e)
	}
}

func TestHashKeeperChoiceMetadataRefusesInventoryRebaseline(t *testing.T) {
	for _, kind := range []string{"wrong_inventory", "ancestor", "root_fingerprint", "root_revision", "file"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 2)
			db := hashChoiceMetadataSourceDB(t, m)
			query := ""
			switch kind {
			case "wrong_inventory":
				query = "DROP TRIGGER inventory_identity_no_update; UPDATE inventory_identity SET token='" + strings.Repeat("b", 64) + "'"
			case "ancestor":
				query = "UPDATE entries SET mtime_ns=mtime_ns+1 WHERE kind='directory' AND path=x'70726f6a656374'"
			case "root_fingerprint":
				query = "UPDATE roots SET volume_id='v1:" + strings.Repeat("b", 64) + "'"
			case "root_revision":
				query = "UPDATE allocation_revisions SET revision=revision+1"
			case "file":
				query = "UPDATE entries SET ctime_ns=ctime_ns+1 WHERE kind='file'"
			}
			if _, err := db.Exec(query); err != nil {
				t.Fatal(err)
			}
			// Root/ancestor edits can be recaptured successfully from the current
			// inventory. The checker must compare their full frozen digest.
			if kind != "file" && kind != "wrong_inventory" {
				current, err := m.f.source.PrepareFileSampleSelection(context.Background(), m.f.inventoryID, m.f.expected)
				if err != nil || len(current) != 2 {
					t.Fatal("fixture did not provide usable changed ancestor evidence", current, err)
				}
			}
			r, err := m.request.Check(context.Background(), m.f.scanner)
			requireHashChoiceMetadataBlocked(t, r, err)
		})
	}
}

func TestHashKeeperChoiceMetadataLiveChangesAndCurrentScope(t *testing.T) {
	for _, kind := range []string{"edit_restore_mtime", "file_replace", "parent_replace", "root_replace", "file_symlink", "parent_symlink", "file_excluded", "parent_excluded", "scope_removed", "protected_alias"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 2)
			member := m.saved.Record.Evidence.Keeper
			path := string(member.PathBytes)
			parent := filepath.Dir(path)
			switch kind {
			case "edit_restore_mtime":
				if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, 65), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, member.SavedModifiedAt, member.SavedModifiedAt); err != nil {
					t.Fatal(err)
				}
			case "file_replace":
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, fullHashContents(65), 0600); err != nil {
					t.Fatal(err)
				}
			case "parent_replace":
				if err := os.Rename(parent, parent+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(parent, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, fullHashContents(65), 0600); err != nil {
					t.Fatal(err)
				}
			case "root_replace":
				if err := os.Rename(m.f.root, m.f.root+"-old"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := os.RemoveAll(m.f.root); err != nil {
						t.Error(err)
					}
					if err := os.Rename(m.f.root+"-old", m.f.root); err != nil {
						t.Error(err)
					}
				})
				if err := os.MkdirAll(parent, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, fullHashContents(65), 0600); err != nil {
					t.Fatal(err)
				}
			case "file_symlink":
				if err := os.Rename(path, path+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(path+"-old", path); err != nil {
					t.Fatal(err)
				}
			case "parent_symlink":
				if err := os.Rename(parent, parent+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(parent+"-old", parent); err != nil {
					t.Fatal(err)
				}
			case "file_excluded":
				m.f.scanner.excludes = append(m.f.scanner.excludes, path)
			case "parent_excluded":
				m.f.scanner.excludes = append(m.f.scanner.excludes, parent)
			case "scope_removed":
				m.f.scanner.roots = map[string]bool{}
			case "protected_alias":
				m.f.scanner.protectedIDs[member.SavedDevice+":"+member.SavedInode] = true
			}
			before := hashStoreSnapshot(t, m.f.store)
			r, err := m.request.Check(context.Background(), m.f.scanner)
			requireHashChoiceMetadataBlocked(t, r, err)
			if after := hashStoreSnapshot(t, m.f.store); !reflect.DeepEqual(after, before) {
				t.Fatal("refusal modified saved work/permission", after)
			}
		})
	}
}

func TestHashKeeperChoiceMetadataLateChangesVetoPositiveReport(t *testing.T) {
	for _, kind := range []string{"inventory_after_initial", "inventory_before_final", "file_while_held", "source_database_before_final", "hash_database_before_final"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 2)
			before := hashStoreSnapshot(t, m.f.store)
			hooks := hashKeeperChoiceMetadataHooks{}
			if strings.HasPrefix(kind, "inventory_") {
				db := hashChoiceMetadataSourceDB(t, m)
				mutate := func() {
					if _, err := db.Exec("UPDATE entries SET ctime_ns=ctime_ns+1 WHERE kind='directory' AND path=x'70726f6a656374'"); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "inventory_after_initial" {
					hooks.beforeTarget = func(i int) {
						if i == 0 {
							mutate()
						}
					}
				} else {
					hooks.beforeFinalCheck = mutate
				}
			} else if kind == "file_while_held" {
				path := string(m.saved.Record.Evidence.Keeper.PathBytes)
				hooks.beforeTargetFinalCheck = func(i int) {
					if i == 0 {
						if err := os.Chmod(path, 0400); err != nil {
							t.Fatal(err)
						}
					}
				}
			} else {
				path := filepath.Join(m.f.base, "hashes", hashStoreFilename)
				if kind == "source_database_before_final" {
					path = filepath.Join(m.f.stateDir, state.Filename)
				}
				hooks.beforeFinalCheck = func() {
					if kind == "source_database_before_final" {
						if err := m.f.source.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if err := os.Rename(path, path+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte("generated replacement private storage"), 0600); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						_ = os.Remove(path)
						if err := os.Rename(path+"-old", path); err != nil {
							t.Error(err)
						}
					})
				}
			}
			r, err := m.request.check(context.Background(), m.f.scanner, hooks)
			requireHashChoiceMetadataBlocked(t, r, err)
			if strings.HasPrefix(kind, "inventory_") && (r.Code != "inventory_changed" || r.InventoryStatus != "changed") {
				t.Fatal("late full-target digest change was ignored", r)
			}
			if kind == "hash_database_before_final" && r.Code != "hash_storage_changed" {
				t.Fatal("held old database authenticated replaced storage", r)
			}
			if kind == "source_database_before_final" && (r.Code != "inventory_storage_changed" || r.InventoryStatus != "unavailable") {
				t.Fatal("held old inventory authenticated its replaced pathname", r)
			}
			if kind != "hash_database_before_final" {
				if after := hashStoreSnapshot(t, m.f.store); !reflect.DeepEqual(after, before) {
					t.Fatal("metadata refusal changed saved records", after)
				}
			}
		})
	}
}

func TestHashKeeperChoiceMetadataCancellationPublishesNoPartialReport(t *testing.T) {
	for _, phase := range []string{"already_canceled", "after_one_target", "while_held", "after_targets", "before_final", "closed_scanner", "nil_scanner"} {
		t.Run(phase, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 2)
			before := hashStoreSnapshot(t, m.f.store)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := hashKeeperChoiceMetadataHooks{}
			scanner := m.f.scanner
			switch phase {
			case "already_canceled":
				cancel()
			case "after_one_target":
				hooks.beforeTarget = func(i int) {
					if i == 1 {
						cancel()
					}
				}
			case "while_held":
				hooks.beforeTargetFinalCheck = func(i int) {
					if i == 0 {
						cancel()
					}
				}
			case "after_targets":
				hooks.afterTargets = cancel
			case "before_final":
				hooks.beforeFinalCheck = cancel
			case "closed_scanner":
				scanner.Close()
			case "nil_scanner":
				scanner = nil
			}
			r, err := m.request.check(ctx, scanner, hooks)
			if err == nil || !reflect.DeepEqual(r, HashKeeperChoiceMetadataReport{}) {
				t.Fatal("cancellation/structural error published partial metadata evidence", r, err)
			}
			if phase != "closed_scanner" && phase != "nil_scanner" && !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation cause was lost", err)
			}
			if after := hashStoreSnapshot(t, m.f.store); !reflect.DeepEqual(after, before) {
				t.Fatal("cancellation changed work/permission", after)
			}
		})
	}
}

func TestHashKeeperChoiceMetadataPreparationCancellationAndEmptyRequests(t *testing.T) {
	m := hashChoiceMetadataFixtureFiles(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if request, err := m.f.store.PrepareKeeperChoiceMetadata(ctx, m.saved.ID); request != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled preparation returned reusable scope", request, err)
	}
	for _, request := range []*KeeperChoiceMetadataRequest{nil, {}} {
		r, err := request.Check(context.Background(), m.f.scanner)
		if !errors.Is(err, ErrHashKeeperChoiceMetadataRequest) || !reflect.DeepEqual(r, HashKeeperChoiceMetadataReport{}) {
			t.Fatal("unprepared request created source evidence", r, err)
		}
	}
}

func TestHashKeeperChoiceMetadataLegacyAndUnavailableInventory(t *testing.T) {
	t.Run("legacy_prepare", func(t *testing.T) {
		f := hashChoiceFixture(t, []byte("same"), []byte("same"))
		p := hashKeeperPreview(t, f.store, hashStoreSnapshot(t, f.store).SelectionID, "1", "2")
		w := hashChoiceWriter(t, f)
		saved := saveHashChoice(t, w, p)
		hashChoiceOffline(t, f)
		if request, err := w.PrepareKeeperChoiceMetadata(context.Background(), saved.ID); err == nil || request != nil {
			t.Fatal("legacy selection without locator prepared a source request", request, err)
		}
		if _, err := os.Lstat(filepath.Join(f.base, "manual")); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("legacy preparation created manual inventory storage", err)
		}
	})
	t.Run("missing_inventory", func(t *testing.T) {
		m := hashChoiceMetadataFixtureFiles(t, 2)
		if err := m.f.source.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(m.f.stateDir, m.f.stateDir+"-offline"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.Rename(m.f.stateDir+"-offline", m.f.stateDir); err != nil {
				t.Error(err)
			}
		})
		r, err := m.request.Check(context.Background(), m.f.scanner)
		requireHashChoiceMetadataBlocked(t, r, err)
		if r.Code != "inventory_unavailable" || r.InventoryStatus != "unavailable" {
			t.Fatal("missing derived inventory was substituted or initialized", r)
		}
		if _, err := os.Lstat(m.f.stateDir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("missing inventory directory was initialized", err)
		}
	})
}

func TestHashKeeperChoiceMetadataPrivateStorageAliasesBeforeSQLite(t *testing.T) {
	for _, kind := range []string{"inventory_database", "inventory_wal", "inventory_directory", "hash_database"} {
		t.Run(kind, func(t *testing.T) {
			m := hashChoiceMetadataFixtureFiles(t, 3)
			if err := m.f.source.Close(); err != nil {
				t.Fatal(err)
			}
			proposal := m.request.Proposal()
			// Protect even an unselected proposal member, before SQLite can read
			// its bytes. Rename makes the known selected inode single-linked.
			file := string(proposal.Targets[2].File.PathBytes)
			if kind == "inventory_directory" {
				if err := os.Rename(m.f.stateDir, m.f.stateDir+"-old"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Dir(file), m.f.stateDir); err != nil {
					t.Fatal(err)
				}
			} else {
				destination := filepath.Join(m.f.stateDir, state.Filename)
				if kind == "inventory_wal" {
					destination += "-wal"
				}
				if kind == "hash_database" {
					destination = filepath.Join(m.f.base, "hashes", hashStoreFilename)
				}
				if _, err := os.Lstat(destination); err == nil {
					if err = os.Rename(destination, destination+"-old"); err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
				if err := os.Rename(file, destination); err != nil {
					t.Fatal(err)
				}
				var info os.FileInfo
				info, err := os.Stat(destination)
				if err != nil || info.Size() != 65 {
					t.Fatal("alias fixture lost the generated inode", info, err)
				}
			}
			reachedLive := false
			r, err := m.request.check(context.Background(), m.f.scanner, hashKeeperChoiceMetadataHooks{beforeTarget: func(int) { reachedLive = true }})
			requireHashChoiceMetadataBlocked(t, r, err)
			if reachedLive {
				t.Fatal("known private-storage alias passed preflight")
			}
			wantCode := "inventory_storage_changed"
			if kind == "hash_database" {
				wantCode = "hash_storage_changed"
			}
			if r.Code != wantCode {
				t.Fatal("SQLite touched an aliased file before the metadata guard", r)
			}
		})
	}
}

func TestHashKeeperChoiceMetadataRawPaths(t *testing.T) {
	for _, name := range []string{"quote\"雪\nfile", "invalid-\xff-file"} {
		t.Run(fmt.Sprintf("name_%x", []byte(name)), func(t *testing.T) {
			data := fullHashContents(65)
			scanner, targets := sampleFixture(t, data, data)
			root := string(targets[0].Root.PathBytes)
			paths := []string{filepath.Join(root, "project", name), string(targets[1].File.PathBytes)}
			requireSampleFixtureFilename(t, name, os.Rename(string(targets[0].File.PathBytes), paths[0]))
			f := hashStoreFixtureFromFiles(t, scanner, captureSampleTargets(t, scanner, root, paths), [][]byte{data, data})
			m := hashChoiceMetadataFromFixture(t, f, "2", "1")
			r, err := m.request.Check(context.Background(), m.f.scanner)
			if err != nil || r.Status != "metadata_matches" || len(r.Targets) != 2 || !bytes.Equal(r.Targets[1].Observation.PathBytes, []byte(paths[0])) {
				t.Fatal("raw selected path was altered or reselected", r, err)
			}
			requireHashChoiceMetadataClaims(t, r)
		})
	}
}
