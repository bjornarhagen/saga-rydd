package inventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func hashExistingWriterModes() map[string]func(context.Context, string) (*HashStore, error) {
	return map[string]func(context.Context, string) (*HashStore, error){
		"writer":           OpenExistingHashWriter,
		"selection_writer": OpenExistingHashSelectionWriter,
	}
}

func TestHashExistingWriterDoesNotRecreateAfterReaderPreflight(t *testing.T) {
	for mode, open := range hashExistingWriterModes() {
		for _, removed := range []string{"base", "hash_directory", "database"} {
			t.Run(mode+"/"+removed, func(t *testing.T) {
				f, req := hashReadFixture(t, []byte("disposable fixture"))
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				r, err := OpenHashReader(context.Background(), f.base)
				if err != nil {
					t.Fatal(err)
				}
				proposal, err := r.Proposal(context.Background(), req.SelectionID)
				if err != nil || proposal.SourceLocator == nil {
					t.Fatal("saved reader preflight failed", proposal, err)
				}
				if err = r.Close(); err != nil {
					t.Fatal(err)
				}
				path := f.base
				if removed != "base" {
					path = filepath.Join(f.base, "hashes")
				}
				if removed == "database" {
					path = filepath.Join(path, hashStoreFilename)
				}
				sidecars := make(map[string]os.FileInfo)
				if removed == "database" {
					for _, suffix := range []string{"-wal", "-shm", "-journal"} {
						info, e := os.Lstat(path + suffix)
						if e != nil && !errors.Is(e, os.ErrNotExist) {
							t.Fatal(e)
						}
						sidecars[suffix] = info
					}
				}
				if err = os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
				w, err := open(context.Background(), f.base)
				if w != nil {
					_ = w.Close()
				}
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("missing storage was accepted", err)
				}
				if _, err = os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("writer recreated storage after preflight", err)
				}
				if removed == "database" {
					for _, suffix := range []string{"-wal", "-shm", "-journal"} {
						current, e := os.Lstat(path + suffix)
						previous := sidecars[suffix]
						if previous == nil {
							if !errors.Is(e, os.ErrNotExist) {
								t.Fatal("writer created a database sidecar", suffix, e)
							}
						} else if e != nil || !os.SameFile(previous, current) || previous.Mode() != current.Mode() || previous.Size() != current.Size() || !previous.ModTime().Equal(current.ModTime()) {
							t.Fatal("writer modified a surviving database sidecar", suffix, e)
						}
					}
				}
			})
		}
	}
}

func TestHashExistingWriterRefusesUninitializedDatabase(t *testing.T) {
	for mode, open := range hashExistingWriterModes() {
		t.Run(mode, func(t *testing.T) {
			base, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			base = filepath.Join(base, "hash-data")
			if err = os.Mkdir(base, 0700); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(base, "hashes")
			if err = os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, hashStoreFilename)
			if err = os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			w, err := open(context.Background(), base)
			if w != nil {
				_ = w.Close()
			}
			if !errors.Is(err, ErrHashStoreCorrupt) {
				t.Fatal("uninitialized database was accepted", err)
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) || after.Size() != 0 || after.Mode().Perm() != 0600 {
				t.Fatal("refusal initialized or replaced the database", after, err)
			}
			for _, suffix := range []string{"-wal", "-shm", "-journal"} {
				if _, err = os.Lstat(path + suffix); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refusal created a database sidecar", suffix, err)
				}
			}
			// The failed open must also release its lifetime lock. The original
			// initializer remains available for a deliberate new-store operation.
			w, err = OpenHashWriter(context.Background(), base)
			if err != nil {
				t.Fatal("legacy initialization changed or failed open retained lock", err)
			}
			defer w.Close()
			if snapshot := hashStoreSnapshot(t, w); snapshot.StoreID == "" || snapshot.SelectionID != "" {
				t.Fatal("initializer did not create an empty store", snapshot)
			}
		})
	}
}

func TestHashExistingWriterMigratesLegacyAndPreservesRecoveryModes(t *testing.T) {
	for mode, open := range hashExistingWriterModes() {
		t.Run(mode, func(t *testing.T) {
			f := hashStoreFixture(t, fullHashContents(129))
			if _, err := f.store.db.Exec("CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF checkpoint ON hash_work BEGIN SELECT RAISE(ABORT,'fixture'); END"); err != nil {
				t.Fatal(err)
			}
			result, err := f.store.RunNext(context.Background(), f.source, f.scanner, 64, 128)
			if !errors.Is(err, ErrHashRecoveryRequired) || result.ReservedBytes != 64 {
				t.Fatal("fixture did not leave reserved work", result, err)
			}
			before := hashStoreSnapshot(t, f.store)
			if _, err = f.store.db.Exec("DROP TRIGGER fail_checkpoint; DROP TABLE hash_read_observation; DROP TABLE hash_read_revocation; DROP TABLE hash_read_approval; PRAGMA user_version=1"); err != nil {
				t.Fatal(err)
			}
			if err = f.store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.base, "hashes", hashStoreFilename)
			original, err := os.Lstat(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := OpenHashReader(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			var version int
			if err = r.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 1 || !reflect.DeepEqual(before, hashStoreSnapshot(t, r)) {
				t.Fatal("reader migrated or reconciled legacy storage", version, err)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			w, err := open(context.Background(), f.base)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if err = w.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 2 {
				t.Fatal("valid existing legacy storage did not migrate", version, err)
			}
			after := hashStoreSnapshot(t, w)
			if after.StoreID != before.StoreID || after.SelectionID != before.SelectionID || after.InventoryID != before.InventoryID || after.Work[0].DurableOffset != 0 || after.Budget.TotalReservedBytes != 64 || after.Budget.TotalReadBytes != 0 {
				t.Fatal("existing writer changed identity, checkpoint or durable charge", after)
			}
			if mode == "selection_writer" {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("metadata writer reconciled existing attempt", after)
				}
				opened := false
				if _, err = w.runNext(context.Background(), f.source, f.scanner, 64, 128, hashStoreHooks{file: fileHashHooks{afterOpen: func() { opened = true }}}); err == nil || opened || !reflect.DeepEqual(after, hashStoreSnapshot(t, w)) {
					t.Fatal("metadata writer dispatched source work or changed records", err)
				}
			} else {
				a := after.Work[0].LatestAttempt
				if after.Work[0].Status != "pending" || after.Work[0].Sequence != 1 || a.Status != "interrupted_unknown" || a.RequestedBytes != nil || a.ReadBytes != nil || a.ElapsedNS != nil || after.Budget.TotalUnknownReservedBytes != 64 {
					t.Fatal("normal writer did not conservatively recover", after)
				}
			}
			current, err := os.Lstat(path)
			if err != nil || !os.SameFile(original, current) || current.Mode().Perm() != 0600 {
				t.Fatal("migration replaced storage or changed permissions", current, err)
			}
			if competing, err := OpenExistingHashSelectionWriter(context.Background(), f.base); !errors.Is(err, localfs.ErrLocked) {
				if competing != nil {
					_ = competing.Close()
				}
				t.Fatal("existing writer did not retain exclusive lock", err)
			}
		})
	}
}

func TestHashExistingWriterRetainsPrivateStorageChecks(t *testing.T) {
	for mode, open := range hashExistingWriterModes() {
		for _, changed := range []string{"shared_base", "shared_hash_directory", "shared_database", "linked_database", "symlink_database"} {
			t.Run(mode+"/"+changed, func(t *testing.T) {
				f := hashStoreFixture(t, []byte("fixture"))
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(f.base, "hashes", hashStoreFilename)
				switch changed {
				case "shared_base":
					path = f.base
				case "shared_hash_directory":
					path = filepath.Dir(path)
				case "linked_database":
					if err := os.Link(path, filepath.Join(f.base, "fixture-alias")); err != nil {
						t.Fatal(err)
					}
				case "symlink_database":
					moved := filepath.Join(f.base, "fixture-database")
					if err := os.Rename(path, moved); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(moved, path); err != nil {
						t.Fatal(err)
					}
				}
				if changed == "shared_base" || changed == "shared_hash_directory" || changed == "shared_database" {
					if err := os.Chmod(path, 0755); err != nil {
						t.Fatal(err)
					}
				}
				before, err := os.Lstat(path)
				if err != nil {
					t.Fatal(err)
				}
				w, err := open(context.Background(), f.base)
				if w != nil {
					_ = w.Close()
				}
				if err == nil {
					t.Fatal("existing writer accepted unsafe storage")
				}
				after, err := os.Lstat(path)
				if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
					t.Fatal("refusal replaced storage or changed permissions", after, err)
				}
			})
		}
	}
}
