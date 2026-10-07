package inventory

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

// This fixture captures real production inventory, rather than installing a
// caller-authored checkpoint. The hashing database is outside its source root.
func nativeHashStoreFixture(t *testing.T) (*Scanner, *state.Store, *HashStore, string, string) {
	t.Helper()
	ctx := context.Background()
	data := fullHashContents(1024*1024 + 65)
	s, targets := sampleFixture(t, data, bytes.Clone(data))
	root := string(targets[0].Root.PathBytes)
	base := filepath.Join(t.TempDir(), "state")
	w, err := state.OpenWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if err = w.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err = w.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err = w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for i := 0; i < 12; i++ {
		job, e := w.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		if job == nil {
			finished = true
			break
		}
		batch, e := s.Next(ctx, *job)
		if e != nil || batch.Fault != "" {
			t.Fatal(batch, e)
		}
		if e = w.CommitScan(ctx, *job, batch); e != nil {
			t.Fatal(e)
		}
	}
	if !finished {
		t.Fatal("production fixture inventory did not drain")
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	base, err = filepath.EvalSymlinks(base)
	if err != nil {
		t.Fatal(err)
	}
	source, err := state.OpenReader(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	report, err := source.SameSizeCandidates(ctx, 20, "", 1)
	if err != nil || len(report.Bands) != 1 || len(report.Bands[0].Files) != 2 {
		t.Fatal(report, err)
	}
	store, err := OpenHashWriter(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	file := report.Bands[0].Files[0]
	if _, err = store.CreateSelection(ctx, source, report.InventoryID, []state.SameSizeFile{file}); err != nil {
		t.Fatal(err)
	}
	return s, source, store, base, file.Path
}

// Native Linux CI runs these in a private mount namespace. Self-bind mounts
// preserve ordinary device/inode metadata, including the persisted baseline.
func TestBindMountBoundaryFullHashStore(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	for _, kind := range []string{"file_between", "parent_between", "root_between", "file_during_read", "parent_during_read"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			scanner, source, store, base, path := nativeHashStoreFixture(t)
			first, err := store.RunNext(ctx, source, scanner, 64, 4096)
			if err != nil || first.DurableOffset != 64 || first.Usage.ReadBytes != 64 {
				t.Fatal("fixture did not persist a checked prefix", first, err)
			}
			if err = store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = OpenHashWriter(ctx, base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if strings.HasPrefix(kind, "parent_") {
				path = filepath.Dir(path)
			} else if kind == "root_between" {
				path = filepath.Dir(filepath.Dir(path))
			}
			bind := func() {
				if e := unix.Mount(path, path, "", unix.MS_BIND, ""); e != nil {
					if errors.Is(e, unix.EPERM) || errors.Is(e, unix.EACCES) {
						t.Skip("requires mount privileges inside an isolated namespace")
					}
					t.Fatal(e)
				}
				t.Cleanup(func() {
					if e := unix.Unmount(path, 0); e != nil {
						t.Error(e)
					}
				})
			}
			hooks := hashStoreHooks{}
			if strings.HasSuffix(kind, "_during_read") {
				hooks.file.beforeFinalCheck = bind
			} else {
				bind()
			}
			result, err := store.runNext(ctx, source, scanner, 64, 4096, hooks)
			if err == nil || result.Progress.Status != "invalidated" || result.Progress.SHA256 != "" || result.DurableOffset != 64 {
				t.Fatal("restored continuation crossed a changed mount", result, err)
			}
			wantRead := int64(0)
			if strings.HasSuffix(kind, "_during_read") {
				wantRead = 64
			}
			if result.Usage.ReadBytes != wantRead || result.Usage.RequestedBytes != wantRead || result.ReservedBytes != 64 {
				t.Fatal("mount fixture did not exercise the expected read phase/charge", result)
			}
			snapshot, err := store.Snapshot(ctx)
			if err != nil || len(snapshot.Work) != 1 || snapshot.Work[0].Status != "invalidated" || snapshot.Work[0].DurableOffset != 64 || snapshot.Work[0].SHA256 != "" || snapshot.Budget == nil || snapshot.Budget.TotalReservedBytes != 128 || snapshot.Budget.TotalReadBytes != 64+wantRead {
				t.Fatal("changed mount published new progress or lost charged usage", snapshot, err)
			}
		})
	}
}
