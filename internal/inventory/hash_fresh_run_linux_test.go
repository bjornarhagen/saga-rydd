package inventory

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// Native CI runs in an isolated mount namespace. Complete original hashes
// precede mounts; fresh hashing must retain their metadata/mount guard without
// importing any of their SHA state or completed offset.
func TestBindMountBoundaryFreshHashConsented(t *testing.T) {
	if os.Getenv("RYDD_TEST_MOUNTS") != "1" {
		t.Skip("requires isolated mount namespace")
	}
	for _, kind := range []string{"file_before_first_read", "parent_before_first_read", "root_before_first_read", "file_during_read", "parent_during_read", "root_between_fresh_steps"} {
		t.Run(kind, func(t *testing.T) {
			f := freshRunFiles(t, int(FileHashStepByteLimit)+65, 2, "2", "1")
			w := f.open(t)
			path := string(f.fresh.job.Record.Request.Targets[0].Target.File.PathBytes)
			if strings.HasPrefix(kind, "parent_") {
				path = filepath.Dir(path)
			} else if strings.HasPrefix(kind, "root_") {
				path = f.fresh.m.f.root
			}
			var before unix.Stat_t
			if err := unix.Lstat(path, &before); err != nil {
				t.Fatal(err)
			}
			bind := func() {
				if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
					if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) {
						t.Skip("requires mount privileges inside an isolated namespace")
					}
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := unix.Unmount(path, 0); err != nil {
						t.Error(err)
					}
				})
				var after unix.Stat_t
				if err := unix.Lstat(path, &after); err != nil || before.Dev != after.Dev || before.Ino != after.Ino {
					t.Fatal("self-bind did not preserve ordinary file identity", after, err)
				}
			}
			priorOffset := int64(0)
			priorRead := int64(0)
			if kind == "root_between_fresh_steps" {
				for _, ordinal := range []int{1, 2} {
					r, err := w.RunFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner)
					if err != nil || r.Ordinal != ordinal || r.DurableOffset != FileHashStepByteLimit || r.Usage.ReadBytes != FileHashStepByteLimit {
						t.Fatal("fixture lacks new checked prefixes", r, err)
					}
				}
				priorOffset = FileHashStepByteLimit
				priorRead = 2 * FileHashStepByteLimit
				w = f.open(t)
			}
			hooks := hashFreshRunHooks{}
			if strings.HasSuffix(kind, "_during_read") {
				hooks.file.beforeFinalCheck = bind
			} else {
				bind()
			}
			r, err := w.runFreshConsented(context.Background(), f.consent.ID, f.fresh.m.f.scanner, hooks)
			if err == nil || r.Ordinal != 1 || r.Progress.Status != "invalidated" || r.Progress.SHA256 != "" || r.DurableOffset != priorOffset {
				t.Fatal("fresh hash crossed changed historical/new mount baseline", r, err)
			}
			wantRead := int64(0)
			if strings.HasSuffix(kind, "_during_read") {
				wantRead = FileHashStepByteLimit
			}
			if r.Usage.ReadBytes != wantRead || r.Usage.RequestedBytes != wantRead || r.ReservedBytes <= 0 || r.ReservedBytes > FileHashStepByteLimit {
				t.Fatal("mount fixture missed expected fresh read phase/reservation", r)
			}
			job := requireFreshRunSaved(t, f, w)
			if job.Progress[0].Status != "invalidated" || job.Progress[0].SHA256 != "" || job.Progress[0].DurableOffset != priorOffset || job.FreshReadBytes != priorRead+wantRead || job.FreshBudget.TotalUnknownReservedBytes != 0 {
				t.Fatal("mount invalidation published new prefix or lost known charged usage", job)
			}
		})
	}
}
