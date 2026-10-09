package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sys/unix"
)

func exclusionFixture(t *testing.T, split bool) (Paths, Config) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(base, 0700); err != nil {
		t.Fatal(err)
	}
	paths := Paths{ConfigFile: filepath.Join(base, "config.toml"), StateDir: base}
	if split {
		paths = Paths{ConfigFile: filepath.Join(base, "config", "config.toml"), StateDir: filepath.Join(base, "state")}
		if err := os.Mkdir(paths.StateDir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Default()
	cfg.Roots = []string{"/generated-root", "/other-offline-root"}
	cfg.Excludes = []string{"/generated-root/keep", "/other-offline-root/retain", "/generated-root/keep"}
	cfg.Scan = Scan{WorkSeconds: 2, IntervalSeconds: 8, MetadataPerSecond: 17, MetadataAttemptsPerDay: 20_000_000, ReadBytesPerSecond: 257, ReadBytesPerDay: 1234, PauseOnBattery: false, MaxScanChunksPerDay: 8}
	if err := Create(paths.ConfigFile, "/generated-home", cfg); err != nil {
		t.Fatal(err)
	}
	lock, err := localfs.AcquireLock(paths.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	// The editor must not even interpret a database in this slot.
	if err = os.WriteFile(filepath.Join(paths.StateDir, "state.sqlite3"), []byte("generated non-SQLite body must remain unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	return paths, cfg
}

func readExclusionFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func assertExclusionSettings(t *testing.T, paths Paths, want Config) {
	t.Helper()
	got, err := Load(paths.ConfigFile, "/generated-home")
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("unrelated settings changed", got, want, err)
	}
	if string(readExclusionFixture(t, filepath.Join(paths.StateDir, "state.sqlite3"))) != "generated non-SQLite body must remain unchanged" {
		t.Fatal("editor touched or interpreted state SQLite")
	}
}

func TestExclusionEditExactSettingsNoOpsAndOffline(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			paths, cfg := exclusionFixture(t, split)
			ctx := context.Background()
			before := readExclusionFixture(t, paths.ConfigFile)
			info, err := os.Lstat(paths.ConfigFile)
			if err != nil {
				t.Fatal(err)
			}
			_, snapshot, err := LoadSnapshot(ctx, paths.ConfigFile, "/generated-home")
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range []struct{ action, path string }{{"add", "/generated-root/keep"}, {"remove", "/generated-root/absent"}} {
				r, err := EditExclusion(ctx, paths, "/generated-home", operation.action, operation.path)
				if err != nil || r.Changed || r.Publication != "not_needed" || r.SyncCompleted || r.ConfigSHA256 != fmt.Sprintf("%x", sha256.Sum256(before)) || !bytes.Equal(readExclusionFixture(t, paths.ConfigFile), before) {
					t.Fatal("exact no-op rewrote or misreported config", r, err)
				}
			}
			after, err := os.Lstat(paths.ConfigFile)
			if err != nil || !os.SameFile(info, after) || snapshot.Check(ctx) != nil {
				t.Fatal("no-op changed snapshot identity", err)
			}
			r, err := EditExclusion(ctx, paths, "/generated-home", "add", "/generated-root/a/../new")
			if err != nil || !r.Changed || r.Publication != "saved" || !r.SyncCompleted || r.Path != "/generated-root/new" || string(r.PathBytes) != r.Path || !bytes.Equal(r.ConfigFileBytes, []byte(paths.ConfigFile)) || !bytes.Equal(r.StateDirBytes, []byte(paths.StateDir)) {
				t.Fatal("changed list did not publish with exact paths", r, err)
			}
			cfg.Excludes = append(cfg.Excludes, "/generated-root/new")
			assertExclusionSettings(t, paths, cfg)
			if err = snapshot.Check(ctx); !errors.Is(err, ErrConfigChanged) {
				t.Fatal("stale complete config capture was accepted", err)
			}
			if r.ConfigSHA256 != fmt.Sprintf("%x", sha256.Sum256(readExclusionFixture(t, paths.ConfigFile))) {
				t.Fatal("returned digest is not the whole serialized config", r)
			}
			if _, err = EditExclusion(ctx, paths, "/generated-home", "remove", "/generated-root/keep"); err != nil {
				t.Fatal(err)
			}
			cfg.Excludes = []string{"/other-offline-root/retain", "/generated-root/new"}
			assertExclusionSettings(t, paths, cfg)
			if _, err = EditExclusion(ctx, paths, "/generated-home", "add", "/generated-root/new-old"); err != nil {
				t.Fatal(err)
			}
			if _, err = EditExclusion(ctx, paths, "/generated-home", "remove", "/generated-root/new"); err != nil {
				t.Fatal(err)
			}
			cfg.Excludes = []string{"/other-offline-root/retain", "/generated-root/new-old"}
			assertExclusionSettings(t, paths, cfg)
			list, err := ListExclusions(ctx, paths, "/generated-home")
			if err != nil || list.Action != "list" || list.Publication != "not_requested" || list.Changed || !reflect.DeepEqual(list.Exclusions, cfg.Excludes) || len(list.ExclusionPathBytes) != len(cfg.Excludes) {
				t.Fatal("offline list promoted settings or changed order", list, err)
			}
			for i, path := range cfg.Excludes {
				if string(list.ExclusionPathBytes[i]) != path {
					t.Fatal("listed raw paths differ", list)
				}
			}
		})
	}
}

func TestExclusionInputsWholeRootAndEditorBounds(t *testing.T) {
	ctx := context.Background()
	missing := filepath.Join(t.TempDir(), "missing")
	paths := Paths{ConfigFile: filepath.Join(missing, "config.toml"), StateDir: missing}
	for _, path := range []string{"", "relative", "~/root", "/bad\x00name", "/bad\xffname", "/" + strings.Repeat("x", 4096)} {
		if _, err := EditExclusion(ctx, paths, "/generated-home", "add", path); !errors.Is(err, ErrExclusionInput) {
			t.Fatal("invalid input reached storage", path, err)
		}
	}
	if _, err := EditExclusion(ctx, paths, "/generated-home", "other", "/valid"); !errors.Is(err, ErrExclusionInput) {
		t.Fatal("invalid action reached storage", err)
	}
	if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid input initialized storage", err)
	}
	paths, cfg := exclusionFixture(t, false)
	before := readExclusionFixture(t, paths.ConfigFile)
	for _, path := range []string{"/generated-root", "/", "/other-offline-root"} {
		if _, err := EditExclusion(ctx, paths, "/generated-home", "add", path); !errors.Is(err, ErrExclusionInput) || !bytes.Equal(before, readExclusionFixture(t, paths.ConfigFile)) {
			t.Fatal("whole configured root became excluded", path, err)
		}
	}
	cfg.Excludes = []string{}
	for i := 0; i < ExclusionPathLimit; i++ {
		cfg.Excludes = append(cfg.Excludes, fmt.Sprintf("/generated-root/path%d", i))
	}
	writeEditorFixture(t, paths.ConfigFile, cfg)
	if _, err := EditExclusion(ctx, paths, "/generated-home", "add", "/generated-root/extra"); !errors.Is(err, ErrExclusionBounds) {
		t.Fatal("editor exceeded exclusion capacity", err)
	}
	if r, err := EditExclusion(ctx, paths, "/generated-home", "remove", cfg.Excludes[0]); err != nil || len(r.Exclusions) != ExclusionPathLimit-1 {
		t.Fatal("capacity prevented exact reduction", r, err)
	}
	cfg.Excludes = append(cfg.Excludes, "/generated-root/extra")
	writeEditorFixture(t, paths.ConfigFile, cfg)
	if _, err := ListExclusions(ctx, paths, "/generated-home"); !errors.Is(err, ErrExclusionBounds) {
		t.Fatal("bounded editor accepted legacy oversized list", err)
	}
	if _, snapshot, err := LoadSnapshot(ctx, paths.ConfigFile, "/generated-home"); err != nil || snapshot.Check(ctx) != nil {
		t.Fatal("daemon snapshot narrowed legacy 1MiB semantics", err)
	}
	cfg.Roots = []string{}
	for i := 0; i <= ExclusionPathLimit; i++ {
		cfg.Roots = append(cfg.Roots, fmt.Sprintf("/generated-root%d", i))
	}
	cfg.Excludes = []string{}
	writeEditorFixture(t, paths.ConfigFile, cfg)
	if _, err := ListExclusions(ctx, paths, "/generated-home"); !errors.Is(err, ErrExclusionBounds) {
		t.Fatal("editor validated an oversized root set", err)
	}
	if _, snapshot, err := LoadSnapshot(ctx, paths.ConfigFile, "/generated-home"); err != nil || snapshot.Check(ctx) != nil {
		t.Fatal("daemon snapshot narrowed legacy root-count semantics", err)
	}
	cfg.Roots = []string{"/generated-root", "/other-offline-root"}
	cfg.Excludes = []string{}
	writeEditorFixture(t, paths.ConfigFile, cfg)
	if r, err := ListExclusions(ctx, paths, "/generated-home"); err != nil || r.Exclusions == nil || r.ExclusionPathBytes == nil || len(r.Exclusions) != 0 {
		t.Fatal("empty list does not encode consistently", r, err)
	}
}

func writeEditorFixture(t *testing.T, path string, cfg Config) {
	t.Helper()
	// Create supplies the exact existing parser's serialization without replacing
	// its file in production. These generated writes model an external editor.
	other := filepath.Join(t.TempDir(), "private", "config.toml")
	if err := Create(other, "/generated-home", cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, readExclusionFixture(t, other), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestExclusionEditMissingBusyAndNoDatabaseAccess(t *testing.T) {
	paths, cfg := exclusionFixture(t, true)
	before := readExclusionFixture(t, paths.ConfigFile)
	lock, err := localfs.AcquireLock(paths.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new"); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal("active or paused writer was not excluded", err)
	}
	if err = lock.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readExclusionFixture(t, paths.ConfigFile)) {
		t.Fatal("busy edit rewrote config")
	}
	lockPath := filepath.Join(paths.StateDir, "writer.lock")
	if err = os.Rename(lockPath, lockPath+".parked"); err != nil {
		t.Fatal(err)
	}
	if _, err = EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing lock was initialized", err)
	}
	if _, err = os.Lstat(lockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("editor created its missing lock", err)
	}
	if _, err = ListExclusions(context.Background(), paths, "/generated-home"); err != nil {
		t.Fatal("listing unnecessarily required a state lock", err)
	}
	assertExclusionSettings(t, paths, cfg)
	if err = os.Rename(paths.StateDir, paths.StateDir+".parked"); err != nil {
		t.Fatal(err)
	}
	if _, err = EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing state was initialized", err)
	}
	if _, err = os.Lstat(paths.StateDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("editor created its missing state directory", err)
	}
}

func TestExclusionEditorRejectsChangedInputAndLock(t *testing.T) {
	for _, kind := range []string{"config", "parent", "lock", "temporary"} {
		t.Run(kind, func(t *testing.T) {
			paths, cfg := exclusionFixture(t, true)
			before := readExclusionFixture(t, paths.ConfigFile)
			hook := func() {
				switch kind {
				case "config":
					info, err := os.Stat(paths.ConfigFile)
					if err != nil {
						t.Fatal(err)
					}
					body := bytes.Replace(before, []byte("metadata_per_second = 17"), []byte("metadata_per_second = 18"), 1)
					if bytes.Equal(body, before) {
						t.Fatal("fixture field was not found")
					}
					if err = os.WriteFile(paths.ConfigFile, body, 0600); err != nil {
						t.Fatal(err)
					}
					if err = os.Chtimes(paths.ConfigFile, info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
				case "parent":
					parent := filepath.Dir(paths.ConfigFile)
					if err := os.Rename(parent, parent+".parked"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(parent, 0700); err != nil {
						t.Fatal(err)
					}
				case "lock":
					path := filepath.Join(paths.StateDir, "writer.lock")
					if err := os.Rename(path, path+".parked"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, nil, 0600); err != nil {
						t.Fatal(err)
					}
				case "temporary":
					names, err := filepath.Glob(filepath.Join(filepath.Dir(paths.ConfigFile), ".config-exclusion-*.tmp"))
					if err != nil || len(names) != 1 {
						t.Fatal(names, err)
					}
					info, err := os.Stat(names[0])
					if err != nil {
						t.Fatal(err)
					}
					body := readExclusionFixture(t, names[0])
					body[len(body)-1] ^= 1
					if err = os.WriteFile(names[0], body, 0600); err != nil {
						t.Fatal(err)
					}
					if err = os.Chtimes(names[0], info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := editExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new", exclusionHooks{beforePublish: hook})
			if err == nil || errors.Is(err, ErrExclusionPublication) || r.ConfigSHA256 != "" {
				t.Fatal("prepublication change was ignored or claimed publication", r, err)
			}
			if kind == "lock" || kind == "temporary" {
				assertExclusionSettings(t, paths, cfg)
			}
			if kind == "config" && !errors.Is(err, ErrConfigChanged) {
				t.Fatal("restored-mtime edit was not refused", err)
			}
		})
	}
}

func TestExclusionPublicationCancellationAndUncertainty(t *testing.T) {
	for _, stage := range []string{"before", "after", "sync_error", "rename_reply_error", "post_replace"} {
		t.Run(stage, func(t *testing.T) {
			paths, cfg := exclusionFixture(t, false)
			before := readExclusionFixture(t, paths.ConfigFile)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cause := errors.New("generated publication failure")
			hooks := exclusionHooks{}
			switch stage {
			case "before":
				hooks.beforePublish = cancel
			case "after":
				hooks.afterPublish = cancel
			case "sync_error":
				hooks.syncParent = func(*os.File) error { return cause }
			case "rename_reply_error":
				hooks.rename = func(a int, b string, c int, d string) error {
					if err := unix.Renameat(a, b, c, d); err != nil {
						return err
					}
					return cause
				}
			case "post_replace":
				hooks.afterPublish = func() {
					if err := os.Rename(paths.ConfigFile, paths.ConfigFile+".parked"); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(paths.ConfigFile, before, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := editExclusion(ctx, paths, "/generated-home", "add", "/generated-root/new", hooks)
			if stage == "before" {
				if !errors.Is(err, context.Canceled) || errors.Is(err, ErrExclusionPublication) || r.ConfigSHA256 != "" || !bytes.Equal(before, readExclusionFixture(t, paths.ConfigFile)) {
					t.Fatal("prepublication cancellation changed settings", r, err)
				}
			} else {
				if !errors.Is(err, ErrExclusionPublication) || r.Publication != "uncertain" || !r.Changed || r.SyncCompleted || len(r.ConfigSHA256) != 64 || !strings.Contains(err.Error(), r.ConfigSHA256) || !strings.Contains(err.Error(), "exclude --list") {
					t.Fatal("uncertain publication lost candidate or inspection guidance", r, err)
				}
				if stage == "after" && !errors.Is(err, context.Canceled) {
					t.Fatal("late cancellation cause was lost", err)
				}
				if stage != "post_replace" {
					cfg.Excludes = append(cfg.Excludes, "/generated-root/new")
					assertExclusionSettings(t, paths, cfg)
				}
			}
			lock, lockErr := localfs.AcquireExistingLock(context.Background(), paths.StateDir)
			if lockErr != nil {
				t.Fatal("edit did not release its writer lock", lockErr)
			}
			lock.Close()
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r, err := EditExclusion(ctx, Paths{}, "", "add", "/valid"); !errors.Is(err, context.Canceled) || r.ConfigSHA256 != "" {
		t.Fatal(r, err)
	}
}

func TestExclusionStableReadRejectsRestoredMtimeMutation(t *testing.T) {
	paths, _ := exclusionFixture(t, false)
	body := append(readExclusionFixture(t, paths.ConfigFile), []byte(strings.Repeat(" ", 96<<10)+"\n")...)
	if err := os.WriteFile(paths.ConfigFile, body, 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(paths.ConfigFile)
	if err != nil {
		t.Fatal(err)
	}
	mutated := false
	hook := func(read int) {
		if mutated {
			return
		}
		mutated = true
		body[read+1] = '\n'
		time.Sleep(2 * time.Millisecond)
		if err := os.WriteFile(paths.ConfigFile, body, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(paths.ConfigFile, info.ModTime(), info.ModTime()); err != nil {
			t.Fatal(err)
		}
	}
	if r, err := editExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new", exclusionHooks{afterReadChunk: hook}); !errors.Is(err, ErrConfigChanged) || r.ConfigSHA256 != "" || !mutated {
		t.Fatal("same-size restored-mtime edit passed the held read", r, err)
	}
	if cfg, err := Load(paths.ConfigFile, "/generated-home"); err != nil || len(cfg.Excludes) != 3 {
		t.Fatal("stable replacement bytes were not valid unchanged settings", cfg, err)
	}
}

func TestExclusionEditPreservesLiteralUnrelatedPaths(t *testing.T) {
	paths, _ := exclusionFixture(t, false)
	body := []byte("version=1\nroots=['~/dev','/other-offline-root']\nexcludes=['~/dev/a/../keep','/other-offline-root/./retain','~/dev/remove','/generated-home/dev/./remove']\n[scan]\nwork_seconds=2\ninterval_seconds=8\nmetadata_per_second=17\nread_bytes_per_second=257\nread_bytes_per_day=1234\npause_on_battery=false\nmax_scan_chunks_per_day=8\n")
	if err := os.WriteFile(paths.ConfigFile, body, 0600); err != nil {
		t.Fatal(err)
	}
	readLiteral := func() Config {
		literal := Default()
		if err := toml.Unmarshal(readExclusionFixture(t, paths.ConfigFile), &literal); err != nil {
			t.Fatal(err)
		}
		return literal
	}
	original := readLiteral()
	r, err := EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-home/dev/new")
	want := original
	want.Excludes = append(append([]string{}, original.Excludes...), "/generated-home/dev/new")
	if err != nil || !reflect.DeepEqual(readLiteral(), want) || !reflect.DeepEqual(r.Exclusions, []string{"/generated-home/dev/keep", "/other-offline-root/retain", "/generated-home/dev/remove", "/generated-home/dev/remove", "/generated-home/dev/new"}) {
		t.Fatal("normalization changed unrelated portable literals", readLiteral(), want, r, err)
	}
	r, err = EditExclusion(context.Background(), paths, "/generated-home", "remove", "/generated-home/dev/remove")
	want.Excludes = []string{"~/dev/a/../keep", "/other-offline-root/./retain", "/generated-home/dev/new"}
	if err != nil || !reflect.DeepEqual(readLiteral(), want) || !reflect.DeepEqual(r.Exclusions, []string{"/generated-home/dev/keep", "/other-offline-root/retain", "/generated-home/dev/new"}) {
		t.Fatal("exact removal rewrote neighbors or missed normalized duplicates", readLiteral(), want, r, err)
	}
}

func TestExclusionDirectoryArtifactAdmissionBound(t *testing.T) {
	for _, entries := range []int{255, 256, 257} {
		t.Run(fmt.Sprint(entries), func(t *testing.T) {
			paths, cfg := exclusionFixture(t, true)
			parent := filepath.Dir(paths.ConfigFile)
			original := readExclusionFixture(t, paths.ConfigFile)
			// The one existing config name counts against the same total as all
			// other immediate names, including interrupted private temporaries.
			for i := 1; i < entries; i++ {
				path := filepath.Join(parent, fmt.Sprintf(".generated-artifact-%03d", i))
				if err := os.WriteFile(path, []byte("generated artifact stays unchanged"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			seen := 0
			hook := func() {
				names, err := os.ReadDir(parent)
				if err != nil {
					t.Fatal(err)
				}
				seen = len(names)
			}
			r, err := editExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new", exclusionHooks{beforePublish: hook})
			if entries < ExclusionDirectoryEntryLimit {
				if err != nil || r.Publication != "saved" || seen != ExclusionDirectoryEntryLimit {
					t.Fatal("bounded directory did not admit exactly one temp", seen, r, err)
				}
				cfg.Excludes = append(cfg.Excludes, "/generated-root/new")
				assertExclusionSettings(t, paths, cfg)
			} else {
				if !errors.Is(err, ErrExclusionBounds) || !strings.Contains(err.Error(), "256 entries") || seen != 0 || r.ConfigSHA256 != "" || !bytes.Equal(original, readExclusionFixture(t, paths.ConfigFile)) {
					t.Fatal("full directory added another artifact or lost refusal", seen, r, err)
				}
				if list, listErr := ListExclusions(context.Background(), paths, "/generated-home"); listErr != nil || !reflect.DeepEqual(list.Exclusions, cfg.Excludes) {
					t.Fatal("inspection was blocked by artifact bound", list, listErr)
				}
				if noop, noopErr := EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/keep"); noopErr != nil || noop.Publication != "not_needed" {
					t.Fatal("exact no-op was blocked by artifact bound", noop, noopErr)
				}
				if _, snapshot, snapshotErr := LoadSnapshot(context.Background(), paths.ConfigFile, "/generated-home"); snapshotErr != nil || snapshot.Check(context.Background()) != nil {
					t.Fatal("generic startup snapshot inherited editor artifact cap", snapshotErr)
				}
			}
			names, err := os.ReadDir(parent)
			if err != nil || len(names) != entries {
				t.Fatal("publication/refusal retained an additional name", len(names), entries, err)
			}
			for i := 1; i < entries; i++ {
				path := filepath.Join(parent, fmt.Sprintf(".generated-artifact-%03d", i))
				if string(readExclusionFixture(t, path)) != "generated artifact stays unchanged" {
					t.Fatal("existing private artifact was modified", path)
				}
			}
		})
	}
}
