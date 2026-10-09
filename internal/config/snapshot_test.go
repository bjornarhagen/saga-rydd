package config

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestConfigSnapshotExactWholeConfigurationAndClones(t *testing.T) {
	paths, want := exclusionFixture(t, false)
	ctx := context.Background()
	cfg, snapshot, err := LoadSnapshot(ctx, paths.ConfigFile, "/generated-home")
	if err != nil || !reflect.DeepEqual(cfg, want) || snapshot.Check(ctx) != nil {
		t.Fatal(cfg, snapshot, err)
	}
	cfg.Roots[0], cfg.Excludes[0] = "/caller-changed", "/caller-changed"
	if err = snapshot.Check(ctx); err != nil {
		t.Fatal("caller mutation changed captured config identity", err)
	}
	body := readExclusionFixture(t, paths.ConfigFile)
	if err = os.Rename(paths.ConfigFile, paths.ConfigFile+".parked"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(paths.ConfigFile, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err = snapshot.Check(ctx); !errors.Is(err, ErrConfigChanged) {
		t.Fatal("identical bytes in a replaced object passed exact startup check", err)
	}
	_, snapshot, err = LoadSnapshot(ctx, paths.ConfigFile, "/generated-home")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(paths.ConfigFile, append(body, []byte("\n# changed complete config\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if err = snapshot.Check(ctx); !errors.Is(err, ErrConfigChanged) {
		t.Fatal("changed complete bytes passed exact startup check", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = snapshot.Check(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err = LoadSnapshot(canceled, paths.ConfigFile, "/generated-home"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = (Snapshot{}).Check(ctx); !errors.Is(err, ErrConfigChanged) {
		t.Fatal("uninitialized capture passed", err)
	}
}

func TestConfigSnapshotRejectsUnsafePrivateInputs(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "fifo", "shared", "shared_parent", "oversized", "malformed", "missing"} {
		t.Run(kind, func(t *testing.T) {
			paths, _ := exclusionFixture(t, true)
			original := readExclusionFixture(t, paths.ConfigFile)
			if err := os.Rename(paths.ConfigFile, paths.ConfigFile+".parked"); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(paths.ConfigFile+".parked", paths.ConfigFile)
			case "hardlink":
				err = os.Link(paths.ConfigFile+".parked", paths.ConfigFile)
			case "fifo":
				err = unix.Mkfifo(paths.ConfigFile, 0600)
			case "shared":
				err = os.WriteFile(paths.ConfigFile, original, 0644)
			case "shared_parent":
				if err = os.WriteFile(paths.ConfigFile, original, 0600); err == nil {
					err = os.Chmod(filepath.Dir(paths.ConfigFile), 0755)
				}
			case "oversized":
				err = os.WriteFile(paths.ConfigFile, []byte(strings.Repeat("#", maxConfigBytes+1)), 0600)
			case "malformed":
				err = os.WriteFile(paths.ConfigFile, []byte("[malformed TOML"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = LoadSnapshot(context.Background(), paths.ConfigFile, "/generated-home"); err == nil {
				t.Fatal("unsafe or invalid config accepted")
			}
			if _, err = ListExclusions(context.Background(), paths, "/generated-home"); err == nil {
				t.Fatal("unsafe editor listing accepted")
			}
			if _, err = EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new"); err == nil {
				t.Fatal("unsafe editor input accepted")
			}
			if string(readExclusionFixture(t, paths.ConfigFile+".parked")) != string(original) {
				t.Fatal("unsafe original body was modified")
			}
		})
	}
}

func TestExclusionResultsPreserveRawPrivateStoragePaths(t *testing.T) {
	for _, tc := range []struct {
		name, leaf string
		invalid    bool
	}{{"unicode_control", "quoted\"\n雪", false}, {"invalid_bytes", "raw\xff", true}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), tc.leaf)
			if err := os.Mkdir(dir, 0700); err != nil {
				// This observed Darwin filename refusal is limited to the
				// generated invalid-byte subcase, never general permissions.
				if tc.invalid && runtime.GOOS == "darwin" && (errors.Is(err, unix.EILSEQ) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EPERM)) {
					t.Skip("Darwin filesystem rejects generated invalid-UTF8 name")
				}
				t.Fatal(err)
			}
			paths := Paths{ConfigFile: filepath.Join(dir, "config.toml"), StateDir: dir}
			cfg := Default()
			cfg.Roots = []string{"/generated-offline-root"}
			if err := Create(paths.ConfigFile, "/generated-home", cfg); err != nil {
				t.Fatal(err)
			}
			lock, err := os.OpenFile(filepath.Join(dir, "writer.lock"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			lock.Close()
			for _, action := range []string{"list", "add"} {
				var result ExclusionResult
				if action == "list" {
					result, err = ListExclusions(context.Background(), paths, "/generated-home")
				} else {
					result, err = EditExclusion(context.Background(), paths, "/generated-home", action, "/generated-offline-root/excluded")
				}
				if err != nil || string(result.ConfigFileBytes) != paths.ConfigFile || string(result.StateDirBytes) != paths.StateDir {
					t.Fatal("raw storage paths were replaced", result, err)
				}
				payload, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				var decoded ExclusionResult
				if err = json.Unmarshal(payload, &decoded); err != nil || string(decoded.ConfigFileBytes) != paths.ConfigFile || string(decoded.StateDirBytes) != paths.StateDir {
					t.Fatal("JSON replaced authoritative private path bytes", decoded, err)
				}
			}
		})
	}
}
