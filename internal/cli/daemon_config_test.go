package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func daemonConfigFixture(t *testing.T) (config.Paths, config.Config) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "private")
	paths := config.Paths{ConfigFile: filepath.Join(base, "config.toml"), StateDir: base}
	cfg := config.Default()
	cfg.Roots = []string{"/generated-offline-root"}
	if err := config.Create(paths.ConfigFile, "/generated-home", cfg); err != nil {
		t.Fatal(err)
	}
	w, err := state.OpenWriter(context.Background(), paths.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), cfg.Roots); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	return paths, cfg
}

func TestDaemonConfigurationEditAfterCaptureRefusesStartup(t *testing.T) {
	paths, _ := daemonConfigFixture(t)
	ctx := context.Background()
	cfg, check, err := loadDaemonConfiguration(ctx, paths, "/generated-home")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = config.EditExclusion(ctx, paths, "/generated-home", "add", "/generated-offline-root/cache"); err != nil {
		t.Fatal(err)
	}
	ready := false
	err = worker.Run(ctx, paths.StateDir, cfg, worker.Options{StartupCheck: check, Ready: func(worker.Snapshot) { ready = true }})
	if !errors.Is(err, config.ErrConfigChanged) || ready {
		t.Fatal("daemon adopted stale exclusions or became ready", err, ready)
	}
	fresh, freshCheck, err := loadDaemonConfiguration(ctx, paths, "/generated-home")
	if err != nil || len(fresh.Excludes) != 1 || fresh.Excludes[0] != "/generated-offline-root/cache" || freshCheck(ctx) != nil {
		t.Fatal("later startup did not capture the updated configuration", fresh, err)
	}
}

func TestDaemonConfigurationPausedWorkerRefusesExclusionEdit(t *testing.T) {
	paths, _ := daemonConfigFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg, check, err := loadDaemonConfiguration(ctx, paths, "/generated-home")
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan worker.Snapshot, 1)
	done := make(chan error, 1)
	go func() {
		done <- worker.Run(ctx, paths.StateDir, cfg, worker.Options{StartupCheck: check, Ready: func(snapshot worker.Snapshot) { ready <- snapshot }})
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(8 * time.Second):
			t.Error("generated worker did not exit")
		}
	}()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("generated worker did not become ready")
	}
	controlCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if snapshot, err := worker.Send(controlCtx, paths.StateDir, "pause"); err != nil || !snapshot.Paused {
		t.Fatal(snapshot, err)
	}
	before, err := os.ReadFile(paths.ConfigFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = config.EditExclusion(controlCtx, paths, "/generated-home", "add", "/generated-offline-root/cache"); !errors.Is(err, localfs.ErrLocked) {
		t.Fatal("paused worker allowed an exclusion edit", err)
	}
	after, err := os.ReadFile(paths.ConfigFile)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("refused exclusion edit changed configuration", err)
	}
}
