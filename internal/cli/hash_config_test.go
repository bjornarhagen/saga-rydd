package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func TestHashConfigHeldReaderRefusesSourceAliasesAndSpecialFiles(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "single_link_selected_identity", "fifo", "oversize", "shared_mode"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			paths := config.Paths{ConfigFile: filepath.Join(dir, "config.toml"), StateDir: dir}
			selected := filepath.Join(dir, "selected")
			if err := os.WriteFile(selected, []byte("deliberately invalid TOML source bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			var selectedStamp unix.Stat_t
			if err := unix.Lstat(selected, &selectedStamp); err != nil {
				t.Fatal(err)
			}
			proposal := inventory.HashProposal{Targets: []inventory.SavedFileTarget{{File: state.SameSizeFile{Device: fmt.Sprint(selectedStamp.Dev), Inode: fmt.Sprint(selectedStamp.Ino)}}}}
			var err error
			switch kind {
			case "symlink":
				err = os.Symlink(selected, paths.ConfigFile)
			case "hardlink":
				err = os.Link(selected, paths.ConfigFile)
			case "single_link_selected_identity":
				err = os.Rename(selected, paths.ConfigFile)
			case "fifo":
				err = unix.Mkfifo(paths.ConfigFile, 0600)
			case "oversize":
				err = os.WriteFile(paths.ConfigFile, []byte(strings.Repeat("x", hashConfigByteLimit+1)), 0600)
			case "shared_mode":
				err = os.WriteFile(paths.ConfigFile, []byte("invalid TOML"), 0644)
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err = loadHashRunConfig(ctx, paths, proposal)
			if err == nil || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "decode config") {
				t.Fatal("unsafe configuration reached parsing or blocked", err)
			}
		})
	}
}

func TestHashConfigHeldReaderMissingAndExistingPolicy(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	paths := config.Paths{ConfigFile: filepath.Join(dir, "config.toml"), StateDir: dir}
	ctx := context.Background()
	cfg, err := loadHashRunConfig(ctx, paths, inventory.HashProposal{})
	if err != nil || len(cfg.Excludes) != 0 {
		t.Fatal("missing manual configuration refused", cfg, err)
	}
	cfg = config.Default()
	cfg.Roots = []string{filepath.Join(dir, "unrelated-root")}
	cfg.Excludes = []string{filepath.Join(dir, "exclude")}
	if err := config.Create(paths.ConfigFile, dir, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := loadHashRunConfig(ctx, paths, inventory.HashProposal{})
	if err != nil || len(got.Excludes) != 1 || got.Excludes[0] != cfg.Excludes[0] {
		t.Fatal("current exclusion policy lost", got, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := loadHashRunConfig(canceled, paths, inventory.HashProposal{}); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled configuration request continued", err)
	}
}
