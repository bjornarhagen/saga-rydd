package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestConfigStateBudgetDefaultsAndBoundaries(t *testing.T) {
	legacy := []byte("roots=['/generated-offline-root']\n[scan]\npause_on_battery=false\n")
	cfg, err := Decode(legacy, "/generated-home")
	if err != nil || cfg.Scan.MaxStateBytes != 1<<30 || cfg.Scan.PauseOnBattery {
		t.Fatal(cfg, err)
	}
	for _, limit := range []int64{MinStateBytes, DefaultMaxStateBytes, MaxStateBytes} {
		data := []byte(fmt.Sprintf("roots=['/generated-offline-root']\n[scan]\nmax_state_bytes=%d\n", limit))
		decoded, err := Decode(data, "/generated-home")
		if err != nil || decoded.Scan.MaxStateBytes != limit {
			t.Fatal(limit, decoded, err)
		}
	}
	for _, value := range []string{"0", "-1", "1048575", "1099511627777", "9223372036854775808", "'1073741824'", "1.5", "true"} {
		if _, err := Decode([]byte("roots=['/generated-offline-root']\n[scan]\nmax_state_bytes="+value+"\n"), "/generated-home"); err == nil {
			t.Fatal("invalid threshold admitted", value)
		}
	}
	for _, value := range []int64{0, -1, MinStateBytes - 1, MaxStateBytes + 1} {
		bad := Default()
		bad.Roots = []string{"/generated-offline-root"}
		bad.Scan.MaxStateBytes = value
		if err := bad.Validate("/generated-home"); err == nil {
			t.Fatal("direct config accepted invalid threshold", value)
		}
	}
}

func TestConfigStateBudgetSnapshotAndEditorPreserveThreshold(t *testing.T) {
	paths, cfg := exclusionFixture(t, false)
	loaded, snapshot, err := LoadSnapshot(context.Background(), paths.ConfigFile, "/generated-home")
	if err != nil || loaded.Scan.MaxStateBytes != cfg.Scan.MaxStateBytes {
		t.Fatal(loaded, err)
	}
	if err := snapshot.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(paths.ConfigFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EditExclusion(context.Background(), paths, "/generated-home", "add", "/generated-root/new"); err != nil {
		t.Fatal(err)
	}
	loaded, err = Load(paths.ConfigFile, "/generated-home")
	want := cfg
	want.Excludes = append(append([]string{}, cfg.Excludes...), "/generated-root/new")
	if err != nil || !reflect.DeepEqual(loaded, want) {
		t.Fatal("editor lost threshold/settings", loaded, want, err)
	}
	if err := snapshot.Check(context.Background()); err == nil {
		t.Fatal("old snapshot accepted edited settings")
	}
	if bytes, err := os.ReadFile(filepath.Join(paths.StateDir, "state.sqlite3")); err != nil || string(bytes) != "generated non-SQLite body must remain unchanged" {
		t.Fatal("editor read or changed generated state body", err)
	}
	if err := os.WriteFile(paths.ConfigFile, before, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err = Load(paths.ConfigFile, "/generated-home")
	if err != nil || !reflect.DeepEqual(loaded, cfg) {
		t.Fatal("exact prior config lost threshold", loaded, cfg, err)
	}
}
