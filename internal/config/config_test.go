package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlatformPaths(t *testing.T) {
	env := func(key string) string {
		return map[string]string{"XDG_CONFIG_HOME": "/config", "XDG_STATE_HOME": "relative"}[key]
	}
	linux, err := PathsFor("linux", "/home/test", "", env)
	if err != nil || linux.ConfigFile != "/config/saga-rydd/config.toml" || linux.StateDir != "/home/test/.local/state/saga-rydd" {
		t.Fatalf("%+v %v", linux, err)
	}
	mac, err := PathsFor("darwin", "/Users/test", "", env)
	if err != nil || mac.StateDir != "/Users/test/Library/Application Support/saga-rydd" {
		t.Fatalf("%+v %v", mac, err)
	}
	portable, err := PathsFor("linux", "/home/test", "~/rydd", env)
	if err != nil || portable.ConfigFile != "/home/test/rydd/config.toml" {
		t.Fatalf("%+v %v", portable, err)
	}
	if _, err := PathsFor("linux", "/home/test", "relative", env); err == nil {
		t.Fatal("relative data dir accepted")
	}
}

func TestRoundTripAndNoOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "config.toml")
	c := Default()
	c.Roots = []string{"~/dev"}
	c.Excludes = []string{"~/dev/private"}
	if err := Create(path, "/home/test", c); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path, "/home/test")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Roots[0] != "/home/test/dev" || !loaded.Scan.PauseOnBattery || loaded.Scan.ReadBytesPerDay != 5<<30 {
		t.Fatalf("%+v", loaded)
	}
	before, _ := os.ReadFile(path)
	if err := Create(path, "/home/test", c); err == nil {
		t.Fatal("overwrote config")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("config changed")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
}

func TestStrictAndDefaultedDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("version = 1\nroots = ['/dev-root']\n[scan]\npause_on_battery = false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path, "/home/test")
	if err != nil || c.Scan.PauseOnBattery || c.Scan.WorkSeconds != 30 {
		t.Fatalf("%+v %v", c, err)
	}
	for _, content := range []string{
		"roots = ['/dev-root']\n[scan]\nread_bytes_per_dayy = 5",
		"version = 99\nroots = ['/dev-root']",
		"roots = ['/dev-root', '/dev-root/sub']",
		"roots = ['/dev-root']\nexcludes = ['/']",
		"roots = ['relative']",
		"roots = ['/dev-root']\n[scan]\nwork_seconds = 0",
		strings.Repeat("#", maxConfigBytes+1),
	} {
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path, "/home/test"); err == nil {
			t.Fatalf("invalid config accepted: %.100s", content)
		}
	}
}

func TestPathBoundariesAndPrivateFiles(t *testing.T) {
	if Within("/dev/project-old", "/dev/project") || !Within("/dev/project/sub", "/dev/project") {
		t.Fatal("wrong path boundary")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("roots=['/dev-root']"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, dir); err == nil {
		t.Fatal("shared config accepted")
	}
	link := filepath.Join(dir, "link.toml")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link, dir); err == nil {
		t.Fatal("symlink config accepted")
	}
}
