package config

import (
	"os"
	"path/filepath"
	"reflect"
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

func TestDecodeParityDefaultsStrictnessAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	for index, content := range []string{
		"roots=['~/dev']\nexcludes=['~/dev/private']\n[scan]\npause_on_battery=false\n",
		"roots=['/offline-root']\n[scan]\nread_bytes_per_day=1024\n",
		"roots=['/offline-root']\nunknown=true\n",
		"roots=['/offline-root']\n[scan]\nread_bytes_per_dayy=1024\n",
		"roots=['/offline-root']\nroots=['/other-root']\n",
		"version=99\nroots=['/offline-root']\n",
		"roots=['/offline-root','/offline-root/sub']\n",
		"roots=['/offline-root']\nexcludes=['/']\n",
		"roots=['relative']\n",
		"roots=['/offline-root']\n[scan]\nwork_seconds=0\n",
		strings.Repeat("#", maxConfigBytes+1),
	} {
		data := []byte(content)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, loadErr := Load(path, "/offline-home")
		decoded, decodeErr := Decode(data, "/offline-home")
		if (decodeErr == nil) != (index < 2) {
			t.Fatalf("unexpected byte-decoder validity for %.100q: %v", content, decodeErr)
		}
		if !reflect.DeepEqual(loaded, decoded) || (loadErr == nil) != (decodeErr == nil) || (loadErr != nil && loadErr.Error() != decodeErr.Error()) {
			t.Fatalf("held-byte decoding differs from file loading for %.100q: %+v/%v versus %+v/%v", content, decoded, decodeErr, loaded, loadErr)
		}
	}
}

func TestDecodeOfflineBytesAndSizeBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	data := []byte("roots=['~/dev']\n[scan]\npause_on_battery=false\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	c, err := Decode(data, "/offline-home")
	if err != nil || len(c.Roots) != 1 || c.Roots[0] != "/offline-home/dev" || c.Scan.PauseOnBattery || c.Scan.WorkSeconds != 30 || c.Scan.ReadBytesPerDay != 5<<30 {
		t.Fatal("offline held bytes lost defaults or path normalization", c, err)
	}
	prefix := "roots=['/offline-root']\n#"
	boundary := []byte(prefix + strings.Repeat("x", maxConfigBytes-len(prefix)))
	if c, err = Decode(boundary, "/offline-home"); err != nil || c.Roots[0] != "/offline-root" {
		t.Fatal("valid exactly1MiB configuration rejected", err)
	}
	if _, err = Decode(append(boundary, 'x'), "/offline-home"); err == nil || err.Error() != "configuration exceeds 1 MiB" {
		t.Fatal("oversized held bytes accepted", err)
	}
}
