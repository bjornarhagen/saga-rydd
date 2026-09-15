package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestInitValidateStatusAndNoOverwrite(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state with ? and #")
	prefix := []string{"--data-dir", dir}
	run := func(args ...string) (int, string, string) {
		var out, errOut bytes.Buffer
		code := Run(context.Background(), append(append([]string{}, prefix...), args...), &out, &errOut)
		return code, out.String(), errOut.String()
	}
	if code, _, _ := run("status"); code != 1 {
		t.Fatal("missing status exit", code)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("status created state")
	}
	if code, _, err := run("init", "--root", "/synthetic/dev", "--exclude", "/synthetic/dev/private"); code != 0 {
		t.Fatal(code, err)
	}
	if code, _, err := run("config", "check"); code != 0 {
		t.Fatal(code, err)
	}
	code, out, err := run("status", "--json")
	if code != 0 {
		t.Fatal(err)
	}
	var report struct {
		Stage string `json:"stage"`
		State struct {
			EnabledRoots int `json:"enabled_roots"`
			Entries      int `json:"entries"`
		} `json:"state"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatal(err)
	}
	if report.Stage != "experimental-inventory" || report.State.EnabledRoots != 1 || report.State.Entries != 0 {
		t.Fatalf("%+v", report)
	}
	before, _ := os.ReadFile(filepath.Join(dir, "config.toml"))
	if code, _, _ := run("init", "--root", "/other"); code == 0 {
		t.Fatal("init overwrote existing config")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "config.toml"))
	if string(before) != string(after) {
		t.Fatal("config replaced")
	}
	if code, _, err := run("state", "init"); code != 0 {
		t.Fatal(code, err)
	}
	if code, _, _ := run("scan"); code != 2 {
		t.Fatal("unimplemented command accepted")
	}
}
