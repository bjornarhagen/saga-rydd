package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestMachineContract(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	cases := []struct {
		args      []string
		exit      int
		errorCode string
	}{
		{[]string{"capabilities", "--json"}, 0, ""},
		{[]string{"nonsense", "--json"}, 2, "invalid_arguments"},
		{[]string{"daemon", "--json"}, 2, "unsupported_output"},
		{[]string{"init", "--bad", "--json"}, 2, "invalid_arguments"},
		{[]string{"init", "--root", "/fixture", "--json"}, 0, ""},
		{[]string{"status", "--json"}, 0, ""},
		{[]string{"config", "check", "--json"}, 0, ""},
		{[]string{"state", "init", "--json"}, 0, ""},
		{[]string{"init", "--root", "/fixture", "--json"}, 1, "already_exists"},
		{[]string{"pause", "--json"}, 1, "worker_not_running"},
	}
	for _, tc := range cases {
		var out, errOut bytes.Buffer
		exit := Run(context.Background(), append([]string{"--data-dir", dir}, tc.args...), &out, &errOut)
		if exit != tc.exit || errOut.Len() != 0 {
			t.Fatalf("%v: exit %d stderr %s stdout %s", tc.args, exit, &errOut, &out)
		}
		var result struct {
			Version int  `json:"api_version"`
			OK      bool `json:"ok"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if result.Version != 1 || result.OK != (exit == 0) || result.Error.Code != tc.errorCode {
			t.Fatalf("%v: %s", tc.args, &out)
		}
	}
}
