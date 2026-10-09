package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestCPUFeedbackHumanUnknownAndObservedZero(t *testing.T) {
	zero, elapsed := int64(0), int64(time.Second)
	for _, test := range []struct {
		name       string
		feedback   state.CPUFeedbackState
		want, omit []string
	}{
		{"unavailable", state.CPUFeedbackState{Status: "unavailable"}, []string{"unavailable", "NOT RECORDED", "not a daily or hourly CPU quota"}, []string{"Saved process CPU", "completed=0"}},
		{"untracked", state.CPUFeedbackState{Status: "untracked"}, []string{"untracked", "NOT RECORDED"}, []string{"Saved process CPU"}},
		{"pending", state.CPUFeedbackState{Status: "pending", Window: &state.CPUWindowRecord{Token: "exact-pending"}}, []string{"UNKNOWN", "not settled", "Writer recovery is required"}, []string{"Saved process CPU: 0"}},
		{"zero", state.CPUFeedbackState{Status: "observed", Window: &state.CPUWindowRecord{CPUTimeNS: &zero, ElapsedNS: &elapsed}}, []string{"Saved process CPU: 0s", "elapsed: 1s", "Saved views do not recover"}, []string{"measurement: UNKNOWN"}},
		{"recovery", state.CPUFeedbackState{Status: "recovered_unknown", UnknownRecoveryBackoffNS: int64(time.Hour), RecoveredUnknownWindows: 1, Window: &state.CPUWindowRecord{Token: "exact-old"}}, []string{"UNKNOWN", "1h0m0s conservative delay", "not a measured CPU charge", "recovered=1", "Battery and physical sleep behavior are not evaluated"}, []string{"Saved process CPU: 0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			printCPUFeedback(&out, &test.feedback)
			rendered := strings.Join(strings.Fields(out.String()), " ")
			for _, want := range test.want {
				if !strings.Contains(rendered, want) {
					t.Fatal(want, out.String())
				}
			}
			for _, omit := range test.omit {
				if strings.Contains(rendered, omit) {
					t.Fatal("unknown was presented as measured zero", omit, out.String())
				}
			}
		})
	}
}

func TestCPUFeedbackSavedStatusOfflineAndFailedRepliesDoNotRecover(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	dir, root := filepath.Join(base, "state"), filepath.Join(base, "offline-source")
	t.Setenv("RYDD_RUNTIME_DIR", filepath.Join(base, "runtime"))
	var out, diagnostic bytes.Buffer
	if code := Run(context.Background(), []string{"--data-dir", dir, "init", "--root", root}, &out, &diagnostic); code != 0 {
		t.Fatal(code, diagnostic.String())
	}
	ctx := context.Background()
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	scope, err := w.ResolveFairInventoryRoots(ctx, []string{root})
	if err != nil {
		t.Fatal(err)
	}
	reserved := time.Now()
	if _, err := w.ReserveScanChunk(ctx, reserved, time.Millisecond, 100); err != nil {
		t.Fatal(err)
	}
	turn, err := w.ClaimFairInventoryTurn(ctx, scope, time.Now(), time.Minute, true, reserved)
	if err != nil || turn == nil {
		t.Fatal(turn, err)
	}
	marker, err := w.BeginCPUWindow(ctx, turn, time.Now(), state.CPUWindowStart{Instance: strings.Repeat("a", 32), WindowStartedAt: reserved})
	if err != nil {
		t.Fatal(err)
	}
	before, err := w.CPUFeedback(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range []bool{false, true} {
		args := []string{"--data-dir", dir, "status"}
		if machine {
			args = append(args, "--json")
		}
		out.Reset()
		diagnostic.Reset()
		if code := Run(ctx, args, &out, &diagnostic); code != 0 {
			t.Fatal(code, out.String(), diagnostic.String())
		}
		if machine {
			var reply struct {
				OK       bool                   `json:"ok"`
				Feedback state.CPUFeedbackState `json:"cpu_feedback"`
			}
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil || !reply.OK || !reflect.DeepEqual(reply.Feedback, before) {
				t.Fatal("status invented CPU settlement", reply, err)
			}
		} else if !strings.Contains(out.String(), marker.Token()) || !strings.Contains(out.String(), "UNKNOWN") {
			t.Fatal(out.String())
		}
		for _, writer := range []interface{ Write([]byte) (int, error) }{serviceFailWriter{}, serviceShortWriter{}} {
			diagnostic.Reset()
			if code := Run(ctx, args, writer, &diagnostic); code != 1 {
				t.Fatal("failed status output succeeded", code, diagnostic.String())
			}
		}
		cancelCtx, cancel := context.WithCancel(ctx)
		cancelWriter := &cpuStatusCancelWriter{cancel: cancel}
		diagnostic.Reset()
		if code := Run(cancelCtx, args, cancelWriter, &diagnostic); code != 1 {
			t.Fatal("late canceled status succeeded", code, diagnostic.String())
		}
		if machine {
			var reply map[string]any
			decoder := json.NewDecoder(bytes.NewReader(cancelWriter.Bytes()))
			if err := decoder.Decode(&reply); err != nil || reply["ok"] != true {
				t.Fatal(reply, err)
			}
			if err := decoder.Decode(&reply); err != io.EOF {
				t.Fatal("second envelope after canceled status", cancelWriter.String(), err)
			}
		}
	}
	after, err := w.CPUFeedback(ctx)
	if err != nil || !reflect.DeepEqual(before, after) || after.Status != "pending" {
		t.Fatal("inspection/reply failure recovered CPU", before, after, err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatal("saved status touched offline source", err)
	}
}

type cpuStatusCancelWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cpuStatusCancelWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.cancel()
	return n, err
}

func TestCPUFeedbackCapabilitiesRemainNarrow(t *testing.T) {
	body, err := json.Marshal(capabilities())
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Features map[string]bool `json:"features"`
		Contract struct {
			Name, Scope string
			Hourly      bool  `json:"hourly_limit_enforced"`
			Global      bool  `json:"global_cpu_quota"`
			Manual      bool  `json:"manual_scans_included"`
			Recovery    int64 `json:"interrupted_recovery_delay_ns"`
		} `json:"cpu_feedback_contract"`
	}
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Features["durable_cpu_feedback"] || !report.Features["interrupted_cpu_window_cooldown"] || report.Features["cpu_limit"] || report.Features["power_controls"] || report.Contract.Name != state.CPUFeedbackContract || report.Contract.Scope != "experimental_inventory_dispatch_windows" || report.Contract.Hourly || report.Contract.Global || report.Contract.Manual || report.Contract.Recovery != int64(time.Hour) {
		t.Fatal("durable cooldown became a full resource guarantee", string(body))
	}
}

func TestCPUFeedbackStatusErrorCodesAdvertised(t *testing.T) {
	body, err := json.Marshal(capabilities())
	if err != nil {
		t.Fatal(err)
	}
	var caps struct {
		Codes []string `json:"error_codes"`
	}
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		err  error
		want string
	}{
		{state.ErrCPUFeedbackCorrupt, "cpu_feedback_invalid"},
		{state.ErrCPUFeedbackUnavailable, "cpu_feedback_unavailable"},
		{errors.Join(state.ErrCPUFeedbackCorrupt, context.Canceled), "canceled"},
	} {
		var out, diagnostic bytes.Buffer
		if code := operationFailure(&out, &diagnostic, "status", test.err); code != 1 {
			t.Fatal(code, diagnostic.String())
		}
		var reply struct {
			OK    bool `json:"ok"`
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.OK || reply.Error.Code != test.want {
			t.Fatal(reply, err)
		}
		found := false
		for _, code := range caps.Codes {
			found = found || code == reply.Error.Code
		}
		if !found {
			t.Fatal("unadvertised status error code", reply.Error.Code)
		}
	}
}
