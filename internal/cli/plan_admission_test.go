package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestPlanAdmissionCLIActualCapacityAndHistoricalAccess(t *testing.T) {
	f := reviewCLIFixture(t, false)
	ctx := context.Background()
	for i := 0; i < plans.PlanLimit-2; i++ {
		r := f.saved.Record
		r.CreatedAt = time.Unix(1700000000, int64(i)).UTC()
		if _, err := plans.SaveRecord(ctx, f.base, r); err != nil {
			t.Fatal(err)
		}
	}
	finding := f.saved.Record.Selection.Targets[0].FindingID
	code, raw := f.run("plan", "--save", "-d", f.root, finding, "--json")
	var last struct {
		OK   bool        `json:"ok"`
		Plan plans.Saved `json:"plan"`
	}
	if err := json.Unmarshal([]byte(raw), &last); err != nil || code != 0 || !last.OK || !plans.ValidID(last.Plan.ID) {
		t.Fatal("127 ->128 CLI save failed", code, raw, err)
	}
	code, raw = f.run("plan", "--save", "-d", f.root, finding, "--json")
	if code != 1 || !strings.Contains(raw, `"code":"plan_capacity"`) || strings.Contains(raw, `"plan_candidate"`) || strings.Contains(raw, `"plan":`) {
		t.Fatal("capacity reply invented publication", code, raw)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	var guided bytes.Buffer
	err = review(ctx, []string{"-d", f.root}, paths, strings.NewReader("1\nsave\n"), &guided)
	if !errors.Is(err, plans.ErrPlanCapacity) || strings.Contains(guided.String(), "SELECTION SAVED") || strings.Contains(guided.String(), "Candidate plan:") {
		t.Fatal("guided capacity fabricated a save", err, guided.String())
	}
	if code, raw = f.run(f.approveArgs()...); code != 0 || !strings.Contains(raw, "review_approved") {
		t.Fatal("cap blocked historical review", code, raw)
	}
	// Independent dismissals do not consume or release plan root slots.
	request, err := plans.NewDismissalRequest([]byte(f.root), f.saved.Record.Selection)
	if err != nil {
		t.Fatal(err)
	}
	dismissed, err := plans.SaveDismissal(ctx, f.base, request)
	if err != nil {
		t.Fatal(err)
	}
	if code, raw = f.run("ignore", "--show", dismissed.ID, "--json"); code != 0 {
		t.Fatal(code, raw)
	}
	if err = os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(manualState(paths, f.root), manualState(paths, f.root)+".offline"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"plan", "--show", f.saved.ID, "--json"}, {"plan", "--revoke", f.saved.ID, "--json"}, {"ignore", "--undo", dismissed.ID, "--json"}} {
		if code, raw = f.run(args...); code != 0 {
			t.Fatal("capacity blocked offline history", args, code, raw)
		}
	}
	if data, err := os.ReadFile(filepath.Join(f.root+".offline", "node_modules", "keep.txt")); err != nil || string(data) != "fixture data" {
		t.Fatal("original fixture body changed", err)
	}
}

func TestPlanAdmissionCLICandidateFailuresAndGuidedPublication(t *testing.T) {
	f := reviewCLIFixture(t, false)
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	finding := f.saved.Record.Selection.Targets[0].FindingID
	for _, cause := range []error{plans.ErrPlanPublication, errors.Join(plans.ErrPlanPublication, context.Canceled), context.Canceled} {
		saver := func(context.Context, string, state.SelectionSnapshot) (plans.Saved, error) { return f.saved, cause }
		result, replyErr := planWithSaver(context.Background(), []string{"--save", "-d", f.root, finding}, paths, saver)
		candidate, ok := result.(planPublicationCandidate)
		if !ok || candidate.Publication != "unconfirmed" || candidate.CleanupAuthorized || candidate.CandidatePlanID != f.saved.ID || !errors.Is(replyErr, cause) || candidate.ReopenCommand != commandPrefix(paths)+" plan --show "+f.saved.ID {
			t.Fatal("plan lost exact candidate or claimed publication", result, replyErr)
		}
		var out, stderr bytes.Buffer
		code := planMachineFailure(&out, &stderr, candidate, replyErr)
		var envelope map[string]json.RawMessage
		if err = json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 1 || stderr.Len() != 0 || envelope["plan_candidate"] == nil || envelope["plan"] != nil || string(envelope["ok"]) != "false" {
			t.Fatal("invalid candidate failure envelope", out.String(), stderr.String(), err)
		}
		want := "canceled"
		if errors.Is(cause, plans.ErrPlanPublication) {
			want = "plan_outcome_unknown"
		}
		if !strings.Contains(string(envelope["error"]), `"code":"`+want+`"`) || strings.Contains(out.String(), "was saved") {
			t.Fatal("wrong failure precedence or saved claim", out.String())
		}
		stderr.Reset()
		if code = planMachineFailure(&planReplyWriter{short: true}, &stderr, candidate, replyErr); code != 1 || !strings.Contains(stderr.String(), candidate.ReopenCommand) || strings.Contains(stderr.String(), "was saved") {
			t.Fatal("short candidate reply hid recovery guidance", stderr.String())
		}
		out.Reset()
		printPlanCandidate(&out, candidate)
		if !strings.Contains(out.String(), "PUBLICATION UNCONFIRMED") || !strings.Contains(out.String(), candidate.ReopenCommand) || strings.Contains(out.String(), "SAVED FOR REVIEW") {
			t.Fatal(out.String())
		}
		out.Reset()
		err = reviewWithPlanSaver(context.Background(), []string{"-d", f.root}, paths, strings.NewReader("1\nsave\n"), &out, saver)
		if !errors.Is(err, cause) || !strings.Contains(out.String(), candidate.ReopenCommand) || strings.Contains(out.String(), "SELECTION SAVED") || strings.Contains(out.String(), "Saved plan:") {
			t.Fatal("guided publication failure claimed save or lost ID", err, out.String())
		}
	}
	// Other command cancellation precedence is unchanged.
	var out, stderr bytes.Buffer
	operationFailure(&out, &stderr, "ignore", errors.Join(plans.ErrPlanPublication, context.Canceled))
	if !strings.Contains(out.String(), `"code":"canceled"`) {
		t.Fatal("plan error changed another command's cancellation", out.String())
	}
}

type planReplyWriter struct {
	bytes.Buffer
	short  bool
	cancel context.CancelFunc
}

func (w *planReplyWriter) Write(p []byte) (int, error) {
	if w.short {
		return 0, nil
	}
	n, err := w.Buffer.Write(p)
	if w.cancel != nil {
		w.cancel()
	}
	return n, err
}

func TestPlanAdmissionCLIShortAndCanceledHistoricalReplies(t *testing.T) {
	f := reviewCLIFixture(t, false)
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	for _, machine := range []bool{false, true} {
		for _, short := range []bool{false, true} {
			ctx, cancel := context.WithCancel(context.Background())
			out := &planReplyWriter{short: short}
			if !short {
				out.cancel = cancel
			}
			var stderr bytes.Buffer
			args := []string{"--data-dir", f.base, "plan", "--show", f.saved.ID}
			if machine {
				args = append(args, "--json")
			}
			code := Run(ctx, args, out, &stderr)
			cancel()
			if code != 1 || !strings.Contains(stderr.String(), commandPrefix(paths)+" plan --show "+f.saved.ID) || strings.Contains(stderr.String(), "retry plan --save") {
				t.Fatal("reply failure lost exact historical guidance", machine, short, code, stderr.String())
			}
		}
	}
	if err := printPlanResult(&planReplyWriter{short: true}, f.saved, paths); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("human plan output ignored short write", err)
	}
}

func TestPlanAdmissionCLICapabilityContract(t *testing.T) {
	var out, stderr bytes.Buffer
	code := Run(context.Background(), []string{"capabilities", "--json"}, &out, &stderr)
	var envelope struct {
		Admission map[string]json.RawMessage `json:"saved_plan_admission_contract"`
		Features  map[string]bool            `json:"features"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || code != 0 || stderr.Len() != 0 || !envelope.Features["saved_plan_admission"] || envelope.Features["cleanup"] || string(envelope.Admission["root_limit"]) != "128" || string(envelope.Admission["maximum_supported_history_json_bytes_per_plan"]) != "23429120" {
		t.Fatal(code, out.String(), err)
	}
	for _, key := range []string{"slots_refunded", "history_deleted", "physical_database_size_limit", "space_reserved", "other_stores_included", "refused_legacy_save_migrates", "unchanged_legacy_stores_fence_old_writers", "cleanup_authorized"} {
		if string(envelope.Admission[key]) != "false" {
			t.Fatal("unqualified capacity capability", key)
		}
	}
	if string(envelope.Admission["scope"]) != `"one_private_plan_store"` || string(envelope.Admission["history_bound_scope"]) != `"plan_v1_review_observation_preparation_only_v1"` || string(envelope.Admission["history_bound_exclusions"]) != `["independent_dismissals","store_identities","sqlite_indexes","sqlite_pages","sqlite_wal"]` {
		t.Fatal("history capacity was relabeled a whole-store bound")
	}
}

type planGuidedCancelWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *planGuidedCancelWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if strings.Contains(string(p), "SELECTION SAVED") {
		w.cancel()
	}
	return n, err
}

func TestPlanAdmissionCLIGuidedConfirmedReplyCancellation(t *testing.T) {
	f := reviewCLIFixture(t, false)
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"at_saved_banner", "after_save_return"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var saved plans.Saved
			saver := func(call context.Context, base string, selection state.SelectionSnapshot) (plans.Saved, error) {
				var e error
				saved, e = plans.Save(call, base, selection)
				if e == nil && mode == "after_save_return" {
					cancel()
				}
				return saved, e
			}
			out := &planGuidedCancelWriter{cancel: cancel}
			err := reviewWithPlanSaver(ctx, []string{"-d", f.root}, paths, strings.NewReader("1\nsave\n"), out, saver)
			if !errors.Is(err, context.Canceled) || !plans.ValidID(saved.ID) || !strings.Contains(err.Error(), commandPrefix(paths)+" plan --show "+saved.ID) || !strings.Contains(err.Error(), "available in saved history") || strings.Contains(out.String(), "PUBLICATION UNCONFIRMED") {
				t.Fatal("confirmed commit lost reply guidance or was relabeled unknown", saved.ID, err, out.String())
			}
			if mode == "at_saved_banner" {
				match := regexp.MustCompile(`Saved plan: (plan-v1-[0-9a-f]{64})`).FindStringSubmatch(out.String())
				if len(match) != 2 || match[1] != saved.ID {
					t.Fatal("canceled confirmed completion lost exact ID")
				}
			}
			if _, err = plans.Load(context.Background(), paths.StateDir, saved.ID); err != nil {
				t.Fatal("canceled reply erased confirmed plan", err)
			}
		})
	}
}
