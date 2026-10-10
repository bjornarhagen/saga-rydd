package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/doctor"
)

func doctorGeneratedReply() doctor.Report {
	zero := int64(0)
	r := doctor.Report{Contract: doctor.Contract, Source: "selected_existing_metadata_configuration_main_header", DataDirectory: "/generated/private", Status: "checks_finished", RequestedBodyBytes: &zero, ReadBodyBytes: &zero, CurrentStateSchema: nil, LedgerIntegrity: "not_checked", WorkerActivity: "unknown", LockAvailability: "unknown", Guidance: []doctor.Guidance{}}
	for _, id := range []string{"directory", "configuration", "state_header", "wal", "shm", "journal", "writer_lock"} {
		r.Checks = append(r.Checks, doctor.Check{ID: id, Status: "not_checked", Qualification: "Generated bounded fixture; no instance was opened."})
	}
	return r
}

func doctorFakeOperations(calls *int, result doctor.Report, err error) doctorOperations {
	return doctorOperations{observe: func(_ context.Context, req doctor.Request, validate doctor.ConfigValidator) (doctor.Report, error) {
		*calls = *calls + 1
		if req.DataDirectory != "/generated/private" || req.Catalog.ApplicationID == 0 || validate == nil {
			return doctor.Report{}, errors.New("generated admission mismatch")
		}
		return result, err
	}}
}

func TestDoctorArgumentsBeforeIO(t *testing.T) {
	cases := [][]string{
		{"doctor"}, {"--json", "doctor"}, {"--data-dir", "relative", "doctor"},
		{"--data-dir", "/", "doctor"}, {"--data-dir", "/a/../b", "doctor"},
		{"--data-dir", "/generated/private", "doctor", "operand"},
		{"--data-dir", "/generated/private", "doctor", "--data-dir", "/other"},
		{"--data-dir", "/generated/private", "-data-dir=/generated/private", "doctor"},
		{"--data-dir", "/generated/private", "doctor", "--json", "--json"},
		{"--data-dir", "/generated/private", "doctor", "--json=false"},
		{"--data-dir", "/generated/private", "doctor", "--unknown"},
		{"--data-dir", "/generated/private", "doctor", "--"},
		{"--data-dir", "/a\x00b", "doctor"}, {"--data-dir", "/a\xffb", "doctor"},
		{"--data-dir", "/" + strings.Repeat("d/", 64) + "d", "doctor"},
	}
	for _, args := range cases {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			calls := 0
			out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
			exit := runDoctorWith(context.Background(), args, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil))
			if exit != 2 || calls != 0 {
				t.Fatal(exit, calls, out.String(), errOut.String())
			}
		})
	}
	for _, args := range [][]string{
		{"--data-dir", "/generated/private", "doctor"},
		{"-data-dir=/generated/private", "--json", "doctor"},
		{"--data-dir=/generated/private", "doctor", "--json"},
	} {
		calls := 0
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		if exit := runDoctorWith(context.Background(), args, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil)); exit != 0 || calls != 1 || errOut.Len() != 0 {
			t.Fatal(exit, calls, errOut.String())
		}
	}
	t.Setenv("HOME", "")
	for _, args := range [][]string{{"doctor"}, {"doctor", "--json"}} {
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		if exit := Run(context.Background(), args, out, errOut); exit != 2 {
			t.Fatal("default HOME resolution was entered", exit, out.String(), errOut.String())
		}
	}
	if doctorInvocation([]string{"--data-dir", "doctor", "status"}) || doctorInvocation([]string{"--version", "doctor"}) || doctorInvocation([]string{"report", "doctor"}) {
		t.Fatal("stole another command")
	}
	args := append([]string{"--data-dir", "/generated/private", "doctor"}, make([]string, 14)...)
	if _, err := parseDoctorArgs(args); !errors.Is(err, doctor.ErrArguments) {
		t.Fatal("token bound", err)
	}
	for _, args := range [][]string{{"doctor", "--help"}, {"--data-dir", "/generated/private", "doctor", "-h"}} {
		calls := 0
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		if exit := runDoctorWith(context.Background(), args, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil)); exit != 0 || calls != 0 || !strings.Contains(out.String(), "ABS_EXISTING_PRIVATE_INSTANCE") {
			t.Fatal("help entered observation", exit, calls)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	if exit := runDoctorWith(ctx, []string{"--data-dir", "/generated/private", "doctor", "--json"}, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil)); exit != 1 || calls != 0 || errOut.Len() != 0 {
		t.Fatal("canceled admission", exit, calls, errOut.String())
	}
}

func TestDoctorConfigurationSubset(t *testing.T) {
	encodePaths := func(paths []string) string {
		parts := make([]string, len(paths))
		for i, p := range paths {
			parts[i] = fmt.Sprintf("%q", p)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	roots := make([]string, 128)
	excludes := make([]string, 128)
	for i := range roots {
		roots[i] = fmt.Sprintf("/generated/r%03d", i)
		excludes[i] = fmt.Sprintf("/excluded/e%03d", i)
	}
	body := func(rs, es []string) []byte {
		return []byte("version = 1\nroots = " + encodePaths(rs) + "\nexcludes = " + encodePaths(es) + "\n")
	}
	got, err := doctorConfiguration(body(roots, excludes))
	if err != nil || got.Roots != 128 || got.Exclusions != 128 {
		t.Fatal(got, err)
	}
	if _, err := doctorConfiguration(body(append(roots, "/other"), nil)); !errors.Is(err, doctor.ErrConfigUnsupported) {
		t.Fatal("root count", err)
	}
	if _, err := doctorConfiguration(body(roots, append(excludes, "/other"))); !errors.Is(err, doctor.ErrConfigUnsupported) {
		t.Fatal("exclude count", err)
	}
	longs := make([]string, 17)
	for i := range longs {
		longs[i] = fmt.Sprintf("/%02d%s", i, strings.Repeat("x", 3990))
	}
	if _, err := doctorConfiguration(body(longs, nil)); !errors.Is(err, doctor.ErrConfigUnsupported) {
		t.Fatal("aggregate path bound", err)
	}
	if _, err := doctorConfiguration(body([]string{"/" + strings.Repeat("x", 4096)}, nil)); !errors.Is(err, doctor.ErrConfigUnsupported) {
		t.Fatal("individual bound", err)
	}
	for _, bad := range [][]byte{[]byte("unknown = true\n"), body([]string{"relative"}, nil), body([]string{"/generated", "/generated/child"}, nil), []byte("version=9\nroots=[\"/generated\"]\n"), []byte("version=1\nroots=[\"/generated\"]\n[scan]\nwork_seconds=0\n")} {
		if _, err := doctorConfiguration(bad); !errors.Is(err, doctor.ErrConfigInvalid) {
			t.Fatal("invalid profile", err)
		}
	}
	t.Setenv("HOME", "")
	if _, err := doctorConfiguration(body([]string{"/generated"}, nil)); err != nil {
		t.Fatal("absolute paths need HOME", err)
	}
	if _, err := doctorConfiguration(body([]string{"~/generated"}, nil)); !errors.Is(err, doctor.ErrConfigEnvironment) {
		t.Fatal("HOME unavailable", err)
	}
	t.Setenv("HOME", "/generated-home")
	if _, err := doctorConfiguration(body([]string{"~/generated"}, nil)); err != nil {
		t.Fatal(err)
	}
}

type doctorFaultWriter struct {
	short  bool
	err    error
	cancel context.CancelFunc
	calls  int
	body   []byte
}

func (w *doctorFaultWriter) Write(p []byte) (int, error) {
	w.calls++
	w.body = append(w.body, p...)
	if w.cancel != nil {
		w.cancel()
	}
	if w.short {
		return len(p) - 1, w.err
	}
	return len(p), w.err
}

func TestDoctorCheckedOutput(t *testing.T) {
	args := []string{"--data-dir", "/generated/private", "doctor", "--json"}
	for _, tc := range []struct {
		name  string
		short bool
		err   error
	}{{"short", true, nil}, {"error", false, io.ErrClosedPipe}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			out := &doctorFaultWriter{short: tc.short, err: tc.err}
			errOut := &bytes.Buffer{}
			if exit := runDoctorWith(context.Background(), args, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil)); exit != 1 || out.calls != 1 || !strings.Contains(errOut.String(), "no second reply") {
				t.Fatal(exit, out.calls, errOut.String())
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	out := &doctorFaultWriter{cancel: cancel}
	errOut := &bytes.Buffer{}
	if exit := runDoctorWith(ctx, args, out, errOut, doctorFakeOperations(&calls, doctorGeneratedReply(), nil)); exit != 1 || out.calls != 1 || !strings.Contains(errOut.String(), "canceled") {
		t.Fatal(exit, out.calls, errOut.String())
	}
	var late doctorReply
	if err := json.Unmarshal(out.body, &late); err != nil || !late.OK {
		t.Fatal("one completed historical reply", err, late)
	}
	for _, machine := range []bool{false, true} {
		r := doctorGeneratedReply()
		r.Checks[0].Qualification = strings.Repeat("large ", doctor.MaxReportBytes)
		calls := 0
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		a := args[:3]
		if machine {
			a = args
		}
		if exit := runDoctorWith(context.Background(), a, out, errOut, doctorFakeOperations(&calls, r, nil)); exit != 1 {
			t.Fatal("reply cap", exit)
		}
		if machine {
			var small doctorReply
			if err := json.Unmarshal(out.Bytes(), &small); err != nil || small.OK || small.Doctor != nil || errOut.Len() != 0 {
				t.Fatal("bounded fallback", small, err, errOut.String())
			}
		} else if out.Len() != 0 {
			t.Fatal("partial human staged report")
		}
	}
}

func TestDoctorHumanMachineQualificationAndPrivacy(t *testing.T) {
	for _, machine := range []bool{false, true} {
		calls := 0
		r := doctorGeneratedReply()
		r.Status = "attention_required"
		out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
		args := []string{"--data-dir", "/generated/private", "doctor"}
		if machine {
			args = append(args, "--json")
		}
		err := errors.Join(doctor.ErrAttention, errors.New("private underlying configuration detail must stay internal"))
		if exit := runDoctorWith(context.Background(), args, out, errOut, doctorFakeOperations(&calls, r, err)); exit != 1 || errOut.Len() != 0 {
			t.Fatal(exit, errOut.String())
		}
		if strings.Contains(out.String(), "private underlying") {
			t.Fatal("private underlying error disclosed")
		}
		if machine {
			var reply doctorReply
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.OK || reply.Error.Code != "doctor_attention_required" || reply.Doctor.CurrentStateSchema != nil || reply.Doctor.WorkerActivity != "unknown" || reply.Doctor.Effects != (doctor.Effects{}) {
				t.Fatal(reply, err)
			}
		} else if !strings.Contains(out.String(), "LIVE HEALTH NOT CHECKED") || !strings.Contains(out.String(), "unknown") {
			t.Fatal(out.String())
		}
	}
	if code, _, exit := doctorError(errors.Join(doctor.ErrAttention, context.Canceled)); code != "canceled" || exit != 1 {
		t.Fatal(code, exit)
	}
	before := doctorGeneratedReply()
	after := doctorGeneratedReply()
	if !reflect.DeepEqual(before.Checks, after.Checks) {
		t.Fatal("generated seven-check scope differs")
	}
}
