package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const inspectManifest = `{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}}`

func inspectLock() string {
	return `{"name":"fixture","version":"1.0.0","lockfileVersion":3,"packages":{"":{"name":"fixture","version":"1.0.0","dependencies":{"fixture-dep":"1.0.0"}},"node_modules/fixture-dep":{"version":"1.0.0","resolved":"https://registry.npmjs.org/fixture-dep/-/fixture-dep-1.0.0.tgz","integrity":"sha512-` + strings.Repeat("A", 86) + `=="}}}`
}

func inspectionFixture(t *testing.T, setup func(project, target string)) (*Scanner, state.LiveTarget) {
	t.Helper()
	return liveFixtureWithSetup(t, func(root, target string) {
		project := filepath.Dir(target)
		for name, contents := range map[string]string{"package.json": inspectManifest, "package-lock.json": inspectLock()} {
			if err := os.WriteFile(filepath.Join(project, name), []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if setup != nil {
			setup(project, target)
		}
	})
}

func TestProjectInputInspection(t *testing.T) {
	s, target := inspectionFixture(t, nil)
	r, err := s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.CurrentStateVerified || r.Executable || r.RegenerationVerified || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" || len(r.Targets) != 1 {
		t.Fatal(r, err)
	}
	inputs := r.Targets[0].Inputs
	if inputs == nil || inputs.LockfileVersion != 3 || inputs.LockedPackages != 1 || len(inputs.Files) != 2 {
		t.Fatal(r)
	}
	for i, contents := range []string{inspectManifest, inspectLock()} {
		digest := sha256.Sum256([]byte(contents))
		if inputs.Files[i].Bytes != len(contents) || inputs.Files[i].SHA256 != hex.EncodeToString(digest[:]) {
			t.Fatal(inputs)
		}
	}
	// Existing saved plans have no lock-content baseline. This input observation
	// must not be presented as equality with bytes seen by the scanner.
	lockPath := filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json")
	newLock := inspectLock() + "\n"
	if err := os.WriteFile(lockPath, []byte(newLock), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.Targets[0].Inputs.Files[1].SHA256 == inputs.Files[1].SHA256 {
		t.Fatal(r, err)
	}
	// Local dependency edits are deliberately unknown in this input-only slice.
	if err := os.WriteFile(filepath.Join(string(target.Finding.PathBytes), "keep.txt"), []byte("local edits"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err = s.Inspect(context.Background(), []state.LiveTarget{target})
	if err != nil || r.Status != "inputs_observed" || r.DependencyContentsChecked || r.LocalDependencyEdits != "unknown" {
		t.Fatal(r, err)
	}
}

func TestProjectInputRefusals(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "fifo", "directory", "oversize_manifest", "oversize_lock", "excluded_lock", "invalid", "duplicate", "v1", ".npmrc", "npm-shrinkwrap.json", "patches", "binding.gyp"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, func(project, target string) {
				p := filepath.Join(project, "package-lock.json")
				var err error
				switch kind {
				case "missing", "symlink", "fifo", "directory":
					if err = os.Remove(p); err != nil {
						t.Fatal(err)
					}
					switch kind {
					case "symlink":
						err = os.Symlink("package.json", p)
					case "fifo":
						err = unix.Mkfifo(p, 0600)
					case "directory":
						err = os.Mkdir(p, 0700)
					}
				case "oversize_manifest":
					err = os.Truncate(filepath.Join(project, "package.json"), ManifestInputLimit+1)
				case "oversize_lock":
					err = os.Truncate(p, LockInputLimit+1)
				case "invalid":
					err = os.WriteFile(p, []byte("not JSON"), 0600)
				case "duplicate":
					err = os.WriteFile(p, []byte(`{"lockfileVersion":3,"lockfileVersion":2}`), 0600)
				case "v1":
					err = os.WriteFile(p, []byte(`{"lockfileVersion":1}`), 0600)
				case "excluded_lock":
				default:
					// These bytes are deliberately not JSON. Presence blocks before
					// opening or parsing config files that may contain secrets.
					err = os.WriteFile(filepath.Join(project, kind), []byte("private fixture setting"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			if kind == "excluded_lock" {
				s.excludes = append(s.excludes, filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json"))
			}
			r, err := s.Inspect(context.Background(), []state.LiveTarget{target})
			if err != nil || r.Status != "blocked" || r.Targets[0].Code == "" || r.Targets[0].Inputs != nil || strings.Contains(r.Targets[0].Message, "private fixture setting") {
				t.Fatal(r, err)
			}
		})
	}
}

func TestProjectInputsChangedDuringInspection(t *testing.T) {
	for _, kind := range []string{"edit_lock", "replace_lock", "symlink_lock", "edit_manifest", "config_appears", "path_swap"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, nil)
			project := filepath.Dir(string(target.Finding.PathBytes))
			_, err := s.inspectTarget(context.Background(), target, func() {
				lock := filepath.Join(project, "package-lock.json")
				var mutationErr error
				switch kind {
				case "edit_lock":
					mutationErr = os.WriteFile(lock, []byte(inspectLock()+"\n"), 0600)
				case "edit_manifest":
					mutationErr = os.WriteFile(filepath.Join(project, "package.json"), []byte(inspectManifest+"\n"), 0600)
				case "replace_lock", "symlink_lock":
					if mutationErr = os.Rename(lock, lock+".old"); mutationErr != nil {
						t.Fatal(mutationErr)
					}
					if kind == "replace_lock" {
						mutationErr = os.WriteFile(lock, []byte(inspectLock()), 0600)
					} else {
						mutationErr = os.Symlink(lock+".old", lock)
					}
				case "config_appears":
					mutationErr = os.WriteFile(filepath.Join(project, ".npmrc"), []byte("private fixture setting"), 0600)
				case "path_swap":
					if mutationErr = os.Rename(project, project+".old"); mutationErr != nil {
						t.Fatal(mutationErr)
					}
					mutationErr = os.Symlink(project+".old", project)
				}
				if mutationErr != nil {
					t.Fatal(mutationErr)
				}
			})
			if err == nil {
				t.Fatal("accepted input/path mutation after content read")
			}
		})
	}
}

func TestProjectInputLimitsAndCancellation(t *testing.T) {
	s, target := inspectionFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Inspect(ctx, []state.LiveTarget{target}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Inspect(context.Background(), make([]state.LiveTarget, 21)); err == nil {
		t.Fatal("ignored target limit")
	}
}

type inspectionReadObservation struct {
	requested, read int64
	calls           int
}

func (o *inspectionReadObservation) observe(want, n int) {
	o.requested += int64(want)
	o.read += int64(n)
	o.calls++
}
func requireInspectionUsage(t *testing.T, usage FileReadUsage, observed inspectionReadObservation) {
	t.Helper()
	if usage.RequestedBytes != observed.requested || usage.ReadBytes != observed.read || usage.ReadBytes > usage.RequestedBytes || usage.Elapsed < 0 {
		t.Fatal("issued-read usage differs", usage, observed)
	}
}

func TestProjectInputUsageParity(t *testing.T) {
	for _, tree := range []bool{false, true} {
		t.Run(map[bool]string{false: "inputs", true: "tree"}[tree], func(t *testing.T) {
			var s *Scanner
			var target state.LiveTarget
			if tree {
				s, target = treeFixture(t, nil)
			} else {
				s, target = inspectionFixture(t, nil)
			}
			targets := []state.LiveTarget{target}
			var old InspectionReport
			var err error
			if tree {
				old, err = s.InspectTree(context.Background(), targets)
			} else {
				old, err = s.Inspect(context.Background(), targets)
			}
			if err != nil {
				t.Fatal(err)
			}
			var observed inspectionReadObservation
			report, usage, err := s.inspectWithUsage(context.Background(), targets, tree, inspectUsageHooks{afterRead: observed.observe})
			if err != nil || observed.calls < 2 || observed.read != int64(len(inspectManifest)+len(inspectLock())) || report.CurrentStateVerified || report.Executable || report.RegenerationVerified || report.DependencyContentsChecked || report.LocalDependencyEdits != "unknown" {
				t.Fatal(report, usage, observed, err)
			}
			requireInspectionUsage(t, usage, observed)
			old.CheckedAt = time.Time{}
			report.CheckedAt = time.Time{}
			a, e := json.Marshal(old)
			if e != nil {
				t.Fatal(e)
			}
			b, e := json.Marshal(report)
			if e != nil || !bytes.Equal(a, b) {
				t.Fatal("usage changed old reportJSON", old, report, e)
			}
			// Two input files can issue multiple Read attempts, including EOF.
			// A supported tree has a real bin link and ordinary dependency body;
			// neither contributes to these observed input-file counters.
			if tree && (report.Targets[0].Tree == nil || report.Targets[0].Tree.InternalBinLinks != 1) {
				t.Fatal(report)
			}
			if tree {
				_, usage, err = s.InspectTreeWithUsage(context.Background(), targets)
			} else {
				_, usage, err = s.InspectWithUsage(context.Background(), targets)
			}
			if err != nil || usage.ReadBytes != observed.read {
				t.Fatal("new public wrapper differs", usage, err)
			}
		})
	}
}

func TestProjectInputUsageZeroAdmission(t *testing.T) {
	for _, kind := range []string{"count_zero", "count_21", "pre_canceled", "deadline_expired", "manifest_symlink", "manifest_fifo", "excluded_manifest", "credential_hint", "oversize_manifest", "protected_manifest"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, func(project, target string) {
				manifest := filepath.Join(project, "package.json")
				var err error
				switch kind {
				case "manifest_symlink", "manifest_fifo":
					if err = os.Remove(manifest); err != nil {
						t.Fatal(err)
					}
					if kind == "manifest_symlink" {
						err = os.Symlink("package-lock.json", manifest)
					} else {
						err = unix.Mkfifo(manifest, 0600)
					}
				case "credential_hint":
					err = os.WriteFile(filepath.Join(project, ".npmrc"), []byte("private fixture credential"), 0600)
				case "oversize_manifest":
					err = os.Truncate(manifest, ManifestInputLimit+1)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			targets := []state.LiveTarget{target}
			ctx := context.Background()
			cancel := func() {}
			switch kind {
			case "count_zero":
				targets = nil
			case "count_21":
				targets = make([]state.LiveTarget, 21)
			case "pre_canceled":
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "deadline_expired":
				ctx, cancel = context.WithDeadline(ctx, time.Unix(0, 1))
				<-ctx.Done()
			case "excluded_manifest":
				s.excludes = append(s.excludes, string(target.Finding.ManifestPathBytes))
			case "protected_manifest":
				var st unix.Stat_t
				if err := unix.Lstat(string(target.Finding.ManifestPathBytes), &st); err != nil {
					t.Fatal(err)
				}
				s.protectedIDs[objectID(st)] = true
			}
			defer cancel()
			var observed inspectionReadObservation
			r, u, err := s.inspectWithUsage(ctx, targets, false, inspectUsageHooks{afterRead: observed.observe})
			requireInspectionUsage(t, u, observed)
			if observed.calls != 0 || u.RequestedBytes != 0 || u.ReadBytes != 0 {
				t.Fatal("pre-read refusal issued a Read", r, u, observed, err)
			}
			if kind == "pre_canceled" && !errors.Is(err, context.Canceled) || kind == "deadline_expired" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if err == nil && (r.Status != "blocked" || r.Targets[0].Inputs != nil || strings.Contains(r.Targets[0].Message, "private fixture credential")) {
				t.Fatal("refusal disclosed evidence", r)
			}
		})
	}
}

func TestProjectInputUsageLaterRefusal(t *testing.T) {
	for _, kind := range []string{"missing_lock", "excluded_lock", "oversize_lock", "invalid_lock"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, func(project, target string) {
				p := filepath.Join(project, "package-lock.json")
				var err error
				switch kind {
				case "missing_lock":
					err = os.Remove(p)
				case "oversize_lock":
					err = os.Truncate(p, LockInputLimit+1)
				case "invalid_lock":
					err = os.WriteFile(p, []byte("not JSON"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			})
			if kind == "excluded_lock" {
				s.excludes = append(s.excludes, filepath.Join(filepath.Dir(string(target.Finding.PathBytes)), "package-lock.json"))
			}
			var observed inspectionReadObservation
			r, u, err := s.inspectWithUsage(context.Background(), []state.LiveTarget{target}, false, inspectUsageHooks{afterRead: observed.observe})
			requireInspectionUsage(t, u, observed)
			if err != nil || r.Status != "blocked" || r.Targets[0].Inputs != nil || r.Targets[0].Tree != nil || u.ReadBytes < int64(len(inspectManifest)) || observed.calls == 0 {
				t.Fatal("later refusal lost manifest usage or published inputs", r, u, err)
			}
		})
	}
}

func TestProjectInputUsageShortEOFAndFailedRead(t *testing.T) {
	for _, kind := range []string{"short_and_EOF", "failed_actual_read"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, nil)
			var observed inspectionReadObservation
			closed := false
			h := inspectUsageHooks{afterRead: observed.observe}
			if kind == "failed_actual_read" {
				h.beforeRead = func(f *os.File, want int) {
					if !closed {
						closed = true
						if err := f.Close(); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			r, u, err := s.inspectWithUsage(context.Background(), []state.LiveTarget{target}, false, h)
			requireInspectionUsage(t, u, observed)
			if err != nil || u.RequestedBytes <= u.ReadBytes || observed.calls == 0 {
				t.Fatal("short/failed allowance not charged", r, u, observed, err)
			}
			if kind == "failed_actual_read" {
				if !closed || observed.calls != 1 || u.ReadBytes != 0 || r.Status != "blocked" || r.Targets[0].Inputs != nil {
					t.Fatal("failed genuine Read fabricated evidence", r, u, observed)
				}
			} else if r.Status != "inputs_observed" || u.ReadBytes != int64(len(inspectManifest)+len(inspectLock())) || observed.calls <= 2 {
				t.Fatal("EOF/short geometry was replaced", r, u, observed)
			}
		})
	}
}

func TestProjectInputUsageCancellationAndLateVeto(t *testing.T) {
	for _, kind := range []string{"after_actual_read", "final_return"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var observed inspectionReadObservation
			h := inspectUsageHooks{afterRead: func(want, n int) {
				observed.observe(want, n)
				if kind == "after_actual_read" {
					if n <= 0 {
						t.Fatal("cancellation preceded actual bytes", n)
					}
					cancel()
				}
			}}
			if kind == "final_return" {
				h.beforeFinalReturn = cancel
			}
			r, u, err := s.inspectWithUsage(ctx, []state.LiveTarget{target}, false, h)
			requireInspectionUsage(t, u, observed)
			if !errors.Is(err, context.Canceled) || !reflectInspectionZero(r) || u.ReadBytes <= 0 {
				t.Fatal("canceled reply lost usage or published inputs", r, u, err)
			}
			if kind == "final_return" && u.ReadBytes != int64(len(inspectManifest)+len(inspectLock())) {
				t.Fatal("late veto lost completed reads", u)
			}
		})
	}
}

func reflectInspectionZero(r InspectionReport) bool {
	b, err := json.Marshal(r)
	zero, e := json.Marshal(InspectionReport{})
	return err == nil && e == nil && bytes.Equal(b, zero)
}

func TestProjectInputUsageFinalRefusals(t *testing.T) {
	for _, kind := range []string{"edit_lock", "replace_lock", "hint_appears", "path_swap"} {
		t.Run(kind, func(t *testing.T) {
			s, target := inspectionFixture(t, nil)
			project := filepath.Dir(string(target.Finding.PathBytes))
			var observed inspectionReadObservation
			h := inspectUsageHooks{afterRead: observed.observe, beforeRecheck: func() {
				p := filepath.Join(project, "package-lock.json")
				var err error
				switch kind {
				case "edit_lock":
					err = os.WriteFile(p, []byte(inspectLock()+"\n"), 0600)
				case "replace_lock":
					if err = os.Rename(p, p+".old"); err == nil {
						err = os.WriteFile(p, []byte(inspectLock()), 0600)
					}
				case "hint_appears":
					err = os.WriteFile(filepath.Join(project, ".npmrc"), []byte("private fixture credential"), 0600)
				case "path_swap":
					if err = os.Rename(project, project+".old"); err == nil {
						err = os.Symlink(project+".old", project)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}}
			r, u, err := s.inspectWithUsage(context.Background(), []state.LiveTarget{target}, false, h)
			requireInspectionUsage(t, u, observed)
			if err != nil || r.Status != "blocked" || r.Targets[0].Inputs != nil || r.Targets[0].Tree != nil || u.ReadBytes != int64(len(inspectManifest)+len(inspectLock())) {
				t.Fatal("final fence lost reads or published tentative inputs", r, u, err)
			}
		})
	}
}

func TestProjectInputUsageMultipleTargetsAndBounds(t *testing.T) {
	for _, count := range []int{3, 20} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var roots []string
			var targets []state.LiveTarget
			for i := 0; i < count; i++ {
				_, target := inspectionFixture(t, nil)
				target.Finding.ID = fmt.Sprintf("generated-usage-%d", i)
				target.Binding.FindingID = target.Finding.ID
				roots = append(roots, string(target.Root.PathBytes))
				targets = append(targets, target)
			}
			s, err := New(roots, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			s.excludes = append(s.excludes, filepath.Join(filepath.Dir(string(targets[1].Finding.PathBytes)), "package-lock.json"))
			var observed inspectionReadObservation
			r, u, err := s.inspectWithUsage(context.Background(), targets, false, inspectUsageHooks{afterRead: observed.observe})
			requireInspectionUsage(t, u, observed)
			want := int64(count)*int64(len(inspectManifest)+len(inspectLock())) - int64(len(inspectLock()))
			if err != nil || len(r.Targets) != count || r.Status != "blocked" || r.Targets[1].Inputs != nil || u.ReadBytes != want {
				t.Fatal("multi-target usage reset or reordered", r, u, err)
			}
			for i, item := range r.Targets {
				if item.FindingID != targets[i].Finding.ID || i != 1 && item.Inputs == nil {
					t.Fatal("ordered target result changed", i, item)
				}
			}
		})
	}
}

func TestProjectInputUsageGrowthAndArithmetic(t *testing.T) {
	s, target := inspectionFixture(t, nil)
	manifest := string(target.Finding.ManifestPathBytes)
	var observed inspectionReadObservation
	grew := false
	h := inspectUsageHooks{afterRead: func(want, n int) {
		observed.observe(want, n)
		if !grew {
			if n <= 0 {
				t.Fatal("growth preceded genuine bytes")
			}
			grew = true
			if err := os.Truncate(manifest, ManifestInputLimit+1); err != nil {
				t.Fatal(err)
			}
		}
	}}
	r, u, err := s.inspectWithUsage(context.Background(), []state.LiveTarget{target}, false, h)
	requireInspectionUsage(t, u, observed)
	if err != nil || !grew || r.Status != "blocked" || r.Targets[0].Code != "input_limit" || r.Targets[0].Inputs != nil || u.ReadBytes != ManifestInputLimit+1 {
		t.Fatal("growth sentinel changed or lost delivered bytes", r, u, err)
	}
	counter := int64(math.MaxInt64 - 1)
	if !addInspectionUsage(&counter, 1) || counter != math.MaxInt64 || addInspectionUsage(&counter, 1) || counter != math.MaxInt64 || addInspectionUsage(&counter, -1) || counter != math.MaxInt64 {
		t.Fatal("integer boundary wrapped/refunded", counter)
	}
	// The pure scalar guard has no source callback. The production Read boundary
	// checks it before the issued call; legacy nil usage has no new refusal.
	zero := int64(0)
	if !addInspectionUsage(&zero, 0) || zero != 0 {
		t.Fatal(zero)
	}
	other, ordinary := inspectionFixture(t, nil)
	overflow := FileReadUsage{RequestedBytes: math.MaxInt64}
	refused, e := other.inspectCore(context.Background(), []state.LiveTarget{ordinary}, false, &overflow, inspectUsageHooks{beforeRead: func(*os.File, int) { t.Fatal("overflow issued an input Read") }})
	if e != nil || refused.Status != "blocked" || refused.Targets[0].Code != "input_usage_limit" || refused.Targets[0].Inputs != nil || overflow.RequestedBytes != math.MaxInt64 || overflow.ReadBytes != 0 {
		t.Fatal("pre-read scalar refusal changed prior counters", refused, overflow, e)
	}
	legacy, e := other.Inspect(context.Background(), []state.LiveTarget{ordinary})
	if e != nil || legacy.Status != "inputs_observed" {
		t.Fatal("legacy nil accounting inherited overflow refusal", legacy, e)
	}
}
