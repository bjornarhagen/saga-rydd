package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func requireSampleUsage(t *testing.T, usage FileReadUsage, requested, read int64, elapsed time.Duration) {
	t.Helper()
	if usage.RequestedBytes != requested || usage.ReadBytes != read || usage.Elapsed < 0 || usage.Elapsed > elapsed {
		t.Fatalf("issued read usage %+v, want requested=%d/read=%d within elapsed=%v", usage, requested, read, elapsed)
	}
}

func requireSampleUsageEmptyReport(t *testing.T, report FileSampleReport) {
	t.Helper()
	if !reflect.DeepEqual(report, FileSampleReport{}) {
		t.Fatal("call-level refusal published sample evidence", report)
	}
}

func sampleUsageReportJSON(t *testing.T, report FileSampleReport) []byte {
	t.Helper()
	report.CheckedAt = time.Time{}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestFileSampleUsageExistingWrapperParity(t *testing.T) {
	for _, kind := range []string{"success", "blocked"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("abc"), []byte("abc"), nil)
			if kind == "blocked" {
				for i := range targets {
					targets[i].File.Generation = 0
				}
			}
			old, oldErr := s.ObserveFileSamples(context.Background(), targets)
			started := time.Now()
			report, usage, err := s.ObserveFileSamplesWithUsage(context.Background(), targets)
			elapsed := time.Since(started)
			private, privateErr := s.observeFileSamples(context.Background(), targets, fileSampleHooks{})
			if oldErr != nil || err != nil || privateErr != nil || !bytes.Equal(sampleUsageReportJSON(t, old), sampleUsageReportJSON(t, report)) || !bytes.Equal(sampleUsageReportJSON(t, old), sampleUsageReportJSON(t, private)) {
				t.Fatal("existing public/private report API or JSON changed", oldErr, err, privateErr)
			}
			want := int64(6)
			if kind == "blocked" {
				want = 0
				for _, item := range report.Targets {
					if item.Status != "blocked" || item.SHA256 != "" {
						t.Fatal("blocked compatibility report gained a digest", item)
					}
				}
			} else if report.Status != "samples_observed" || len(report.Targets) != 3 || report.Targets[0].SHA256 == "" || report.Targets[2].SHA256 == "" {
				t.Fatal("successful self-cancel falsely refused the report", report)
			}
			requireSampleUsage(t, usage, want, want, elapsed)
			if report.RequestedBytes != want || report.ReadBytes != want || report.Executable || report.ProvenanceVerified || report.ContentVerified || report.CurrentStateVerified || report.DuplicatesVerified || report.EstimatedReclaimableBytes != nil {
				t.Fatal("report totals or evidence authority changed", report)
			}
		})
	}
}

func TestFileSampleUsageActualAfterReadCancellation(t *testing.T) {
	s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	started := time.Now()
	report, usage, err := s.observeFileSamplesWithUsage(ctx, targets, fileSampleHooks{afterRead: func(n int) {
		reads++
		if int64(n) != FileSampleWindowBytes {
			t.Fatal("generated first Pread returned unexpected bytes", n)
		}
		cancel()
	}})
	if !errors.Is(err, context.Canceled) || reads != 1 {
		t.Fatal("continued reading after actual-read cancellation", reads, err)
	}
	requireSampleUsageEmptyReport(t, report)
	requireSampleUsage(t, usage, FileSampleWindowBytes, FileSampleWindowBytes, time.Since(started))
	if usage.Elapsed <= 0 {
		t.Fatal("actual Pread has no measured elapsed time", usage)
	}
}

func TestFileSampleUsageMultipleCompletedTargets(t *testing.T) {
	s, targets := sampleFixture(t, bytes.Repeat([]byte{'a'}, 256*1024), bytes.Repeat([]byte{'b'}, 256*1024), bytes.Repeat([]byte{'c'}, 256*1024))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	started := time.Now()
	report, usage, err := s.observeFileSamplesWithUsage(ctx, targets, fileSampleHooks{afterRead: func(n int) {
		reads++
		if int64(n) != FileSampleWindowBytes {
			t.Fatal(n)
		}
		if reads == 7 {
			cancel()
		}
	}})
	if !errors.Is(err, context.Canceled) || reads != 7 {
		t.Fatal("prior completed targets were not charged or an eighth read occurred", reads, err)
	}
	requireSampleUsageEmptyReport(t, report)
	requireSampleUsage(t, usage, 7*FileSampleWindowBytes, 7*FileSampleWindowBytes, time.Since(started))
}

func TestFileSampleUsageActualShortReadAndEOF(t *testing.T) {
	for _, remaining := range []int64{17, 0} {
		t.Run(fmt.Sprint(remaining), func(t *testing.T) {
			s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			reads, returned := 0, 0
			started := time.Now()
			report, usage, err := s.observeFileSamplesWithUsage(context.Background(), targets, fileSampleHooks{
				afterOpen: func() {
					if err := os.Truncate(string(targets[0].File.PathBytes), remaining); err != nil {
						t.Fatal(err)
					}
				},
				afterRead: func(n int) { reads++; returned += n },
			})
			item := requireSampleBlocked(t, report, err)
			if item.Code != "sample_incomplete" || reads != 1 || int64(returned) != remaining || item.RequestedBytes != FileSampleWindowBytes || item.ReadBytes != remaining || report.RequestedBytes != FileSampleWindowBytes || report.ReadBytes != remaining {
				t.Fatal("actual short/EOF read was not charged exactly", item, reads, returned)
			}
			requireSampleUsage(t, usage, FileSampleWindowBytes, remaining, time.Since(started))
		})
	}
}

func TestFileSampleUsageFinalMetadataRefusal(t *testing.T) {
	body := bytes.Repeat([]byte{'x'}, 256*1024)
	s, targets := sampleFixture(t, body)
	reads, checks := 0, 0
	path := string(targets[0].File.PathBytes)
	started := time.Now()
	report, usage, err := s.observeFileSamplesWithUsage(context.Background(), targets, fileSampleHooks{
		afterRead: func(n int) {
			reads++
			if int64(n) != FileSampleWindowBytes {
				t.Fatal(n)
			}
		},
		beforeFinalCheck: func() {
			checks++
			if err := os.Rename(path, path+".held-original"); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
		},
	})
	item := requireSampleBlocked(t, report, err)
	if reads != 3 || checks != 1 || item.RequestedBytes != FileSampleFileByteLimit || item.ReadBytes != FileSampleFileByteLimit || report.RequestedBytes != FileSampleFileByteLimit || report.ReadBytes != FileSampleFileByteLimit {
		t.Fatal("same-body substituted inode lost actual read accounting", item, reads, checks)
	}
	requireSampleUsage(t, usage, FileSampleFileByteLimit, FileSampleFileByteLimit, time.Since(started))
}

func TestFileSampleUsageScannerCloseAfterRead(t *testing.T) {
	for _, kind := range []string{"after_read", "before_return"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			reads, returns := 0, 0
			hooks := fileSampleHooks{afterRead: func(n int) {
				reads++
				if int64(n) != FileSampleWindowBytes {
					t.Fatal(n)
				}
				if kind == "after_read" {
					s.Close()
				}
			}}
			if kind == "before_return" {
				hooks.beforeReturn = func() { returns++; s.Close() }
			}
			started := time.Now()
			report, usage, err := s.observeFileSamplesWithUsage(context.Background(), targets, hooks)
			wantReads := 1
			if kind == "before_return" {
				wantReads = 3
				if returns != 1 {
					t.Fatal("final return seam was not reached", returns)
				}
			}
			if err == nil || err.Error() != "scanner closed" || reads != wantReads {
				t.Fatal("scanner-close refusal or read boundary changed", reads, err)
			}
			requireSampleUsageEmptyReport(t, report)
			requireSampleUsage(t, usage, int64(wantReads)*FileSampleWindowBytes, int64(wantReads)*FileSampleWindowBytes, time.Since(started))
		})
	}
}

func TestFileSampleUsageZeroAdmissionAndPrecedence(t *testing.T) {
	for _, kind := range []string{"empty", "twenty_one", "oversized_path", "oversized_ancestors", "duplicate_id", "duplicate_path", "mixed_token", "mixed_root", "pre_canceled", "expired", "closed", "invalid_canceled", "invalid_closed"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("abc"), []byte("def"))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := ""
			switch kind {
			case "empty", "invalid_canceled", "invalid_closed":
				targets = nil
				want = "file samples require 1–20 exact targets"
				if kind == "invalid_canceled" {
					cancel()
				}
				if kind == "invalid_closed" {
					s.Close()
				}
			case "twenty_one":
				for len(targets) < FileSampleTargetLimit+1 {
					targets = append(targets, targets[0])
				}
				want = "file samples require 1–20 exact targets"
			case "oversized_path":
				targets[0].File.PathBytes = bytes.Repeat([]byte{'p'}, 4097)
				want = "file sample evidence exceeds its bounded size"
			case "oversized_ancestors":
				targets[0].Ancestors = make([]state.Entry, state.LivePathDepthLimit+2)
				want = "file sample ancestor evidence exceeds the 256-directory limit"
			case "duplicate_id":
				targets[1].File.ID = targets[0].File.ID
				want = "file samples require distinct exact file IDs and paths"
			case "duplicate_path":
				targets[1].File.PathBytes = targets[0].File.PathBytes
				want = "file samples require distinct exact file IDs and paths"
			case "mixed_token":
				targets[1].InventoryID = strings.Repeat("b", 64)
				want = "file samples require one caller-supplied inventory token"
			case "mixed_root":
				targets[1].Root.Revision++
				want = "file samples require consistent saved root bindings"
			case "pre_canceled":
				cancel()
				want = context.Canceled.Error()
			case "expired":
				var expiredCancel context.CancelFunc
				ctx, expiredCancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer expiredCancel()
				want = context.DeadlineExceeded.Error()
			case "closed":
				s.Close()
				want = "scanner closed"
			}
			effects := 0
			hooks := fileSampleHooks{afterOpen: func() { effects++ }, afterRead: func(int) { effects++ }, beforeFinalCheck: func() { effects++ }, beforeReturn: func() { effects++ }}
			started := time.Now()
			report, usage, err := s.observeFileSamplesWithUsage(ctx, targets, hooks)
			if err == nil || err.Error() != want || effects != 0 {
				t.Fatal("zero-admission error precedence or read behavior changed", kind, effects, err)
			}
			requireSampleUsageEmptyReport(t, report)
			requireSampleUsage(t, usage, 0, 0, time.Since(started))
		})
	}
}

func TestFileSampleUsageLastReturnCancellation(t *testing.T) {
	s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads, returns := 0, 0
	started := time.Now()
	report, usage, err := s.observeFileSamplesWithUsage(ctx, targets, fileSampleHooks{
		afterRead: func(n int) {
			reads++
			if int64(n) != FileSampleWindowBytes {
				t.Fatal(n)
			}
		},
		beforeReturn: func() { returns++; cancel() },
	})
	if !errors.Is(err, context.Canceled) || reads != 3 || returns != 1 {
		t.Fatal("final publication veto missed last-return cancellation", reads, returns, err)
	}
	requireSampleUsageEmptyReport(t, report)
	requireSampleUsage(t, usage, FileSampleFileByteLimit, FileSampleFileByteLimit, time.Since(started))
}

func TestFileSampleUsageActualFullCaps(t *testing.T) {
	contents := make([][]byte, FileSampleTargetLimit)
	for i := range contents {
		contents[i] = bytes.Repeat([]byte{byte(i)}, 256*1024)
	}
	s, targets := sampleFixture(t, contents...)
	reads := 0
	started := time.Now()
	report, usage, err := s.observeFileSamplesWithUsage(context.Background(), targets, fileSampleHooks{afterRead: func(n int) {
		reads++
		if int64(n) != FileSampleWindowBytes {
			t.Fatal(n)
		}
	}})
	if err != nil || report.Status != "samples_observed" || len(report.Targets) != FileSampleTargetLimit || reads != 60 || report.RequestedBytes != FileSampleCallByteLimit || report.ReadBytes != FileSampleCallByteLimit {
		t.Fatal("actual20-target issued-read caps changed", report, reads, err)
	}
	for _, item := range report.Targets {
		if item.RequestedBytes != FileSampleFileByteLimit || item.ReadBytes != FileSampleFileByteLimit || len(item.Ranges) != 3 || len(item.SHA256) != 64 {
			t.Fatal("per-file sample cap or digest changed", item)
		}
	}
	requireSampleUsage(t, usage, FileSampleCallByteLimit, FileSampleCallByteLimit, time.Since(started))
}

func TestFileSampleUsageFixedValueAndElapsed(t *testing.T) {
	typ := reflect.TypeOf(FileReadUsage{})
	if typ.NumField() != 3 || unsafe.Sizeof(FileReadUsage{}) != 24 {
		t.Fatal("usage no longer has its fixed three-scalar representation", typ)
	}
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).Type.Kind() != reflect.Int64 {
			t.Fatal("usage gained variable evidence", typ.Field(i))
		}
	}
	s, targets := sampleFixture(t, []byte("abc"))
	started := time.Now()
	report, usage, err := s.ObserveFileSamplesWithUsage(context.Background(), targets)
	if err != nil || len(report.Targets) != 1 || report.Targets[0].SHA256 == "" || usage.Elapsed <= 0 {
		t.Fatal(report, usage, err)
	}
	requireSampleUsage(t, usage, 3, 3, time.Since(started))
	encoded, err := json.Marshal(usage)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 || fields["requested_bytes"] == nil || fields["read_bytes"] == nil || fields["elapsed_ns"] == nil {
		t.Fatal("usage JSON gained a path, digest or evidence list", string(encoded))
	}
}
