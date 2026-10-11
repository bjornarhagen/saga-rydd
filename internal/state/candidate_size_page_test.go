package state

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func candidateSizeTestReport(count int) FindingReport {
	report := FindingReport{Source: "saved_inventory", EntryLimit: FindingEntryLimit, EntriesExamined: count}
	for i := 0; i < count; i++ {
		path := []byte(fmt.Sprintf("/fixture/project%d/node_modules", i))
		logical, allocated := int64(i+1), int64(4096)
		report.Findings = append(report.Findings, Finding{
			ID: fmt.Sprintf("node-modules-v1:1:%d", i+1), RootID: 1, EntryID: int64(i + 1),
			Device: "generated-device", Inode: fmt.Sprint(i + 1), PathBytes: path,
			Measurement: DirectoryReport{Source: "saved_inventory", RootID: 1, PathBytes: path,
				Status: "recorded_complete", CoverageSource: "bounded_entry_check", EntryLimit: DirectoryEntryLimit,
				EntriesExamined: 2, FilePaths: 1, LogicalBytes: &logical, AllocatedBytes: &allocated,
				AllocatedSizeSource: "bounded_identity_check"},
		})
	}
	return report
}

func candidateSizeIDs(report FindingReport) []string {
	ids := make([]string, len(report.Findings))
	for i := range report.Findings {
		ids[i] = report.Findings[i].ID
	}
	return ids
}

func candidateSizeSummary(t *testing.T, report FindingReport) CandidatePageSizeSummary {
	t.Helper()
	receipt, err := freezeCandidateSizePage(report)
	if err != nil {
		t.Fatal(err)
	}
	result, err := receipt.SummarizeVisible(candidateSizeIDs(report))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func candidateSizeAmount(t *testing.T, got *int64, want int64) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("amount %v, want %d", got, want)
	}
}

func TestCandidateSizePageOneWALSnapshot(t *testing.T) {
	w, id := savedSelectionFixture(t)
	ctx := context.Background()
	r, err := OpenReader(ctx, filepath.Dir(w.path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	before, beforeReceipt, err := r.NodeModulesSizePage(ctx, "", FindingAgeDays)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var token string
	if err = tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity").Scan(&token); err != nil {
		t.Fatal(err)
	}
	// Commit a different binding and amount while the WAL reader keeps its
	// earlier snapshot. This is an actual database commit, not a mocked read.
	writer, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.ExecContext(ctx, "UPDATE roots SET volume_id='replacement'; UPDATE entries SET inode='replacement',ctime_ns=999 WHERE path=CAST('a/node_modules' AS BLOB); UPDATE entries SET size=999 WHERE path=CAST('a/node_modules/file' AS BLOB)"); err != nil {
		writer.Rollback()
		t.Fatal(err)
	}
	if err = writer.Commit(); err != nil {
		t.Fatal(err)
	}
	page, receipt, err := r.nodeModulesSizePage(ctx, "", FindingAgeDays, tx)
	if err != nil {
		t.Fatal(err)
	}
	if page.InventoryID != token || len(page.Targets) != 1 || len(page.Roots) != 1 || page.Targets[0].Target.Inode != before.Targets[0].Target.Inode || page.Roots[0].Fingerprint != before.Roots[0].Fingerprint {
		t.Fatal("binding crossed the held inventory snapshot", page, before)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	old, err := receipt.SummarizeVisible([]string{id})
	if err != nil {
		t.Fatal(err)
	}
	want, err := beforeReceipt.SummarizeVisible([]string{id})
	if err != nil || !reflect.DeepEqual(old, want) {
		t.Fatal("measurement crossed the held inventory snapshot", old, want, err)
	}
	candidateSizeAmount(t, old.LogicalPathBytes, 3)
	fresh, freshReceipt, err := r.NodeModulesSizePage(ctx, "", FindingAgeDays)
	if err != nil || fresh.Targets[0].Target.Inode != "replacement" || fresh.Roots[0].Fingerprint != "replacement" {
		t.Fatal(fresh, err)
	}
	newer, err := freshReceipt.SummarizeVisible([]string{id})
	if err != nil {
		t.Fatal(err)
	}
	candidateSizeAmount(t, newer.LogicalPathBytes, 999)
	// The receipt remains usable after the reader is closed. Summarization
	// therefore cannot depend on a subsequent database query or transaction.
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	afterClose, err := receipt.SummarizeVisible([]string{id})
	if err != nil || !reflect.DeepEqual(old, afterClose) {
		t.Fatal(afterClose, err)
	}
}

func TestCandidateSizePageMutationIsolationAndExistingJSON(t *testing.T) {
	s, id := savedSelectionFixture(t)
	page, receipt, err := s.NodeModulesSizePage(context.Background(), "", FindingAgeDays)
	if err != nil {
		t.Fatal(err)
	}
	want, err := receipt.SummarizeVisible([]string{id})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(page)
	if err != nil || bytes.Contains(payload, []byte("candidate_page_size")) || bytes.Contains(payload, []byte("logical_path_bytes")) {
		t.Fatal("receipt changed saved selection JSON", string(payload), err)
	}
	var keys map[string]json.RawMessage
	if err = json.Unmarshal(payload, &keys); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 4 {
		t.Fatal("selection gained a public field", keys)
	}
	f := &page.Evidence.Findings[0]
	*f.Measurement.LogicalBytes = 12345
	*f.Measurement.AllocatedBytes = 12345
	f.Measurement.FilePaths, f.Measurement.UnknownInodes = 99, 99
	f.Measurement.Status, f.Measurement.CoverageSource = "unknown", "changed"
	f.PathBytes[0], f.Measurement.PathBytes[0] = 'x', 'x'
	f.ID, f.Device, f.Inode = "changed", "changed", "changed"
	page.Targets[0].FindingID, page.Targets[0].Target.Inode = "changed", "changed"
	page.Roots[0].PathBytes[0] = 'x'
	page.Evidence.Findings = nil
	got, err := receipt.SummarizeVisible([]string{id})
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("public page mutation reached receipt", got, want, err)
	}
	*got.LogicalPathBytes = -1
	again, err := receipt.SummarizeVisible([]string{id})
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("returned summary pointer reached receipt", again, want, err)
	}
}

func TestCandidateSizePageExactVisibleSubset(t *testing.T) {
	report := candidateSizeTestReport(3)
	receipt, err := freezeCandidateSizePage(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, ids := range [][]string{{"foreign"}, {report.Findings[0].ID, report.Findings[0].ID}, {report.Findings[0].ID, report.Findings[1].ID, report.Findings[2].ID, "foreign"}} {
		if _, err = receipt.SummarizeVisible(ids); !errors.Is(err, ErrCandidateSizePage) {
			t.Fatal(ids, err)
		}
	}
	for _, invalid := range []*CandidateSizePage{nil, {}} {
		if _, err = invalid.SummarizeVisible(nil); !errors.Is(err, ErrCandidateSizePage) {
			t.Fatal("manufactured empty receipt accepted", err)
		}
	}
	ids := []string{report.Findings[2].ID, report.Findings[0].ID}
	got, err := receipt.SummarizeVisible(ids)
	if err != nil || got.DisplayedFolders != 2 || got.KnownLogicalMeasurements != 2 {
		t.Fatal(got, err)
	}
	candidateSizeAmount(t, got.LogicalPathBytes, 4)
	candidateSizeAmount(t, got.MeasuredFilePaths, 2)
	ids[0] = "changed"
	if got.WithinFolderAllocatedBytes != nil || got.AllocatedUnavailableReason != "cross_folder_identity_evidence_unavailable" {
		t.Fatal("multi-folder allocation claimed", got)
	}
	empty, err := receipt.SummarizeVisible(nil)
	if err != nil || empty.Status != "empty_selection" || empty.DisplayedFolders != 0 {
		t.Fatal(empty, err)
	}
	candidateSizeAmount(t, empty.LogicalPathBytes, 0)
	candidateSizeAmount(t, empty.MeasuredFilePaths, 0)
	candidateSizeAmount(t, empty.WithinFolderAllocatedBytes, 0)
}

func TestCandidateSizePageRawIdentityBounds(t *testing.T) {
	for _, length := range []int{0, 1, 256, 257} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			report := candidateSizeTestReport(2)
			raw := strings.Repeat("x", length)
			if length > 0 {
				raw = "\xff" + raw[1:]
			}
			for i := range report.Findings {
				report.Findings[i].Device, report.Findings[i].Inode = raw, raw
			}
			receipt, err := freezeCandidateSizePage(report)
			if err != nil {
				t.Fatal("raw identity hid candidates", err)
			}
			got, err := receipt.SummarizeVisible(candidateSizeIDs(report))
			if err != nil || got.DisplayedFolders != 2 {
				t.Fatal(got, err)
			}
			if length == 0 || length == 257 {
				if got.UnknownDirectoryIdentities != 2 || got.KnownDirectoryAliasPairs != 0 {
					t.Fatal(got)
				}
			} else if got.UnknownDirectoryIdentities != 0 || got.KnownDirectoryAliasPairs != 1 || !bytes.Equal(receipt.rows[0].inode.data[:length], []byte(raw)) {
				t.Fatal("identity bytes were normalized", got)
			}
			single, err := receipt.SummarizeVisible([]string{report.Findings[0].ID})
			if err != nil {
				t.Fatal(err)
			}
			if length == 0 || length == 257 {
				if single.WithinFolderAllocatedBytes != nil || single.AllocatedUnavailableReason != "single_folder_identity_unavailable" {
					t.Fatal(single)
				}
			} else {
				candidateSizeAmount(t, single.WithinFolderAllocatedBytes, 4096)
			}
		})
	}
	for _, component := range []string{"device", "inode"} {
		report := candidateSizeTestReport(1)
		if component == "device" {
			report.Findings[0].Device = ""
		} else {
			report.Findings[0].Inode = strings.Repeat("y", 257)
		}
		got := candidateSizeSummary(t, report)
		if got.UnknownDirectoryIdentities != 1 || got.WithinFolderAllocatedBytes != nil {
			t.Fatal(component, got)
		}
	}
}

func TestCandidateSizePageOrdinaryRawIdentityRemainsVisible(t *testing.T) {
	s, id := savedSelectionFixture(t)
	for _, raw := range []string{"", "device\xff", strings.Repeat("x", 257)} {
		if _, err := s.db.Exec("UPDATE entries SET device=?,inode=? WHERE path=CAST('a/node_modules' AS BLOB)", raw, raw); err != nil {
			t.Fatal(err)
		}
		page, receipt, err := s.NodeModulesSizePage(context.Background(), "", FindingAgeDays)
		if err != nil || len(page.Evidence.Findings) != 1 || page.Evidence.Findings[0].ID != id || page.Evidence.Findings[0].Device != raw || page.Evidence.Findings[0].Inode != raw {
			t.Fatal("raw identity changed or removed an ordinary candidate", page, err)
		}
		got, err := receipt.SummarizeVisible([]string{id})
		if err != nil {
			t.Fatal(err)
		}
		wantUnknown := 0
		if len(raw) == 0 || len(raw) > candidateSizeIdentityLimit {
			wantUnknown = 1
		}
		if got.UnknownDirectoryIdentities != wantUnknown {
			t.Fatal("bounded identity availability changed", got)
		}
		candidateSizeAmount(t, got.LogicalPathBytes, 3)
	}
}

func TestCandidateSizePageRawPathOverlap(t *testing.T) {
	for _, pair := range []struct {
		a, b    string
		overlap bool
	}{
		{"/fixture/a", "/fixture/ab", false}, {"/fixture/a", "/fixture/a/child", true},
		{"/fixture/a", "/fixture/a", true}, {"/", "/fixture/a", true},
		{"/fixture/raw\xff", "/fixture/raw\xfe", false}, {"/fixture/raw\xff", "/fixture/raw\xff/child", true},
	} {
		t.Run(fmt.Sprintf("%q:%q", pair.a, pair.b), func(t *testing.T) {
			report := candidateSizeTestReport(2)
			for i, path := range []string{pair.a, pair.b} {
				report.Findings[i].PathBytes = []byte(path)
				report.Findings[i].Measurement.PathBytes = []byte(path)
			}
			got := candidateSizeSummary(t, report)
			if pair.overlap {
				if got.OverlapPairs != 1 || got.LogicalPathBytes != nil || got.MeasuredFilePaths != nil || got.LogicalUnavailableReason != "overlap_detected" || got.FilePathsUnavailableReason != "overlap_detected" {
					t.Fatal(got)
				}
			} else {
				candidateSizeAmount(t, got.LogicalPathBytes, 3)
				candidateSizeAmount(t, got.MeasuredFilePaths, 2)
			}
			if got.DisplayedFolders != 2 {
				t.Fatal("overlap removed a candidate", got)
			}
		})
	}
}

func TestCandidateSizePageMissingAndQualifiedMeasurements(t *testing.T) {
	report := candidateSizeTestReport(3)
	partial := &report.Findings[0].Measurement
	partial.Status, partial.SkippedEntries, partial.UnconfirmedEntries, partial.IncompleteDirectories = "partial", 1, 1, 1
	partial.LogicalBytes = nil
	stale := &report.Findings[1].Measurement
	stale.Status, stale.DirectoryErrors, stale.RootError = "stale", 1, "generated saved error"
	unknown := &report.Findings[2].Measurement
	unknown.Status, unknown.UnknownReason, unknown.EntriesExamined, unknown.FilePaths = "unknown", "no saved observation", 0, 0
	unknown.LogicalBytes, unknown.AllocatedBytes, unknown.AllocatedSizeSource = nil, nil, "unknown"
	got := candidateSizeSummary(t, report)
	if got.Status != "historical_measured_portion" || got.PartialFolders != 1 || got.StaleFolders != 1 || got.UnknownFolders != 1 || got.KnownLogicalMeasurements != 1 || got.FoldersWithSkippedEntries != 1 || got.FoldersWithUnconfirmedEntries != 1 || got.FoldersWithIncompleteDirectories != 1 || got.FoldersWithDirectoryErrors != 1 || got.LogicalPathBytes != nil || got.MeasuredFilePaths != nil || got.LogicalUnavailableReason != "measurement_unavailable" || got.FilePathsUnavailableReason != "measurement_unavailable" {
		t.Fatal(got)
	}
	report = candidateSizeTestReport(1)
	report.Findings[0].Measurement.Status = "partial"
	report.Findings[0].Measurement.LogicalBytes = nil
	got = candidateSizeSummary(t, report)
	if got.LogicalPathBytes != nil || got.LogicalUnavailableReason != "measurement_unavailable" {
		t.Fatal(got)
	}
	candidateSizeAmount(t, got.MeasuredFilePaths, 1)
	report = candidateSizeTestReport(1)
	report.Findings[0].Measurement.AllocatedBytes, report.Findings[0].Measurement.AllocatedSizeSource = nil, "unknown"
	got = candidateSizeSummary(t, report)
	candidateSizeAmount(t, got.LogicalPathBytes, 1)
	if got.WithinFolderAllocatedBytes != nil || got.AllocatedUnavailableReason != "measurement_unavailable" {
		t.Fatal(got)
	}
	report = candidateSizeTestReport(1)
	report.Findings[0].Measurement.UnknownInodes = 1
	got = candidateSizeSummary(t, report)
	if got.WithinFolderAllocatedBytes != nil || got.AllocatedUnavailableReason != "single_folder_identity_unavailable" {
		t.Fatal(got)
	}
	report = candidateSizeTestReport(1)
	m := &report.Findings[0].Measurement
	m.Status, m.Truncated, m.EntriesExamined, m.InodeEntriesExamined = "partial", true, DirectoryEntryLimit, DirectoryEntryLimit
	m.CompactedDirectories, m.CompactedFiles, m.FilePaths = 1, DirectoryEntryLimit, DirectoryEntryLimit
	got = candidateSizeSummary(t, report)
	if got.TruncatedFolders != 1 || got.FoldersAtEntryLimit != 1 || got.FoldersAtCompactIdentityLimit != 1 || got.Status != "historical_measured_portion" {
		t.Fatal(got)
	}
	candidateSizeAmount(t, got.LogicalPathBytes, 1)
}

func TestCandidateSizePageCorruptScalarsRefuseCapture(t *testing.T) {
	cases := map[string]func(*Finding){
		"logical":                    func(f *Finding) { *f.Measurement.LogicalBytes = -1 },
		"allocated":                  func(f *Finding) { *f.Measurement.AllocatedBytes = -1 },
		"compacted_directories":      func(f *Finding) { f.Measurement.CompactedDirectories = -1 },
		"compacted_files":            func(f *Finding) { f.Measurement.CompactedFiles = -1 },
		"inode_examined":             func(f *Finding) { f.Measurement.InodeEntriesExamined = -1 },
		"compact_inode_relationship": func(f *Finding) { f.Measurement.InodeEntriesExamined = 1 },
		"unknown_compact_inode_relationship": func(f *Finding) {
			m := &f.Measurement
			m.Status, m.UnknownReason, m.EntriesExamined, m.FilePaths = "unknown", "no saved observation", 0, 0
			m.LogicalBytes, m.AllocatedBytes, m.AllocatedSizeSource = nil, nil, "unknown"
			m.InodeEntriesExamined = 1
		},
		"file_paths":            func(f *Finding) { f.Measurement.FilePaths = -1 },
		"repeated_inodes":       func(f *Finding) { f.Measurement.RepeatedInodes = -1 },
		"unknown_inodes":        func(f *Finding) { f.Measurement.UnknownInodes = -1 },
		"entries_examined":      func(f *Finding) { f.Measurement.EntriesExamined = -1 },
		"entry_limit":           func(f *Finding) { f.Measurement.EntryLimit = -1 },
		"unconfirmed":           func(f *Finding) { f.Measurement.UnconfirmedEntries = -1 },
		"excluded":              func(f *Finding) { f.Measurement.ExcludedEntries = -1 },
		"incomplete":            func(f *Finding) { f.Measurement.IncompleteDirectories = -1 },
		"errors":                func(f *Finding) { f.Measurement.DirectoryErrors = -1 },
		"skipped":               func(f *Finding) { f.Measurement.SkippedEntries = -1 },
		"counter_relationship":  func(f *Finding) { f.Measurement.RepeatedInodes = 2 },
		"identity_relationship": func(f *Finding) { f.Measurement.UnknownInodes = 2 },
		"compact_relationship":  func(f *Finding) { f.Measurement.CompactedFiles = 2 },
		"bounded_limit":         func(f *Finding) { f.Measurement.EntriesExamined = DirectoryEntryLimit + 1 },
		"cached_limit":          func(f *Finding) { f.Measurement.CoverageSource = "cached_reduction" },
		"complete_missing":      func(f *Finding) { f.Measurement.LogicalBytes = nil },
		"complete_skipped":      func(f *Finding) { f.Measurement.SkippedEntries = 1 },
		"missing_allocation":    func(f *Finding) { f.Measurement.AllocatedBytes = nil },
		"unknown_allocation":    func(f *Finding) { f.Measurement.AllocatedSizeSource = "unknown" },
		"source":                func(f *Finding) { f.Measurement.Source = "live" },
		"coverage":              func(f *Finding) { f.Measurement.CoverageSource = "other" },
		"allocation_source":     func(f *Finding) { f.Measurement.AllocatedSizeSource = "other" },
		"status":                func(f *Finding) { f.Measurement.Status = "other" },
		"authority":             func(f *Finding) { f.Measurement.CurrentStateVerified = true },
		"root":                  func(f *Finding) { f.Measurement.RootID = 2 },
		"path":                  func(f *Finding) { f.PathBytes = []byte("/fixture/a/../b"); f.Measurement.PathBytes = f.PathBytes },
		"path_nul":              func(f *Finding) { f.PathBytes = []byte("/fixture/\x00"); f.Measurement.PathBytes = f.PathBytes },
		"path_mismatch":         func(f *Finding) { f.Measurement.PathBytes = []byte("/fixture/different") },
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			report := candidateSizeTestReport(2)
			// An unavailable first component must not hide a corrupt component
			// that could later be excluded by the caller's visible subset.
			report.Findings[0].Measurement.Status, report.Findings[0].Measurement.LogicalBytes = "partial", nil
			corrupt(&report.Findings[1])
			if receipt, err := freezeCandidateSizePage(report); !errors.Is(err, ErrCandidateSizePage) || receipt != nil {
				t.Fatal("corruption became an unknown amount", receipt, err)
			}
		})
	}
	receipt, err := freezeCandidateSizePage(candidateSizeTestReport(2))
	if err != nil {
		t.Fatal(err)
	}
	receipt.rows[1].allocated = -1
	if _, err = receipt.SummarizeVisible([]string{receipt.rows[0].id}); !errors.Is(err, ErrCandidateSizePage) {
		t.Fatal("excluded frozen corruption accepted", err)
	}
}

func TestCandidateSizePageAggregateOverflowIsIndependent(t *testing.T) {
	for _, component := range []string{"logical", "file_paths"} {
		t.Run(component, func(t *testing.T) {
			report := candidateSizeTestReport(2)
			if component == "logical" {
				*report.Findings[0].Measurement.LogicalBytes = math.MaxInt64
			} else {
				if int64(int(math.MaxInt64)) != math.MaxInt64 {
					t.Skip("cached int counters require a 64-bit target")
				}
				m := &report.Findings[0].Measurement
				m.CoverageSource, m.EntryLimit, m.CompactedDirectories = "cached_reduction", 0, 1
				m.FilePaths, m.CompactedFiles = int(math.MaxInt64), int(math.MaxInt64)
			}
			got := candidateSizeSummary(t, report)
			if component == "logical" {
				if got.LogicalPathBytes != nil || got.LogicalUnavailableReason != "aggregate_overflow" {
					t.Fatal(got)
				}
				candidateSizeAmount(t, got.MeasuredFilePaths, 2)
			} else {
				if got.MeasuredFilePaths != nil || got.FilePathsUnavailableReason != "aggregate_overflow" {
					t.Fatal(got)
				}
				candidateSizeAmount(t, got.LogicalPathBytes, 3)
			}
			if got.DisplayedFolders != 2 || got.KnownLogicalMeasurements != 2 {
				t.Fatal("overflow changed page membership", got)
			}
		})
	}
}

func TestCandidateSizePageActualGeneratedHardlinks(t *testing.T) {
	root := privateDir(t)
	for _, path := range []string{"a/node_modules", "b/node_modules"} {
		if err := os.MkdirAll(filepath.Join(root, path), 0700); err != nil {
			t.Fatal(err)
		}
	}
	body := []byte("generated payload")
	first := filepath.Join(root, "a/node_modules/file")
	if err := os.WriteFile(first, body, 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"a/node_modules/link", "b/node_modules/link"} {
		if err := os.Link(first, filepath.Join(root, path)); err != nil {
			t.Fatal(err)
		}
	}
	var info unix.Stat_t
	if err := unix.Lstat(first, &info); err != nil {
		t.Fatal(err)
	}
	if info.Nlink != 3 {
		t.Fatal("fixture did not create three hardlink names", info.Nlink)
	}
	device, inode := fmt.Sprint(info.Dev), fmt.Sprint(info.Ino)
	allocated := info.Blocks * 512
	s, _ := savedSelectionFixture(t)
	if _, err := s.db.Exec("UPDATE roots SET path=?; UPDATE directories SET complete=1 WHERE path=CAST('a/node_modules' AS BLOB)", []byte(root)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE entries SET device=?,inode=?,size=?,allocated=? WHERE path=CAST('a/node_modules/file' AS BLOB)", device, inode, len(body), allocated); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		path, parent, kind, ino string
		generation              int64
	}{
		{"a/node_modules/link", "a/node_modules", "file", inode, 2},
		{"b", ".", "directory", "b", 1}, {"b/package.json", "b", "file", "manifest-b", 1},
		{"b/node_modules", "b", "directory", "node-b", 1}, {"b/node_modules/link", "b/node_modules", "file", inode, 1},
	} {
		size, blocks := int64(0), int64(0)
		if strings.HasSuffix(entry.path, "/link") {
			size, blocks = int64(len(body)), allocated
		}
		if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,?,?,1,2,?,?,?,10)`, []byte(entry.path), []byte(entry.parent), entry.kind, size, blocks, device, entry.ino, entry.generation); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"b", "b/node_modules"} {
		if _, err := s.db.Exec("INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,20)", []byte(path)); err != nil {
			t.Fatal(err)
		}
	}
	page, receipt, err := s.NodeModulesSizePage(context.Background(), "", FindingAgeDays)
	if err != nil || len(page.Evidence.Findings) != 2 {
		t.Fatal(page, err)
	}
	var aID string
	for _, finding := range page.Evidence.Findings {
		if bytes.HasSuffix(finding.PathBytes, []byte("/a/node_modules")) {
			aID = finding.ID
			if finding.Measurement.RepeatedInodes != 1 {
				t.Fatal("within-folder link was not deduplicated", finding)
			}
		}
	}
	one, err := receipt.SummarizeVisible([]string{aID})
	if err != nil {
		t.Fatal(err)
	}
	candidateSizeAmount(t, one.LogicalPathBytes, 2*int64(len(body)))
	candidateSizeAmount(t, one.MeasuredFilePaths, 2)
	candidateSizeAmount(t, one.WithinFolderAllocatedBytes, allocated)
	all, err := receipt.SummarizeVisible(candidateSizeIDs(page.Evidence))
	if err != nil {
		t.Fatal(err)
	}
	candidateSizeAmount(t, all.LogicalPathBytes, 3*int64(len(body)))
	candidateSizeAmount(t, all.MeasuredFilePaths, 3)
	if all.WithinFolderAllocatedBytes != nil || all.AllocatedUnavailableReason != "cross_folder_identity_evidence_unavailable" || all.CrossFolderObjectDeduplicationVerified || all.PhysicalAllocationVerified {
		t.Fatal("cross-folder hardlinks became a physical total", all)
	}
}

func candidateSizeCachedFixture(t *testing.T) *Store {
	t.Helper()
	s, _ := compactFixture(t, true)
	if _, err := s.db.Exec("UPDATE roots SET path=CAST('/fixture' AS BLOB)"); err != nil {
		t.Fatal(err)
	}
	compactTreePass(t, s, 1, map[string][]Entry{
		".":            {{Path: []byte("node_modules"), Kind: "directory", Device: "d", Inode: "node"}, compactFile("package.json", "manifest", 1)},
		"node_modules": {compactFile("node_modules/one", "shared", 5), compactFile("node_modules/two", "shared", 5)},
	})
	if _, err := s.db.Exec("UPDATE entries SET mtime_ns=1"); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCandidateSizePageCachedCoverageAndCorruptStoredTypes(t *testing.T) {
	s := candidateSizeCachedFixture(t)
	ctx := context.Background()
	page, receipt, err := s.NodeModulesSizePage(ctx, "", FindingAgeDays)
	if err != nil || len(page.Evidence.Findings) != 1 {
		t.Fatal(page, err)
	}
	got, err := receipt.SummarizeVisible(candidateSizeIDs(page.Evidence))
	if err != nil || got.CachedCoverageFolders != 1 || got.CachedAllocationFolders != 1 || got.BoundedCoverageFolders != 0 || got.BoundedAllocationFolders != 0 {
		t.Fatal(got, err)
	}
	candidateSizeAmount(t, got.LogicalPathBytes, 10)
	candidateSizeAmount(t, got.MeasuredFilePaths, 2)
	candidateSizeAmount(t, got.WithinFolderAllocatedBytes, 4096)
	var original []byte
	if err = s.db.QueryRow("SELECT coverage FROM allocation_cache WHERE path=CAST('node_modules' AS BLOB)").Scan(&original); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"logical", "counter", "type"} {
		t.Run(field, func(t *testing.T) {
			var coverage scopeCoverage
			if err := json.Unmarshal(original, &coverage); err != nil {
				t.Fatal(err)
			}
			if field == "counter" {
				coverage.Report.FilePaths = -1
			} else {
				coverage.Logical = -1
			}
			bad, err := json.Marshal(coverage)
			if err != nil {
				t.Fatal(err)
			}
			if field == "type" {
				var object map[string]json.RawMessage
				if err = json.Unmarshal(bad, &object); err != nil {
					t.Fatal(err)
				}
				object["Logical"] = json.RawMessage(`"not an integer"`)
				bad, err = json.Marshal(object)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec("UPDATE allocation_cache SET coverage=? WHERE path=CAST('node_modules' AS BLOB)", bad); err != nil {
				t.Fatal(err)
			}
			if page, receipt, err := s.NodeModulesSizePage(ctx, "", FindingAgeDays); err == nil || receipt != nil || len(page.Evidence.Findings) != 0 {
				t.Fatal("corrupt stored scalar yielded positive evidence", page, receipt, err)
			}
			if _, err = s.db.Exec("UPDATE allocation_cache SET coverage=? WHERE path=CAST('node_modules' AS BLOB)", original); err != nil {
				t.Fatal(err)
			}
		})
	}
	// A detailed row with a wrong SQLite type must propagate the existing
	// measurement error, rather than be converted to a missing summary amount.
	detailed, _ := savedSelectionFixture(t)
	if _, err = detailed.db.Exec("UPDATE entries SET size='not an integer' WHERE path=CAST('a/node_modules/file' AS BLOB)"); err != nil {
		t.Fatal(err)
	}
	if page, receipt, err := detailed.NodeModulesSizePage(ctx, "", FindingAgeDays); err == nil || receipt != nil || len(page.Evidence.Findings) != 0 {
		t.Fatal(page, receipt, err)
	}
}

func TestCandidateSizePageWithinFolderOverflowRemainsError(t *testing.T) {
	s, _ := savedSelectionFixture(t)
	if _, err := s.db.Exec("UPDATE entries SET size=? WHERE path=CAST('a/node_modules/file' AS BLOB)", int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,CAST('a/node_modules/extra' AS BLOB),CAST('a/node_modules' AS BLOB),'file',1,0,1,2,'device','extra',2,10)`); err != nil {
		t.Fatal(err)
	}
	if page, receipt, err := s.NodeModulesSizePage(context.Background(), "", FindingAgeDays); err == nil || receipt != nil || len(page.Evidence.Findings) != 0 {
		t.Fatal("within-folder overflow became a successful null aggregate", page, receipt, err)
	}
}

func TestCandidateSizePageRawAndCandidateCaps(t *testing.T) {
	for _, noise := range []int{0, FindingEntryLimit + 1} {
		t.Run(fmt.Sprint(noise), func(t *testing.T) {
			s := dismissalPageFixture(t, noise)
			ctx := context.Background()
			page, receipt, err := s.NodeModulesSizePage(ctx, "", FindingAgeDays)
			old, oldErr := s.NodeModulesFindingPage(ctx, "", FindingAgeDays)
			if err != nil || oldErr != nil || page.Evidence.EntriesExamined != old.Evidence.EntriesExamined || page.Evidence.EntryLimit != FindingEntryLimit || page.Evidence.NextCursor != old.Evidence.NextCursor || !reflect.DeepEqual(page.Evidence.Diagnostics, old.Evidence.Diagnostics) || len(page.Evidence.Findings) != len(old.Evidence.Findings) {
				t.Fatal(page, old, err, oldErr)
			}
			if noise != 0 {
				if receipt.count != 0 || page.Evidence.EntriesExamined != FindingEntryLimit || page.Evidence.NextCursor == "" {
					t.Fatal("empty raw page was refilled", page)
				}
				empty, err := receipt.SummarizeVisible(nil)
				if err != nil || empty.Status != "empty_selection" {
					t.Fatal(empty, err)
				}
				page, receipt, err = s.NodeModulesSizePage(ctx, page.Evidence.NextCursor, FindingAgeDays)
			}
			if err != nil || receipt.count != 20 || len(page.Evidence.Findings) != 20 || page.Evidence.NextCursor == "" {
				t.Fatal("candidate cap changed", page, err)
			}
			visible, err := receipt.SummarizeVisible([]string{page.Evidence.Findings[0].ID})
			if err != nil || visible.DisplayedFolders != 1 || receipt.count != 20 {
				t.Fatal("subset refilled or rewrote page", visible, receipt.count, err)
			}
			next, nextReceipt, err := s.NodeModulesSizePage(ctx, page.Evidence.NextCursor, FindingAgeDays)
			if err != nil || nextReceipt.count != 1 || len(next.Evidence.Findings) != 1 || next.Evidence.NextCursor != "" {
				t.Fatal("raw continuation lost final candidate", next, err)
			}
		})
	}
	for _, corrupt := range []func(*FindingReport){
		func(r *FindingReport) { r.Findings = candidateSizeTestReport(21).Findings },
		func(r *FindingReport) { r.EntriesExamined = FindingEntryLimit + 1 },
		func(r *FindingReport) { r.EntriesExamined = 0 },
		func(r *FindingReport) {
			r.Findings[1].ID = r.Findings[0].ID
			r.Findings[1].EntryID = r.Findings[0].EntryID
		},
	} {
		r := candidateSizeTestReport(2)
		corrupt(&r)
		if receipt, err := freezeCandidateSizePage(r); !errors.Is(err, ErrCandidateSizePage) || receipt != nil {
			t.Fatal(receipt, err)
		}
	}
}

func TestCandidateSizePageLegacySchemaAndCancellation(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	legacy, err := connect(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err = legacy.db.Exec(migrations[i].sql); err != nil {
			legacy.Close()
			t.Fatal(err)
		}
		if _, err = legacy.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
			legacy.Close()
			t.Fatal(err)
		}
	}
	if _, err = legacy.db.Exec("PRAGMA application_id=0x52594444; PRAGMA user_version=8"); err != nil {
		legacy.Close()
		t.Fatal(err)
	}
	if err = legacy.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if ordinary, err := reader.NodeModulesFindings(ctx, "", FindingAgeDays); err != nil || len(ordinary.Findings) != 0 {
		t.Fatal("legacy ordinary report changed", ordinary, err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	// A closed schema-8 reader cannot query or migrate. The exact schema error
	// must precede any access to its closed database.
	if page, receipt, err := reader.NodeModulesSizePage(ctx, "", FindingAgeDays); !errors.Is(err, ErrPlanSchema) || receipt != nil || len(page.Evidence.Findings) != 0 {
		t.Fatal(page, receipt, err)
	}
	s, _ := savedSelectionFixture(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if page, receipt, err := s.NodeModulesSizePage(canceled, "", FindingAgeDays); !errors.Is(err, context.Canceled) || receipt != nil || len(page.Evidence.Findings) != 0 {
		t.Fatal(page, receipt, err)
	}
	for _, cursor := range []string{"bad", "nm1:0", "nm2:30:1"} {
		if _, receipt, err := s.NodeModulesSizePage(ctx, cursor, FindingAgeDays); !errors.Is(err, ErrReportCursor) || receipt != nil {
			t.Fatal(cursor, receipt, err)
		}
	}
	if _, receipt, err := s.NodeModulesSizePage(ctx, "", 0); !errors.Is(err, ErrFindingAge) || receipt != nil {
		t.Fatal(receipt, err)
	}
}

func TestCandidateSizePageWorstCaseRetainedMemoryAndJSON(t *testing.T) {
	report := candidateSizeTestReport(candidateSizePageLimit)
	for i := range report.Findings {
		f := &report.Findings[i]
		f.PathBytes = []byte("/" + strings.Repeat("p", 4080) + fmt.Sprintf("%015d", i))
		f.Measurement.PathBytes = f.PathBytes
		f.Device, f.Inode = strings.Repeat("d", candidateSizeIdentityLimit), strings.Repeat("i", candidateSizeIdentityLimit)
		m := &f.Measurement
		logical := int64(math.MaxInt64)
		m.LogicalBytes, m.Status, m.Truncated = &logical, "stale", true
		m.EntriesExamined, m.InodeEntriesExamined = DirectoryEntryLimit, DirectoryEntryLimit
		m.CompactedDirectories, m.CompactedFiles, m.FilePaths = 1, DirectoryEntryLimit, DirectoryEntryLimit
		m.SkippedEntries, m.UnconfirmedEntries, m.DirectoryErrors, m.IncompleteDirectories = 1, 1, 1, 1
		m.RootError = "generated saved error"
		if len(f.PathBytes) != 4096 {
			t.Fatal("worst-case path fixture changed", len(f.PathBytes))
		}
	}
	receipt, err := freezeCandidateSizePage(report)
	if err != nil {
		t.Fatal(err)
	}
	retained := int(unsafe.Sizeof(*receipt))
	for i := 0; i < receipt.count; i++ {
		retained += cap(receipt.rows[i].path) + len(receipt.rows[i].id)
	}
	// Include a conservative allocation overhead for the receipt and both
	// copied variable values in every row. Identity arrays are already inline.
	retained += (1 + 2*candidateSizePageLimit) * 64
	if retained > candidateSizeRetainedHeapLimit {
		t.Fatal("fixed receipt exceeds retained heap contract", retained)
	}
	measurement := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			frozen, err := freezeCandidateSizePage(report)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(frozen)
		}
	})
	if measurement.AllocedBytesPerOp() > candidateSizeRetainedHeapLimit {
		t.Fatal("worst-case capture allocation exceeds heap envelope", measurement.AllocedBytesPerOp())
	}
	got, err := receipt.SummarizeVisible(candidateSizeIDs(report))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(got)
	if err != nil || len(encoded) > 4096 {
		t.Fatal("summary JSON exceeds fixed contract", len(encoded), err)
	}
	if got.Contract != "candidate_page_size_summary_v1" || got.Scope != "visible_candidate_page" || got.Source != "saved_inventory" || got.EstimatedReclaimableBytes != nil || got.CurrentStateVerified || got.ContentVerified || got.PhysicalAllocationVerified || got.CrossFolderObjectDeduplicationVerified || got.ApprovalAvailable || got.Executable {
		t.Fatal("historical summary gained authority", got)
	}
	for _, key := range []string{"path", "findings", "identities", "targets"} {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &object); err != nil {
			t.Fatal(err)
		}
		if _, exists := object[key]; exists {
			t.Fatal("summary retained a variable evidence list", key)
		}
	}
}

// Compare the byte validator with the platform library across canonical paths,
// malformed components and raw filename bytes. This preserves the prior guard
// while preventing full-path temporary copies in the twenty-row receipt.
func TestCandidateSizeCanonicalBytePaths(t *testing.T) {
	if filepath.Separator != '/' {
		t.Fatal("candidate path contract requires macOS or Linux")
	}
	paths := []string{"", "/", ".", "..", "relative", "/a", "//a", "/a/", "/a//b", "/a/./b", "/a/../b", "/.", "/..", "/a/.hidden", "/a/...", "/a/..name", "/a\\b", "/a:\xff", "/a\x00b", "/" + strings.Repeat("a", 4095), "/" + strings.Repeat("a", 4096)}
	for _, path := range paths {
		got := candidateSizeCanonicalPath([]byte(path))
		want := len(path) > 0 && len(path) <= 4096 && filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsRune(path, 0)
		if got != want {
			t.Fatalf("byte path guard differs for %q: got %t want %t", path, got, want)
		}
	}
	// Exhaustive short strings include separators, dot components, NUL,
	// backslash and non-UTF-8 bytes. Canonical paths are not normalized.
	alphabet := []byte{'/', '.', 'a', 0, '\\', 0xff}
	for length := 0; length <= 5; length++ {
		count := 1
		for i := 0; i < length; i++ {
			count *= len(alphabet)
		}
		for n := 0; n < count; n++ {
			path := make([]byte, length)
			value := n
			for i := range path {
				path[i] = alphabet[value%len(alphabet)]
				value /= len(alphabet)
			}
			text := string(path)
			want := len(path) > 0 && filepath.IsAbs(text) && filepath.Clean(text) == text && bytes.IndexByte(path, 0) < 0
			if got := candidateSizeCanonicalPath(path); got != want {
				t.Fatalf("byte path guard differs for %q: got %t want %t", path, got, want)
			}
		}
	}
	path := []byte("/" + strings.Repeat("a", 4095))
	if allocs := testing.AllocsPerRun(100, func() {
		if !candidateSizeCanonicalPath(path) {
			panic("canonical path rejected")
		}
	}); allocs != 0 {
		t.Fatal("bounded byte guard allocated", allocs)
	}
}
