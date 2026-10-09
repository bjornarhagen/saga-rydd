package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrDismissalSelection = errors.New("dismissal requires one exact saved finding with usable root, directory and manifest identities")
var ErrDismissalChanged = errors.New("saved finding evidence changed; preview a new dismissal request")

// CanonicalDismissalSelection returns an independent, selected-only historical
// scope. Display text and generation times are not identity evidence. Nullable
// sizes, observation times and all measurement qualifications remain exact.
// Retained text must survive JSON exactly; arbitrary Unix names use PathBytes.
func CanonicalDismissalSelection(saved SelectionSnapshot) (SelectionSnapshot, error) {
	if len(saved.Roots) != 1 || len(saved.Targets) != 1 || len(saved.Evidence.Findings) != 1 || !dismissalInventoryID(saved.InventoryID) {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	r, root, target, f := saved.Evidence, saved.Roots[0], saved.Targets[0], saved.Evidence.Findings[0]
	if r.Source != "saved_inventory" || r.CurrentStateVerified || r.MinimumAgeDays < 1 || r.MinimumAgeDays > MaxFindingAgeDays || r.PageCoverage != "selected_entries_only" || r.EntriesExamined != 1 || r.EntryLimit != 1 || r.NextCursor != "" || !dismissalDiagnostics(r.Diagnostics) {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	if root.ID <= 0 || !canonicalPath(root.PathBytes) || !dismissalIdentity(root.Fingerprint) || root.Revision <= 0 || f.RootID != root.ID || f.EntryID <= 0 || f.ID != fmt.Sprintf("node-modules-v1:%d:%d", f.RootID, f.EntryID) || target.FindingID != f.ID || !knownTarget(target) {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	for _, binding := range []EntryBinding{target.Target, target.Manifest} {
		if !dismissalIdentity(binding.Device) || !dismissalIdentity(binding.Inode) {
			return SelectionSnapshot{}, ErrDismissalSelection
		}
	}
	if target.Target.Device != f.Device || target.Target.Inode != f.Inode || f.Rule != "node_modules_old_metadata" || f.RuleVersion != 1 || f.Classification != "review_required" || f.Recognition != "manifest_filename_only" || len(f.Actions) != 0 || !canonicalPath(f.PathBytes) || !canonicalPath(f.ManifestPathBytes) || filepath.Base(string(f.PathBytes)) != "node_modules" || string(f.ManifestPathBytes) != filepath.Join(filepath.Dir(string(f.PathBytes)), "package.json") {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	rel, err := filepath.Rel(string(root.PathBytes), string(f.PathBytes))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.Contains("/"+filepath.Dir(rel)+"/", "/node_modules/") {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	for _, stamp := range []time.Time{f.DirectoryModifiedAt, f.ManifestModifiedAt, f.DirectoryObservedAt, f.ManifestObservedAt} {
		if !dismissalObservationTime(stamp) {
			return SelectionSnapshot{}, ErrDismissalSelection
		}
	}
	if !dismissalMeasurement(f.Measurement, f) {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	root.PathBytes = bytes.Clone(root.PathBytes)
	f.Path, f.ManifestPath = "", ""
	f.PathBytes, f.ManifestPathBytes = bytes.Clone(f.PathBytes), bytes.Clone(f.ManifestPathBytes)
	if f.Actions != nil {
		f.Actions = append([]string{}, f.Actions...)
	}
	f.DirectoryModifiedAt, f.ManifestModifiedAt = f.DirectoryModifiedAt.UTC(), f.ManifestModifiedAt.UTC()
	f.DirectoryObservedAt, f.ManifestObservedAt = f.DirectoryObservedAt.UTC(), f.ManifestObservedAt.UTC()
	m := f.Measurement
	m.Path, m.GeneratedAt, m.Notes = "", time.Time{}, nil
	m.PathBytes = bytes.Clone(m.PathBytes)
	if m.LogicalBytes != nil {
		v := *m.LogicalBytes
		m.LogicalBytes = &v
	}
	if m.AllocatedBytes != nil {
		v := *m.AllocatedBytes
		m.AllocatedBytes = &v
	}
	if m.OldestObservation != nil {
		v := m.OldestObservation.UTC()
		m.OldestObservation = &v
	}
	if m.NewestObservation != nil {
		v := m.NewestObservation.UTC()
		m.NewestObservation = &v
	}
	f.Measurement = m
	r.GeneratedAt, r.Notes = time.Time{}, nil
	r.Diagnostics = append([]SelectionDiagnostic{}, r.Diagnostics...)
	for i := range r.Diagnostics {
		r.Diagnostics[i].Explanation = ""
	}
	r.Findings = []Finding{f}
	return SelectionSnapshot{InventoryID: saved.InventoryID, Roots: []RootBinding{root}, Targets: []TargetBinding{target}, Evidence: r}, nil
}

func dismissalInventoryID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func dismissalIdentity(id string) bool {
	return id != "" && len(id) <= 1024 && utf8.ValidString(id) && strings.TrimSpace(id) == id && !strings.ContainsRune(id, 0)
}

func dismissalObservationTime(stamp time.Time) bool {
	n := stamp.UnixNano()
	return n > 0 && stamp.Equal(time.Unix(0, n))
}

func dismissalDiagnostics(d []SelectionDiagnostic) bool {
	expected := selectionDiagnostics()
	if len(d) != len(expected) {
		return false
	}
	for i := range d {
		count := 0
		if expected[i].Code == "selected" {
			count = 1
		}
		if d[i].Code != expected[i].Code || d[i].Count != count {
			return false
		}
	}
	return true
}

func dismissalMeasurement(m DirectoryReport, f Finding) bool {
	if m.Source != "saved_inventory" || m.CurrentStateVerified || m.RootID != f.RootID || !bytes.Equal(m.PathBytes, f.PathBytes) || len(m.UnknownReason) > 4096 || len(m.RootError) > 4096 || !utf8.ValidString(m.UnknownReason) || !utf8.ValidString(m.RootError) {
		return false
	}
	if m.Status != "unknown" && m.Status != "partial" && m.Status != "stale" && m.Status != "recorded_complete" || m.CoverageSource != "bounded_entry_check" && m.CoverageSource != "cached_reduction" || m.AllocatedSizeSource != "unknown" && m.AllocatedSizeSource != "bounded_identity_check" && m.AllocatedSizeSource != "cached_reduction" {
		return false
	}
	for _, count := range []int{m.CompactedDirectories, m.CompactedFiles, m.InodeEntriesExamined, m.FilePaths, m.RepeatedInodes, m.UnknownInodes, m.EntriesExamined, m.EntryLimit, m.UnconfirmedEntries, m.ExcludedEntries, m.IncompleteDirectories, m.DirectoryErrors, m.SkippedEntries} {
		if count < 0 {
			return false
		}
	}
	if m.LogicalBytes != nil && *m.LogicalBytes < 0 || m.AllocatedBytes != nil && *m.AllocatedBytes < 0 {
		return false
	}
	if m.OldestObservation != nil && m.OldestObservation.UnixNano() < 0 || m.NewestObservation != nil && m.NewestObservation.UnixNano() < 0 || m.OldestObservation != nil && m.NewestObservation != nil && m.OldestObservation.After(*m.NewestObservation) {
		return false
	}
	return true
}

// SingleDismissalSelection extracts a finding from an already captured page.
// It does not refill or remeasure the page; diagnostics describe this one scope.
func SingleDismissalSelection(page SelectionSnapshot, findingID string) (SelectionSnapshot, error) {
	if len(page.Evidence.Findings) < 1 || len(page.Evidence.Findings) > PreviewTargetLimit || len(page.Targets) != len(page.Evidence.Findings) || len(page.Roots) < 1 || len(page.Roots) > len(page.Targets) || page.Evidence.EntriesExamined < len(page.Targets) || page.Evidence.EntriesExamined > FindingEntryLimit || page.Evidence.Source != "saved_inventory" || page.Evidence.CurrentStateVerified {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	index := -1
	for i, f := range page.Evidence.Findings {
		if f.ID == findingID {
			if index != -1 || page.Targets[i].FindingID != findingID {
				return SelectionSnapshot{}, ErrDismissalSelection
			}
			index = i
		}
	}
	if index == -1 {
		return SelectionSnapshot{}, ErrDismissalSelection
	}
	f := page.Evidence.Findings[index]
	var roots []RootBinding
	for _, root := range page.Roots {
		if root.ID == f.RootID {
			roots = append(roots, root)
		}
	}
	r := page.Evidence
	r.Findings, r.EntryLimit, r.EntriesExamined = []Finding{f}, 1, 1
	r.PageCoverage, r.NextCursor = "selected_entries_only", ""
	r.Diagnostics = selectionDiagnostics()
	r.Diagnostics[len(r.Diagnostics)-1].Count = 1
	return CanonicalDismissalSelection(SelectionSnapshot{InventoryID: page.InventoryID, Roots: roots, Targets: []TargetBinding{page.Targets[index]}, Evidence: r})
}

// NodeModulesFindingPage preserves the existing raw page and cursor while
// measuring its eligible candidates and capturing bindings in one snapshot.
func (s *Store) NodeModulesFindingPage(ctx context.Context, cursor string, minimumAgeDays int) (SelectionSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return SelectionSnapshot{}, err
	}
	if s.schema < 9 {
		return SelectionSnapshot{}, ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	defer tx.Rollback()
	r, err := s.nodeModulesFindingPage(ctx, cursor, minimumAgeDays, tx)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return SelectionSnapshot{}, err
	}
	if err = ctx.Err(); err != nil {
		return SelectionSnapshot{}, err
	}
	return r, nil
}

func (s *Store) nodeModulesFindingPage(ctx context.Context, cursor string, minimumAgeDays int, tx *sql.Tx) (SelectionSnapshot, error) {
	var r SelectionSnapshot
	if err := tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&r.InventoryID); err != nil {
		return SelectionSnapshot{}, err
	}
	var err error
	r.Evidence, err = s.nodeModulesFindingsSnapshot(ctx, cursor, minimumAgeDays, nil, tx)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	return s.captureSelectionBindings(ctx, r, tx)
}

// CheckDismissalSelection checks saved inventory only. Incarnation is checked
// before resolving reusable numeric IDs; a match grants no live-file authority.
func (s *Store) CheckDismissalSelection(ctx context.Context, saved SelectionSnapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	want, err := CanonicalDismissalSelection(saved)
	if err != nil {
		return err
	}
	if s.schema < 9 {
		return ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var identity string
	if err = tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&identity); err != nil {
		return err
	}
	if identity != want.InventoryID {
		return ErrDismissalChanged
	}
	current, err := s.snapshotSelection(ctx, []string{want.Targets[0].FindingID}, want.Evidence.MinimumAgeDays, tx)
	if errors.Is(err, ErrFindingSelection) || errors.Is(err, ErrDirectoryScope) || errors.Is(err, sql.ErrNoRows) {
		return ErrDismissalChanged
	}
	if err != nil {
		return err
	}
	current, err = CanonicalDismissalSelection(current)
	if err != nil || !reflect.DeepEqual(want, current) {
		return ErrDismissalChanged
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return ctx.Err()
}
