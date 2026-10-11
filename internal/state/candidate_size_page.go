package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
)

const candidateSizePageLimit = 20
const candidateSizeIdentityLimit = 256
const candidateSizeRetainedHeapLimit = 131072

var ErrCandidateSizePage = errors.New("candidate page size evidence is invalid")

// CandidatePageSizeSummary describes only the visible historical path measurements
// on one captured page. It is never part of a saved finding or selection payload.
type CandidatePageSizeSummary struct {
	Contract                               string `json:"contract"`
	Source                                 string `json:"source"`
	Scope                                  string `json:"scope"`
	Status                                 string `json:"status"`
	DisplayedFolders                       int    `json:"displayed_folders"`
	KnownLogicalMeasurements               int    `json:"known_logical_measurements"`
	PartialFolders                         int    `json:"partial_folders"`
	StaleFolders                           int    `json:"stale_folders"`
	UnknownFolders                         int    `json:"unknown_folders"`
	TruncatedFolders                       int    `json:"truncated_folders"`
	OverlapPairs                           int    `json:"overlap_pairs"`
	UnknownDirectoryIdentities             int    `json:"unknown_directory_identities"`
	KnownDirectoryAliasPairs               int    `json:"known_directory_alias_pairs"`
	FoldersWithSkippedEntries              int    `json:"folders_with_skipped_entries"`
	FoldersWithUnconfirmedEntries          int    `json:"folders_with_unconfirmed_entries"`
	FoldersWithDirectoryErrors             int    `json:"folders_with_directory_errors"`
	FoldersWithIncompleteDirectories       int    `json:"folders_with_incomplete_directories"`
	FoldersAtEntryLimit                    int    `json:"folders_at_entry_limit"`
	FoldersAtCompactIdentityLimit          int    `json:"folders_at_compact_identity_limit"`
	BoundedCoverageFolders                 int    `json:"bounded_coverage_folders"`
	CachedCoverageFolders                  int    `json:"cached_coverage_folders"`
	BoundedAllocationFolders               int    `json:"bounded_allocation_folders"`
	CachedAllocationFolders                int    `json:"cached_allocation_folders"`
	LogicalPathBytes                       *int64 `json:"logical_path_bytes"`
	MeasuredFilePaths                      *int64 `json:"measured_file_paths"`
	WithinFolderAllocatedBytes             *int64 `json:"within_folder_allocated_bytes"`
	LogicalUnavailableReason               string `json:"logical_unavailable_reason,omitempty"`
	FilePathsUnavailableReason             string `json:"file_paths_unavailable_reason,omitempty"`
	AllocatedUnavailableReason             string `json:"allocated_unavailable_reason,omitempty"`
	EstimatedReclaimableBytes              *int64 `json:"estimated_reclaimable_bytes"`
	CurrentStateVerified                   bool   `json:"current_state_verified"`
	ContentVerified                        bool   `json:"content_verified"`
	PhysicalAllocationVerified             bool   `json:"physical_allocation_verified"`
	CrossFolderObjectDeduplicationVerified bool   `json:"cross_folder_object_deduplication_verified"`
	ApprovalAvailable                      bool   `json:"approval_available"`
	Executable                             bool   `json:"executable"`
}

type candidateRawIdentity struct {
	data    [candidateSizeIdentityLimit]byte
	length  uint16
	present bool
}

func freezeCandidateIdentity(raw string) candidateRawIdentity {
	var identity candidateRawIdentity
	if len(raw) > 0 && len(raw) <= len(identity.data) {
		copy(identity.data[:], raw)
		identity.length, identity.present = uint16(len(raw)), true
	}
	return identity
}

func sameCandidateIdentity(a, b candidateRawIdentity) bool {
	return a.present && b.present && a.length == b.length &&
		bytes.Equal(a.data[:a.length], b.data[:b.length])
}

type candidateSizeRow struct {
	id                                                                      string
	path                                                                    []byte
	device, inode                                                           candidateRawIdentity
	logical, allocated, filePaths, unknownInodes                            int64
	hasLogical, hasAllocated, hasFilePaths                                  bool
	status, coverage, allocation                                            uint8
	truncated, skipped, unconfirmed, directoryErrors, incomplete, rootError bool
	entryLimit, compactIdentityLimit                                        bool
}

const (
	candidateComplete uint8 = iota + 1
	candidatePartial
	candidateStale
	candidateUnknown
)
const (
	candidateBounded uint8 = iota + 1
	candidateCached
	candidateUnavailable
)

// CandidateSizePage is an opaque, process-local receipt. It retains no Store,
// transaction, descriptor, public Finding, variable identity string or authority.
// The zero value cannot be used to manufacture a captured page.
type CandidateSizePage struct {
	valid bool
	count int
	rows  [candidateSizePageLimit]candidateSizeRow
}

// NodeModulesSizePage uses the existing page, measurement and binding queries in
// one read transaction. No extra folder measurement or later inventory read occurs.
func (s *Store) NodeModulesSizePage(ctx context.Context, cursor string, minimumAgeDays int) (SelectionSnapshot, *CandidateSizePage, error) {
	if err := ctx.Err(); err != nil {
		return SelectionSnapshot{}, nil, err
	}
	if s.schema < 9 {
		return SelectionSnapshot{}, nil, ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionSnapshot{}, nil, err
	}
	defer tx.Rollback()
	page, receipt, err := s.nodeModulesSizePage(ctx, cursor, minimumAgeDays, tx)
	if err != nil {
		return SelectionSnapshot{}, nil, err
	}
	if err = tx.Commit(); err != nil {
		return SelectionSnapshot{}, nil, err
	}
	if err = ctx.Err(); err != nil {
		return SelectionSnapshot{}, nil, err
	}
	return page, receipt, nil
}

func (s *Store) nodeModulesSizePage(ctx context.Context, cursor string, minimumAgeDays int, tx *sql.Tx) (SelectionSnapshot, *CandidateSizePage, error) {
	page, err := s.nodeModulesFindingPage(ctx, cursor, minimumAgeDays, tx)
	if err != nil {
		return SelectionSnapshot{}, nil, err
	}
	receipt, err := freezeCandidateSizePage(page.Evidence)
	if err != nil {
		return SelectionSnapshot{}, nil, err
	}
	return page, receipt, nil
}

func candidateSizeError(detail string) error {
	return fmt.Errorf("%w: %s", ErrCandidateSizePage, detail)
}

// candidateSizeCanonicalPath validates a bounded absolute Unix path without
// copying its bytes. macOS and Linux use slash separators. Preserve raw filename
// bytes, including invalid UTF-8; reject only paths changed by filepath.Clean
// or containing NUL. The root path itself is canonical.
func candidateSizeCanonicalPath(path []byte) bool {
	if filepath.Separator != '/' || len(path) == 0 || len(path) > 4096 || path[0] != '/' {
		return false
	}
	if len(path) == 1 {
		return true
	}
	start := 1
	for i := 1; i <= len(path); i++ {
		if i < len(path) && path[i] == 0 {
			return false
		}
		if i < len(path) && path[i] != '/' {
			continue
		}
		length := i - start
		if length == 0 || length == 1 && path[start] == '.' || length == 2 && path[start] == '.' && path[start+1] == '.' {
			return false
		}
		start = i + 1
	}
	return true
}

func validateCandidateMeasurement(f Finding) error {
	m := f.Measurement
	if f.RootID <= 0 || f.EntryID <= 0 || f.ID != fmt.Sprintf("node-modules-v1:%d:%d", f.RootID, f.EntryID) || len(f.ID) > 128 ||
		len(f.PathBytes) == 0 || len(f.PathBytes) > 4096 {
		return candidateSizeError("finding path or historical source invariant")
	}
	if !candidateSizeCanonicalPath(f.PathBytes) ||
		!bytes.Equal(f.PathBytes, m.PathBytes) || m.RootID != f.RootID || m.Source != "saved_inventory" || m.CurrentStateVerified {
		return candidateSizeError("finding path or historical source invariant")
	}
	for _, n := range []int{m.CompactedDirectories, m.CompactedFiles, m.InodeEntriesExamined, m.FilePaths, m.RepeatedInodes, m.UnknownInodes, m.EntriesExamined, m.EntryLimit, m.UnconfirmedEntries, m.ExcludedEntries, m.IncompleteDirectories, m.DirectoryErrors, m.SkippedEntries} {
		if n < 0 {
			return candidateSizeError("negative measurement counter")
		}
	}
	if (m.LogicalBytes != nil && *m.LogicalBytes < 0) || (m.AllocatedBytes != nil && *m.AllocatedBytes < 0) {
		return candidateSizeError("negative measurement amount")
	}
	if m.InodeEntriesExamined > DirectoryEntryLimit || m.InodeEntriesExamined > m.CompactedFiles || m.CompactedDirectories > m.EntriesExamined || m.CompactedFiles > m.FilePaths ||
		m.RepeatedInodes > m.FilePaths || m.UnknownInodes > m.FilePaths-m.RepeatedInodes || m.FilePaths-m.CompactedFiles > m.EntriesExamined ||
		m.ExcludedEntries > m.EntriesExamined || m.UnconfirmedEntries > m.EntriesExamined || m.IncompleteDirectories > m.EntriesExamined || m.DirectoryErrors > m.EntriesExamined ||
		(m.SkippedEntries > m.CompactedFiles && m.SkippedEntries-m.CompactedFiles > m.EntriesExamined) {
		return candidateSizeError("measurement counter invariant")
	}
	switch m.CoverageSource {
	case "bounded_entry_check":
		if m.EntryLimit != DirectoryEntryLimit || m.EntriesExamined > DirectoryEntryLimit || (m.Truncated && m.EntriesExamined != DirectoryEntryLimit) {
			return candidateSizeError("bounded coverage invariant")
		}
	case "cached_reduction":
		if m.EntryLimit != 0 || m.Truncated {
			return candidateSizeError("cached coverage invariant")
		}
	default:
		return candidateSizeError("unsupported coverage source")
	}
	switch m.AllocatedSizeSource {
	case "bounded_identity_check", "cached_reduction":
		if m.AllocatedBytes == nil {
			return candidateSizeError("known allocation source has no amount")
		}
	case "unknown":
		if m.AllocatedBytes != nil {
			return candidateSizeError("unknown allocation source has an amount")
		}
	default:
		return candidateSizeError("unsupported allocation source")
	}
	switch m.Status {
	case "unknown":
		if m.EntriesExamined != 0 || m.FilePaths != 0 || m.LogicalBytes != nil || m.AllocatedBytes != nil || m.Truncated {
			return candidateSizeError("unknown measurement invariant")
		}
	case "recorded_complete":
		if m.EntriesExamined == 0 || m.LogicalBytes == nil || m.Truncated || m.SkippedEntries != 0 || m.UnconfirmedEntries != 0 || m.IncompleteDirectories != 0 || m.DirectoryErrors != 0 || m.RootError != "" || m.UnknownReason != "" {
			return candidateSizeError("recorded completeness invariant")
		}
	case "partial", "stale":
		if m.EntriesExamined == 0 || m.UnknownReason != "" {
			return candidateSizeError("measured portion invariant")
		}
	default:
		return candidateSizeError("unsupported measurement status")
	}
	return nil
}

func freezeCandidateSizePage(report FindingReport) (*CandidateSizePage, error) {
	if report.Source != "saved_inventory" || report.CurrentStateVerified || report.EntryLimit != FindingEntryLimit || report.EntriesExamined < 0 || report.EntriesExamined > FindingEntryLimit || len(report.Findings) > candidateSizePageLimit || len(report.Findings) > report.EntriesExamined {
		return nil, candidateSizeError("page bounds or historical source invariant")
	}
	receipt := &CandidateSizePage{count: len(report.Findings)}
	for i, f := range report.Findings {
		if err := validateCandidateMeasurement(f); err != nil {
			return nil, err
		}
		for j := 0; j < i; j++ {
			if receipt.rows[j].id == f.ID {
				return nil, candidateSizeError("repeated frozen finding ID")
			}
		}
		m := f.Measurement
		row := candidateSizeRow{id: strings.Clone(f.ID), path: bytes.Clone(f.PathBytes), device: freezeCandidateIdentity(f.Device), inode: freezeCandidateIdentity(f.Inode),
			filePaths: int64(m.FilePaths), unknownInodes: int64(m.UnknownInodes), hasFilePaths: m.Status != "unknown",
			truncated: m.Truncated, skipped: m.SkippedEntries > 0, unconfirmed: m.UnconfirmedEntries > 0, directoryErrors: m.DirectoryErrors > 0, incomplete: m.IncompleteDirectories > 0, rootError: m.RootError != "",
			entryLimit: m.CoverageSource == "bounded_entry_check" && m.EntriesExamined == DirectoryEntryLimit, compactIdentityLimit: m.InodeEntriesExamined == DirectoryEntryLimit}
		if m.LogicalBytes != nil {
			row.logical, row.hasLogical = *m.LogicalBytes, true
		}
		if m.AllocatedBytes != nil {
			row.allocated, row.hasAllocated = *m.AllocatedBytes, true
		}
		switch m.Status {
		case "recorded_complete":
			row.status = candidateComplete
		case "partial":
			row.status = candidatePartial
		case "stale":
			row.status = candidateStale
		case "unknown":
			row.status = candidateUnknown
		}
		if m.CoverageSource == "cached_reduction" {
			row.coverage = candidateCached
		} else {
			row.coverage = candidateBounded
		}
		switch m.AllocatedSizeSource {
		case "cached_reduction":
			row.allocation = candidateCached
		case "bounded_identity_check":
			row.allocation = candidateBounded
		case "unknown":
			row.allocation = candidateUnavailable
		}
		receipt.rows[i] = row
	}
	receipt.valid = true
	return receipt, nil
}

func candidatePathsOverlap(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	if len(a) > len(b) {
		a, b = b, a
	}
	return len(a) < len(b) && bytes.HasPrefix(b, a) && (len(a) == 1 || b[len(a)] == '/')
}

// SummarizeVisible projects an exact unique subset without reading inventory or
// consulting dismissal identities. Caller mutations of the public page are ignored.
func (page *CandidateSizePage) SummarizeVisible(ids []string) (CandidatePageSizeSummary, error) {
	var result CandidatePageSizeSummary
	if page == nil || !page.valid || page.count < 0 || page.count > candidateSizePageLimit || len(ids) > page.count {
		return result, candidateSizeError("uncaptured receipt or visible subset bounds")
	}
	var selected [candidateSizePageLimit]bool
	for _, id := range ids {
		found := -1
		for i := 0; i < page.count; i++ {
			if page.rows[i].id == id {
				found = i
				break
			}
		}
		if found < 0 || selected[found] {
			return result, candidateSizeError("foreign or repeated visible ID")
		}
		selected[found] = true
	}
	result = CandidatePageSizeSummary{Contract: "candidate_page_size_summary_v1", Source: "saved_inventory", Scope: "visible_candidate_page", Status: "historical_measurements", DisplayedFolders: len(ids)}
	var logical, paths int64
	logicalMissing, pathsMissing, logicalOverflow, pathsOverflow := false, false, false, false
	single := -1
	for i := 0; i < page.count; i++ {
		row := &page.rows[i]
		if row.logical < 0 || row.allocated < 0 || row.filePaths < 0 || row.unknownInodes < 0 || row.unknownInodes > row.filePaths || row.status < candidateComplete || row.status > candidateUnknown || row.coverage < candidateBounded || row.coverage > candidateCached || row.allocation < candidateBounded || row.allocation > candidateUnavailable {
			return CandidatePageSizeSummary{}, candidateSizeError("frozen scalar invariant")
		}
		if !selected[i] {
			continue
		}
		single = i
		if row.hasLogical {
			result.KnownLogicalMeasurements++
			if logical > math.MaxInt64-row.logical {
				logicalOverflow = true
			} else {
				logical += row.logical
			}
		} else {
			logicalMissing = true
		}
		if !row.hasFilePaths {
			pathsMissing = true
		} else if paths > math.MaxInt64-row.filePaths {
			pathsOverflow = true
		} else {
			paths += row.filePaths
		}
		switch row.status {
		case candidatePartial:
			result.PartialFolders++
		case candidateStale:
			result.StaleFolders++
		case candidateUnknown:
			result.UnknownFolders++
		}
		if row.truncated {
			result.TruncatedFolders++
		}
		if row.skipped {
			result.FoldersWithSkippedEntries++
		}
		if row.unconfirmed {
			result.FoldersWithUnconfirmedEntries++
		}
		if row.directoryErrors || row.rootError {
			result.FoldersWithDirectoryErrors++
		}
		if row.incomplete {
			result.FoldersWithIncompleteDirectories++
		}
		if row.entryLimit {
			result.FoldersAtEntryLimit++
		}
		if row.compactIdentityLimit {
			result.FoldersAtCompactIdentityLimit++
		}
		if row.coverage == candidateCached {
			result.CachedCoverageFolders++
		} else {
			result.BoundedCoverageFolders++
		}
		if row.allocation == candidateCached {
			result.CachedAllocationFolders++
		} else if row.allocation == candidateBounded {
			result.BoundedAllocationFolders++
		}
		if !row.device.present || !row.inode.present {
			result.UnknownDirectoryIdentities++
		}
		for j := 0; j < i; j++ {
			if !selected[j] {
				continue
			}
			prior := &page.rows[j]
			if candidatePathsOverlap(row.path, prior.path) {
				result.OverlapPairs++
			}
			if !bytes.Equal(row.path, prior.path) && sameCandidateIdentity(row.device, prior.device) && sameCandidateIdentity(row.inode, prior.inode) {
				result.KnownDirectoryAliasPairs++
			}
		}
	}
	if result.DisplayedFolders == 0 {
		result.Status = "empty_selection"
	} else if result.PartialFolders+result.StaleFolders+result.UnknownFolders+result.TruncatedFolders > 0 {
		result.Status = "historical_measured_portion"
	}
	result.LogicalPathBytes, result.MeasuredFilePaths = &logical, &paths
	switch {
	case result.OverlapPairs > 0:
		result.LogicalUnavailableReason = "overlap_detected"
	case logicalMissing:
		result.LogicalUnavailableReason = "measurement_unavailable"
	case logicalOverflow:
		result.LogicalUnavailableReason = "aggregate_overflow"
	}
	switch {
	case result.OverlapPairs > 0:
		result.FilePathsUnavailableReason = "overlap_detected"
	case pathsMissing:
		result.FilePathsUnavailableReason = "measurement_unavailable"
	case pathsOverflow:
		result.FilePathsUnavailableReason = "aggregate_overflow"
	}
	if result.LogicalUnavailableReason != "" {
		result.LogicalPathBytes = nil
	}
	if result.FilePathsUnavailableReason != "" {
		result.MeasuredFilePaths = nil
	}
	switch result.DisplayedFolders {
	case 0:
		zero := int64(0)
		result.WithinFolderAllocatedBytes = &zero
	case 1:
		row := &page.rows[single]
		switch {
		case row.status != candidateComplete:
			result.AllocatedUnavailableReason = "single_folder_incomplete_or_stale"
		case !row.device.present || !row.inode.present || row.unknownInodes != 0:
			result.AllocatedUnavailableReason = "single_folder_identity_unavailable"
		case !row.hasAllocated || row.allocation == candidateUnavailable:
			result.AllocatedUnavailableReason = "measurement_unavailable"
		default:
			allocated := row.allocated
			result.WithinFolderAllocatedBytes = &allocated
		}
	default:
		result.AllocatedUnavailableReason = "cross_folder_identity_evidence_unavailable"
	}
	return result, nil
}
