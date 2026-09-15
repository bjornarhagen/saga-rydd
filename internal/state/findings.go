package state

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const FindingEntryLimit = 1000
const FindingAgeDays = 90

type Finding struct {
	ID                  string          `json:"id"`
	Rule                string          `json:"rule"`
	RuleVersion         int             `json:"rule_version"`
	RootID              int64           `json:"root_id"`
	EntryID             int64           `json:"entry_id"`
	Device              string          `json:"device"`
	Inode               string          `json:"inode"`
	Path                string          `json:"path"`
	PathBytes           []byte          `json:"path_bytes"`
	ManifestPath        string          `json:"manifest_path"`
	ManifestPathBytes   []byte          `json:"manifest_path_bytes"`
	DirectoryModifiedAt time.Time       `json:"directory_modified_at"`
	ManifestModifiedAt  time.Time       `json:"manifest_modified_at"`
	DirectoryObservedAt time.Time       `json:"directory_observed_at"`
	ManifestObservedAt  time.Time       `json:"manifest_observed_at"`
	Classification      string          `json:"classification"`
	Recognition         string          `json:"recognition"`
	Actions             []string        `json:"available_actions"`
	Measurement         DirectoryReport `json:"measurement"`
}

// SelectionDiagnostic counts one first-match outcome per examined entry.
// Slice order defines precedence; later conditions may also be true.
type SelectionDiagnostic struct {
	Code        string `json:"code"`
	Count       int    `json:"count"`
	Explanation string `json:"explanation"`
}

func selectionDiagnostics() []SelectionDiagnostic {
	return []SelectionDiagnostic{
		{Code: "not_node_modules", Explanation: "Entry is not named node_modules."},
		{Code: "nested_dependency", Explanation: "Nested node_modules entry suppressed to avoid overlapping candidates."},
		{Code: "not_directory", Explanation: "node_modules was not recorded as a directory."},
		{Code: "skipped", Explanation: "Dependency directory or manifest has a saved skip reason."},
		{Code: "manifest_missing_or_unsupported", Explanation: "No saved regular-file package.json sibling; contents are not validated."},
		{Code: "parent_incomplete_or_error", Explanation: "Parent listing is missing, incomplete or has a saved error."},
		{Code: "parent_unconfirmed", Explanation: "Dependency directory or manifest is unconfirmed in the saved parent generation."},
		{Code: "timestamp_unknown", Explanation: "Directory or manifest modification timestamp is nonpositive and cannot establish the age rule."},
		{Code: "age_not_met", Explanation: "Directory or manifest modification timestamp is newer than the age cutoff (including future timestamps)."},
		{Code: "selected", Explanation: "Selected for review; this is not deletion authorization."},
	}
}

type FindingReport struct {
	Diagnostics          []SelectionDiagnostic `json:"selection_diagnostics"`
	PageCoverage         string                `json:"page_coverage"`
	GeneratedAt          time.Time             `json:"generated_at"`
	Source               string                `json:"source"`
	CurrentStateVerified bool                  `json:"current_state_verified"`
	Findings             []Finding             `json:"findings"`
	EntriesExamined      int                   `json:"entries_examined"`
	EntryLimit           int                   `json:"entry_limit"`
	MinimumAgeDays       int                   `json:"minimum_age_days"`
	NextCursor           string                `json:"next_cursor,omitempty"`
	Notes                []string              `json:"notes"`
}

// NodeModulesFindings derives review candidates from durable observations. It
// examines one bounded ID page, then measures at most 20 candidates separately.
// Each measurement has its own snapshot; no multi-snapshot totals are offered.
func (s *Store) NodeModulesFindings(ctx context.Context, token string) (FindingReport, error) {
	r := FindingReport{Diagnostics: selectionDiagnostics(), PageCoverage: "saved_entries_exhausted", GeneratedAt: time.Now().UTC(), Source: "saved_inventory", Findings: []Finding{}, EntryLimit: FindingEntryLimit, MinimumAgeDays: FindingAgeDays, Notes: []string{
		"Review required. Old recorded directory and package.json modification times do not prove inactivity, continuous stability or safe deletion.",
		"Project recognition uses a sibling regular-file package.json observation only. Manifest contents, lockfiles, source activity and dependency modifications have not been inspected.",
		"Removing dependencies can break builds and applications or lose local edits. Regeneration may require the correct package manager, lockfile, credentials, network access and packages that remain available.",
		"Sizes are saved measurements, not reclaimable space. Each candidate is measured in a separate snapshot; partial/stale sizes may overestimate or underestimate current contents. Do not sum shared storage.",
		"Selection diagnostics count only this page, with one first-match outcome per examined entry in displayed order. Exhausted saved entries do not mean scanning is complete or the machine is clean.",
		"Nested node_modules paths are suppressed. Empty pages can still have a next cursor. Pages are not a frozen snapshot; concurrent updates can change results.",
		"Findings are derived on demand, not persisted approvals. IDs are local inventory references, not action authorization; cleanup, dismissal and automatic policies are unavailable.",
	}}
	var after int64
	if token != "" {
		var err error
		if !strings.HasPrefix(token, "nm1:") {
			return r, ErrReportCursor
		}
		after, err = strconv.ParseInt(strings.TrimPrefix(token, "nm1:"), 10, 64)
		if err != nil || after <= 0 {
			return r, ErrReportCursor
		}
	}
	// Walk by primary key rather than an unindexed basename search. Bound both
	// query results and candidate measurements even when most entries are files.
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.root_id,e.path,r.path,e.kind,e.skip_reason,e.device,e.inode,e.mtime_ns,e.observed_at_ns,
 COALESCE(m.kind,''),COALESCE(m.skip_reason,''),COALESCE(m.mtime_ns,0),COALESCE(m.observed_at_ns,0),
 e.generation,COALESCE(m.generation,0),COALESCE(p.generation,0),COALESCE(p.complete,0),COALESCE(p.last_error,'')
 FROM entries e JOIN roots r ON r.id=e.root_id
 LEFT JOIN entries m ON m.root_id=e.root_id AND m.path=CAST(CASE WHEN e.parent=X'2e' THEN 'package.json' ELSE CAST(e.parent AS TEXT)||'/package.json' END AS BLOB)
 LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent
 WHERE e.id>? AND r.enabled=1 ORDER BY e.id LIMIT ?`, after, FindingEntryLimit+1)
	if err != nil {
		return r, err
	}
	cutoff := r.GeneratedAt.Add(-FindingAgeDays * 24 * time.Hour).UnixNano()
	for rows.Next() {
		if r.EntriesExamined == FindingEntryLimit || len(r.Findings) == 20 {
			r.NextCursor = fmt.Sprintf("nm1:%d", after)
			r.PageCoverage = "more_saved_entries"
			break
		}
		var f Finding
		var rel, root []byte
		var kind, skip, mkind, mskip, perr string
		var mt, ot, mmt, mot, gen, mgen, pgen int64
		var complete int
		if err = rows.Scan(&f.EntryID, &f.RootID, &rel, &root, &kind, &skip, &f.Device, &f.Inode, &mt, &ot, &mkind, &mskip, &mmt, &mot, &gen, &mgen, &pgen, &complete, &perr); err != nil {
			rows.Close()
			return r, err
		}
		after = f.EntryID
		r.EntriesExamined++
		path := string(rel)
		// First matching outcome wins. Eligibility is unchanged; evidence
		// failures precede age so uncertain records are not described as recent.
		outcome := 9
		switch {
		case filepath.Base(path) != "node_modules":
			outcome = 0
		case strings.Contains("/"+filepath.Dir(path)+"/", "/node_modules/"):
			outcome = 1
		case kind != "directory":
			outcome = 2
		case skip != "" || mskip != "":
			outcome = 3
		case mkind != "file":
			outcome = 4
		case complete != 1 || perr != "":
			outcome = 5
		case gen != pgen || mgen != pgen:
			outcome = 6
		case mt <= 0 || mmt <= 0:
			outcome = 7
		case mt > cutoff || mmt > cutoff:
			outcome = 8
		}
		r.Diagnostics[outcome].Count++
		if outcome != 9 {
			continue
		}
		f.Path = filepath.Join(string(root), path)
		f.PathBytes = []byte(f.Path)
		f.ManifestPath = filepath.Join(filepath.Dir(f.Path), "package.json")
		f.ManifestPathBytes = []byte(f.ManifestPath)
		f.ID = fmt.Sprintf("node-modules-v1:%d:%d", f.RootID, f.EntryID)
		f.Rule = "node_modules_old_metadata"
		f.RuleVersion = 1
		f.Classification = "review_required"
		f.Recognition = "manifest_filename_only"
		f.Actions = []string{}
		f.DirectoryModifiedAt = time.Unix(0, mt).UTC()
		f.ManifestModifiedAt = time.Unix(0, mmt).UTC()
		f.DirectoryObservedAt = time.Unix(0, ot).UTC()
		f.ManifestObservedAt = time.Unix(0, mot).UTC()
		r.Findings = append(r.Findings, f)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	for i := range r.Findings {
		r.Findings[i].Measurement, err = s.MeasureDirectory(ctx, r.Findings[i].Path)
		if err != nil {
			return r, err
		}
	}
	return r, nil
}
