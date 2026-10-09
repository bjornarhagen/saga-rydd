package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const BuildOutputContract = "cargo_target_build_output_metadata_v1"
const BuildOutputRule = "cargo-target-layout-v1"
const BuildOutputEntryLimit = 1000
const BuildOutputFindingLimit = 20

// This category recognizes only the saved default Cargo layout. It does not
// validate Cargo inputs, reproduce a build or authorize an operation.
type BuildOutputMarker struct {
	Role             string    `json:"role"`
	EntryID          int64     `json:"entry_id"`
	Kind             string    `json:"kind"`
	Path             string    `json:"path"`
	PathBytes        []byte    `json:"path_bytes"`
	Device           string    `json:"device"`
	Inode            string    `json:"inode"`
	CtimeNS          int64     `json:"ctime_ns"`
	Generation       int64     `json:"generation"`
	ParentGeneration int64     `json:"parent_generation"`
	ModifiedAt       time.Time `json:"modified_at"`
	ObservedAt       time.Time `json:"observed_at"`
}

type BuildOutputFinding struct {
	ID                        string              `json:"id"`
	Rule                      string              `json:"rule"`
	RuleVersion               int                 `json:"rule_version"`
	RootID                    int64               `json:"root_id"`
	EntryID                   int64               `json:"entry_id"`
	Device                    string              `json:"device"`
	Inode                     string              `json:"inode"`
	Path                      string              `json:"path"`
	PathBytes                 []byte              `json:"path_bytes"`
	Profile                   string              `json:"profile"`
	Markers                   []BuildOutputMarker `json:"markers"`
	Classification            string              `json:"classification"`
	Recognition               string              `json:"recognition"`
	Actions                   []string            `json:"available_actions"`
	Measurement               DirectoryReport     `json:"measurement"`
	ContentVerified           bool                `json:"content_verified"`
	CurrentStateVerified      bool                `json:"current_state_verified"`
	ApprovalAvailable         bool                `json:"approval_available"`
	Executable                bool                `json:"executable"`
	EstimatedReclaimableBytes *int64              `json:"estimated_reclaimable_bytes"`
}

type BuildOutputReport struct {
	Contract                      string                `json:"contract"`
	Source                        string                `json:"source"`
	GeneratedAt                   time.Time             `json:"generated_at"`
	Diagnostics                   []SelectionDiagnostic `json:"selection_diagnostics"`
	PageCoverage                  string                `json:"page_coverage"`
	Findings                      []BuildOutputFinding  `json:"findings"`
	EntriesExamined               int                   `json:"entries_examined"`
	EntryLimit                    int                   `json:"entry_limit"`
	MinimumAgeDays                int                   `json:"minimum_age_days"`
	NextCursor                    string                `json:"next_cursor,omitempty"`
	Notes                         []string              `json:"notes"`
	SelectedContentRequestedBytes int64                 `json:"selected_file_body_requested_bytes"`
	SelectedContentReadBytes      int64                 `json:"selected_file_body_read_bytes"`
	ContentVerified               bool                  `json:"content_verified"`
	CurrentStateVerified          bool                  `json:"current_state_verified"`
	ApprovalAvailable             bool                  `json:"approval_available"`
	Executable                    bool                  `json:"executable"`
	EstimatedReclaimableBytes     *int64                `json:"estimated_reclaimable_bytes"`
}

const (
	buildNotTarget = iota
	buildNested
	buildUnsupportedPath
	buildNotDirectory
	buildSkipped
	buildManifestUnsupported
	buildProfileUnsupported
	buildParentIncomplete
	buildParentUnconfirmed
	buildTimestampUnknown
	buildAgeNotMet
	buildSelected
)

func buildOutputDiagnostics() []SelectionDiagnostic {
	return []SelectionDiagnostic{
		{Code: "not_target", Explanation: "Saved entry is not named target."},
		{Code: "nested_output", Explanation: "A target or node_modules ancestor suppresses overlapping output candidates."},
		{Code: "unsupported_saved_path", Explanation: "Saved path or marker evidence exceeds the supported bounds or scope."},
		{Code: "not_directory", Explanation: "target was not saved as a directory."},
		{Code: "skipped", Explanation: "A required saved marker has a skip reason."},
		{Code: "manifest_missing_or_unsupported", Explanation: "The target parent lacks saved regular Cargo.toml and Cargo.lock siblings."},
		{Code: "profile_missing_or_unsupported", Explanation: "Neither supported profile has the required .cargo-lock, deps and .fingerprint marker kinds."},
		{Code: "parent_incomplete_or_error", Explanation: "A required marker's direct saved parent listing is missing, incomplete or has an error."},
		{Code: "parent_unconfirmed", Explanation: "A required marker is not confirmed in its direct saved parent's generation."},
		{Code: "timestamp_unknown", Explanation: "A required marker modification time is nonpositive."},
		{Code: "age_not_met", Explanation: "A required marker is newer than the effective age cutoff, including future dates."},
		{Code: "selected", Explanation: "Selected for historical review; no current verification or cleanup authority."},
	}
}

type buildOutputHooks struct{ beforeCommit func() }

// CargoBuildOutputs reads one existing inventory snapshot, including bounded
// measurements. It opens no source path and never initializes or migrates state.
// Marker confirmation concerns direct parent passes only. MeasureDirectory's
// independent ancestor/subtree qualifications remain visible in every finding.
func (s *Store) CargoBuildOutputs(ctx context.Context, cursor string, minimumAgeDays int) (BuildOutputReport, error) {
	return s.cargoBuildOutputs(ctx, cursor, minimumAgeDays, buildOutputHooks{})
}

func (s *Store) cargoBuildOutputs(ctx context.Context, cursor string, minimumAgeDays int, hooks buildOutputHooks) (BuildOutputReport, error) {
	if err := ctx.Err(); err != nil {
		return BuildOutputReport{}, err
	}
	if minimumAgeDays < 1 || minimumAgeDays > MaxFindingAgeDays {
		return BuildOutputReport{}, ErrFindingAge
	}
	prefix := fmt.Sprintf("cargo1:%d:", minimumAgeDays)
	var after int64
	if cursor != "" {
		if !strings.HasPrefix(cursor, prefix) {
			return BuildOutputReport{}, ErrReportCursor
		}
		parsed, err := strconv.ParseInt(strings.TrimPrefix(cursor, prefix), 10, 64)
		if err != nil || parsed <= 0 || cursor != prefix+strconv.FormatInt(parsed, 10) {
			return BuildOutputReport{}, ErrReportCursor
		}
		after = parsed
	}
	if s == nil || s.db == nil {
		return BuildOutputReport{}, errors.New("build output reports require an open inventory reader")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BuildOutputReport{}, err
	}
	defer tx.Rollback()
	r := BuildOutputReport{Contract: BuildOutputContract, Source: "saved_inventory", GeneratedAt: time.Now().UTC(), Diagnostics: buildOutputDiagnostics(), Findings: []BuildOutputFinding{}, EntryLimit: BuildOutputEntryLimit, MinimumAgeDays: minimumAgeDays, PageCoverage: "saved_entries_exhausted", Notes: []string{
		"Review required. Recognition uses saved filenames, object kinds and direct parent passes only; Cargo.toml, Cargo.lock and artifact contents were not read or validated.",
		"Every required marker's recorded modification time must meet the displayed age filter. Old dates do not prove inactivity, continuous stability, reproducibility or safe removal.",
		"Only the default outermost target layout with debug or release, .cargo-lock, deps and .fingerprint is recognized. Custom target/build locations, cross-target and newer internal layouts are unsupported.",
		"Debug is checked before release; the first eligible profile supplies evidence. If neither qualifies, the first structurally supported profile supplies the rejection reason, otherwise skipped or missing markers do.",
		"Direct marker confirmation does not establish ancestor or current-filesystem freshness. Each saved measurement retains full ancestor/subtree partial, stale or unknown qualifications.",
		"Sizes and selection use one saved inventory snapshot. Logical and allocated sizes are not reclaimable space; hardlinks, clones, snapshots and overlapping reports must not be summed.",
		"Rebuilding may require the correct toolchain, inputs, build scripts, available dependencies, credentials and network access. Locally edited or unique artifacts and use by other tools remain unverified.",
		"Each page examines at most 1000 saved entries and measures at most 20 findings. Empty pages can continue. Cursors preserve the age filter; concurrent updates can change later pages, and local IDs can be reused after rebuilding inventory.",
		"No source bodies, configuration or Cargo commands are read or invoked. This category grants no approval, cleanup action, permanent keep policy or automatic policy; node_modules plans and dismissals do not accept these findings.",
	}}
	cutoff := r.GeneratedAt.Add(-time.Duration(minimumAgeDays) * 24 * time.Hour).UnixNano()
	rows, err := tx.QueryContext(ctx, `SELECT e.id,e.root_id,substr(e.path,1,4097),substr(r.path,1,4097)
 FROM entries e JOIN roots r ON r.id=e.root_id WHERE r.enabled=1 AND e.id>? ORDER BY e.id LIMIT ?`, after, BuildOutputEntryLimit+1)
	if err != nil {
		return BuildOutputReport{}, err
	}
	for rows.Next() {
		if r.EntriesExamined == BuildOutputEntryLimit || len(r.Findings) == BuildOutputFindingLimit {
			r.PageCoverage, r.NextCursor = "more_saved_entries", prefix+strconv.FormatInt(after, 10)
			break
		}
		var entryID, rootID int64
		var path, root []byte
		if err = rows.Scan(&entryID, &rootID, &path, &root); err != nil {
			rows.Close()
			return BuildOutputReport{}, err
		}
		after, r.EntriesExamined = entryID, r.EntriesExamined+1
		finding, code, checkErr := readBuildOutputFinding(ctx, tx, rootID, entryID, root, path, cutoff)
		if checkErr != nil {
			rows.Close()
			return BuildOutputReport{}, checkErr
		}
		r.Diagnostics[code].Count++
		if code == buildSelected {
			r.Findings = append(r.Findings, finding)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return BuildOutputReport{}, err
	}
	for i := range r.Findings {
		r.Findings[i].Measurement, err = s.measureDirectory(ctx, r.Findings[i].Path, tx)
		if err != nil {
			return BuildOutputReport{}, err
		}
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return BuildOutputReport{}, err
	}
	if err = tx.Commit(); err != nil {
		return BuildOutputReport{}, err
	}
	if err = ctx.Err(); err != nil {
		return BuildOutputReport{}, err
	}
	return r, nil
}

type buildOutputEntry struct {
	id, modified, changed, generation, observed, parentGeneration int64
	parentComplete                                                int
	kind, parent, device, inode, skip, parentError                string
}

func readBuildOutputEntry(ctx context.Context, tx *sql.Tx, rootID int64, path string) (buildOutputEntry, bool, error) {
	var e buildOutputEntry
	err := tx.QueryRowContext(ctx, `SELECT e.id,substr(e.parent,1,4097),substr(e.kind,1,33),substr(e.device,1,1025),substr(e.inode,1,1025),e.mtime_ns,e.ctime_ns,e.generation,e.observed_at_ns,
 substr(e.skip_reason,1,4097),COALESCE(p.generation,0),COALESCE(p.complete,0),substr(COALESCE(p.last_error,''),1,4097)
 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent WHERE e.root_id=? AND e.path=?`, rootID, []byte(path)).Scan(&e.id, &e.parent, &e.kind, &e.device, &e.inode, &e.modified, &e.changed, &e.generation, &e.observed, &e.skip, &e.parentGeneration, &e.parentComplete, &e.parentError)
	if errors.Is(err, sql.ErrNoRows) {
		return e, false, nil
	}
	return e, err == nil, err
}

type buildOutputRequired struct {
	role, path, kind string
	entry            buildOutputEntry
	exists           bool
}

func readBuildOutputRequired(ctx context.Context, tx *sql.Tx, rootID int64, role, path, kind string) (buildOutputRequired, error) {
	e, exists, err := readBuildOutputEntry(ctx, tx, rootID, path)
	return buildOutputRequired{role: role, path: path, kind: kind, entry: e, exists: exists}, err
}

func buildOutputEvidenceCode(required []buildOutputRequired, cutoff int64) int {
	for _, m := range required {
		if len(m.path) > 4096 || len(m.entry.parent) > 4096 || len(m.entry.device) > 1024 || len(m.entry.inode) > 1024 || len(m.entry.kind) > 32 || len(m.entry.skip) > 4096 || len(m.entry.parentError) > 4096 || m.exists && m.entry.parent != filepath.Dir(m.path) {
			return buildUnsupportedPath
		}
	}
	for _, m := range required {
		if m.exists && m.entry.skip != "" {
			return buildSkipped
		}
	}
	for _, m := range required {
		if !m.exists || m.entry.kind != m.kind {
			return buildProfileUnsupported
		}
	}
	for _, m := range required {
		if m.entry.parentComplete != 1 || m.entry.parentError != "" {
			return buildParentIncomplete
		}
	}
	for _, m := range required {
		if m.entry.parentGeneration <= 0 || m.entry.generation <= 0 || m.entry.parentGeneration != m.entry.generation {
			return buildParentUnconfirmed
		}
	}
	for _, m := range required {
		if m.entry.modified <= 0 {
			return buildTimestampUnknown
		}
	}
	for _, m := range required {
		if m.entry.modified > cutoff {
			return buildAgeNotMet
		}
	}
	return buildSelected
}

func readBuildOutputFinding(ctx context.Context, tx *sql.Tx, rootID, entryID int64, root, relative []byte, cutoff int64) (BuildOutputFinding, int, error) {
	if filepath.Base(string(relative)) != "target" {
		return BuildOutputFinding{}, buildNotTarget, nil
	}
	path := filepath.Join(string(root), string(relative))
	if !canonicalPath(root) || len(relative) > 4096 || filepath.IsAbs(string(relative)) || filepath.Clean(string(relative)) != string(relative) || string(relative) == ".." || strings.HasPrefix(string(relative), "../") || len(path) > 4096 || strings.ContainsRune(string(relative), 0) {
		return BuildOutputFinding{}, buildUnsupportedPath, nil
	}
	for _, part := range strings.Split(filepath.Dir(path), string(filepath.Separator)) {
		if part == "target" || part == "node_modules" {
			return BuildOutputFinding{}, buildNested, nil
		}
	}
	common := make([]buildOutputRequired, 0, 3)
	for _, spec := range []struct{ role, path, kind string }{
		{"target", string(relative), "directory"},
		{"manifest", filepath.Join(filepath.Dir(string(relative)), "Cargo.toml"), "file"},
		{"lockfile", filepath.Join(filepath.Dir(string(relative)), "Cargo.lock"), "file"},
	} {
		m, err := readBuildOutputRequired(ctx, tx, rootID, spec.role, spec.path, spec.kind)
		if err != nil {
			return BuildOutputFinding{}, 0, err
		}
		common = append(common, m)
	}
	if !common[0].exists || common[0].entry.kind != "directory" {
		return BuildOutputFinding{}, buildNotDirectory, nil
	}
	for _, m := range common {
		if m.exists && m.entry.skip != "" {
			return BuildOutputFinding{}, buildSkipped, nil
		}
	}
	if !common[1].exists || common[1].entry.kind != "file" || !common[2].exists || common[2].entry.kind != "file" {
		return BuildOutputFinding{}, buildManifestUnsupported, nil
	}
	type failure struct {
		code      int
		supported bool
	}
	failures := []failure{}
	for _, profile := range []string{"debug", "release"} {
		profilePath := filepath.Join(string(relative), profile)
		required := append([]buildOutputRequired{}, common...)
		for _, spec := range []struct{ role, path, kind string }{
			{"profile", profilePath, "directory"},
			{"profile_lock", filepath.Join(profilePath, ".cargo-lock"), "file"},
			{"dependencies", filepath.Join(profilePath, "deps"), "directory"},
			{"fingerprints", filepath.Join(profilePath, ".fingerprint"), "directory"},
		} {
			m, err := readBuildOutputRequired(ctx, tx, rootID, spec.role, spec.path, spec.kind)
			if err != nil {
				return BuildOutputFinding{}, 0, err
			}
			required = append(required, m)
		}
		code := buildOutputEvidenceCode(required, cutoff)
		if code != buildSelected {
			supported := true
			for _, m := range required {
				if !m.exists || m.entry.kind != m.kind {
					supported = false
				}
			}
			failures = append(failures, failure{code, supported})
			continue
		}
		f := BuildOutputFinding{ID: fmt.Sprintf("cargo-target-v1:%d:%d", rootID, entryID), Rule: BuildOutputRule, RuleVersion: 1, RootID: rootID, EntryID: entryID, Device: common[0].entry.device, Inode: common[0].entry.inode, Path: path, PathBytes: []byte(path), Profile: profile, Classification: "review_required", Recognition: "saved_layout_metadata_only", Actions: []string{}, Markers: []BuildOutputMarker{}}
		for _, m := range required {
			e := m.entry
			absolute := filepath.Join(string(root), m.path)
			if len(absolute) > 4096 {
				return BuildOutputFinding{}, buildUnsupportedPath, nil
			}
			observed := time.Time{}
			if e.observed > 0 {
				observed = time.Unix(0, e.observed).UTC()
			}
			f.Markers = append(f.Markers, BuildOutputMarker{Role: m.role, EntryID: e.id, Kind: e.kind, Path: absolute, PathBytes: []byte(absolute), Device: e.device, Inode: e.inode, CtimeNS: e.changed, Generation: e.generation, ParentGeneration: e.parentGeneration, ModifiedAt: time.Unix(0, e.modified).UTC(), ObservedAt: observed})
		}
		return f, buildSelected, nil
	}
	for _, f := range failures {
		if f.supported {
			return BuildOutputFinding{}, f.code, nil
		}
	}
	for _, f := range failures {
		if f.code == buildSkipped || f.code == buildUnsupportedPath {
			return BuildOutputFinding{}, f.code, nil
		}
	}
	return BuildOutputFinding{}, buildProfileUnsupported, nil
}
