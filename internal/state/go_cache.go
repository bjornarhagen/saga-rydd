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

const GoCacheContract = "go_build_cache_file_metadata_v1"
const GoCacheRule = "go-local-build-cache-layout-v1"
const GoCacheEntryLimit = 1000
const GoCacheFindingLimit = 20

var ErrGoCacheRoot = errors.New("Go cache reports require an exact canonical absolute saved root of at most 4096 bytes without NUL")

// Layout paths are relative to the report's exact root. The bounded marker
// array records layout metadata, never README contents or cache provenance.
type GoCacheLayoutMarker struct {
	Role              string    `json:"role"`
	RelativePath      string    `json:"relative_path"`
	RelativePathBytes []byte    `json:"relative_path_bytes"`
	EntryID           int64     `json:"entry_id"`
	Kind              string    `json:"kind"`
	Device            string    `json:"device"`
	Inode             string    `json:"inode"`
	CtimeNS           int64     `json:"ctime_ns"`
	Generation        int64     `json:"generation"`
	ParentGeneration  int64     `json:"parent_generation"`
	ListingGeneration int64     `json:"listing_generation"`
	ListingComplete   bool      `json:"listing_complete"`
	ListingCheckedAt  time.Time `json:"listing_checked_at"`
	ObservedAt        time.Time `json:"observed_at"`
	ModifiedAt        time.Time `json:"modified_at"`
}

type GoCacheLayout struct {
	Status          string                `json:"status"`
	Code            string                `json:"code"`
	Message         string                `json:"message"`
	RequiredShards  int                   `json:"required_shards"`
	ConfirmedShards int                   `json:"confirmed_shards"`
	Markers         []GoCacheLayoutMarker `json:"markers"`
}

type GoCacheFile struct {
	ID                        string    `json:"id"`
	EntryID                   int64     `json:"entry_id"`
	RootID                    int64     `json:"root_id"`
	Path                      string    `json:"path"`
	PathBytes                 []byte    `json:"path_bytes"`
	RelativePathBytes         []byte    `json:"relative_path_bytes"`
	EntryType                 string    `json:"entry_type"`
	LogicalBytes              *int64    `json:"logical_bytes"`
	AllocatedBytes            *int64    `json:"allocated_bytes"`
	Device                    string    `json:"device"`
	Inode                     string    `json:"inode"`
	CtimeNS                   int64     `json:"ctime_ns"`
	Generation                int64     `json:"generation"`
	ParentGeneration          int64     `json:"parent_generation"`
	ObservedAt                time.Time `json:"observed_at"`
	ModifiedAt                time.Time `json:"modified_at"`
	IdentityStatus            string    `json:"identity_status"`
	Classification            string    `json:"classification"`
	ContentVerified           bool      `json:"content_verified"`
	CurrentStateVerified      bool      `json:"current_state_verified"`
	RegenerationVerified      bool      `json:"regeneration_verified"`
	ApprovalAvailable         bool      `json:"approval_available"`
	AutomationEligible        bool      `json:"automation_eligible"`
	Executable                bool      `json:"executable"`
	EstimatedReclaimableBytes *int64    `json:"estimated_reclaimable_bytes"`
}

type GoCacheReport struct {
	Contract                      string                `json:"contract"`
	Rule                          string                `json:"rule"`
	Source                        string                `json:"source"`
	GeneratedAt                   time.Time             `json:"generated_at"`
	RootID                        int64                 `json:"root_id"`
	RootPath                      string                `json:"root_path"`
	RootPathBytes                 []byte                `json:"root_path_bytes"`
	Layout                        GoCacheLayout         `json:"layout"`
	Files                         []GoCacheFile         `json:"files"`
	EntriesExamined               int                   `json:"entries_examined"`
	EntryLimit                    int                   `json:"entry_limit"`
	MinimumAgeDays                int                   `json:"minimum_age_days"`
	NextCursor                    string                `json:"next_cursor,omitempty"`
	PageCoverage                  string                `json:"page_coverage"`
	Diagnostics                   []SelectionDiagnostic `json:"selection_diagnostics"`
	KnownObjects                  int                   `json:"known_objects"`
	RepeatedSavedObjects          int                   `json:"repeated_saved_objects"`
	UnknownIdentities             int                   `json:"unknown_identities"`
	ConflictingIdentities         int                   `json:"conflicting_identities"`
	Notes                         []string              `json:"notes"`
	SelectedContentRequestedBytes int64                 `json:"selected_file_body_requested_bytes"`
	SelectedContentReadBytes      int64                 `json:"selected_file_body_read_bytes"`
	ContentVerified               bool                  `json:"content_verified"`
	CurrentStateVerified          bool                  `json:"current_state_verified"`
	RegenerationVerified          bool                  `json:"regeneration_verified"`
	ApprovalAvailable             bool                  `json:"approval_available"`
	AutomationEligible            bool                  `json:"automation_eligible"`
	Executable                    bool                  `json:"executable"`
	EstimatedReclaimableBytes     *int64                `json:"estimated_reclaimable_bytes"`
}

const (
	goCacheDifferentRoot = iota
	goCacheUnsupportedPath
	goCacheOutsideLayout
	goCacheExecutableDirectory
	goCacheWrongKind
	goCacheSkipped
	goCacheParentIncomplete
	goCacheParentUnconfirmed
	goCacheTimestampUnknown
	goCacheAgeNotMet
	goCacheSelected
)

func goCacheDiagnostics() []SelectionDiagnostic {
	return []SelectionDiagnostic{
		{Code: "different_root", Explanation: "Raw saved entry is outside the exact requested root."},
		{Code: "unsupported_saved_path", Explanation: "Saved path or parent is invalid or exceeds the supported byte bounds."},
		{Code: "outside_supported_layout", Explanation: "Entry is outside the exact shard and regular -a/-d filename shapes; fuzz, module, custom and auxiliary objects are not selected."},
		{Code: "executable_directory", Explanation: "A -d directory may hold a cached executable; this file-only category does not support it."},
		{Code: "not_regular_file", Explanation: "The supported filename was not saved as a regular file."},
		{Code: "skipped", Explanation: "File has a saved skip reason."},
		{Code: "parent_incomplete_or_error", Explanation: "The shard's saved listing is missing, incomplete or has an error."},
		{Code: "parent_unconfirmed", Explanation: "The file was not confirmed in its saved shard listing generation."},
		{Code: "timestamp_unknown", Explanation: "The file's saved modification time is nonpositive."},
		{Code: "age_not_met", Explanation: "The file is newer than the age cutoff, including future dates."},
		{Code: "selected", Explanation: "Selected for historical metadata review; no regeneration or cleanup authority."},
	}
}

// Fence the global rowid work bound before rejecting other root IDs. There is
// no (root_id,id) index in schemas 4-9; filtering root first could scan/sort an
// unbounded prefix. The materialized page and indexed parent join stay bounded.
const goCachePageQuery = `WITH selected AS MATERIALIZED (
 SELECT id,root_id,substr(CAST(path AS BLOB),1,4097) AS path,substr(CAST(parent AS BLOB),1,4097) AS parent,
 substr(CAST(kind AS BLOB),1,33) AS kind,size,allocated,mtime_ns,ctime_ns,generation,observed_at_ns,
 CASE WHEN skip_reason='' THEN 0 ELSE 1 END AS skipped,
 CASE WHEN length(CAST(device AS BLOB))<=20 THEN device ELSE '' END AS device,
 CASE WHEN length(CAST(inode AS BLOB))<=20 THEN inode ELSE '' END AS inode
 FROM entries WHERE id>? ORDER BY id LIMIT ?
)
SELECT e.id,e.root_id,e.path,e.parent,e.kind,e.size,e.allocated,e.mtime_ns,e.ctime_ns,e.generation,e.observed_at_ns,e.skipped,e.device,e.inode,
 COALESCE(p.generation,0),COALESCE(p.complete,0),CASE WHEN COALESCE(p.last_error,'')='' THEN 0 ELSE 1 END
 FROM selected e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent ORDER BY e.id`

type goCacheHooks struct {
	afterLayout  func()
	beforeCommit func()
}

// GoBuildCache reads saved metadata only. No source/configuration/tool access,
// initialization, migration, cache validation or whole-cache total is performed.
func (s *Store) GoBuildCache(ctx context.Context, root, cursor string, minimumAgeDays int) (GoCacheReport, error) {
	return s.goBuildCache(ctx, root, cursor, minimumAgeDays, goCacheHooks{})
}

func (s *Store) goBuildCache(ctx context.Context, root, cursor string, minimumAgeDays int, hooks goCacheHooks) (GoCacheReport, error) {
	if err := ctx.Err(); err != nil {
		return GoCacheReport{}, err
	}
	if !canonicalPath([]byte(root)) {
		return GoCacheReport{}, ErrGoCacheRoot
	}
	if minimumAgeDays < 1 || minimumAgeDays > MaxFindingAgeDays {
		return GoCacheReport{}, ErrFindingAge
	}
	prefix := fmt.Sprintf("gocache1:%d:", minimumAgeDays)
	var after int64
	if cursor != "" {
		if len(cursor) > 64 {
			return GoCacheReport{}, ErrReportCursor
		}
		n, err := strconv.ParseInt(strings.TrimPrefix(cursor, prefix), 10, 64)
		if err != nil || n <= 0 || cursor != prefix+strconv.FormatInt(n, 10) {
			return GoCacheReport{}, ErrReportCursor
		}
		after = n
	}
	if s == nil || s.db == nil {
		return GoCacheReport{}, errors.New("Go cache reports require an open inventory reader")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return GoCacheReport{}, err
	}
	defer tx.Rollback()
	r := GoCacheReport{Contract: GoCacheContract, Rule: GoCacheRule, Source: "saved_inventory", GeneratedAt: time.Now().UTC(), RootPath: root, RootPathBytes: []byte(root), Files: []GoCacheFile{}, EntryLimit: GoCacheEntryLimit, MinimumAgeDays: minimumAgeDays, PageCoverage: "layout_not_recognized", Diagnostics: goCacheDiagnostics(), Layout: GoCacheLayout{Status: "layout_unsupported", RequiredShards: 256, Markers: []GoCacheLayoutMarker{}}, Notes: []string{
		"Historical layout metadata only. README and cache bodies were not read; provenance, effective GOCACHE, GOENV, GOCACHEPROG and external management remain unverified.",
		"Recognition requires the exact saved root, regular README and all 256 two-hex shard directories with complete, error-free, confirmed saved listings. Unknown or incomplete layouts yield a qualified empty report.",
		"Only depth-two regular 64-lowercase-hex -a/-d entries matching their shard are selected. Executable -d directories, fuzz, module/download caches, auxiliary files and custom layouts are outside this category.",
		"Age applies to each selected file's saved modification time, not root or shard dates. Age does not prove inactivity, continuous stability, provenance, dispensability or regeneration safety. Go already performs its own cache trimming.",
		"Files and layout use one saved snapshot. Parent confirmation is historical; current objects, contents and continuous inode identity remain unverified. Rebuilding can require original inputs, toolchains and external prerequisites.",
		"Each page examines at most 1000 raw saved entries plus one lookahead and selects at most 20 files. Raw entries outside the requested root consume the bound. Empty pages can continue; cursors bind age/local ID only, not an inventory incarnation or frozen export.",
		"Identity counts concern selected rows on this page only. Differing size, allocation, ctime or mtime for one saved device/inode is a conflict; aliases and unknown identities remain visible and cannot be interpreted as storage you can free.",
		"Per-file logical and allocated observations are not whole-cache totals or reclaimable space. Do not sum paths or pages: hardlinks, clones and snapshots can share storage.",
		"No source paths, configuration or Go commands are read or invoked. No saved records change; regeneration, automation, approval and execution remain unavailable. This rule is separate from node_modules plans and dismissals.",
	}}
	var rootError int
	err = tx.QueryRowContext(ctx, "SELECT id,CASE WHEN last_error='' THEN 0 ELSE 1 END FROM roots WHERE path=? AND enabled=1", []byte(root)).Scan(&r.RootID, &rootError)
	if errors.Is(err, sql.ErrNoRows) {
		r.Layout.Code, r.Layout.Message = "root_not_saved", "The exact requested root is not enabled in this saved inventory."
	} else if err != nil {
		return GoCacheReport{}, err
	} else if rootError != 0 {
		r.Layout.Code, r.Layout.Message = "root_error", "The saved root has an error."
	} else {
		r.Layout, err = readGoCacheLayout(ctx, tx, r.RootID, root)
		if err != nil {
			return GoCacheReport{}, err
		}
	}
	if hooks.afterLayout != nil {
		hooks.afterLayout()
	}
	if r.Layout.Status == "layout_metadata_recognized" {
		r.PageCoverage = "saved_entries_exhausted"
		rows, queryErr := tx.QueryContext(ctx, goCachePageQuery, after, GoCacheEntryLimit+1)
		if queryErr != nil {
			return GoCacheReport{}, queryErr
		}
		cutoff := r.GeneratedAt.Add(-time.Duration(minimumAgeDays) * 24 * time.Hour).UnixNano()
		for rows.Next() {
			if r.EntriesExamined == GoCacheEntryLimit || len(r.Files) == GoCacheFindingLimit {
				r.PageCoverage, r.NextCursor = "more_saved_entries", prefix+strconv.FormatInt(after, 10)
				break
			}
			var e goCacheEntry
			if err = rows.Scan(&e.id, &e.rootID, &e.path, &e.parent, &e.kind, &e.size, &e.allocated, &e.modified, &e.changed, &e.generation, &e.observed, &e.skipped, &e.device, &e.inode, &e.parentGeneration, &e.parentComplete, &e.parentError); err != nil {
				rows.Close()
				return GoCacheReport{}, err
			}
			after, r.EntriesExamined = e.id, r.EntriesExamined+1
			file, code := goCacheSelectFile(e, r.RootID, root, cutoff)
			r.Diagnostics[code].Count++
			if code == goCacheSelected {
				r.Files = append(r.Files, file)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return GoCacheReport{}, err
		}
		qualifyGoCacheIdentities(&r)
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return GoCacheReport{}, err
	}
	if err = tx.Commit(); err != nil {
		return GoCacheReport{}, err
	}
	if err = ctx.Err(); err != nil {
		return GoCacheReport{}, err
	}
	return r, nil
}

type goCacheEntry struct {
	id, rootID, size, allocated, modified, changed, generation, observed, parentGeneration int64
	path, parent                                                                           []byte
	kind, device, inode                                                                    string
	skipped, parentComplete, parentError                                                   int
}

func readGoCacheLayout(ctx context.Context, tx *sql.Tx, rootID int64, root string) (GoCacheLayout, error) {
	l := GoCacheLayout{Status: "layout_unsupported", RequiredShards: 256, Markers: []GoCacheLayoutMarker{}}
	for i := -2; i < 256; i++ {
		path, role, kind, parent := ".", "root", "directory", ""
		if i == -1 {
			path, role, kind, parent = "README", "readme", "file", "."
		} else if i >= 0 {
			path, role, parent = fmt.Sprintf("%02x", i), "shard", "."
		}
		if len(filepath.Join(root, path)) > 4096 {
			l.Code, l.Message = "unsupported_saved_path", "Required layout paths exceed the supported byte bound."
			return l, nil
		}
		var m GoCacheLayoutMarker
		var savedParent []byte
		var skipped, listingComplete, listingError int
		var observed, modified, checked int64
		// Root confirms its own pass; other layout entries belong to the root
		// listing. Directory entries separately retain their own listing evidence.
		parentPath := parent
		if path == "." {
			parentPath = "."
		}
		err := tx.QueryRowContext(ctx, `SELECT e.id,substr(CAST(e.parent AS BLOB),1,4097),substr(CAST(e.kind AS BLOB),1,33),
 CASE WHEN length(CAST(e.device AS BLOB))<=20 THEN e.device ELSE '' END,CASE WHEN length(CAST(e.inode AS BLOB))<=20 THEN e.inode ELSE '' END,
 e.ctime_ns,e.generation,e.observed_at_ns,e.mtime_ns,CASE WHEN e.skip_reason='' THEN 0 ELSE 1 END,
 COALESCE(p.generation,0),COALESCE(p.complete,0),CASE WHEN COALESCE(p.last_error,'')='' THEN 0 ELSE 1 END
 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=? WHERE e.root_id=? AND e.path=?`, []byte(parentPath), rootID, []byte(path)).Scan(&m.EntryID, &savedParent, &m.Kind, &m.Device, &m.Inode, &m.CtimeNS, &m.Generation, &observed, &modified, &skipped, &m.ParentGeneration, &listingComplete, &listingError)
		if errors.Is(err, sql.ErrNoRows) {
			l.Code, l.Message = "marker_missing", fmt.Sprintf("Required saved %s marker %q is missing.", role, path)
			return l, nil
		}
		if err != nil {
			return GoCacheLayout{}, err
		}
		if string(savedParent) != parent || m.Kind != kind {
			l.Code, l.Message = "marker_kind_or_parent", fmt.Sprintf("Required saved %s marker %q has an unsupported kind or parent.", role, path)
			return l, nil
		}
		if skipped != 0 {
			l.Code, l.Message = "marker_skipped", fmt.Sprintf("Required saved %s marker %q was skipped.", role, path)
			return l, nil
		}
		if listingComplete != 1 || listingError != 0 {
			l.Code, l.Message = "parent_incomplete_or_error", fmt.Sprintf("Required saved %s marker %q lacks a complete error-free parent pass.", role, path)
			return l, nil
		}
		if m.Generation <= 0 || m.ParentGeneration <= 0 || m.Generation != m.ParentGeneration {
			l.Code, l.Message = "parent_unconfirmed", fmt.Sprintf("Required saved %s marker %q is not confirmed in its parent generation.", role, path)
			return l, nil
		}
		if kind == "directory" {
			err = tx.QueryRowContext(ctx, "SELECT generation,complete,CASE WHEN last_error='' THEN 0 ELSE 1 END,checked_at_ns FROM directories WHERE root_id=? AND path=?", rootID, []byte(path)).Scan(&m.ListingGeneration, &listingComplete, &listingError, &checked)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return GoCacheLayout{}, err
			}
			if errors.Is(err, sql.ErrNoRows) || listingComplete != 1 || listingError != 0 || m.ListingGeneration <= 0 {
				l.Code, l.Message = "listing_incomplete_or_error", fmt.Sprintf("Required saved %s directory %q lacks a complete error-free listing.", role, path)
				return l, nil
			}
			m.ListingComplete = true
			m.ListingCheckedAt = goCacheTime(checked)
		}
		m.Role, m.RelativePath, m.RelativePathBytes = role, path, []byte(path)
		m.ObservedAt, m.ModifiedAt = goCacheTime(observed), goCacheTime(modified)
		l.Markers = append(l.Markers, m)
		if i >= 0 {
			l.ConfirmedShards++
		}
	}
	l.Status, l.Code, l.Message = "layout_metadata_recognized", "saved_layout_matches", "Required saved layout markers and listing generations are confirmed; cache contents and management are unverified."
	return l, nil
}

func goCacheTime(ns int64) time.Time {
	if ns <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

func goCacheSelectFile(e goCacheEntry, rootID int64, root string, cutoff int64) (GoCacheFile, int) {
	if e.rootID != rootID {
		return GoCacheFile{}, goCacheDifferentRoot
	}
	path := string(e.path)
	expectedParent := filepath.Dir(path)
	if path == "." {
		expectedParent = ""
	}
	if !validRelative(e.path) || len(e.parent) > 4096 || string(e.parent) != expectedParent || len(filepath.Join(root, path)) > 4096 {
		return GoCacheFile{}, goCacheUnsupportedPath
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 66 || !goCacheLowerHex(parts[0]) || !goCacheLowerHex(parts[1][:64]) || parts[1][:2] != parts[0] || parts[1][64] != '-' || (parts[1][65] != 'a' && parts[1][65] != 'd') {
		return GoCacheFile{}, goCacheOutsideLayout
	}
	if e.kind == "directory" && parts[1][65] == 'd' {
		return GoCacheFile{}, goCacheExecutableDirectory
	}
	if e.kind != "file" {
		return GoCacheFile{}, goCacheWrongKind
	}
	if e.skipped != 0 {
		return GoCacheFile{}, goCacheSkipped
	}
	if e.parentComplete != 1 || e.parentError != 0 {
		return GoCacheFile{}, goCacheParentIncomplete
	}
	if e.generation <= 0 || e.parentGeneration <= 0 || e.generation != e.parentGeneration {
		return GoCacheFile{}, goCacheParentUnconfirmed
	}
	if e.modified <= 0 {
		return GoCacheFile{}, goCacheTimestampUnknown
	}
	if e.modified > cutoff {
		return GoCacheFile{}, goCacheAgeNotMet
	}
	abs := filepath.Join(root, path)
	f := GoCacheFile{ID: fmt.Sprintf("go-cache-file-v1:%d:%d", rootID, e.id), EntryID: e.id, RootID: rootID, Path: abs, PathBytes: []byte(abs), RelativePathBytes: append([]byte(nil), e.path...), EntryType: "action_index", Device: e.device, Inode: e.inode, CtimeNS: e.changed, Generation: e.generation, ParentGeneration: e.parentGeneration, ObservedAt: goCacheTime(e.observed), ModifiedAt: goCacheTime(e.modified), Classification: "review_required"}
	if parts[1][65] == 'd' {
		f.EntryType = "data"
	}
	if e.size >= 0 {
		n := e.size
		f.LogicalBytes = &n
	}
	if e.allocated >= 0 {
		n := e.allocated
		f.AllocatedBytes = &n
	}
	return f, goCacheSelected
}

func goCacheLowerHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func qualifyGoCacheIdentities(r *GoCacheReport) {
	type key struct{ device, inode string }
	groups := map[key][]int{}
	for i, f := range r.Files {
		if !sameSizeIdentityNumber(f.Device, false) || !sameSizeIdentityNumber(f.Inode, true) || f.CtimeNS <= 0 || f.Generation <= 0 {
			r.Files[i].IdentityStatus = "unknown"
			r.UnknownIdentities++
			continue
		}
		k := key{f.Device, f.Inode}
		groups[k] = append(groups[k], i)
	}
	for _, members := range groups {
		r.KnownObjects++
		status := "known"
		if len(members) > 1 {
			status = "alias"
			r.RepeatedSavedObjects += len(members) - 1
			first := r.Files[members[0]]
			for _, i := range members[1:] {
				next := r.Files[i]
				if first.CtimeNS != next.CtimeNS || !first.ModifiedAt.Equal(next.ModifiedAt) || !goCacheSameSize(first.LogicalBytes, next.LogicalBytes) || !goCacheSameSize(first.AllocatedBytes, next.AllocatedBytes) {
					status = "conflicting"
					r.ConflictingIdentities++
					break
				}
			}
		}
		for _, i := range members {
			r.Files[i].IdentityStatus = status
		}
	}
}

func goCacheSameSize(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
