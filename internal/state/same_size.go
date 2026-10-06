package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const SameSizeMinimumBytes int64 = 1 << 20
const sameSizeCursorLimit = 512

var ErrSameSizeMinimum = errors.New("same-size minimum must be a positive number of bytes")
var ErrSameSizeLimit = errors.New("same-size report limit must be 1–200 saved file entries")
var ErrSameSizeSchema = errors.New("same-size reports require inventory schema 9; migrate configured state with state init, or finish a manual scan with scan -d PATH (compact inventories can use measure -d PATH)")

// SameSizeFile contains saved metadata only. The identity and parent pass do
// not prove current contents, continuous inode identity or ancestor freshness.
type SameSizeFile struct {
	ReportFile
	Device     string `json:"device"`
	Inode      string `json:"inode"`
	ChangedNS  int64  `json:"ctime_ns"`
	Generation int64  `json:"generation"`
}

// SameSizeBand is a page-local logical-size band, not a duplicate group.
// Object counts use only the displayed saved metadata and cannot be summed
// across pages. Continuation describes the raw size band, before exclusions.
type SameSizeBand struct {
	LogicalBytes          int64          `json:"logical_bytes"`
	Files                 []SameSizeFile `json:"files"`
	ContinuesBefore       bool           `json:"continues_before"`
	ContinuesAfter        bool           `json:"continues_after"`
	KnownObjects          int            `json:"known_objects"`
	RepeatedSavedObjects  int            `json:"repeated_saved_objects"`
	UnknownIdentities     int            `json:"unknown_identities"`
	ConflictingIdentities int            `json:"conflicting_identities"`
}

type SameSizeReport struct {
	GeneratedAt               time.Time             `json:"generated_at"`
	Source                    string                `json:"source"`
	InventoryID               string                `json:"inventory_id"`
	CurrentStateVerified      bool                  `json:"current_state_verified"`
	ContentVerified           bool                  `json:"content_verified"`
	EstimatedReclaimableBytes *int64                `json:"estimated_reclaimable_bytes"`
	MinimumBytes              int64                 `json:"minimum_bytes"`
	Limit                     int                   `json:"limit"`
	EntriesExamined           int                   `json:"entries_examined"`
	Bands                     []SameSizeBand        `json:"bands"`
	NextCursor                string                `json:"next_cursor,omitempty"`
	PageCoverage              string                `json:"page_coverage"`
	Diagnostics               []SelectionDiagnostic `json:"selection_diagnostics"`
	Notes                     []string              `json:"notes"`
}

type sameSizeCursor struct {
	Version      int    `json:"version"`
	InventoryID  string `json:"inventory_id"`
	MinimumBytes int64  `json:"minimum_bytes"`
	Size         int64  `json:"size"`
	ID           int64  `json:"id"`
}

func decodeSameSizeCursor(token string) (sameSizeCursor, error) {
	var c sameSizeCursor
	if token == "" {
		return c, nil
	}
	if len(token) > sameSizeCursorLimit {
		return c, ErrReportCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return c, ErrReportCursor
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil || d.Decode(new(any)) != io.EOF || c.Version != 1 ||
		!sameSizeInventoryID(c.InventoryID) || c.MinimumBytes <= 0 || c.Size < c.MinimumBytes || c.ID <= 0 {
		return sameSizeCursor{}, ErrReportCursor
	}
	// One encoding rejects duplicate/omitted keys, alternate number spellings,
	// padding and trailing bodies without retaining unbounded cursor state.
	canonical, _ := json.Marshal(c)
	if base64.RawURLEncoding.EncodeToString(canonical) != token {
		return sameSizeCursor{}, ErrReportCursor
	}
	return c, nil
}

func sameSizeInventoryID(id string) bool {
	if len(id) != 64 || strings.ToLower(id) != id {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

func sameSizeDiagnostics() []SelectionDiagnostic {
	return []SelectionDiagnostic{
		{Code: "disabled_root", Explanation: "File belongs to a disabled saved root."},
		{Code: "skipped", Explanation: "File has a saved skip reason."},
		{Code: "generated_dependency", Explanation: "File belongs to a generated node_modules tree."},
		{Code: "compact_parent", Explanation: "File has a saved compacted parent and is outside ordinary-file reporting."},
		{Code: "invalid_saved_path", Explanation: "Saved path is invalid or exceeds the 4096-byte path bound."},
		{Code: "eligible", Explanation: "Ordinary file passed this page's metadata filters; complete singleton size bands are omitted."},
	}
}

// The materialized selection fences the raw-row work bound before any root,
// skip, generated-tree or compact-parent filtering. Joins use indexed keys and
// can add no rows. Even an entirely excluded size band remains resumable.
const sameSizeQueryPrefix = `WITH selected AS MATERIALIZED (
 SELECT id,root_id,substr(CAST(path AS BLOB),1,4097) AS path,substr(CAST(parent AS BLOB),1,4097) AS parent,size,allocated,
 observed_at_ns,mtime_ns,ctime_ns,generation,
 CASE WHEN skip_reason='' THEN 0 ELSE 1 END AS skipped,
 CASE WHEN length(CAST(device AS BLOB))<=20 THEN device ELSE '' END AS device,
 CASE WHEN length(CAST(inode AS BLOB))<=20 THEN inode ELSE '' END AS inode
 FROM entries WHERE kind='file' AND `

const sameSizeQuerySuffix = `
)
SELECT e.id,e.root_id,e.path,substr(CAST(r.path AS BLOB),1,4097),r.enabled,e.size,e.allocated,
 e.observed_at_ns,e.mtime_ns,e.ctime_ns,e.generation,e.device,e.inode,e.skipped,
 CASE WHEN d.generation IS NULL THEN 'unknown' WHEN d.last_error!='' THEN 'directory_error'
 WHEN e.generation!=d.generation THEN 'unconfirmed' WHEN d.complete=0 THEN 'partial' ELSE 'observed_in_completed_parent_pass' END,
 CASE WHEN c.path IS NULL THEN 0 ELSE 1 END
 FROM selected e JOIN roots r ON r.id=e.root_id
 LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.parent
 LEFT JOIN compact_dirs c ON c.root_id=e.root_id AND c.path=e.parent
 ORDER BY e.size DESC,e.id DESC`

const sameSizeInitialQuery = sameSizeQueryPrefix + `size>=? ORDER BY size DESC,id DESC LIMIT ?` + sameSizeQuerySuffix
const sameSizeContinueQuery = sameSizeQueryPrefix + `size=? AND id<? ORDER BY id DESC LIMIT ?` + sameSizeQuerySuffix
const sameSizeLowerQuery = sameSizeQueryPrefix + `size>=? AND size<? ORDER BY size DESC,id DESC LIMIT ?` + sameSizeQuerySuffix

type sameSizeRawFile struct {
	file             SameSizeFile
	root, relative   []byte
	enabled, skipped int
	compacted        int
}

// SameSizeCandidates reads at most limit+1 saved regular-file rows above the
// minimum in one snapshot. It opens no source path and writes no saved state.
// The cursor is bound to the inventory incarnation and minimum, but each page
// is a new snapshot: scans may change ordering and band membership between pages.
func (s *Store) SameSizeCandidates(ctx context.Context, limit int, token string, minimumBytes int64) (SameSizeReport, error) {
	r := SameSizeReport{GeneratedAt: time.Now().UTC(), Source: "saved_inventory", MinimumBytes: minimumBytes, Limit: limit,
		Bands: []SameSizeBand{}, PageCoverage: "saved_entries_exhausted", Diagnostics: sameSizeDiagnostics(), Notes: []string{
			"Same-size files can have different contents. No file contents, samples or hashes have been read; this report cannot establish duplicates or authorize cleanup.",
			"Only ordinary saved regular files at or above the minimum are examined. Disabled roots, saved skips, node_modules trees and compacted parents are excluded. Exclusion diagnostics count one first-match outcome per examined entry.",
			"Each page uses one saved inventory snapshot. Scans can change sizes, ordering and membership between pages; the cursor binds the inventory incarnation and minimum, not a frozen export.",
			"Bands and object counts cover this page only. Continuation flags describe raw size bands before exclusions; a boundary singleton is retained because its size band crosses a page boundary. Complete singleton bands are omitted.",
			"Known objects use canonical saved device/inode numbers with positive ctime and scan generation. Repeated saved objects are additional paths to that saved identity. Differing saved ctime, modification time or allocation marks an identity conflict; scan generations alone do not identify inode incarnations.",
			"Saved parent-pass labels describe direct-parent observations only. Current availability, ancestor freshness and file contents have not been verified.",
			"Logical sizes, allocation and saved aliases do not establish storage you can free. Hardlinks, clones and snapshots can retain storage. Do not sum counts or sizes across pages.",
			"Exhausted saved entries do not prove scanning is complete or that no duplicates exist. Files below the minimum and complete singleton size bands are outside this report.",
		}}
	if limit < 1 || limit > 200 {
		return r, ErrSameSizeLimit
	}
	if minimumBytes <= 0 {
		return r, ErrSameSizeMinimum
	}
	if s.schema < 9 {
		return r, ErrSameSizeSchema
	}
	cursor, err := decodeSameSizeCursor(token)
	if err != nil {
		return r, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&r.InventoryID); err != nil {
		return r, err
	}
	if !sameSizeInventoryID(r.InventoryID) {
		return r, errors.New("saved inventory incarnation is invalid")
	}
	if token != "" && (cursor.InventoryID != r.InventoryID || cursor.MinimumBytes != minimumBytes) {
		return r, ErrReportCursor
	}
	var raw []sameSizeRawFile
	if token == "" {
		raw, err = readSameSizeRows(ctx, tx, sameSizeInitialQuery, minimumBytes, limit+1)
	} else {
		// A row-value (size,id) range may seek only size and walk all consumed
		// IDs again. Equality on size enables the existing index's rowid seek.
		raw, err = readSameSizeRows(ctx, tx, sameSizeContinueQuery, cursor.Size, cursor.ID, limit+1)
		if err == nil && len(raw) < limit+1 {
			var lower []sameSizeRawFile
			lower, err = readSameSizeRows(ctx, tx, sameSizeLowerQuery, minimumBytes, cursor.Size, limit+1-len(raw))
			raw = append(raw, lower...)
		}
	}
	if err != nil {
		return r, err
	}
	processed := raw
	if len(raw) > limit {
		processed = raw[:limit]
		last := processed[len(processed)-1].file
		payload, _ := json.Marshal(sameSizeCursor{Version: 1, InventoryID: r.InventoryID, MinimumBytes: minimumBytes, Size: last.Size, ID: last.ID})
		r.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
		r.PageCoverage = "more_saved_entries"
	}
	r.EntriesExamined = len(processed)
	var bands []SameSizeBand
	for _, entry := range processed {
		outcome := 5
		switch {
		case entry.enabled != 1:
			outcome = 0
		case entry.skipped != 0:
			outcome = 1
		case sameSizeGeneratedParent(entry.root, entry.relative):
			outcome = 2
		case entry.compacted != 0:
			outcome = 3
		case !canonicalPath(entry.root) || !validRelative(entry.relative) || string(entry.relative) == "." || len(entry.file.PathBytes) > 4096:
			outcome = 4
		}
		r.Diagnostics[outcome].Count++
		if outcome != 5 {
			continue
		}
		if len(bands) == 0 || bands[len(bands)-1].LogicalBytes != entry.file.Size {
			bands = append(bands, SameSizeBand{LogicalBytes: entry.file.Size, Files: []SameSizeFile{},
				ContinuesBefore: token != "" && cursor.Size == entry.file.Size,
				ContinuesAfter:  len(raw) > limit && raw[limit].file.Size == entry.file.Size})
		}
		last := &bands[len(bands)-1]
		last.Files = append(last.Files, entry.file)
	}
	for _, band := range bands {
		if len(band.Files) >= 2 || band.ContinuesBefore || band.ContinuesAfter {
			countSameSizeObjects(&band)
			r.Bands = append(r.Bands, band)
		}
	}
	return r, tx.Commit()
}

func readSameSizeRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]sameSizeRawFile, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []sameSizeRawFile
	for rows.Next() {
		var entry sameSizeRawFile
		var observed, modified int64
		f := &entry.file
		if err = rows.Scan(&f.ID, &f.RootID, &entry.relative, &entry.root, &entry.enabled, &f.Size, &f.Allocated,
			&observed, &modified, &f.ChangedNS, &f.Generation, &f.Device, &f.Inode, &entry.skipped, &f.ParentPass, &entry.compacted); err != nil {
			return nil, err
		}
		f.Path = filepath.Join(string(entry.root), string(entry.relative))
		f.PathBytes = []byte(f.Path)
		f.ObservedAt, f.ModifiedAt = time.Unix(0, observed).UTC(), time.Unix(0, modified).UTC()
		result = append(result, entry)
	}
	return result, rows.Err()
}

func countSameSizeObjects(band *SameSizeBand) {
	type identity struct{ device, inode string }
	type evidence struct {
		file     SameSizeFile
		conflict bool
	}
	seen := make(map[identity]evidence, len(band.Files))
	for _, file := range band.Files {
		if !sameSizeIdentityNumber(file.Device, false) || !sameSizeIdentityNumber(file.Inode, true) || file.ChangedNS <= 0 || file.Generation <= 0 {
			band.UnknownIdentities++
			continue
		}
		key := identity{file.Device, file.Inode}
		previous, exists := seen[key]
		if !exists {
			band.KnownObjects++
			seen[key] = evidence{file: file}
			continue
		}
		band.RepeatedSavedObjects++
		if previous.file.ChangedNS != file.ChangedNS || !previous.file.ModifiedAt.Equal(file.ModifiedAt) || previous.file.Allocated != file.Allocated {
			if !previous.conflict {
				band.ConflictingIdentities++
				previous.conflict = true
				seen[key] = previous
			}
		}
	}
}

func sameSizeIdentityNumber(value string, nonzero bool) bool {
	if len(value) == 0 || len(value) > 20 {
		return false
	}
	number, err := strconv.ParseUint(value, 10, 64)
	return err == nil && (!nonzero || number != 0) && strconv.FormatUint(number, 10) == value
}

func sameSizeGeneratedParent(root, relative []byte) bool {
	// All saved parent components matter, including a manual root already
	// inside a dependency tree. A regular file named node_modules is ordinary.
	parent := filepath.Join(string(root), filepath.Dir(string(relative)))
	for _, component := range strings.Split(parent, string(filepath.Separator)) {
		if component == "node_modules" {
			return true
		}
	}
	return false
}
