package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"
)

var ErrReportCursor = errors.New("invalid report cursor")

type reportCursor struct {
	Size int64 `json:"size"`
	ID   int64 `json:"id"`
}

func decodeReportCursor(token string) (reportCursor, error) {
	var c reportCursor
	if token == "" {
		return c, nil
	}
	if len(token) > 256 {
		return c, ErrReportCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return c, ErrReportCursor
	}
	if err = json.Unmarshal(data, &c); err != nil || c.Size < 0 || c.ID <= 0 {
		return c, ErrReportCursor
	}
	return c, nil
}

type ReportFile struct {
	ID         int64     `json:"id"`
	RootID     int64     `json:"root_id"`
	Path       string    `json:"path"`
	PathBytes  []byte    `json:"path_bytes"`
	Size       int64     `json:"logical_bytes"`
	Allocated  int64     `json:"allocated_bytes"`
	ObservedAt time.Time `json:"observed_at"`
	ModifiedAt time.Time `json:"modified_at"`
	ParentPass string    `json:"parent_pass"`
	SkipReason string    `json:"skip_reason"`
}
type ReportRoot struct {
	ID              int64      `json:"id"`
	Path            string     `json:"path"`
	PathBytes       []byte     `json:"path_bytes"`
	LastRootPass    *time.Time `json:"last_root_directory_pass_at"`
	LastError       string     `json:"last_error"`
	PendingJobs     int64      `json:"pending_jobs"`
	RunningJobs     int64      `json:"running_jobs"`
	DirectoryErrors int64      `json:"directory_errors"`
}
type FileReport struct {
	Directory            *DirectoryReport `json:"directory,omitempty"`
	GeneratedAt          time.Time        `json:"generated_at"`
	Source               string           `json:"source"`
	CurrentStateVerified bool             `json:"current_state_verified"`
	Files                []ReportFile     `json:"files"`
	Roots                []ReportRoot     `json:"roots"`
	RootsTruncated       bool             `json:"roots_truncated"`
	NextCursor           string           `json:"next_cursor,omitempty"`
	Limit                int              `json:"limit"`
	Notes                []string         `json:"notes"`
}

// LargestFiles uses keyset pagination and short-lived read snapshots. No path
// on disk is opened. Cursor pages are live views, not a frozen export.
func (s *Store) LargestFiles(ctx context.Context, limit int, token string) (FileReport, error) {
	r := FileReport{GeneratedAt: time.Now().UTC(), Source: "saved_inventory", Limit: limit, Files: []ReportFile{}, Roots: []ReportRoot{}, Notes: []string{
		"Historical observations, not verified current files or deletion suggestions. Ancestor freshness is not verified.",
		"Logical and allocated bytes are not guaranteed reclaimable space; hardlinks, clones and snapshots can share storage.",
		"Root directory pass times cover direct children only. Empty queues do not prove whole-tree coverage or current availability.",
		"Each page reads a new snapshot; concurrent scans can change ordering between pages. Root diagnostics are limited to 100 enabled roots.",
	}}
	if limit < 1 || limit > 200 {
		return r, errors.New("report limit must be 1–200")
	}
	c, err := decodeReportCursor(token)
	if err != nil {
		return r, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	query := `SELECT e.id,e.root_id,r.path,e.path,e.size,e.allocated,e.observed_at_ns,e.mtime_ns,e.skip_reason,
 CASE WHEN d.generation IS NULL THEN 'unknown' WHEN d.last_error!='' THEN 'directory_error'
 WHEN e.generation!=d.generation THEN 'unconfirmed' WHEN d.complete=0 THEN 'partial' ELSE 'observed_in_completed_parent_pass' END
 FROM entries e JOIN roots r ON r.id=e.root_id
 LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.parent
 WHERE e.kind='file' AND r.enabled=1`
	args := []any{}
	if token != "" {
		query += ` AND (e.size,e.id)<(?,?)`
		args = append(args, c.Size, c.ID)
	}
	query += ` ORDER BY e.size DESC,e.id DESC LIMIT ?`
	args = append(args, limit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var f ReportFile
		var root, path []byte
		var observed, modified int64
		if err = rows.Scan(&f.ID, &f.RootID, &root, &path, &f.Size, &f.Allocated, &observed, &modified, &f.SkipReason, &f.ParentPass); err != nil {
			rows.Close()
			return r, err
		}
		f.Path = filepath.Join(string(root), string(path))
		f.PathBytes = []byte(f.Path)
		f.ObservedAt = time.Unix(0, observed).UTC()
		f.ModifiedAt = time.Unix(0, modified).UTC()
		r.Files = append(r.Files, f)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	if len(r.Files) > limit {
		r.Files = r.Files[:limit]
		last := r.Files[limit-1]
		data, _ := json.Marshal(reportCursor{last.Size, last.ID})
		r.NextCursor = base64.RawURLEncoding.EncodeToString(data)
	}
	rows, err = tx.QueryContext(ctx, `SELECT r.id,r.path,COALESCE(r.last_scan_ns,0),r.last_error,
 (SELECT count(*) FROM jobs j WHERE j.root_id=r.id AND j.status='pending'),
 (SELECT count(*) FROM jobs j WHERE j.root_id=r.id AND j.status='running'),
 (SELECT count(*) FROM directories d WHERE d.root_id=r.id AND d.last_error!='')
 FROM roots r WHERE enabled=1 ORDER BY r.id LIMIT 101`)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var root ReportRoot
		var stamp int64
		if err = rows.Scan(&root.ID, &root.PathBytes, &stamp, &root.LastError, &root.PendingJobs, &root.RunningJobs, &root.DirectoryErrors); err != nil {
			rows.Close()
			return r, err
		}
		root.Path = string(root.PathBytes)
		if stamp != 0 {
			t := time.Unix(0, stamp).UTC()
			root.LastRootPass = &t
		}
		r.Roots = append(r.Roots, root)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return r, err
	}
	if len(r.Roots) > 100 {
		r.Roots = r.Roots[:100]
		r.RootsTruncated = true
	}
	return r, tx.Commit()
}
