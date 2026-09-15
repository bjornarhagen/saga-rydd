package state

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"time"
)

const DirectoryEntryLimit = 10000

var ErrDirectoryScope = errors.New("directory must be inside an enabled saved root")

type DirectoryReport struct {
	GeneratedAt           time.Time  `json:"generated_at"`
	Source                string     `json:"source"`
	CurrentStateVerified  bool       `json:"current_state_verified"`
	Path                  string     `json:"path"`
	PathBytes             []byte     `json:"path_bytes"`
	RootID                int64      `json:"root_id"`
	Status                string     `json:"status"`
	UnknownReason         string     `json:"unknown_reason,omitempty"`
	LogicalBytes          *int64     `json:"logical_file_bytes"`
	AllocatedBytes        *int64     `json:"unique_inode_allocated_file_bytes"`
	FilePaths             int        `json:"file_paths"`
	RepeatedInodes        int        `json:"repeated_inodes"`
	UnknownInodes         int        `json:"unknown_inodes"`
	EntriesExamined       int        `json:"entries_examined"`
	EntryLimit            int        `json:"entry_limit"`
	Truncated             bool       `json:"truncated"`
	UnconfirmedEntries    int        `json:"unconfirmed_entries"`
	IncompleteDirectories int        `json:"incomplete_directories"`
	DirectoryErrors       int        `json:"directory_errors"`
	SkippedEntries        int        `json:"skipped_entries"`
	OldestObservation     *time.Time `json:"oldest_observation"`
	NewestObservation     *time.Time `json:"newest_observation"`
	RootError             string     `json:"root_error"`
	Notes                 []string   `json:"notes"`
}

// MeasureDirectory measures at most DirectoryEntryLimit stored entries in one
// read snapshot. No filesystem calls or unbounded recursive CTEs are used.
func (s *Store) MeasureDirectory(ctx context.Context, path string) (DirectoryReport, error) {
	r := DirectoryReport{GeneratedAt: time.Now().UTC(), Source: "saved_inventory", Path: path, PathBytes: []byte(path), Status: "unknown", EntryLimit: DirectoryEntryLimit, Notes: []string{
		"Saved observations only; current filesystem state is not verified. Recorded completeness is not proof of current contents.",
		"Logical bytes sum regular-file paths; allocated file bytes count each known device/inode once within the measured portion. Directory metadata, symlinks and other objects are excluded.",
		"Hardlinks outside this folder, clones and snapshots can retain storage. These are not reclaimable-space estimates; overlapping folder reports must not be added together.",
		"Stale or partial inventories can overestimate or underestimate current size. A truncated measurement is only a portion of the saved subtree.",
	}}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || len(path) > 4096 {
		return r, ErrDirectoryScope
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	var root []byte
	err = tx.QueryRowContext(ctx, `SELECT id,path,last_error FROM roots WHERE enabled=1 AND
 (path=? OR (substr(?,1,length(path))=path AND (substr(?,length(path)+1,1)=X'2f' OR path=X'2f')))
 ORDER BY length(path) DESC LIMIT 1`, []byte(path), []byte(path), []byte(path)).Scan(&r.RootID, &root, &r.RootError)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrDirectoryScope
	}
	if err != nil {
		return r, err
	}
	relative, err := filepath.Rel(string(root), path)
	if err != nil {
		return r, err
	}
	stale, partial := r.RootError != "", false
	var logicalBytes, allocatedBytes int64
	reasonSeen := map[string]bool{}
	reason := func(message string) {
		if !reasonSeen[message] {
			r.Notes = append(r.Notes, message)
			reasonSeen[message] = true
		}
	}
	// Validate saved ancestor links, including a selected directory whose parent
	// was re-enumerated without it. Bound this independently of subtree size.
	ancestor := relative
	for depth := 0; ; depth++ {
		if depth >= 256 {
			partial = true
			reason("Ancestor validation reached its 256-directory limit.")
			break
		}
		var kind, skip, parentError string
		var generation, parentGeneration, observed, checked int64
		var complete int
		err = tx.QueryRowContext(ctx, `SELECT e.kind,e.skip_reason,e.generation,COALESCE(d.generation,0),COALESCE(d.complete,0),COALESCE(d.last_error,''),e.observed_at_ns,COALESCE(own.checked_at_ns,0)
 FROM entries e LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.parent
 LEFT JOIN directories own ON own.root_id=e.root_id AND own.path=e.path
 WHERE e.root_id=? AND e.path=?`, r.RootID, []byte(ancestor)).Scan(&kind, &skip, &generation, &parentGeneration, &complete, &parentError, &observed, &checked)
		if errors.Is(err, sql.ErrNoRows) {
			r.UnknownReason = "selected directory or ancestor has no saved observation"
			return r, tx.Commit()
		}
		if err != nil {
			return r, err
		}
		if kind != "directory" {
			r.UnknownReason = "selected path or ancestor was not recorded as a directory"
			return r, tx.Commit()
		}
		if skip != "" || parentError != "" {
			partial = true
		}
		if checked == 0 {
			partial = true
			reason("An ancestor or selected directory has no saved listing timestamp.")
		}
		if checked > 0 && observed > checked {
			stale = true
			reason("An ancestor or selected directory was observed after its saved listing.")
		}
		if skip != "" || parentError != "" {
			reason("An ancestor or selected directory is skipped or has a recorded listing error.")
		}
		if ancestor == "." {
			break
		}
		if parentGeneration == 0 || complete == 0 {
			partial = true
			reason("A saved ancestor listing is incomplete or unknown.")
		}
		if generation != parentGeneration {
			stale = true
			reason("A directory is unconfirmed in its saved ancestor listing.")
		}
		ancestor = filepath.Dir(ancestor)
	}
	query := `SELECT e.path,e.parent,e.kind,e.size,e.allocated,e.device,e.inode,e.generation,e.observed_at_ns,e.skip_reason,
 COALESCE(p.generation,0),COALESCE(p.complete,0),COALESCE(p.last_error,''),
 COALESCE(d.complete,0),COALESCE(d.checked_at_ns,0),COALESCE(d.last_error,'')
 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent
 LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.path WHERE e.root_id=?`
	args := []any{r.RootID}
	if relative != "." {
		prefix := []byte(relative + "/")
		upper := []byte(relative + "0")
		query += ` AND (e.path=? OR (e.path>=? AND e.path<?))`
		args = append(args, []byte(relative), prefix, upper)
	}
	query += ` ORDER BY e.path LIMIT ?`
	args = append(args, DirectoryEntryLimit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return r, err
	}
	defer rows.Close()
	type inodeKey struct{ dev, ino string }
	type inodeSize struct{ allocated, size int64 }
	inodes := map[inodeKey]inodeSize{}
	validDirectories := map[string]bool{}
	add := func(total *int64, n int64) error {
		if n < 0 || *total > math.MaxInt64-n {
			return errors.New("directory byte total exceeds supported range")
		}
		*total += n
		return nil
	}
	for rows.Next() {
		if r.EntriesExamined == DirectoryEntryLimit {
			r.Truncated = true
			partial = true
			break
		}
		var p, parent []byte
		var kind, dev, ino, skip, parentError, dirError string
		var size, allocated, generation, observed, parentGeneration, checked int64
		var parentComplete, dirComplete int
		if err = rows.Scan(&p, &parent, &kind, &size, &allocated, &dev, &ino, &generation, &observed, &skip, &parentGeneration, &parentComplete, &parentError, &dirComplete, &checked, &dirError); err != nil {
			return r, err
		}
		r.EntriesExamined++
		observedAt := time.Unix(0, observed).UTC()
		if r.OldestObservation == nil || observedAt.Before(*r.OldestObservation) {
			v := observedAt
			r.OldestObservation = &v
		}
		if r.NewestObservation == nil || observedAt.After(*r.NewestObservation) {
			v := observedAt
			r.NewestObservation = &v
		}
		confirmed := true
		if string(p) != relative {
			// Lexicographic path order visits ancestors before their descendants.
			if !validDirectories[string(parent)] || generation != parentGeneration {
				confirmed = false
				r.UnconfirmedEntries++
				stale = true
			}
			if parentComplete == 0 || parentError != "" {
				partial = true
			}
		}
		if skip != "" {
			r.SkippedEntries++
			partial = true
		}
		if kind == "directory" {
			validDirectories[string(p)] = confirmed && skip == ""
			if dirComplete == 0 {
				r.IncompleteDirectories++
				partial = true
			}
			if dirError != "" {
				r.DirectoryErrors++
				partial = true
			}
			if checked > 0 && observed > checked {
				stale = true
			}
		}
		if kind != "file" {
			continue
		}
		r.FilePaths++
		if err = add(&logicalBytes, size); err != nil {
			return r, err
		}
		if dev == "" || ino == "" {
			r.UnknownInodes++
			if err = add(&allocatedBytes, allocated); err != nil {
				return r, err
			}
			continue
		}
		key := inodeKey{dev, ino}
		prior, exists := inodes[key]
		if exists {
			r.RepeatedInodes++
			if prior.allocated != allocated || prior.size != size {
				stale = true
			}
		}
		if !exists || allocated > prior.allocated {
			if err = add(&allocatedBytes, allocated-prior.allocated); err != nil {
				return r, err
			}
			inodes[key] = inodeSize{allocated, size}
		}
	}
	if err = rows.Err(); err != nil {
		return r, err
	}
	rows.Close()
	switch {
	case r.EntriesExamined == 0:
		r.Status = "unknown"
	case stale:
		r.Status = "stale"
	case partial:
		r.Status = "partial"
	default:
		r.Status = "recorded_complete"
	}
	if r.UnknownInodes > 0 {
		reason("Some file identities are unknown; their allocated bytes are counted per path and may include duplicates.")
	}
	if r.EntriesExamined > 0 {
		r.LogicalBytes = &logicalBytes
		r.AllocatedBytes = &allocatedBytes
	}
	return r, tx.Commit()
}
