package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"
)

const (
	FileSampleTargetLimit   = 20
	FileSampleEvidenceLimit = 1 << 20
)

var (
	ErrFileSampleSelection = errors.New("file sampling requires 1–20 distinct exact saved files and their inventory identity")
	ErrFileSampleEvidence  = errors.New("selected file evidence changed or is incomplete; review a new saved report")
	ErrFileSampleLimit     = errors.New("selected file evidence exceeds the bounded capture limits")
	ErrFileSampleSchema    = errors.New("file sampling evidence requires inventory schema 9")
)

// FileSampleTarget binds one saved file to its current inventory snapshot.
// It is historical evidence, never permission to read or modify a source file.
type FileSampleTarget struct {
	InventoryID string       `json:"inventory_id"`
	Root        RootBinding  `json:"root"`
	File        SameSizeFile `json:"file"`
	Ancestors   []Entry      `json:"ancestors"` // ordered root through parent, with relative byte paths
}

// PrepareFileSampleSelection compares the exact displayed file fields and
// inventory incarnation, then captures root/ancestor evidence in one snapshot.
// Root and ancestor stamps were not part of SameSizeFile: these are a newly
// captured baseline, not proof that they equal an earlier displayed baseline.
// PathBytes is authoritative; the lossy display Path is ignored. No source
// paths, configuration, migrations or writes are involved.
func (s *Store) PrepareFileSampleSelection(ctx context.Context, inventoryID string, expected []SameSizeFile) ([]FileSampleTarget, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !sameSizeInventoryID(inventoryID) || len(expected) < 1 || len(expected) > FileSampleTargetLimit {
		return nil, ErrFileSampleSelection
	}
	seen := make(map[int64]bool, len(expected))
	for _, f := range expected {
		if seen[f.ID] || !validFileSampleEvidence(f) {
			return nil, ErrFileSampleSelection
		}
		seen[f.ID] = true
	}
	if s.schema < 9 {
		return nil, ErrFileSampleSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result, err := s.prepareFileSampleSelection(ctx, inventoryID, expected, tx)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// Exact primary-key selection bounds source records before materializing text
// or blob values. Error/skip text is only classified, never returned.
const fileSampleRowQuery = `SELECT e.id,e.root_id,substr(CAST(e.path AS BLOB),1,4097),substr(CAST(e.parent AS BLOB),1,4097),
 e.size,e.allocated,e.observed_at_ns,e.mtime_ns,e.ctime_ns,e.generation,
 CASE WHEN length(CAST(e.device AS BLOB))<=20 THEN e.device ELSE '' END,
 CASE WHEN length(CAST(e.inode AS BLOB))<=20 THEN e.inode ELSE '' END,
 e.kind='file',e.skip_reason='',substr(CAST(r.path AS BLOB),1,4097),r.enabled,r.last_error='',
 CASE WHEN length(CAST(r.volume_id AS BLOB))<=67 THEN r.volume_id ELSE '' END,COALESCE(v.revision,0),
 COALESCE(d.generation,0),COALESCE(d.complete,0),COALESCE(d.checked_at_ns,0),COALESCE(d.last_error='',0),
 EXISTS(SELECT 1 FROM subtree_reconcile t WHERE t.root_id=e.root_id)
 OR EXISTS(SELECT 1 FROM subtree_retirement t WHERE t.root_id=e.root_id)
 OR EXISTS(SELECT 1 FROM compact_retirement t WHERE t.root_id=e.root_id)
 FROM entries e JOIN roots r ON r.id=e.root_id
 LEFT JOIN allocation_revisions v ON v.root_id=e.root_id
 LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.parent WHERE e.id=?`

const fileSampleAncestorQuery = `SELECT substr(CAST(e.parent AS BLOB),1,4097),e.kind='directory',e.skip_reason='',
 CASE WHEN length(CAST(e.device AS BLOB))<=20 THEN e.device ELSE '' END,
 CASE WHEN length(CAST(e.inode AS BLOB))<=20 THEN e.inode ELSE '' END,
 e.size,e.allocated,e.mtime_ns,e.ctime_ns,e.generation,e.observed_at_ns,
 COALESCE(d.generation,0),COALESCE(d.complete,0),COALESCE(d.checked_at_ns,0),COALESCE(d.last_error='',0),
 COALESCE(p.generation,0),COALESCE(p.complete,0),COALESCE(p.checked_at_ns,0),COALESCE(p.last_error='',0),
 EXISTS(SELECT 1 FROM compact_dirs c WHERE c.root_id=e.root_id AND c.path=e.path),
 EXISTS(SELECT 1 FROM jobs j WHERE j.root_id=e.root_id AND j.kind='inventory' AND j.path=e.path)
 FROM entries e LEFT JOIN directories d ON d.root_id=e.root_id AND d.path=e.path
 LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent WHERE e.root_id=? AND e.path=?`

func (s *Store) prepareFileSampleSelection(ctx context.Context, inventoryID string, expected []SameSizeFile, tx *sql.Tx) ([]FileSampleTarget, error) {
	var current string
	if err := tx.QueryRowContext(ctx, "SELECT substr(CAST(token AS BLOB),1,65) FROM inventory_identity WHERE singleton=1").Scan(&current); err != nil {
		return nil, err
	}
	if current != inventoryID {
		return nil, ErrFileSampleEvidence
	}
	result := make([]FileSampleTarget, 0, len(expected))
	used := 2
	// Siblings commonly share a long ancestor chain. Reuse rows already checked
	// in this snapshot, but charge and copy each target's returned evidence.
	type ancestorKey struct {
		root int64
		path string
	}
	ancestors := make(map[ancestorKey]Entry)
	for _, wanted := range expected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target := FileSampleTarget{InventoryID: inventoryID}
		f := &target.File
		var relative, parent []byte
		var observed, modified, parentGeneration, checked int64
		var regular, unskipped, enabled, rootOK, complete, listingOK, lifecycle bool
		err := tx.QueryRowContext(ctx, fileSampleRowQuery, wanted.ID).Scan(&f.ID, &f.RootID, &relative, &parent, &f.Size, &f.Allocated, &observed, &modified, &f.ChangedNS, &f.Generation,
			&f.Device, &f.Inode, &regular, &unskipped, &target.Root.PathBytes, &enabled, &rootOK, &target.Root.Fingerprint, &target.Root.Revision,
			&parentGeneration, &complete, &checked, &listingOK, &lifecycle)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFileSampleEvidence
		}
		if err != nil {
			return nil, err
		}
		target.Root.ID = f.RootID
		if !regular || !unskipped || !enabled || !rootOK || lifecycle || !canonicalPath(target.Root.PathBytes) || !validRelative(relative) || string(relative) == "." || !bytes.Equal(parent, []byte(filepath.Dir(string(relative)))) ||
			!strings.HasPrefix(target.Root.Fingerprint, "v1:") || !sameSizeInventoryID(strings.TrimPrefix(target.Root.Fingerprint, "v1:")) || target.Root.Revision <= 0 ||
			sameSizeGeneratedParent(target.Root.PathBytes, relative) || !complete || !listingOK || parentGeneration != f.Generation || checked <= 0 || observed > checked {
			return nil, ErrFileSampleEvidence
		}
		f.Path = filepath.Join(string(target.Root.PathBytes), string(relative))
		f.PathBytes = []byte(f.Path)
		f.ObservedAt, f.ModifiedAt = time.Unix(0, observed).UTC(), time.Unix(0, modified).UTC()
		f.ParentPass = "observed_in_completed_parent_pass"
		if !validFileSampleEvidence(*f) || !sameFileSampleEvidence(*f, wanted) {
			return nil, ErrFileSampleEvidence
		}
		depth := len(strings.Split(strings.TrimPrefix(filepath.Dir(f.Path), "/"), "/"))
		if filepath.Dir(f.Path) == "/" {
			depth = 0
		}
		if depth > LivePathDepthLimit {
			return nil, ErrFileSampleLimit
		}
		if err = addFileSampleEvidence(&used, struct {
			InventoryID string
			Root        RootBinding
			File        SameSizeFile
		}{inventoryID, target.Root, *f}); err != nil {
			return nil, err
		}
		paths := []string{"."}
		if string(parent) != "." {
			parts := strings.Split(string(parent), "/")
			for i := range parts {
				paths = append(paths, filepath.Join(parts[:i+1]...))
			}
		}
		for _, path := range paths {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			key := ancestorKey{f.RootID, path}
			e, cached := ancestors[key]
			e.Path = []byte(path)
			if !cached {
				e.Kind = "directory"
				var savedParent []byte
				var directory, unskipped, ownComplete, ownOK, parentComplete, parentOK, compact, job bool
				var generation, observed, ownGeneration, ownChecked, parentGeneration, parentChecked int64
				err = tx.QueryRowContext(ctx, fileSampleAncestorQuery, f.RootID, e.Path).Scan(&savedParent, &directory, &unskipped, &e.Device, &e.Inode, &e.Size, &e.Allocated, &e.MtimeNS, &e.CtimeNS, &generation, &observed,
					&ownGeneration, &ownComplete, &ownChecked, &ownOK, &parentGeneration, &parentComplete, &parentChecked, &parentOK, &compact, &job)
				if errors.Is(err, sql.ErrNoRows) {
					return nil, ErrFileSampleEvidence
				}
				if err != nil {
					return nil, err
				}
				parentMatches := path == "." && len(savedParent) == 0 && generation == ownGeneration
				if path != "." {
					parentMatches = bytes.Equal(savedParent, []byte(filepath.Dir(path))) && parentComplete && parentOK && parentChecked > 0 && observed <= parentChecked && generation == parentGeneration
				}
				if !directory || !unskipped || compact || job || !parentMatches || !ownComplete || !ownOK || ownGeneration <= 0 || ownChecked <= 0 || generation <= 0 || observed <= 0 || observed > ownChecked ||
					e.Size < 0 || e.Allocated < 0 || e.MtimeNS <= 0 || e.CtimeNS <= 0 || !sameSizeIdentityNumber(e.Device, false) || !sameSizeIdentityNumber(e.Inode, true) {
					return nil, ErrFileSampleEvidence
				}
				stored := e
				stored.Path = nil
				ancestors[key] = stored
			}
			if e.Device != f.Device {
				return nil, ErrFileSampleEvidence
			}
			if err = addFileSampleEvidence(&used, e); err != nil {
				return nil, err
			}
			target.Ancestors = append(target.Ancestors, e)
		}
		result = append(result, target)
	}
	return result, nil
}

func validFileSampleEvidence(f SameSizeFile) bool {
	return f.ID > 0 && f.RootID > 0 && canonicalPath(f.PathBytes) && f.Size > 0 && f.Allocated >= 0 && f.ChangedNS > 0 && f.Generation > 0 &&
		sameSizeIdentityNumber(f.Device, false) && sameSizeIdentityNumber(f.Inode, true) && f.SkipReason == "" && f.ParentPass == "observed_in_completed_parent_pass" &&
		f.ObservedAt.UnixNano() > 0 && time.Unix(0, f.ObservedAt.UnixNano()).Equal(f.ObservedAt) && f.ModifiedAt.UnixNano() > 0 && time.Unix(0, f.ModifiedAt.UnixNano()).Equal(f.ModifiedAt)
}

func sameFileSampleEvidence(a, b SameSizeFile) bool {
	return a.ID == b.ID && a.RootID == b.RootID && bytes.Equal(a.PathBytes, b.PathBytes) && a.Size == b.Size && a.Allocated == b.Allocated && a.ObservedAt.Equal(b.ObservedAt) && a.ModifiedAt.Equal(b.ModifiedAt) &&
		a.ParentPass == b.ParentPass && a.SkipReason == b.SkipReason && a.Device == b.Device && a.Inode == b.Inode && a.ChangedNS == b.ChangedNS && a.Generation == b.Generation
}

func addFileSampleEvidence(used *int, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// Include separators and container overhead as well as exact encoded fields.
	if len(encoded)+64 > FileSampleEvidenceLimit-*used {
		return ErrFileSampleLimit
	}
	*used += len(encoded) + 64
	return nil
}
