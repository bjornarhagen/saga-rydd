package state

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
)

var ErrPlanSchema = errors.New("saving or checking a plan requires inventory schema 9; migrate configured state with state init, or finish a manual scan with scan -d PATH (compact inventories can use measure -d PATH)")

type RootBinding struct {
	ID          int64  `json:"root_id"`
	PathBytes   []byte `json:"path_bytes"`
	Fingerprint string `json:"saved_fingerprint"`
	Revision    int64  `json:"saved_revision"`
}

type EntryBinding struct {
	Device     string `json:"device"`
	Inode      string `json:"inode"`
	ChangedNS  int64  `json:"ctime_ns"`
	Generation int64  `json:"generation"`
}

type TargetBinding struct {
	FindingID string       `json:"finding_id"`
	Target    EntryBinding `json:"target"`
	Manifest  EntryBinding `json:"manifest"`
}

// SelectionSnapshot is historical evidence, never live action authorization.
// PathBytes in the evidence and roots are authoritative for arbitrary Unix names.
type SelectionSnapshot struct {
	InventoryID string          `json:"inventory_id"`
	Roots       []RootBinding   `json:"roots"`
	Targets     []TargetBinding `json:"targets"`
	Evidence    FindingReport   `json:"evidence"`
}

func (s *Store) SnapshotSelection(ctx context.Context, ids []string, minimumAgeDays int) (SelectionSnapshot, error) {
	var r SelectionSnapshot
	if s.schema < 9 {
		return r, ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer tx.Rollback()
	r, err = s.snapshotSelection(ctx, ids, minimumAgeDays, tx)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return SelectionSnapshot{}, err
	}
	return r, nil
}

func (s *Store) snapshotSelection(ctx context.Context, ids []string, minimumAgeDays int, tx *sql.Tx) (SelectionSnapshot, error) {
	var r SelectionSnapshot
	var err error
	if err = tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&r.InventoryID); err != nil {
		return SelectionSnapshot{}, err
	}
	r.Evidence, err = s.previewFindings(ctx, ids, minimumAgeDays, tx)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	seen := map[int64][]byte{}
	for _, f := range r.Evidence.Findings {
		rootPath, ok := seen[f.RootID]
		if !ok {
			root := RootBinding{ID: f.RootID}
			err = tx.QueryRowContext(ctx, `SELECT r.path,r.volume_id,COALESCE(v.revision,0)
 FROM roots r LEFT JOIN allocation_revisions v ON v.root_id=r.id WHERE r.id=? AND r.enabled=1`, f.RootID).Scan(&root.PathBytes, &root.Fingerprint, &root.Revision)
			if err != nil {
				return SelectionSnapshot{}, err
			}
			r.Roots = append(r.Roots, root)
			rootPath = root.PathBytes
			seen[f.RootID] = rootPath
		}
		binding := TargetBinding{FindingID: f.ID}
		manifest, err := filepath.Rel(string(rootPath), string(f.ManifestPathBytes))
		if err != nil {
			return SelectionSnapshot{}, err
		}
		err = tx.QueryRowContext(ctx, `SELECT e.device,e.inode,e.ctime_ns,e.generation,m.device,m.inode,m.ctime_ns,m.generation
 FROM entries e JOIN entries m ON m.root_id=e.root_id AND m.path=? WHERE e.root_id=? AND e.id=?`, []byte(manifest), f.RootID, f.EntryID).Scan(
			&binding.Target.Device, &binding.Target.Inode, &binding.Target.ChangedNS, &binding.Target.Generation,
			&binding.Manifest.Device, &binding.Manifest.Inode, &binding.Manifest.ChangedNS, &binding.Manifest.Generation)
		if err != nil {
			return SelectionSnapshot{}, err
		}
		r.Targets = append(r.Targets, binding)
	}
	return r, nil
}
