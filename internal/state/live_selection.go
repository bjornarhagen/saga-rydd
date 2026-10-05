package state

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
)

const LivePathDepthLimit = 256

// LiveTarget is evidence for metadata comparison, not a filesystem action.
type LiveTarget struct {
	Root      RootBinding
	Finding   Finding
	Binding   TargetBinding
	Ancestors []Entry // root through the target's parent; relative byte paths
}

// PrepareLiveSelection checks and captures historical ancestor identities in
// one read snapshot. The unchanged root revision binds these to the saved plan.
func (s *Store) PrepareLiveSelection(ctx context.Context, saved SelectionSnapshot) (SelectionCheck, []LiveTarget, error) {
	if len(saved.Targets) < 1 || len(saved.Targets) > PreviewTargetLimit || len(saved.Targets) != len(saved.Evidence.Findings) || len(saved.Roots) < 1 || len(saved.Roots) > len(saved.Targets) {
		return SelectionCheck{}, nil, ErrFindingSelection
	}
	if s.schema < 9 {
		return SelectionCheck{}, nil, ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionCheck{}, nil, err
	}
	defer tx.Rollback()
	check, err := s.checkSelection(ctx, saved, tx)
	if err != nil {
		return SelectionCheck{}, nil, err
	}
	if check.Status != "matches_saved_inventory" {
		return check, nil, tx.Commit()
	}
	var targets []LiveTarget
	for i, f := range saved.Evidence.Findings {
		target := LiveTarget{Finding: f, Binding: saved.Targets[i]}
		for _, root := range saved.Roots {
			if root.ID == f.RootID {
				target.Root = root
				break
			}
		}
		rel, err := filepath.Rel(string(target.Root.PathBytes), filepath.Dir(string(f.PathBytes)))
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			return SelectionCheck{}, nil, ErrDirectoryScope
		}
		paths := []string{"."}
		if rel != "." {
			parts := strings.Split(rel, "/")
			if len(parts)+1 > LivePathDepthLimit {
				check.add("unverifiable", "ancestor_limit", f.ID, "The saved path exceeds the live check's directory limit.")
				return check, nil, tx.Commit()
			}
			for j := range parts {
				paths = append(paths, filepath.Join(parts[:j+1]...))
			}
		}
		for _, path := range paths {
			e := Entry{Path: []byte(path)}
			err = tx.QueryRowContext(ctx, "SELECT kind,device,inode,mtime_ns,ctime_ns FROM entries WHERE root_id=? AND path=?", f.RootID, e.Path).Scan(&e.Kind, &e.Device, &e.Inode, &e.MtimeNS, &e.CtimeNS)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return SelectionCheck{}, nil, err
			}
			if err != nil || e.Kind != "directory" || e.Device == "" || e.Inode == "" || e.Inode == "0" || e.CtimeNS <= 0 {
				check.add("unverifiable", "ancestor_evidence_unknown", f.ID, "A saved ancestor lacks usable identity evidence. Scan and review a new selection.")
				return check, nil, tx.Commit()
			}
			target.Ancestors = append(target.Ancestors, e)
		}
		targets = append(targets, target)
	}
	return check, targets, tx.Commit()
}
