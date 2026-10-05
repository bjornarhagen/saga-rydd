package state

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

type SelectionIssue struct {
	Code      string `json:"code"`
	FindingID string `json:"finding_id,omitempty"`
	Message   string `json:"message"`
}

// SelectionCheck describes saved observations only. It is never an approval or
// a reusable action-time check, even when every recorded field agrees.
type SelectionCheck struct {
	Status               string           `json:"status"`
	Source               string           `json:"source"`
	CurrentStateVerified bool             `json:"current_state_verified"`
	ApprovalAvailable    bool             `json:"approval_available"`
	Executable           bool             `json:"executable"`
	SelectedTargets      int              `json:"selected_targets"`
	Issues               []SelectionIssue `json:"issues"`
}

func (s *Store) CheckSelection(ctx context.Context, saved SelectionSnapshot) (SelectionCheck, error) {
	if len(saved.Evidence.Findings) < 1 || len(saved.Evidence.Findings) > PreviewTargetLimit || len(saved.Targets) != len(saved.Evidence.Findings) || len(saved.Roots) < 1 || len(saved.Roots) > len(saved.Targets) {
		return SelectionCheck{}, ErrFindingSelection
	}
	if s.schema < 9 {
		return SelectionCheck{}, ErrPlanSchema
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionCheck{}, err
	}
	defer tx.Rollback()
	r, err := s.checkSelection(ctx, saved, tx)
	if err != nil {
		return SelectionCheck{}, err
	}
	return r, tx.Commit()
}

func (s *Store) checkSelection(ctx context.Context, saved SelectionSnapshot, tx *sql.Tx) (SelectionCheck, error) {
	r := SelectionCheck{Status: "matches_saved_inventory", Source: "saved_inventory", SelectedTargets: len(saved.Targets), Issues: []SelectionIssue{}}
	var identity string
	if err := tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&identity); err != nil {
		return SelectionCheck{}, err
	}
	// Check incarnation before resolving any reused numeric finding IDs.
	if identity != saved.InventoryID {
		r.add("changed", "different_inventory", "", "This inventory is not the one used to save the selection. Use the original inventory, or save a new selection for review.")
		return r, nil
	}
	ids := make([]string, len(saved.Targets))
	for i, target := range saved.Targets {
		ids[i] = target.FindingID
	}
	current, err := s.snapshotSelection(ctx, ids, saved.Evidence.MinimumAgeDays, tx)
	if errors.Is(err, ErrFindingSelection) {
		r.add("changed", "selection_unavailable", "", "One or more selected findings are missing, disabled or no longer eligible. Review a new candidate report.")
		return r, nil
	}
	if err != nil {
		return SelectionCheck{}, err
	}
	if !reflect.DeepEqual(saved.Roots, current.Roots) {
		r.add("changed", "root_evidence_changed", "", "A saved root path, identity or inventory revision changed. Review and save a new selection.")
	}
	for _, root := range append(append([]RootBinding{}, saved.Roots...), current.Roots...) {
		if root.Fingerprint == "" || root.Revision <= 0 || !canonicalPath(root.PathBytes) {
			r.add("unverifiable", "root_evidence_unknown", "", "A root has missing identity or revision evidence. Scan the selected folder and review a new selection.")
			break
		}
	}
	for i, target := range saved.Targets {
		old, now := saved.Evidence.Findings[i], current.Evidence.Findings[i]
		if target != current.Targets[i] {
			r.add("changed", "target_identity_changed", target.FindingID, "The saved identity or scan generation for the dependency directory or package.json changed. Review and save a new selection.")
		}
		if !sameFindingEvidence(old, now) {
			r.add("changed", "finding_evidence_changed", target.FindingID, "Recorded paths, timestamps or measurements changed. Review and save a new selection.")
		}
		if !knownTarget(target) || !knownTarget(current.Targets[i]) || !knownFinding(old, saved.Roots) || !knownFinding(now, current.Roots) {
			r.add("unverifiable", "target_evidence_unknown", target.FindingID, "Identity or measurement evidence is incomplete, stale or unknown. Finish scanning and review a new selection; exclusions and unsupported files may keep evidence incomplete.")
		}
	}
	return r, nil
}

func (r *SelectionCheck) add(status, code, id, message string) {
	// A definite change takes precedence over additional unknown evidence.
	if r.Status != "changed" {
		r.Status = status
	}
	r.Issues = append(r.Issues, SelectionIssue{Code: code, FindingID: id, Message: message})
}

func canonicalPath(path []byte) bool {
	p := string(path)
	return len(p) <= 4096 && !strings.ContainsRune(p, 0) && filepath.IsAbs(p) && filepath.Clean(p) == p
}

func knownTarget(t TargetBinding) bool {
	for _, e := range []EntryBinding{t.Target, t.Manifest} {
		if e.Device == "" || e.Inode == "" || e.Inode == "0" || e.ChangedNS <= 0 || e.Generation <= 0 {
			return false
		}
	}
	return true
}

func knownFinding(f Finding, roots []RootBinding) bool {
	m := f.Measurement
	if !canonicalPath(f.PathBytes) || !canonicalPath(f.ManifestPathBytes) || filepath.Base(string(f.PathBytes)) != "node_modules" || string(f.ManifestPathBytes) != filepath.Join(filepath.Dir(string(f.PathBytes)), "package.json") || !bytes.Equal(m.PathBytes, f.PathBytes) || m.RootID != f.RootID {
		return false
	}
	inside := false
	for _, root := range roots {
		if root.ID == f.RootID {
			rel, err := filepath.Rel(string(root.PathBytes), string(f.PathBytes))
			inside = err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
		}
	}
	return inside && f.Rule == "node_modules_old_metadata" && f.RuleVersion == 1 &&
		f.DirectoryModifiedAt.UnixNano() > 0 && f.ManifestModifiedAt.UnixNano() > 0 && f.DirectoryObservedAt.UnixNano() > 0 && f.ManifestObservedAt.UnixNano() > 0 &&
		m.Status == "recorded_complete" && m.LogicalBytes != nil && *m.LogicalBytes >= 0 && m.AllocatedBytes != nil && *m.AllocatedBytes >= 0 &&
		!m.Truncated && m.UnknownInodes == 0 && m.UnconfirmedEntries == 0 && m.IncompleteDirectories == 0 && m.DirectoryErrors == 0 && m.SkippedEntries == 0 && m.RootError == "" && m.UnknownReason == ""
}

func sameFindingEvidence(a, b Finding) bool {
	// Generation time, explanatory wording and JSON's lossy display strings do
	// not identify objects. Authoritative byte paths and observation times do.
	for _, f := range []*Finding{&a, &b} {
		f.Path, f.ManifestPath, f.Measurement.Path = "", "", ""
		f.Measurement.GeneratedAt = time.Time{}
		f.Measurement.Notes = nil
	}
	return reflect.DeepEqual(a, b)
}
