package state

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const PreviewTargetLimit = 20

var ErrFindingSelection = errors.New("select 1–20 unique current node-modules finding IDs; missing or ineligible selections require a new candidate report")

type findingReference struct{ root, entry int64 }

// PreviewFindings selects exact local references without scanning all candidate
// pages. It never grants approval or treats an inventory ID as durable identity.
func (s *Store) PreviewFindings(ctx context.Context, ids []string, minimumAgeDays int) (FindingReport, error) {
	if len(ids) < 1 || len(ids) > PreviewTargetLimit {
		return FindingReport{}, ErrFindingSelection
	}
	refs := make([]findingReference, 0, len(ids))
	seen := map[string]bool{}
	for _, id := range ids {
		parts := strings.Split(id, ":")
		if len(parts) != 3 || parts[0] != "node-modules-v1" || seen[id] {
			return FindingReport{}, ErrFindingSelection
		}
		root, e1 := strconv.ParseInt(parts[1], 10, 64)
		entry, e2 := strconv.ParseInt(parts[2], 10, 64)
		if e1 != nil || e2 != nil || root <= 0 || entry <= 0 || id != fmt.Sprintf("node-modules-v1:%d:%d", root, entry) {
			return FindingReport{}, ErrFindingSelection
		}
		refs = append(refs, findingReference{root, entry})
		seen[id] = true
	}
	r, err := s.nodeModulesFindings(ctx, "", minimumAgeDays, refs)
	if err != nil {
		return FindingReport{}, err
	}
	if len(r.Findings) != len(ids) {
		return FindingReport{}, ErrFindingSelection
	}
	return r, nil
}
