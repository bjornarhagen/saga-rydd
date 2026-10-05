package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Scalar progress only. Ancestor membership and identity deduplication live in
// bounded SQLite scratch batches, not an in-memory map of the entire subtree.
type scopeCoverage struct {
	Report  DirectoryReport
	Logical int64
	Stale   bool
	Partial bool
}

type coverageEntry struct {
	path, parent                                                              []byte
	kind, dev, ino, parentError, skip, dirError                               string
	generation, size, allocated, parentGeneration, compactGeneration, unknown int64
	observed, checked, compactLogical, compactFiles, compactSkipped           int64
	parentComplete, dirComplete                                               bool
}

func addCoverageCount(total *int, n int64) error {
	value := int64(*total)
	if err := addCompact(&value, n); err != nil {
		return err
	}
	*total = int(value)
	return nil
}

func (c *scopeCoverage) add(e coverageEntry, selected, excluded, confirmed bool) error {
	r := &c.Report
	if err := addCoverageCount(&r.EntriesExamined, 1); err != nil {
		return err
	}
	if excluded {
		return addCoverageCount(&r.ExcludedEntries, 1)
	}
	observed := time.Unix(0, e.observed).UTC()
	if r.OldestObservation == nil || observed.Before(*r.OldestObservation) {
		v := observed
		r.OldestObservation = &v
	}
	if r.NewestObservation == nil || observed.After(*r.NewestObservation) {
		v := observed
		r.NewestObservation = &v
	}
	if !selected {
		if !confirmed {
			if err := addCoverageCount(&r.UnconfirmedEntries, 1); err != nil {
				return err
			}
			c.Stale = true
		}
		c.Partial = c.Partial || !e.parentComplete || e.parentError != ""
	}
	if e.skip != "" {
		if err := addCoverageCount(&r.SkippedEntries, 1); err != nil {
			return err
		}
		c.Partial = true
	}
	if e.kind == "directory" {
		if !e.dirComplete {
			if err := addCoverageCount(&r.IncompleteDirectories, 1); err != nil {
				return err
			}
			c.Partial = true
		}
		if e.dirError != "" {
			if err := addCoverageCount(&r.DirectoryErrors, 1); err != nil {
				return err
			}
			c.Partial = true
		}
		c.Stale = c.Stale || (e.checked > 0 && e.observed > e.checked)
		if e.compactGeneration > 0 {
			for _, pair := range []struct {
				total *int
				n     int64
			}{{&r.CompactedDirectories, 1}, {&r.CompactedFiles, e.compactFiles}, {&r.FilePaths, e.compactFiles}, {&r.UnknownInodes, e.unknown}, {&r.SkippedEntries, e.compactSkipped}} {
				if err := addCoverageCount(pair.total, pair.n); err != nil {
					return err
				}
			}
			c.Partial = c.Partial || e.unknown > 0 || e.compactSkipped > 0
			if err := addCompact(&c.Logical, e.compactLogical); err != nil {
				return err
			}
			if e.checked > 0 {
				stamp := time.Unix(0, e.checked).UTC()
				if r.NewestObservation == nil || stamp.After(*r.NewestObservation) {
					r.NewestObservation = &stamp
				}
			}
		}
	}
	if e.kind == "file" {
		if err := addCoverageCount(&r.FilePaths, 1); err != nil {
			return err
		}
		if err := addCompact(&c.Logical, e.size); err != nil {
			return err
		}
		if e.dev == "" || e.ino == "" {
			if err := addCoverageCount(&r.UnknownInodes, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

// Called after the normal bounded ancestor checks, within the same read snapshot.
func cachedScopeReport(ctx context.Context, tx *sql.Tx, base DirectoryReport, relative string, stale, partial bool) (DirectoryReport, bool, error) {
	var raw []byte
	var allocated, repeated int64
	var unknown, conflict bool
	err := tx.QueryRowContext(ctx, `SELECT c.coverage,c.allocated,c.repeated,c.unknown,c.conflicting FROM allocation_cache c
 JOIN allocation_revisions v ON v.root_id=c.root_id AND v.revision=c.revision
 WHERE c.root_id=? AND c.path=? AND c.ready=1 AND length(c.coverage)>0`, base.RootID, []byte(relative)).Scan(&raw, &allocated, &repeated, &unknown, &conflict)
	if errors.Is(err, sql.ErrNoRows) {
		return base, false, nil
	}
	if err != nil {
		return base, false, err
	}
	var c scopeCoverage
	if err = json.Unmarshal(raw, &c); err != nil {
		return base, false, err
	}
	r := c.Report
	r.GeneratedAt, r.Source, r.Path, r.PathBytes, r.RootID, r.RootError = base.GeneratedAt, base.Source, base.Path, base.PathBytes, base.RootID, base.RootError
	r.Notes = base.Notes
	r.CoverageSource = "cached_reduction"
	r.AllocatedSizeSource = "unknown"
	r.RepeatedInodes = int(repeated)
	r.Notes = append(r.Notes, "Coverage and sizes use a completed scoped reduction matching the saved inventory revision. Entries were processed in resumable batches; no synchronous subtree walk was needed.")
	if r.ExcludedEntries > 0 {
		r.Notes = append(r.Notes, "Historical entries absent from successful completed ancestor listings, or below non-directory replacements, are excluded from sizes and file counts.")
	}
	if r.CompactedDirectories > 0 {
		r.Notes = append(r.Notes, "Generated-tree files use compact directory totals; individual filenames are not retained.")
	}
	if r.UnknownInodes > 0 {
		r.Notes = append(r.Notes, "Some file identities are unknown; ordinary file allocations count each path and may include duplicates. Missing compact identities leave allocated size unknown.")
	}
	switch {
	case r.EntriesExamined == 0:
		r.Status = "unknown"
	case stale || c.Stale || conflict:
		r.Status = "stale"
	case partial || c.Partial:
		r.Status = "partial"
	default:
		r.Status = "recorded_complete"
	}
	if r.EntriesExamined > 0 {
		r.LogicalBytes = &c.Logical
		if !unknown {
			r.AllocatedBytes = &allocated
			r.AllocatedSizeSource = "cached_reduction"
		}
	}
	return r, true, nil
}
