package inventory

import (
	"bytes"
	"context"
	"database/sql"
	"sort"
	"time"
)

const HashGroupsContract = "historical_full_sha256_groups_v1"

// HashGroupsReport compares completed observations in one finite saved
// selection. Observations need not be simultaneous and are not current-file
// verification. Unfinished work includes invalidated and reserved work.
type HashGroupsReport struct {
	StoreID                        string           `json:"store_id"`
	SelectionID                    string           `json:"selection_id,omitempty"`
	InventoryID                    string           `json:"inventory_id,omitempty"`
	Source                         string           `json:"source"`
	Contract                       string           `json:"contract"`
	HashContract                   string           `json:"hash_contract"`
	Scope                          string           `json:"scope"`
	BudgetScope                    string           `json:"budget_scope"`
	Budget                         *HashBudget      `json:"budget,omitempty"`
	ReadConsent                    *HashReadConsent `json:"read_consent,omitempty"`
	SelectedWork                   int              `json:"selected_work"`
	CompletedObservations          int              `json:"completed_observations"`
	UnfinishedWork                 int              `json:"unfinished_work"`
	UnmatchedCompletedObservations int              `json:"unmatched_completed_observations"`
	Groups                         []SavedHashGroup `json:"groups"`
	ProvenanceVerified             bool             `json:"provenance_verified"`
	ContentVerified                bool             `json:"content_verified"`
	CurrentStateVerified           bool             `json:"current_state_verified"`
	DuplicatesVerified             bool             `json:"duplicates_verified"`
	Executable                     bool             `json:"executable"`
	EstimatedReclaimableBytes      *int64           `json:"estimated_reclaimable_bytes"`
}

// SavedHashGroup contains at least two paths with equal saved size and full
// SHA-256. Identity counts describe saved device/inode records, not independent
// physical copies, current hardlinks or reclaimable space.
type SavedHashGroup struct {
	LogicalBytes               int64                  `json:"logical_bytes"`
	SHA256                     string                 `json:"sha256"`
	Members                    []SavedHashGroupMember `json:"members"`
	SavedIdentities            int                    `json:"saved_identities"`
	RepeatedSavedPaths         int                    `json:"repeated_saved_paths"`
	ConflictingSavedIdentities int                    `json:"conflicting_saved_identities"`
}

// Alias/conflict qualifiers compare all selected rows, including unfinished
// rows and completed observations outside this member's matching-hash group.
type SavedHashGroupMember struct {
	WorkID                string    `json:"work_id"`
	FileID                int64     `json:"file_id"`
	RootID                int64     `json:"root_id"`
	PathBytes             []byte    `json:"path_bytes"`
	CheckedAt             time.Time `json:"checked_at"`
	SavedDevice           string    `json:"saved_device"`
	SavedInode            string    `json:"saved_inode"`
	SavedChangedNS        int64     `json:"saved_ctime_ns"`
	SavedModifiedAt       time.Time `json:"saved_modified_at"`
	SavedAllocatedBytes   int64     `json:"saved_allocated_bytes"`
	RepeatedSavedIdentity bool      `json:"repeated_saved_identity"`
	SavedIdentityConflict bool      `json:"saved_identity_conflict"`
}

// Groups reads existing saved records only. It neither evaluates current
// read permission nor opens inventory or source files, and does not recover
// interrupted work. Selection, observations, identity qualifiers, budget and
// consent come from the same bounded SQLite read transaction.
func (s *HashStore) Groups(ctx context.Context) (HashGroupsReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashGroupsReport{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashGroupsReport{}, err
	}
	defer tx.Rollback()
	report, err := s.readHashGroups(ctx, tx)
	if err != nil {
		return HashGroupsReport{}, err
	}
	if err = tx.Commit(); err != nil {
		return HashGroupsReport{}, err
	}
	if err = ctx.Err(); err != nil {
		return HashGroupsReport{}, err
	}
	return report, nil
}

func (s *HashStore) readHashGroups(ctx context.Context, db hashQuery) (HashGroupsReport, error) {
	snapshot, record, err := s.readHashSnapshot(ctx, db)
	if err != nil {
		return HashGroupsReport{}, err
	}
	return groupSavedHashes(snapshot, record), nil
}

type savedHashIdentity struct{ device, inode string }
type savedHashGroupKey struct {
	size   int64
	digest string
}

// Inputs are the jointly validated snapshot and immutable selection returned
// by readHashSnapshot. This classifier accepts no caller-facing evidence.
func groupSavedHashes(snapshot HashSnapshot, record *hashSelectionRecord) HashGroupsReport {
	out := HashGroupsReport{StoreID: snapshot.StoreID, SelectionID: snapshot.SelectionID, InventoryID: snapshot.InventoryID,
		Source: "saved_hash_observations", Contract: HashGroupsContract, HashContract: FileHashContract,
		Scope: "whole_saved_selection", BudgetScope: "whole_saved_selection", Budget: snapshot.Budget, ReadConsent: snapshot.ReadConsent,
		SelectedWork: len(snapshot.Work), Groups: []SavedHashGroup{}}
	if record == nil {
		return out
	}
	counts := map[savedHashIdentity]int{}
	conflicts := map[savedHashIdentity]bool{}
	first := map[savedHashIdentity]int{}
	digests := map[savedHashIdentity]string{}
	for i, target := range record.Targets {
		file := target.File
		identity := savedHashIdentity{file.Device, file.Inode}
		counts[identity]++
		if j, exists := first[identity]; exists {
			other := record.Targets[j].File
			if other.Size != file.Size || other.Allocated != file.Allocated || other.ChangedNS != file.ChangedNS || !other.ModifiedAt.Equal(file.ModifiedAt) {
				conflicts[identity] = true
			}
		} else {
			first[identity] = i
		}
		if work := snapshot.Work[i]; work.Status == "complete" {
			if digest, exists := digests[identity]; exists && digest != work.SHA256 {
				conflicts[identity] = true
			}
			digests[identity] = work.SHA256
		}
	}
	groups := map[savedHashGroupKey][]SavedHashGroupMember{}
	for i, work := range snapshot.Work {
		if work.Status != "complete" {
			out.UnfinishedWork++
			continue
		}
		out.CompletedObservations++
		file := record.Targets[i].File
		identity := savedHashIdentity{file.Device, file.Inode}
		key := savedHashGroupKey{work.LogicalBytes, work.SHA256}
		groups[key] = append(groups[key], SavedHashGroupMember{WorkID: work.ID, FileID: work.FileID, RootID: file.RootID,
			PathBytes: bytes.Clone(work.PathBytes), CheckedAt: work.CheckedAt, SavedDevice: file.Device, SavedInode: file.Inode,
			SavedChangedNS: file.ChangedNS, SavedModifiedAt: file.ModifiedAt, SavedAllocatedBytes: file.Allocated,
			RepeatedSavedIdentity: counts[identity] > 1, SavedIdentityConflict: conflicts[identity]})
	}
	out.UnmatchedCompletedObservations = out.CompletedObservations
	for key, members := range groups {
		if len(members) < 2 {
			continue
		}
		group := SavedHashGroup{LogicalBytes: key.size, SHA256: key.digest, Members: members}
		identities := map[savedHashIdentity]bool{}
		for _, member := range members {
			identity := savedHashIdentity{member.SavedDevice, member.SavedInode}
			if !identities[identity] && conflicts[identity] {
				group.ConflictingSavedIdentities++
			}
			identities[identity] = true
		}
		group.SavedIdentities = len(identities)
		group.RepeatedSavedPaths = len(members) - group.SavedIdentities
		out.UnmatchedCompletedObservations -= len(members)
		out.Groups = append(out.Groups, group)
	}
	sort.Slice(out.Groups, func(i, j int) bool {
		if out.Groups[i].LogicalBytes != out.Groups[j].LogicalBytes {
			return out.Groups[i].LogicalBytes > out.Groups[j].LogicalBytes
		}
		return out.Groups[i].SHA256 < out.Groups[j].SHA256
	})
	return out
}
