package inventory

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const HashKeeperPreviewContract = "historical_keeper_preview_v1"

var ErrHashKeeperRequest = errors.New("keeper preview requires one canonical work ID and 1–19 distinct canonical copy work IDs, without keeper overlap")
var ErrHashKeeperSelection = errors.New("requested keeper and copies need complete matching observations in the exact saved selection")
var ErrHashKeeperIdentity = errors.New("requested saved identities repeat or conflict across the saved selection")

// SavedHashPreviewMember binds an explicitly requested role to a completed
// historical observation. It makes no current-content or independence claim.
type SavedHashPreviewMember struct {
	SavedHashGroupMember
	Sequence int64 `json:"observation_sequence"`
}

// HashKeeperPreview is an ephemeral preview of owner-specified roles. It is
// not a saved plan, read approval or cleanup authority. Coverage and budget
// describe the whole saved selection; Keeper and Copies contain only the
// explicitly requested subset. Observations need not be simultaneous.
type HashKeeperPreview struct {
	Source                         string                   `json:"source"`
	Contract                       string                   `json:"contract"`
	HashContract                   string                   `json:"hash_contract"`
	StoreID                        string                   `json:"store_id"`
	SelectionID                    string                   `json:"selection_id"`
	InventoryID                    string                   `json:"inventory_id"`
	Scope                          string                   `json:"scope"`
	BudgetScope                    string                   `json:"budget_scope"`
	Budget                         *HashBudget              `json:"budget,omitempty"`
	ReadConsent                    *HashReadConsent         `json:"read_consent,omitempty"`
	SelectedWork                   int                      `json:"selected_work"`
	CompletedObservations          int                      `json:"completed_observations"`
	UnfinishedWork                 int                      `json:"unfinished_work"`
	UnmatchedCompletedObservations int                      `json:"unmatched_completed_observations"`
	LogicalBytes                   int64                    `json:"logical_bytes"`
	SHA256                         string                   `json:"sha256"`
	Keeper                         SavedHashPreviewMember   `json:"keeper"`
	Copies                         []SavedHashPreviewMember `json:"copies"`
	ApprovalAvailable              bool                     `json:"approval_available"`
	ProvenanceVerified             bool                     `json:"provenance_verified"`
	ContentVerified                bool                     `json:"content_verified"`
	CurrentStateVerified           bool                     `json:"current_state_verified"`
	DuplicatesVerified             bool                     `json:"duplicates_verified"`
	Executable                     bool                     `json:"executable"`
	EstimatedReclaimableBytes      *int64                   `json:"estimated_reclaimable_bytes"`
}

func canonicalHashPreviewWorkID(id string) bool {
	if len(id) < 1 || len(id) > 2 {
		return false
	}
	ordinal, err := strconv.Atoi(id)
	return err == nil && ordinal >= 1 && ordinal <= FileSampleTargetLimit && strconv.Itoa(ordinal) == id
}

func validateHashKeeperRequest(selectionID, keeperID string, copies []string) error {
	if !hashStoreDigest(selectionID) {
		return ErrHashSelectionID
	}
	if !canonicalHashPreviewWorkID(keeperID) || len(copies) < 1 || len(copies) >= FileSampleTargetLimit {
		return ErrHashKeeperRequest
	}
	seen := map[string]bool{keeperID: true}
	for _, id := range copies {
		if !canonicalHashPreviewWorkID(id) || seen[id] {
			return ErrHashKeeperRequest
		}
		seen[id] = true
	}
	return nil
}

// PreviewKeeper reads existing saved observations only. It does not choose or
// add paths, evaluate current permission, open source/inventory/configuration,
// recover interrupted work or persist role choices. Known saved aliases and
// conflicts are refused even when their other paths are outside the group.
func (s *HashStore) PreviewKeeper(ctx context.Context, selectionID, keeperWorkID string, copyWorkIDs []string) (HashKeeperPreview, error) {
	if len(copyWorkIDs) < 1 || len(copyWorkIDs) >= FileSampleTargetLimit {
		return HashKeeperPreview{}, ErrHashKeeperRequest
	}
	copies := append([]string(nil), copyWorkIDs...)
	if err := validateHashKeeperRequest(selectionID, keeperWorkID, copies); err != nil {
		return HashKeeperPreview{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashKeeperPreview{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashKeeperPreview{}, err
	}
	defer tx.Rollback()
	preview, err := s.readHashKeeperPreview(ctx, tx, selectionID, keeperWorkID, copies)
	if err != nil {
		return HashKeeperPreview{}, err
	}
	if err = tx.Commit(); err != nil {
		return HashKeeperPreview{}, err
	}
	if err = ctx.Err(); err != nil {
		return HashKeeperPreview{}, err
	}
	return preview, nil
}

func (s *HashStore) readHashKeeperPreview(ctx context.Context, db hashQuery, selectionID, keeperID string, copies []string) (HashKeeperPreview, error) {
	snapshot, record, err := s.readHashSnapshot(ctx, db)
	if err != nil {
		return HashKeeperPreview{}, err
	}
	return previewSavedHashKeeper(snapshot, record, selectionID, keeperID, copies)
}

// Inputs are the jointly validated saved snapshot and immutable selection.
// This private classifier is not an arbitrary-evidence submission API.
func previewSavedHashKeeper(snapshot HashSnapshot, record *hashSelectionRecord, selectionID, keeperID string, copies []string) (HashKeeperPreview, error) {
	if record == nil || snapshot.SelectionID != selectionID {
		return HashKeeperPreview{}, fmt.Errorf("%w: saved selection differs from the requested ID", ErrHashKeeperSelection)
	}
	work := make(map[string]SavedHashWork, len(snapshot.Work))
	for _, observation := range snapshot.Work {
		work[observation.ID] = observation
	}
	keeper, exists := work[keeperID]
	if !exists || keeper.Status != "complete" {
		return HashKeeperPreview{}, fmt.Errorf("%w: work %s has no complete historical observation", ErrHashKeeperSelection, keeperID)
	}
	for _, id := range copies {
		copy, exists := work[id]
		if !exists || copy.Status != "complete" || copy.LogicalBytes != keeper.LogicalBytes || copy.SHA256 != keeper.SHA256 {
			return HashKeeperPreview{}, fmt.Errorf("%w: work %s has no complete matching historical observation", ErrHashKeeperSelection, id)
		}
	}
	groups := groupSavedHashes(snapshot, record)
	var members map[string]SavedHashGroupMember
	for _, group := range groups.Groups {
		if group.LogicalBytes == keeper.LogicalBytes && group.SHA256 == keeper.SHA256 {
			members = make(map[string]SavedHashGroupMember, len(group.Members))
			for _, member := range group.Members {
				members[member.WorkID] = member
			}
			break
		}
	}
	roles := append([]string{keeperID}, copies...)
	for _, id := range roles {
		member, exists := members[id]
		if !exists {
			return HashKeeperPreview{}, ErrHashKeeperSelection
		}
		if member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			return HashKeeperPreview{}, fmt.Errorf("%w: work %s needs separate identity review", ErrHashKeeperIdentity, id)
		}
	}
	cloneMember := func(id string) SavedHashPreviewMember {
		member := members[id]
		member.PathBytes = bytes.Clone(member.PathBytes)
		return SavedHashPreviewMember{SavedHashGroupMember: member, Sequence: work[id].Sequence}
	}
	out := HashKeeperPreview{Source: groups.Source, Contract: HashKeeperPreviewContract, HashContract: groups.HashContract,
		StoreID: groups.StoreID, SelectionID: groups.SelectionID, InventoryID: groups.InventoryID,
		Scope: "explicit_saved_subset", BudgetScope: groups.BudgetScope, Budget: groups.Budget, ReadConsent: groups.ReadConsent,
		SelectedWork: groups.SelectedWork, CompletedObservations: groups.CompletedObservations,
		UnfinishedWork: groups.UnfinishedWork, UnmatchedCompletedObservations: groups.UnmatchedCompletedObservations,
		LogicalBytes: keeper.LogicalBytes, SHA256: keeper.SHA256, Keeper: cloneMember(keeperID), Copies: make([]SavedHashPreviewMember, 0, len(copies))}
	for _, id := range copies {
		out.Copies = append(out.Copies, cloneMember(id))
	}
	return out, nil
}
