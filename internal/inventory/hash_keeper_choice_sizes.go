package inventory

import (
	"context"
	"fmt"
	"math"
	"time"
)

const HistoricalCopySizesContract = "historical_selected_copy_sizes_v1"

// HistoricalCopySizes counts only the explicitly selected possible copy
// paths. Saved identities and allocation do not establish independent physical
// storage, current contents or reclaimable space.
type HistoricalCopySizes struct {
	Contract                      string `json:"contract"`
	Basis                         string `json:"basis"`
	Scope                         string `json:"scope"`
	SelectedCopyPaths             int    `json:"selected_copy_paths"`
	KnownSavedObjects             int    `json:"known_saved_objects"`
	RepeatedSavedPaths            int    `json:"repeated_saved_paths"`
	UnknownSavedIdentityPaths     int    `json:"unknown_saved_identity_paths"`
	ConflictingSavedObjects       int    `json:"conflicting_saved_objects"`
	KnownAllocationPaths          int    `json:"known_allocation_paths"`
	UnknownAllocationPaths        int    `json:"unknown_allocation_paths"`
	LogicalBytes                  *int64 `json:"logical_bytes"`
	LogicalBytesStatus            string `json:"logical_bytes_status"`
	SavedAllocatedBytes           *int64 `json:"saved_allocated_bytes"`
	SavedAllocatedBytesStatus     string `json:"saved_allocated_bytes_status"`
	ReclaimableBytes              *int64 `json:"reclaimable_bytes"`
	CurrentStateVerified          bool   `json:"current_state_verified"`
	IndependentAllocationVerified bool   `json:"independent_allocation_verified"`
	Executable                    bool   `json:"executable"`
}

// HashKeeperChoiceSizesReport adds a derived sibling to the existing id and
// record. Nothing is added to the immutable choice record or its ID codec.
type HashKeeperChoiceSizesReport struct {
	SavedHashKeeperChoice
	HistoricalCopySizes HistoricalCopySizes `json:"historical_copy_sizes"`
}

// KeeperChoiceSizes reads one existing choice using the existing jointly
// validated snapshot. It does not inspect source files, load configuration,
// migrate storage, recover work or evaluate current permission.
func (s *HashStore) KeeperChoiceSizes(ctx context.Context, id string) (HashKeeperChoiceSizesReport, error) {
	if !ValidHashKeeperChoiceID(id) {
		return HashKeeperChoiceSizesReport{}, ErrHashKeeperChoiceID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	saved, err := s.KeeperChoice(ctx, id)
	if err != nil {
		return HashKeeperChoiceSizesReport{}, hashChoiceSizesReadError(id, err)
	}
	sizes, err := historicalCopySizes(saved)
	if err != nil {
		return HashKeeperChoiceSizesReport{}, hashChoiceSizesReadError(id, err)
	}
	if err = ctx.Err(); err != nil {
		return HashKeeperChoiceSizesReport{}, hashChoiceSizesReadError(id, err)
	}
	return HashKeeperChoiceSizesReport{SavedHashKeeperChoice: saved, HistoricalCopySizes: sizes}, nil
}

func hashChoiceSizesReadError(id string, err error) error {
	return fmt.Errorf("requested historical choice %s could not be shown; inspect hashes --choice %s: %w", id, id, err)
}

// Only KeeperChoice's jointly validated result reaches this classifier in
// production. These defensive checks do not replace the saved-record binding.
func historicalCopySizes(saved SavedHashKeeperChoice) (HistoricalCopySizes, error) {
	if !ValidHashKeeperChoiceID(saved.ID) || validateHashKeeperChoiceRecord(saved.Record) != nil {
		return HistoricalCopySizes{}, ErrHashKeeperChoiceCorrupt
	}
	p := saved.Record.Evidence
	var identities [FileSampleTargetLimit]savedHashIdentity
	identities[0] = savedHashIdentity{p.Keeper.SavedDevice, p.Keeper.SavedInode}
	allocated := int64(0)
	allocationOverflow := false
	for i, member := range p.Copies {
		identity := savedHashIdentity{member.SavedDevice, member.SavedInode}
		for j := 0; j <= i; j++ {
			if identities[j] == identity {
				return HistoricalCopySizes{}, ErrHashKeeperChoiceCorrupt
			}
		}
		identities[i+1] = identity
		if !allocationOverflow {
			if member.SavedAllocatedBytes > math.MaxInt64-allocated {
				allocationOverflow = true
			} else {
				allocated += member.SavedAllocatedBytes
			}
		}
	}
	n := len(p.Copies)
	out := HistoricalCopySizes{Contract: HistoricalCopySizesContract, Basis: "immutable_choice_evidence", Scope: "explicit_saved_copy_paths",
		SelectedCopyPaths: n, KnownSavedObjects: n, KnownAllocationPaths: n,
		LogicalBytesStatus: "overflow", SavedAllocatedBytesStatus: "overflow"}
	if p.LogicalBytes <= math.MaxInt64/int64(n) {
		logical := p.LogicalBytes * int64(n)
		out.LogicalBytes, out.LogicalBytesStatus = &logical, "known"
	}
	if !allocationOverflow {
		out.SavedAllocatedBytes, out.SavedAllocatedBytesStatus = &allocated, "known"
	}
	return out, nil
}
