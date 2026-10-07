package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

var ErrHashSelectionID = errors.New("use the full saved hash selection ID")
var ErrHashManualRoot = errors.New("manual hash selection requires one normalized absolute root matching every selected saved file")

// HashSourceLocator identifies saved manual inventory. It never establishes
// source availability, current configuration, permission or content identity.
type HashSourceLocator struct {
	Kind          string `json:"kind"`
	RootPathBytes []byte `json:"root_path_bytes"`
	InventoryKey  string `json:"inventory_key"`
}

// HashProposal exposes the complete frozen evidence for review. It contains no
// checkpoint state, resume offset, consent or executable operation.
type HashProposal struct {
	StoreID                   string             `json:"store_id"`
	SelectionID               string             `json:"selection_id"`
	InventoryID               string             `json:"inventory_id"`
	Source                    string             `json:"source"`
	Contract                  string             `json:"contract"`
	SourceLocator             *HashSourceLocator `json:"source_locator,omitempty"`
	Targets                   []SavedFileTarget  `json:"targets"`
	ProvenanceVerified        bool               `json:"provenance_verified"`
	ContentVerified           bool               `json:"content_verified"`
	CurrentStateVerified      bool               `json:"current_state_verified"`
	DuplicatesVerified        bool               `json:"duplicates_verified"`
	Executable                bool               `json:"executable"`
	EstimatedReclaimableBytes *int64             `json:"estimated_reclaimable_bytes"`
}

func validHashManualRoot(root []byte) bool {
	return len(root) > 0 && len(root) <= 4096 && filepath.IsAbs(string(root)) && filepath.Clean(string(root)) == string(root) && !strings.ContainsRune(string(root), 0)
}
func hashManualInventoryKey(root []byte) string {
	sum := sha256.Sum256(root)
	return hex.EncodeToString(sum[:])
}

func validHashSelectionSource(record hashSelectionRecord) bool {
	if record.Version == 1 {
		return record.SourceLocator == nil
	}
	if record.Version != 2 || record.SourceLocator == nil {
		return false
	}
	locator := record.SourceLocator
	if locator.Kind != "manual_inventory_v1" || !validHashManualRoot(locator.RootPathBytes) || locator.InventoryKey != hashManualInventoryKey(locator.RootPathBytes) {
		return false
	}
	for _, target := range record.Targets {
		if !bytes.Equal(target.Root.PathBytes, locator.RootPathBytes) {
			return false
		}
	}
	return true
}

// CreateManualSelection captures exact displayed rows plus current root and
// ancestor bindings using saved inventory only. The root is checked lexically;
// no source filesystem path is resolved or opened. A matching retry preserves
// its existing immutable proposal and any work state without recovery.
func (s *HashStore) CreateManualSelection(ctx context.Context, source *state.Store, inventoryID string, expected []state.SameSizeFile, root []byte) (HashProposal, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if !validHashManualRoot(root) {
		return HashProposal{}, ErrHashManualRoot
	}
	locator := &HashSourceLocator{Kind: "manual_inventory_v1", RootPathBytes: bytes.Clone(root), InventoryKey: hashManualInventoryKey(root)}
	snapshot, err := s.createHashSelection(ctx, source, inventoryID, expected, locator)
	if err != nil {
		return HashProposal{}, err
	}
	return s.Proposal(ctx, snapshot.SelectionID)
}

// Proposal reopens only local saved hash storage. The source root and inventory
// can be offline; a legacy selection returns nil SourceLocator. Returned path
// bytes and ancestors are independent copies of the stored review evidence.
func (s *HashStore) Proposal(ctx context.Context, selectionID string) (HashProposal, error) {
	if !hashStoreDigest(selectionID) {
		return HashProposal{}, ErrHashSelectionID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		return HashProposal{}, err
	}
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return HashProposal{}, err
	}
	defer tx.Rollback()
	snapshot, record, err := s.readHashSnapshot(ctx, tx)
	if err != nil {
		return HashProposal{}, err
	}
	if record == nil || snapshot.SelectionID != selectionID {
		return HashProposal{}, fmt.Errorf("saved hash selection not found: %w", os.ErrNotExist)
	}
	proposal := HashProposal{StoreID: snapshot.StoreID, SelectionID: snapshot.SelectionID, InventoryID: record.InventoryID, Source: "saved_hash_selection", Contract: FileHashContract, Targets: make([]SavedFileTarget, len(record.Targets))}
	for i, target := range record.Targets {
		proposal.Targets[i] = cloneHashTarget(target)
	}
	if locator := record.SourceLocator; locator != nil {
		proposal.SourceLocator = &HashSourceLocator{Kind: locator.Kind, RootPathBytes: bytes.Clone(locator.RootPathBytes), InventoryKey: locator.InventoryKey}
	}
	return proposal, tx.Commit()
}
