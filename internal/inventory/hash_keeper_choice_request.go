package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const HashKeeperChoiceFreshRequestContract = "choice_bound_fresh_full_hash_request_v1"

const hashKeeperChoiceFreshRequestPrefix = "hash-choice-request-v1-"

var ErrHashKeeperChoiceFreshRequestLocator = errors.New("fresh observation request requires the saved choice's supported manual inventory locator; no source was read")
var ErrHashKeeperChoiceFreshRequestEvidence = errors.New("fresh observation request requires 2–20 exact bounded historical roles and their complete frozen targets; no request was prepared")

// KeeperChoiceFreshRequest is an immutable saved-evidence request for possible
// future full-file observations. It is not a durable job, content-read consent,
// metadata-check token or execution permission. It retains no descriptors,
// checkpoint bytes, SHA continuation state or source bodies.
type KeeperChoiceFreshRequest struct {
	core *keeperChoiceFreshRequestCore
}

type keeperChoiceFreshRequestCore struct {
	report       HashKeeperChoiceFreshRequestReport
	proposal     HashProposal
	base         string
	storageIDs   map[string]string
	protectedIDs map[string]bool
}

// HashKeeperChoiceFreshRequestTarget preserves one exact historical role and
// its full frozen root/file/ancestor evidence. Its work ID is historical, not a
// reusable work ordinal for a later fresh job.
type HashKeeperChoiceFreshRequestTarget struct {
	Role        string                 `json:"role"`
	Observation SavedHashPreviewMember `json:"observation"`
	Target      SavedFileTarget        `json:"target"`
}

// HashKeeperChoiceFreshRequestReport exposes saved review evidence only.
// HistoricalChoice retains its original save-time coverage, budget and consent
// without refreshing or evaluating them. RequestID identifies exact requested
// scope; a future durable job must have a separate identity and accounting.
type HashKeeperChoiceFreshRequestReport struct {
	Version                   int                                  `json:"version"`
	Contract                  string                               `json:"contract"`
	HashContract              string                               `json:"hash_contract"`
	RequestID                 string                               `json:"request_id"`
	Source                    string                               `json:"source"`
	Status                    string                               `json:"status"`
	ChoiceID                  string                               `json:"choice_id"`
	StoreID                   string                               `json:"store_id"`
	SelectionID               string                               `json:"selection_id"`
	InventoryID               string                               `json:"inventory_id"`
	SourceLocator             HashSourceLocator                    `json:"source_locator"`
	Targets                   []HashKeeperChoiceFreshRequestTarget `json:"targets"`
	HistoricalChoice          SavedHashKeeperChoice                `json:"historical_choice"`
	ApprovalAvailable         bool                                 `json:"approval_available"`
	ProvenanceVerified        bool                                 `json:"provenance_verified"`
	ContentVerified           bool                                 `json:"content_verified"`
	CurrentStateVerified      bool                                 `json:"current_state_verified"`
	DuplicatesVerified        bool                                 `json:"duplicates_verified"`
	Executable                bool                                 `json:"executable"`
	EstimatedReclaimableBytes *int64                               `json:"estimated_reclaimable_bytes"`
}

// Hooks expose cancellation and private-object replacement boundaries to
// generated fixtures. The production path uses no hooks and publishes nothing.
type hashKeeperChoiceFreshRequestHooks struct {
	afterSnapshot func()
	afterCommit   func()
}

func ValidHashKeeperChoiceFreshRequestID(id string) bool {
	return len(id) == len(hashKeeperChoiceFreshRequestPrefix)+64 && strings.HasPrefix(id, hashKeeperChoiceFreshRequestPrefix) && hashStoreDigest(id[len(hashKeeperChoiceFreshRequestPrefix):])
}

// PrepareKeeperChoiceFreshRequest reads one exact saved choice and immutable
// selection in one bounded read transaction. It opens no source, inventory or
// configuration, initializes/migrates/recovers no state and changes no consent,
// clocks or accounting. Current storage context never replaces archived choice
// context. An earlier metadata screen is neither required nor accepted here.
func (s *HashStore) PrepareKeeperChoiceFreshRequest(ctx context.Context, choiceID string) (*KeeperChoiceFreshRequest, error) {
	return s.prepareKeeperChoiceFreshRequest(ctx, choiceID, hashKeeperChoiceFreshRequestHooks{})
}

func (s *HashStore) prepareKeeperChoiceFreshRequest(ctx context.Context, choiceID string, hooks hashKeeperChoiceFreshRequestHooks) (*KeeperChoiceFreshRequest, error) {
	if !ValidHashKeeperChoiceID(choiceID) {
		return nil, ErrHashKeeperChoiceID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.acquire(ctx, false); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	if err := s.checkHashStorage(nil); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	choice, err := s.readHashKeeperChoice(ctx, tx, choiceID)
	if err != nil {
		return nil, err
	}
	snapshot, selection, err := s.readHashSnapshot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if selection == nil || selection.SourceLocator == nil || !validHashReadLocator(*selection.SourceLocator) {
		return nil, ErrHashKeeperChoiceFreshRequestLocator
	}
	core := &keeperChoiceFreshRequestCore{base: s.base, storageIDs: make(map[string]string, len(s.storageIDs)), protectedIDs: make(map[string]bool)}
	for path, identity := range s.storageIDs {
		core.storageIDs[path] = identity
	}
	for _, target := range selection.Targets {
		core.protectedIDs[target.File.Device+":"+target.File.Inode] = true
		for _, ancestor := range target.Ancestors {
			core.protectedIDs[ancestor.Device+":"+ancestor.Inode] = true
		}
	}
	// The complete original proposal supports later private alias guards, but
	// its consent is archived choice context, not the current store lifecycle.
	core.proposal = hashKeeperMetadataProposal(snapshot, selection)
	core.proposal.ReadConsent = cloneHashKeeperChoiceFreshConsent(choice.Record.Evidence.ReadConsent)
	p := choice.Record.Evidence
	core.report = HashKeeperChoiceFreshRequestReport{Version: 1, Contract: HashKeeperChoiceFreshRequestContract,
		HashContract: FileHashContract, Source: "saved_hash_choice", Status: "unapproved", ChoiceID: choice.ID,
		StoreID: p.StoreID, SelectionID: p.SelectionID, InventoryID: p.InventoryID,
		SourceLocator: cloneHashReadLocator(*selection.SourceLocator), HistoricalChoice: choice,
		Targets: make([]HashKeeperChoiceFreshRequestTarget, 0, len(p.Copies)+1)}
	targets := make([]SavedFileTarget, 0, len(p.Copies)+1)
	for i, observation := range append([]SavedHashPreviewMember{p.Keeper}, p.Copies...) {
		ordinal, parseErr := strconv.Atoi(observation.WorkID)
		if parseErr != nil || ordinal < 1 || ordinal > len(selection.Targets) {
			return nil, ErrHashKeeperChoiceCorrupt
		}
		target := cloneHashTarget(selection.Targets[ordinal-1])
		if !bytes.Equal(target.Root.PathBytes, core.report.SourceLocator.RootPathBytes) {
			return nil, ErrHashKeeperChoiceFreshRequestLocator
		}
		if _, digestErr := hashTargetDigest(target); digestErr != nil {
			return nil, ErrHashKeeperChoiceFreshRequestEvidence
		}
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		observation.PathBytes = bytes.Clone(observation.PathBytes)
		core.report.Targets = append(core.report.Targets, HashKeeperChoiceFreshRequestTarget{Role: role, Observation: observation, Target: target})
		targets = append(targets, target)
	}
	if len(targets) < 2 || boundedSampleRequest(targets) != nil {
		return nil, ErrHashKeeperChoiceFreshRequestEvidence
	}
	core.report.RequestID, err = hashKeeperChoiceFreshRequestID(core.report)
	if err != nil {
		return nil, err
	}
	if hooks.afterSnapshot != nil {
		hooks.afterSnapshot()
	}
	if err := core.checkStorage(ctx); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	if err := core.checkStorage(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &KeeperChoiceFreshRequest{core: core}, nil
}

// Request identity has no capture time, mutable current-store context, private
// storage pathname or future job ID. ChoiceID already binds immutable archived
// context. Full ordered targets and observations bind exact requested scope.
func hashKeeperChoiceFreshRequestID(report HashKeeperChoiceFreshRequestReport) (string, error) {
	identity := struct {
		Version       int                                  `json:"version"`
		Contract      string                               `json:"contract"`
		HashContract  string                               `json:"hash_contract"`
		ChoiceID      string                               `json:"choice_id"`
		StoreID       string                               `json:"store_id"`
		SelectionID   string                               `json:"selection_id"`
		InventoryID   string                               `json:"inventory_id"`
		SourceLocator HashSourceLocator                    `json:"source_locator"`
		Targets       []HashKeeperChoiceFreshRequestTarget `json:"targets"`
	}{report.Version, report.Contract, report.HashContract, report.ChoiceID, report.StoreID,
		report.SelectionID, report.InventoryID, report.SourceLocator, report.Targets}
	body, err := json.Marshal(identity)
	if err != nil {
		return "", ErrHashKeeperChoiceFreshRequestEvidence
	}
	return fmt.Sprintf("%s%x", hashKeeperChoiceFreshRequestPrefix, sha256.Sum256(body)), nil
}

func (core *keeperChoiceFreshRequestCore) checkStorage(ctx context.Context) error {
	// Reuse the metadata-only private-name and all-proposal alias guard, never
	// its source-inventory or ordinary-file observation path. SQLite remains
	// pathname based; these checks do not authenticate coherent same-user clones.
	guard := keeperChoiceMetadataCore{base: core.base, storageIDs: core.storageIDs, protectedIDs: core.protectedIDs}
	return guard.checkOriginalHashStorage(ctx)
}

func (r *KeeperChoiceFreshRequest) ID() string {
	if r == nil || r.core == nil {
		return ""
	}
	return r.core.report.RequestID
}

func (r *KeeperChoiceFreshRequest) SourceLocator() HashSourceLocator {
	if r == nil || r.core == nil {
		return HashSourceLocator{}
	}
	return cloneHashReadLocator(r.core.report.SourceLocator)
}

func (r *KeeperChoiceFreshRequest) Report() HashKeeperChoiceFreshRequestReport {
	if r == nil || r.core == nil {
		return HashKeeperChoiceFreshRequestReport{}
	}
	report := r.core.report
	report.SourceLocator = cloneHashReadLocator(report.SourceLocator)
	report.HistoricalChoice = cloneHashKeeperChoiceFreshHistorical(report.HistoricalChoice)
	report.Targets = make([]HashKeeperChoiceFreshRequestTarget, len(r.core.report.Targets))
	for i, target := range r.core.report.Targets {
		target.Observation.PathBytes = bytes.Clone(target.Observation.PathBytes)
		target.Target = cloneHashTarget(target.Target)
		report.Targets[i] = target
	}
	return report
}

func (r *KeeperChoiceFreshRequest) Proposal() HashProposal {
	if r == nil || r.core == nil {
		return HashProposal{}
	}
	proposal := r.core.proposal
	proposal.SourceLocator = new(HashSourceLocator)
	*proposal.SourceLocator = cloneHashReadLocator(r.core.report.SourceLocator)
	proposal.ReadConsent = cloneHashKeeperChoiceFreshConsent(proposal.ReadConsent)
	proposal.Targets = make([]SavedFileTarget, len(r.core.proposal.Targets))
	for i, target := range r.core.proposal.Targets {
		proposal.Targets[i] = cloneHashTarget(target)
	}
	return proposal
}

func cloneHashKeeperChoiceFreshHistorical(choice SavedHashKeeperChoice) SavedHashKeeperChoice {
	p := &choice.Record.Evidence
	p.Keeper.PathBytes = bytes.Clone(p.Keeper.PathBytes)
	copies := make([]SavedHashPreviewMember, len(p.Copies))
	for i, observation := range p.Copies {
		observation.PathBytes = bytes.Clone(observation.PathBytes)
		copies[i] = observation
	}
	p.Copies = copies
	if p.Budget != nil {
		budget := *p.Budget
		p.Budget = &budget
	}
	p.ReadConsent = cloneHashKeeperChoiceFreshConsent(p.ReadConsent)
	return choice
}

func cloneHashKeeperChoiceFreshConsent(original *HashReadConsent) *HashReadConsent {
	if original == nil {
		return nil
	}
	consent := *original
	consent.Approval.SourceLocator = cloneHashReadLocator(original.Approval.SourceLocator)
	if original.Revocation != nil {
		revocation := *original.Revocation
		consent.Revocation = &revocation
	}
	if original.EstimatedReclaimableBytes != nil {
		bytes := *original.EstimatedReclaimableBytes
		consent.EstimatedReclaimableBytes = &bytes
	}
	return &consent
}
