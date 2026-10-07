package inventory

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const HashKeeperChoiceMetadataContract = "historical_choice_metadata_check_v1"

var ErrHashKeeperChoiceMetadataRequest = errors.New("metadata screening requires one prepared historical choice and an open scanner")
var ErrHashKeeperChoiceMetadataLocator = errors.New("historical choice has no supported saved manual inventory locator; no source was checked")

// KeeperChoiceMetadataRequest is an immutable metadata comparison baseline,
// not a read approval or a reusable execution token. It retains no descriptors.
// Accessors return copies; they cannot replace the request's frozen scope.
type KeeperChoiceMetadataRequest struct {
	core *keeperChoiceMetadataCore
}

type keeperChoiceMetadataCore struct {
	choice        SavedHashKeeperChoice
	proposal      HashProposal
	locator       HashSourceLocator
	targets       []SavedFileTarget
	targetDigests []string
	baselines     []keeperChoiceMetadataBaseline
	base          string
	storageIDs    map[string]string
	protectedIDs  map[string]bool
}

type keeperChoiceMetadataBaseline struct {
	stamp         hashLiveStamp
	volume, mount string
}

// HashKeeperChoiceMetadataTarget describes one sequential comparison against
// the frozen historical observation. Its metadata result is not content proof.
type HashKeeperChoiceMetadataTarget struct {
	Role              string                 `json:"role"`
	Observation       SavedHashPreviewMember `json:"observation"`
	MetadataCheckedAt time.Time              `json:"metadata_checked_at"`
	Status            string                 `json:"status"`
	Code              string                 `json:"code,omitempty"`
	Message           string                 `json:"message"`
}

// Body counters measure ordinary-file Read requests, which this screen never
// issues. Saved SQLite page I/O is separate; private-name/known-alias guards
// are not authentication against a coherent same-user namespace rewrite.
type HashKeeperChoiceMetadataReport struct {
	Contract                      string                           `json:"contract"`
	Source                        string                           `json:"source"`
	ChoiceID                      string                           `json:"choice_id"`
	StoreID                       string                           `json:"store_id"`
	SelectionID                   string                           `json:"selection_id"`
	InventoryID                   string                           `json:"inventory_id"`
	Status                        string                           `json:"status"`
	InventoryStatus               string                           `json:"inventory_status"`
	CheckedAt                     time.Time                        `json:"checked_at"`
	Code                          string                           `json:"code,omitempty"`
	Message                       string                           `json:"message"`
	Targets                       []HashKeeperChoiceMetadataTarget `json:"targets"`
	SelectedContentRequestedBytes int64                            `json:"selected_file_body_requested_bytes"`
	SelectedContentReadBytes      int64                            `json:"selected_file_body_read_bytes"`
	ApprovalAvailable             bool                             `json:"approval_available"`
	ProvenanceVerified            bool                             `json:"provenance_verified"`
	ContentVerified               bool                             `json:"content_verified"`
	CurrentStateVerified          bool                             `json:"current_state_verified"`
	DuplicatesVerified            bool                             `json:"duplicates_verified"`
	Executable                    bool                             `json:"executable"`
	EstimatedReclaimableBytes     *int64                           `json:"estimated_reclaimable_bytes"`
}

// Private hooks exercise namespace and saved-inventory changes while the
// production path uses no hooks and makes no ordinary-file Read requests.
type hashKeeperChoiceMetadataHooks struct {
	beforeTarget           func(int)
	beforeTargetFinalCheck func(int)
	afterTargets           func()
	beforeFinalCheck       func()
}

// PrepareKeeperChoiceMetadata captures the exact choice, immutable proposal
// and selected full targets in one saved transaction. Legacy missing locators
// refuse here, before a caller can derive configuration or source access.
func (s *HashStore) PrepareKeeperChoiceMetadata(ctx context.Context, id string) (*KeeperChoiceMetadataRequest, error) {
	if !ValidHashKeeperChoiceID(id) {
		return nil, ErrHashKeeperChoiceID
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
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
	choice, err := s.readHashKeeperChoice(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	snapshot, selection, err := s.readHashSnapshot(ctx, tx)
	if err != nil {
		return nil, err
	}
	if selection == nil || selection.SourceLocator == nil || !validHashReadLocator(*selection.SourceLocator) {
		return nil, ErrHashKeeperChoiceMetadataLocator
	}
	core := &keeperChoiceMetadataCore{choice: choice, base: s.base, locator: cloneHashReadLocator(*selection.SourceLocator)}
	core.proposal = hashKeeperMetadataProposal(snapshot, selection)
	core.protectedIDs = make(map[string]bool)
	for _, target := range selection.Targets {
		core.protectedIDs[target.File.Device+":"+target.File.Inode] = true
		for _, ancestor := range target.Ancestors {
			core.protectedIDs[ancestor.Device+":"+ancestor.Inode] = true
		}
	}
	members := append([]SavedHashPreviewMember{choice.Record.Evidence.Keeper}, choice.Record.Evidence.Copies...)
	for _, member := range members {
		ordinal, _ := strconv.Atoi(member.WorkID)
		if ordinal < 1 || ordinal > len(selection.Targets) {
			return nil, ErrHashKeeperChoiceCorrupt
		}
		target := cloneHashTarget(selection.Targets[ordinal-1])
		if !bytes.Equal(target.Root.PathBytes, core.locator.RootPathBytes) {
			return nil, ErrHashKeeperChoiceMetadataLocator
		}
		digest, digestErr := hashTargetDigest(target)
		if digestErr != nil {
			return nil, ErrHashKeeperChoiceCorrupt
		}
		work, workErr := readHashWork(ctx, tx, selection, snapshot.SelectionID, ordinal)
		if workErr != nil {
			return nil, workErr
		}
		if work.status != "complete" || !work.checkpoint.complete || work.sequence != member.Sequence || !work.checkpoint.checkedAt.Equal(member.CheckedAt) || work.checkpoint.finalSHA != choice.Record.Evidence.SHA256 {
			return nil, ErrHashKeeperChoiceCorrupt
		}
		core.targets = append(core.targets, target)
		core.targetDigests = append(core.targetDigests, digest)
		// Retain only the bounded observed stamp and filesystem/mount identity,
		// never checkpoint bytes, SHA continuation state or file bodies.
		core.baselines = append(core.baselines, keeperChoiceMetadataBaseline{stamp: work.checkpoint.stamp, volume: work.checkpoint.volume, mount: work.checkpoint.mount})
	}
	if len(core.targets) < 2 || boundedSampleRequest(core.targets) != nil {
		return nil, ErrHashKeeperChoiceMetadataRequest
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = s.checkHashStorage(nil); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	core.storageIDs = make(map[string]string, len(s.storageIDs))
	for path, identity := range s.storageIDs {
		core.storageIDs[path] = identity
	}
	return &KeeperChoiceMetadataRequest{core: core}, nil
}

func hashKeeperMetadataProposal(snapshot HashSnapshot, selection *hashSelectionRecord) HashProposal {
	proposal := HashProposal{StoreID: snapshot.StoreID, SelectionID: snapshot.SelectionID, InventoryID: selection.InventoryID,
		Source: "saved_hash_selection", Contract: FileHashContract, ReadConsent: snapshot.ReadConsent,
		Targets: make([]SavedFileTarget, len(selection.Targets))}
	for i, target := range selection.Targets {
		proposal.Targets[i] = cloneHashTarget(target)
	}
	locator := cloneHashReadLocator(*selection.SourceLocator)
	proposal.SourceLocator = &locator
	return proposal
}

func (r *KeeperChoiceMetadataRequest) SourceLocator() HashSourceLocator {
	if r == nil || r.core == nil {
		return HashSourceLocator{}
	}
	return cloneHashReadLocator(r.core.locator)
}

// Proposal supplies cloned saved evidence for later CLI alias guards. Check
// does not trust edits to this copy or use it as an alternate source scope.
func (r *KeeperChoiceMetadataRequest) Proposal() HashProposal {
	if r == nil || r.core == nil {
		return HashProposal{}
	}
	proposal := r.core.proposal
	proposal.Targets = make([]SavedFileTarget, len(r.core.proposal.Targets))
	for i, target := range r.core.proposal.Targets {
		proposal.Targets[i] = cloneHashTarget(target)
	}
	locator := cloneHashReadLocator(r.core.locator)
	proposal.SourceLocator = &locator
	if original := proposal.ReadConsent; original != nil {
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
		proposal.ReadConsent = &consent
	}
	return proposal
}

// Check derives existing manual inventory solely from the frozen locator and
// opens its own saved reader after private-object and known-alias guards.
// SQLite remains pathname based: guards are not authentication against
// coherent same-user namespace rewrites. Configuration is caller-owned.
// Files are checked sequentially; no result proves simultaneous equality,
// current contents, inode continuity or safe later execution. The five-second
// deadline is cooperative and cannot preempt a blocked filesystem syscall.
func (r *KeeperChoiceMetadataRequest) Check(ctx context.Context, scanner *Scanner) (HashKeeperChoiceMetadataReport, error) {
	return r.check(ctx, scanner, hashKeeperChoiceMetadataHooks{})
}

func (r *KeeperChoiceMetadataRequest) check(ctx context.Context, scanner *Scanner, hooks hashKeeperChoiceMetadataHooks) (HashKeeperChoiceMetadataReport, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return HashKeeperChoiceMetadataReport{}, err
	}
	if r == nil || r.core == nil || scanner == nil || scanner.closed.Load() {
		return HashKeeperChoiceMetadataReport{}, ErrHashKeeperChoiceMetadataRequest
	}
	core := r.core
	if len(core.targets) < 2 || len(core.targets) != len(core.targetDigests) || len(core.targets) != len(core.baselines) || boundedSampleRequest(core.targets) != nil {
		return HashKeeperChoiceMetadataReport{}, ErrHashKeeperChoiceMetadataRequest
	}
	for _, target := range core.targets {
		if validateSampleTarget(target) != nil {
			return HashKeeperChoiceMetadataReport{}, ErrHashKeeperChoiceMetadataRequest
		}
	}
	report := r.metadataReport()
	// Check original named object identities BEFORE opening SQLite. A renamed
	// selected file in the database slot must not be opened as saved storage.
	if err := core.checkOriginalHashStorage(ctx); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_storage_changed", "Original private hash storage changed or became unavailable; no live target was checked."))
	}
	reader, err := OpenHashReader(ctx, core.base)
	if err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_storage_unavailable", "Existing saved hash storage is invalid or unavailable; no live target was checked."))
	}
	defer reader.Close()
	if err = core.checkSavedChoice(ctx, reader); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_choice_changed", "The original hash storage or selected historical evidence changed; no live target was checked."))
	}
	sourceIDs, err := core.checkMetadataSourceStorage(ctx, nil)
	if err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataSourceStorage(report, err))
	}
	source, err := state.OpenReader(ctx, core.metadataSourceDirectory())
	if err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataInventory(report, err))
	}
	defer source.Close()
	sourceIDs, err = core.checkMetadataSourceStorage(ctx, sourceIDs)
	if err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataSourceStorage(report, err))
	}
	if err = core.compareMetadataInventory(ctx, source); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataInventory(report, err))
	}
	if _, err = core.checkMetadataSourceStorage(ctx, sourceIDs); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataSourceStorage(report, err))
	}
	report.InventoryStatus = "matches_saved_inventory"
	for i, target := range core.targets {
		if hooks.beforeTarget != nil {
			hooks.beforeTarget(i)
		}
		if err = ctx.Err(); err != nil {
			return HashKeeperChoiceMetadataReport{}, err
		}
		if scanner.closed.Load() {
			return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "scanner_closed", "The scanner closed before metadata screening finished."))
		}
		var beforeFinal func()
		if hooks.beforeTargetFinalCheck != nil {
			beforeFinal = func() { hooks.beforeTargetFinalCheck(i) }
		}
		// This metadata-only visitor ignores the descriptor and compares only
		// the original completed observation's stamp and mount identity. It
		// makes no Read request, hash call or directory listing.
		baseline := core.baselines[i]
		checkErr := scanner.withSavedRegularFile(ctx, target, func(_ int, live unix.Stat_t, volume, mount string) error {
			if volume != baseline.volume || mount != baseline.mount {
				return blocked("mount_boundary", "The selected file's filesystem or mount differs from its historical full-hash observation.")
			}
			if !baseline.stamp.matches(live) {
				return blocked("file_changed", "The selected file's identity, mode, link count or metadata differs from its historical full-hash observation.")
			}
			return nil
		}, beforeFinal)
		if err = ctx.Err(); err != nil {
			return HashKeeperChoiceMetadataReport{}, err
		}
		item := &report.Targets[i]
		item.MetadataCheckedAt = time.Now().UTC()
		item.Status, item.Code, item.Message = "metadata_matches", "", "Frozen root, parent and regular-file metadata matched during this individual check. Selected contents were not read."
		if checkErr != nil {
			item.Status, item.Code, item.Message = "blocked", "path_unavailable", "A selected path is missing, inaccessible, a symlink or otherwise unsupported; no content comparison is available."
			var failure liveError
			if errors.As(checkErr, &failure) {
				item.Code, item.Message = failure.code, failure.message
			}
			report.Status = "blocked"
		}
	}
	if hooks.afterTargets != nil {
		hooks.afterTargets()
	}
	if hooks.beforeFinalCheck != nil {
		hooks.beforeFinalCheck()
	}
	if _, err = core.checkMetadataSourceStorage(ctx, sourceIDs); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataSourceStorage(report, err))
	}
	if err = core.compareMetadataInventory(ctx, source); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataInventory(report, err))
	}
	if scanner.closed.Load() {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "scanner_closed", "The scanner closed before metadata screening finished."))
	}
	if _, err = core.checkMetadataSourceStorage(ctx, sourceIDs); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataSourceStorage(report, err))
	}
	if err = core.checkOriginalHashStorage(ctx); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_storage_changed", "Original private hash storage changed during metadata screening; no overall match is available."))
	}
	if err = core.checkSavedChoice(ctx, reader); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_choice_changed", "The original hash storage or selected historical evidence changed during screening; no overall match is available."))
	}
	if err = source.Close(); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadataInventory(report, err))
	}
	if err = reader.Close(); err != nil {
		return finishHashKeeperMetadata(ctx, blockHashKeeperMetadata(report, "hash_storage_unavailable", "Closing saved hash storage failed; the screen remains uncertain."))
	}
	return finishHashKeeperMetadata(ctx, report)
}

func (core *keeperChoiceMetadataCore) checkOriginalHashStorage(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// This helper only checks private names and object IDs. It never opens a
	// database connection or uses an initialized HashStore's work state.
	probe := HashStore{base: core.base, storageIDs: core.storageIDs}
	if err := probe.checkHashStorage(nil); err != nil {
		return err
	}
	dir := filepath.Join(core.base, "hashes")
	path := filepath.Join(dir, hashStoreFilename)
	for _, name := range []string{core.base, dir, path, path + "-wal", path + "-shm", path + "-journal"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		var st unix.Stat_t
		err := unix.Lstat(name, &st)
		if errors.Is(err, os.ErrNotExist) && name != core.base && name != dir && name != path {
			continue
		}
		if err != nil {
			return err
		}
		if core.protectedIDs[objectID(st)] {
			return errors.New("private hash storage aliases frozen file or ancestor evidence")
		}
	}
	return ctx.Err()
}

func (core *keeperChoiceMetadataCore) metadataSourceDirectory() string {
	return filepath.Join(core.base, "manual", core.locator.InventoryKey)
}

// Validate the derived private inventory names before SQLite can read them.
// Capture object identities for rechecks; no file bodies are opened here.
func (core *keeperChoiceMetadataCore) checkMetadataSourceStorage(ctx context.Context, expected map[string]string) (map[string]string, error) {
	dir := core.metadataSourceDirectory()
	current := make(map[string]string)
	checkIdentity := func(path string) error {
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			return err
		}
		id := objectID(st)
		if core.protectedIDs[id] {
			return errors.New("private inventory storage aliases frozen file or ancestor evidence")
		}
		if original, ok := expected[path]; ok && original != id {
			return errors.New("private inventory storage object identity changed")
		}
		current[path] = id
		return nil
	}
	for _, path := range []string{filepath.Dir(dir), dir} {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := localfs.CheckOwnedDir(path); err != nil {
			return nil, err
		}
		canonical, err := filepath.EvalSymlinks(path)
		if err != nil || canonical != path {
			return nil, errors.New("private inventory directory is aliased or unavailable")
		}
		if err = checkIdentity(path); err != nil {
			return nil, err
		}
	}
	path := filepath.Join(dir, state.Filename)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name := path + suffix
		if err := hashPrivateFile(name); err != nil {
			if suffix != "" && errors.Is(err, os.ErrNotExist) {
				if _, existed := expected[name]; existed {
					return nil, errors.New("private inventory sidecar disappeared during screening")
				}
				continue
			}
			return nil, err
		}
		if err := checkIdentity(name); err != nil {
			return nil, err
		}
	}
	return current, ctx.Err()
}

func (core *keeperChoiceMetadataCore) checkSavedChoice(ctx context.Context, reader *HashStore) error {
	if err := core.checkOriginalHashStorage(ctx); err != nil {
		return err
	}
	choice, err := reader.KeeperChoice(ctx, core.choice.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(choice, core.choice) {
		return ErrHashKeeperChoiceCorrupt
	}
	return core.checkOriginalHashStorage(ctx)
}

func (core *keeperChoiceMetadataCore) compareMetadataInventory(ctx context.Context, source *state.Store) error {
	expected := make([]state.SameSizeFile, len(core.targets))
	for i, target := range core.targets {
		expected[i] = target.File
	}
	current, err := source.PrepareFileSampleSelection(ctx, core.choice.Record.Evidence.InventoryID, expected)
	if err != nil {
		return err
	}
	if len(current) != len(core.targetDigests) {
		return state.ErrFileSampleEvidence
	}
	for i, target := range current {
		digest, digestErr := hashTargetDigest(target)
		if digestErr != nil || digest != core.targetDigests[i] {
			return state.ErrFileSampleEvidence
		}
	}
	return ctx.Err()
}

func (r *KeeperChoiceMetadataRequest) metadataReport() HashKeeperChoiceMetadataReport {
	p := r.core.choice.Record.Evidence
	report := HashKeeperChoiceMetadataReport{Contract: HashKeeperChoiceMetadataContract, Source: "live_metadata_with_saved_inventory",
		ChoiceID: r.core.choice.ID, StoreID: p.StoreID, SelectionID: p.SelectionID, InventoryID: p.InventoryID,
		Status: "metadata_matches", InventoryStatus: "not_checked",
		Message: "Selected files were screened sequentially. Matching metadata does not prove simultaneous or current content equality, safe deletion or permission for another content read.",
		Targets: make([]HashKeeperChoiceMetadataTarget, 0, len(r.core.targets))}
	for i, observation := range append([]SavedHashPreviewMember{p.Keeper}, p.Copies...) {
		observation.PathBytes = bytes.Clone(observation.PathBytes)
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		report.Targets = append(report.Targets, HashKeeperChoiceMetadataTarget{Role: role, Observation: observation, Status: "blocked", Code: "not_checked", Message: "No live metadata result is available for this historical role."})
	}
	return report
}

func blockHashKeeperMetadata(report HashKeeperChoiceMetadataReport, code, message string) HashKeeperChoiceMetadataReport {
	report.Status, report.Code, report.Message = "blocked", code, message
	for i := range report.Targets {
		report.Targets[i].Status, report.Targets[i].Code, report.Targets[i].Message = "blocked", code, message
	}
	return report
}

func blockHashKeeperMetadataInventory(report HashKeeperChoiceMetadataReport, err error) HashKeeperChoiceMetadataReport {
	if errors.Is(err, state.ErrFileSampleEvidence) || errors.Is(err, state.ErrFileSampleSchema) || errors.Is(err, state.ErrFileSampleSelection) || errors.Is(err, state.ErrFileSampleLimit) {
		report.InventoryStatus = "changed"
		return blockHashKeeperMetadata(report, "inventory_changed", "Current selected inventory or full root/ancestor evidence differs from the frozen proposal; no replacement baseline was used.")
	}
	report.InventoryStatus = "unavailable"
	return blockHashKeeperMetadata(report, "inventory_unavailable", "The existing source inventory is unavailable or invalid; selected metadata cannot establish a match.")
}

func blockHashKeeperMetadataSourceStorage(report HashKeeperChoiceMetadataReport, err error) HashKeeperChoiceMetadataReport {
	report.InventoryStatus = "unavailable"
	if errors.Is(err, os.ErrNotExist) {
		return blockHashKeeperMetadata(report, "inventory_unavailable", "The derived existing manual inventory is unavailable; no replacement storage was initialized.")
	}
	return blockHashKeeperMetadata(report, "inventory_storage_changed", "The derived inventory storage changed, aliases frozen evidence or is invalid; the screen cannot establish a match.")
}

func finishHashKeeperMetadata(ctx context.Context, report HashKeeperChoiceMetadataReport) (HashKeeperChoiceMetadataReport, error) {
	if err := ctx.Err(); err != nil {
		return HashKeeperChoiceMetadataReport{}, err
	}
	report.CheckedAt = time.Now().UTC()
	if report.Status == "blocked" && report.Code == "" {
		report.Code = "selected_metadata_blocked"
		report.Message = "At least one selected metadata check was blocked. Individual checks are sequential and do not authorize content reads or cleanup."
	}
	if err := ctx.Err(); err != nil {
		return HashKeeperChoiceMetadataReport{}, err
	}
	return report, nil
}
