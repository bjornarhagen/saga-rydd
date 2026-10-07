package inventory

import "bytes"

const HashFreshJobComparisonContract = "historical_fresh_job_hash_comparison_v1"

// HashFreshJobComparisonReport relates saved full-hash observations from one
// exact fresh job. Each observation was checked separately; matching hashes
// grant no current-equality, read, cleanup or reclaimable-space authority.
type HashFreshJobComparisonReport struct {
	Source                         string                       `json:"source"`
	Contract                       string                       `json:"contract"`
	HashContract                   string                       `json:"hash_contract"`
	Scope                          string                       `json:"scope"`
	JobID                          string                       `json:"job_id"`
	JobKey                         string                       `json:"job_key"`
	RequestID                      string                       `json:"request_id"`
	ChoiceID                       string                       `json:"choice_id"`
	StoreID                        string                       `json:"store_id"`
	SelectionID                    string                       `json:"selection_id"`
	InventoryID                    string                       `json:"inventory_id"`
	ProgressInitialized            bool                         `json:"progress_initialized"`
	Status                         string                       `json:"status"`
	Keeper                         HashFreshJobComparisonMember `json:"keeper"`
	Copies                         []HashFreshJobCopyComparison `json:"copies"`
	MatchingCopies                 int                          `json:"matching_copies"`
	DifferingCopies                int                          `json:"differing_copies"`
	IncompleteCopies               int                          `json:"incomplete_copies"`
	BlockedCopies                  int                          `json:"blocked_copies"`
	CurrentReadPermissionEvaluated bool                         `json:"current_read_permission_evaluated"`
	ApprovalAvailable              bool                         `json:"approval_available"`
	ProvenanceVerified             bool                         `json:"provenance_verified"`
	ContentVerified                bool                         `json:"content_verified"`
	CurrentStateVerified           bool                         `json:"current_state_verified"`
	DuplicatesVerified             bool                         `json:"duplicates_verified"`
	Executable                     bool                         `json:"executable"`
	EstimatedReclaimableBytes      *int64                       `json:"estimated_reclaimable_bytes"`
}

// A nil Observation means only the immutable pending seed exists. It does not
// describe an observed empty file or a checked zero-byte prefix. PathBytes and
// TargetDigest bind the full frozen scope already retained in the job record.
type HashFreshJobComparisonMember struct {
	Ordinal          int                 `json:"ordinal"`
	HistoricalWorkID string              `json:"historical_work_id"`
	Role             string              `json:"role"`
	FileID           int64               `json:"file_id"`
	RootID           int64               `json:"root_id"`
	PathBytes        []byte              `json:"path_bytes"`
	TargetDigest     string              `json:"target_digest"`
	Observation      *SavedFreshHashWork `json:"observation"`
}

type HashFreshJobCopyComparison struct {
	Copy     HashFreshJobComparisonMember `json:"copy"`
	Relation string                       `json:"relation"`
}

// This private classifier receives only the jointly validated job returned by
// readFreshJob. It accepts no caller-authored evidence, opens nothing and does
// not inspect time, permission, metadata tokens or original digests as fresh
// observations. All returned paths and optional usage pointers are independent.
func compareFreshJob(job SavedFreshJob) (HashFreshJobComparisonReport, error) {
	r := job.Record.Request
	n := len(r.Targets)
	if !ValidHashFreshJobID(job.ID) || !ValidHashFreshJobKey(job.Record.JobKey) || !ValidHashKeeperChoiceFreshRequestID(r.RequestID) || !ValidHashKeeperChoiceID(r.ChoiceID) || !hashStoreDigest(r.StoreID) || !hashStoreDigest(r.SelectionID) || !hashStoreDigest(r.InventoryID) || r.HashContract != FileHashContract || n < 2 || n > FileSampleTargetLimit || len(job.Work) != n || len(job.Progress) != 0 && len(job.Progress) != n {
		return HashFreshJobComparisonReport{}, ErrHashFreshProgressCorrupt
	}
	out := HashFreshJobComparisonReport{Source: "saved_fresh_job_observations", Contract: HashFreshJobComparisonContract, HashContract: FileHashContract, Scope: "exact_saved_fresh_job", JobID: job.ID, JobKey: job.Record.JobKey, RequestID: r.RequestID, ChoiceID: r.ChoiceID, StoreID: r.StoreID, SelectionID: r.SelectionID, InventoryID: r.InventoryID, ProgressInitialized: len(job.Progress) != 0, Status: "historical_hashes_match", Copies: make([]HashFreshJobCopyComparison, 0, n-1)}
	for i, target := range r.Targets {
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		seed := job.Work[i]
		digest, err := hashTargetDigest(target.Target)
		if err != nil || target.Role != role || seed.Ordinal != i+1 || seed.HistoricalWorkID != target.Observation.WorkID || seed.Role != role || seed.TargetDigest != digest || seed.Status != "pending" || seed.Sequence != 0 || seed.CheckedOffset != 0 {
			return HashFreshJobComparisonReport{}, ErrHashFreshProgressCorrupt
		}
		member := HashFreshJobComparisonMember{Ordinal: i + 1, HistoricalWorkID: seed.HistoricalWorkID, Role: role, FileID: target.Target.File.ID, RootID: target.Target.File.RootID, PathBytes: bytes.Clone(target.Target.File.PathBytes), TargetDigest: digest}
		if out.ProgressInitialized {
			work := job.Progress[i]
			if !validFreshComparisonObservation(work, member, target.Target.File.Size) {
				return HashFreshJobComparisonReport{}, ErrHashFreshProgressCorrupt
			}
			member.Observation = cloneFreshComparisonObservation(work)
		}
		if i == 0 {
			out.Keeper = member
			continue
		}
		relation := freshComparisonRelation(out.Keeper.Observation, member.Observation)
		out.Copies = append(out.Copies, HashFreshJobCopyComparison{Copy: member, Relation: relation})
		switch relation {
		case "historical_hashes_match":
			out.MatchingCopies++
		case "historical_hashes_differ":
			out.DifferingCopies++
		case "incomplete":
			out.IncompleteCopies++
		case "blocked":
			out.BlockedCopies++
		}
	}
	if out.BlockedCopies != 0 {
		out.Status = "blocked"
	} else if out.IncompleteCopies != 0 {
		out.Status = "incomplete"
	} else if out.DifferingCopies != 0 {
		out.Status = "historical_hashes_differ"
	}
	return out, nil
}

func validFreshComparisonObservation(w SavedFreshHashWork, member HashFreshJobComparisonMember, size int64) bool {
	if w.Ordinal != member.Ordinal || w.HistoricalWorkID != member.HistoricalWorkID || w.Role != member.Role || w.FileID != member.FileID || !bytes.Equal(w.PathBytes, member.PathBytes) || w.LogicalBytes != size || w.Sequence < 0 || w.DurableOffset < 0 || w.DurableOffset > size || w.Status != "pending" && w.Status != "running" && w.Status != "complete" && w.Status != "invalidated" || (w.Status == "invalidated") != (w.Code != "") || !w.CheckedAt.IsZero() && !validHashReadClock(w.CheckedAt) {
		return false
	}
	if w.Status == "complete" {
		if w.Sequence == 0 || w.DurableOffset != size || w.CheckedAt.IsZero() || !hashStoreDigest(w.SHA256) || w.LatestAttempt == nil || w.LatestAttempt.Status != "settled" {
			return false
		}
	} else if w.SHA256 != "" {
		return false
	}
	a := w.LatestAttempt
	if a == nil {
		return w.Status == "pending" && w.Sequence == 0 && w.DurableOffset == 0 && w.CheckedAt.IsZero()
	}
	if a.Status == "reserved" {
		return w.Status == "running" && a.RequestedBytes == nil && a.ReadBytes == nil && a.ElapsedNS == nil
	}
	if a.Status == "interrupted_unknown" {
		return w.Status == "pending" && w.Sequence > 0 && a.RequestedBytes == nil && a.ReadBytes == nil && a.ElapsedNS == nil
	}
	return a.Status == "settled" && w.Status != "running" && w.Sequence > 0 && a.RequestedBytes != nil && a.ReadBytes != nil && a.ElapsedNS != nil && *a.RequestedBytes >= 0 && *a.ReadBytes >= 0 && *a.ReadBytes <= *a.RequestedBytes && *a.RequestedBytes <= a.ReservedBytes && *a.ElapsedNS >= 0
}

func cloneFreshComparisonObservation(work SavedFreshHashWork) *SavedFreshHashWork {
	work.PathBytes = bytes.Clone(work.PathBytes)
	if work.LatestAttempt != nil {
		a := *work.LatestAttempt
		if a.RequestedBytes != nil {
			value := *a.RequestedBytes
			a.RequestedBytes = &value
		}
		if a.ReadBytes != nil {
			value := *a.ReadBytes
			a.ReadBytes = &value
		}
		if a.ElapsedNS != nil {
			value := *a.ElapsedNS
			a.ElapsedNS = &value
		}
		work.LatestAttempt = &a
	}
	return &work
}

func freshComparisonRelation(keeper, copy *SavedFreshHashWork) string {
	if keeper != nil && keeper.Status == "invalidated" || copy != nil && copy.Status == "invalidated" {
		return "blocked"
	}
	if keeper == nil || copy == nil || keeper.Status != "complete" || copy.Status != "complete" {
		return "incomplete"
	}
	if keeper.LogicalBytes == copy.LogicalBytes && keeper.SHA256 == copy.SHA256 {
		return "historical_hashes_match"
	}
	return "historical_hashes_differ"
}
