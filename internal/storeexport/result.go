// Package storeexport exports one committed plans store. It grants no authority
// to restore history, evaluate permission, or change source objects.
package storeexport

import (
	"errors"
	"time"
)

const Contract = "plans_store_committed_snapshot_v1"
const (
	maxPathBytes            = 4096
	maxDatabaseBytes  int64 = 1 << 30
	maxSidecarBytes   int64 = 32 << 20
	bufferBytes             = 64 << 10
	maxAttempts             = 32769
	maxManifestBytes        = 64 << 10
	maxManifestTokens       = 4096
	maxManifestDepth        = 16
	maxParentNames          = 255
	maxSchemaRows           = 64
	wholeLimit              = 120 * time.Second
)

var (
	ErrBounds  = errors.New("plans export exceeds its fixed bounds")
	ErrChanged = errors.New("plans export input or owned object changed")
	ErrProfile = errors.New("unsupported plans export profile")
	ErrClock   = errors.New("plans export clock moved backwards or its cooperative deadline expired")
)

// ExportResult remains useful after an error. A candidate can be retained, and
// publication can have happened before a later sync, close or cancellation error.
type ExportResult struct {
	Contract  string `json:"contract"`
	Candidate string `json:"candidate,omitempty"`
	// CandidateIdentityObserved is a historical successful stage-directory
	// observation. False makes Candidate inspection-only; true grants no
	// authority and does not establish the current object at that name.
	CandidateIdentityObserved bool   `json:"candidate_identity_observed"`
	Destination               string `json:"destination"`
	ExportID                  string `json:"export_id,omitempty"`
	Published                 bool   `json:"published"`
	Verified                  bool   `json:"structurally_verified"`
	SyncCompleted             bool   `json:"sync_api_completed"`
	// Retained paths record that this invocation left the named attempt for
	// inspection. They do not assert which object currently occupies the name.
	CandidateRetained                 bool  `json:"candidate_retained"`
	EngineClosureKnown                bool  `json:"engine_closure_known"`
	FullInstanceBackup                bool  `json:"full_instance_backup"`
	RestoreAvailable                  bool  `json:"restore_available"`
	CleanupAuthority                  bool  `json:"cleanup_authority"`
	CurrentPermissionEvaluated        bool  `json:"current_permission_evaluated"`
	RecordSemanticsChecked            bool  `json:"record_semantics_checked"`
	SourceBytesEqual                  *bool `json:"source_bytes_equal"`
	SourceStorageEffectsAuthenticated bool  `json:"source_storage_effects_authenticated"`
}

type VerificationResult struct {
	Contract string `json:"contract"`
	ExportID string `json:"export_id,omitempty"`
	Verified bool   `json:"structurally_verified"`
	Scratch  string `json:"scratch,omitempty"`
	// ScratchIdentityObserved has the same historical-only meaning as
	// CandidateIdentityObserved. An unconfirmed Scratch is inspection-only.
	ScratchIdentityObserved bool `json:"scratch_identity_observed"`
	// ScratchRetained does not establish the current object at Scratch.
	ScratchRetained            bool `json:"scratch_retained"`
	EngineClosureKnown         bool `json:"engine_closure_known"`
	RecordSemanticsChecked     bool `json:"record_semantics_checked"`
	RestoreAvailable           bool `json:"restore_available"`
	CleanupAuthority           bool `json:"cleanup_authority"`
	CurrentPermissionEvaluated bool `json:"current_permission_evaluated"`
}

type profile struct {
	ApplicationID  int64  `json:"application_id"`
	SchemaVersion  int64  `json:"schema_version"`
	StoreToken     string `json:"store_token,omitempty"`
	DismissalToken string `json:"dismissal_token,omitempty"`
	PageSize       int64  `json:"page_size"`
}
