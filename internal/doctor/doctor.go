// Package doctor observes one explicit existing instance. It never opens SQLite,
// locks a writer, contacts a worker/provider, repairs records or probes scan roots.
package doctor

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

const Contract = "existing_private_configured_instance_metadata_config_main_header_v1"
const MaxReportBytes = 128 << 10
const MaxConfigBytes = 1 << 20
const MaxPathComponents = 64
const MaxPathBytes = 4074
const MaxConfiguredPathBytes = 64 << 10
const MaxConfiguredPaths = 128
const HeaderBytes = 100
const MaxMetadataAttempts = 2048
const MaxReadAttempts = 4096

var ErrArguments = errors.New("doctor requires one explicit clean absolute private data directory within its path profile")
var ErrPlatform = errors.New("doctor filesystem observations are supported only on macOS and Linux")
var ErrAttention = errors.New("selected doctor observations require attention; no repair was performed")
var ErrIncomplete = errors.New("doctor observations could not be completed")
var ErrChanged = errors.New("selected doctor objects changed during observation")
var ErrBounds = errors.New("doctor observation exceeds its fixed bounds")
var ErrConfigInvalid = errors.New("configuration did not pass current validation")
var ErrConfigUnsupported = errors.New("configuration is outside the bounded doctor profile")
var ErrConfigEnvironment = errors.New("configuration path expansion cannot be validated in this environment")

// HeaderCatalog is supplied by the known CLI's pure state catalog. It is not an
// argv/environment override, an engine query or proof of the effective schema.
type HeaderCatalog struct {
	ApplicationID   uint32
	MinReaderSchema uint32
	MaxReaderSchema uint32
}

type Request struct {
	DataDirectory string
	Catalog       HeaderCatalog
}

type Configuration struct {
	Version    int `json:"version"`
	Roots      int `json:"root_count"`
	Exclusions int `json:"exclusion_count"`
}

type ConfigValidator func([]byte) (Configuration, error)

type Metadata struct {
	Kind              string `json:"kind"`
	LogicalBytes      *int64 `json:"logical_bytes"`
	Suitable          *bool  `json:"private_owned_single_link"`
	IdentityRechecked bool   `json:"identity_rechecked"`
}

type Header struct {
	MagicMatches  bool   `json:"magic_matches"`
	PageSize      uint32 `json:"page_size"`
	WriteVersion  uint8  `json:"write_version"`
	ReadVersion   uint8  `json:"read_version"`
	UserVersion   uint32 `json:"header_user_version_u32"`
	ApplicationID uint32 `json:"header_application_id_u32"`
	Declaration   string `json:"declaration"`
}

type Check struct {
	ID            string         `json:"id"`
	RelativeName  *string        `json:"relative_name"`
	Status        string         `json:"observation_status"`
	Reason        string         `json:"reason_code"`
	Attention     bool           `json:"attention_required"`
	ObservedAt    *string        `json:"observed_at"`
	Metadata      *Metadata      `json:"metadata"`
	Header        *Header        `json:"header"`
	Configuration *Configuration `json:"configuration"`
	Qualification string         `json:"qualification"`
}

type Guidance struct {
	ID            string   `json:"id"`
	Basis         []string `json:"basis_check_ids"`
	Inferred      bool     `json:"inferred"`
	Message       string   `json:"message"`
	Argv          []string `json:"argv"`
	Effect        string   `json:"effect"`
	Preconditions []string `json:"preconditions"`
	Automatic     bool     `json:"automatic"`
}

type Limits struct {
	DirectoryComponents  int `json:"selected_directory_components"`
	DirectoryDescriptors int `json:"directory_descriptors"`
	RegularDescriptors   int `json:"regular_file_descriptors"`
	RetainedDescriptors  int `json:"retained_descriptors"`
	DerivedPathBytes     int `json:"derived_path_bytes"`
	Children             int `json:"known_child_names"`
	ConfigBytes          int `json:"configuration_bytes"`
	HeaderBytes          int `json:"state_header_bytes"`
	BodyBytes            int `json:"total_selected_body_bytes"`
	MetadataAttempts     int `json:"metadata_or_open_attempts"`
	ReadAttempts         int `json:"body_read_attempts"`
	ContextSeconds       int `json:"cooperative_context_seconds"`
	ReportBytes          int `json:"report_bytes"`
}

type Effects struct {
	Initialization              bool `json:"initialization"`
	Migration                   bool `json:"migration"`
	Recovery                    bool `json:"recovery"`
	Repair                      bool `json:"repair"`
	RecordWrites                bool `json:"record_writes"`
	FileCreation                bool `json:"file_creation"`
	FileRemoval                 bool `json:"file_removal"`
	LockAcquisition             bool `json:"lock_acquisition"`
	WorkerControl               bool `json:"worker_control"`
	ManagerContact              bool `json:"manager_contact"`
	PowerSampling               bool `json:"power_sampling"`
	ConfiguredRootProbes        bool `json:"configured_root_probes"`
	ManualInventoryEnumeration  bool `json:"manual_inventory_enumeration"`
	PlansStoreOpen              bool `json:"plans_store_open"`
	HashStoreOpen               bool `json:"hash_store_open"`
	CurrentWorkerHealthVerified bool `json:"current_worker_health_verified"`
	CleanupAuthority            bool `json:"cleanup_authority"`
	HardLatencyBound            bool `json:"hard_latency_bound"`
}

type Report struct {
	attentionError     error      // Internal causes are never serialized or printed.
	Contract           string     `json:"contract"`
	Source             string     `json:"source"`
	DataDirectory      string     `json:"data_directory"`
	Status             string     `json:"status"`
	StartedAt          *string    `json:"started_at"`
	FinishedAt         *string    `json:"finished_at"`
	Checks             []Check    `json:"checks"`
	Guidance           []Guidance `json:"guidance"`
	RequestedBodyBytes *int64     `json:"requested_body_bytes"`
	ReadBodyBytes      *int64     `json:"read_body_bytes"`
	Effects            Effects    `json:"effects"`
	Limits             Limits     `json:"limits"`
	Unchecked          []string   `json:"unchecked"`
	CurrentStateSchema *uint32    `json:"current_effective_state_schema"`
	LedgerIntegrity    string     `json:"whole_migration_ledger_integrity"`
	WorkerActivity     string     `json:"worker_activity"`
	LockAvailability   string     `json:"writer_lock_availability"`
	Qualification      string     `json:"qualification"`
}

var childNames = [...]string{"config.toml", "state.sqlite3", "state.sqlite3-wal", "state.sqlite3-shm", "state.sqlite3-journal", "writer.lock"}
var checkIDs = [...]string{"directory", "configuration", "state_header", "wal", "shm", "journal", "writer_lock"}

// ValidateDirectory performs no filesystem or environment access. The 64 depth
// limit counts selected directory components, not the fixed child filename.
func ValidateDirectory(path string) error {
	if path == "" || len(path) > MaxPathBytes || !utf8.ValidString(path) || strings.IndexByte(path, 0) >= 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) == path {
		return ErrArguments
	}
	if len(directoryComponents(path)) > MaxPathComponents {
		return ErrArguments
	}
	for _, name := range childNames {
		if len(filepath.Join(path, name)) > 4096 {
			return ErrArguments
		}
	}
	return nil
}

func directoryComponents(path string) []string {
	path = strings.TrimPrefix(path, filepath.VolumeName(path))
	path = strings.TrimPrefix(path, string(filepath.Separator))
	if path == "" {
		return nil
	}
	return strings.Split(path, string(filepath.Separator))
}

func fixedLimits() Limits {
	return Limits{64, 65, 2, 67, 4096, 6, MaxConfigBytes, HeaderBytes, MaxConfigBytes + HeaderBytes, MaxMetadataAttempts, MaxReadAttempts, 5, MaxReportBytes}
}

func observedTime() *string {
	s := time.Now().UTC().Format(time.RFC3339Nano)
	return &s
}

func newReport(path string) Report {
	requested, read := int64(0), int64(0)
	r := Report{Contract: Contract, Source: "selected_existing_metadata_configuration_main_header", DataDirectory: path, Status: "incomplete", StartedAt: observedTime(), RequestedBodyBytes: &requested, ReadBodyBytes: &read, Limits: fixedLimits(), LedgerIntegrity: "not_checked", WorkerActivity: "unknown", LockAvailability: "unknown", Guidance: []Guidance{}, Unchecked: []string{"current_state_schema", "database_integrity", "migration_ledger", "writer_activity", "lock_availability", "service_runtime", "power", "scan_root_availability", "manual_inventory_coverage"}, Qualification: "These are paired historical observations, not an atomic snapshot or proof of live health. Current schema and whole ledger integrity are unknown. Access times can change. Entered native calls, parsing and output can exceed the cooperative deadline."}
	for i, id := range checkIDs {
		c := Check{ID: id, Status: "not_checked", Reason: "not_checked", Qualification: "Not checked; no conclusion about current health or permission."}
		if i > 0 {
			name := childNames[i-1]
			c.RelativeName = &name
		}
		r.Checks = append(r.Checks, c)
	}
	return r
}

// Observe uses only the platform's selected existing reader. The callback is
// supplied by the known CLI; it cannot be selected through user options.
func Observe(ctx context.Context, req Request, validate ConfigValidator) (Report, error) {
	return observe(ctx, req, validate, readExisting)
}

func observe(ctx context.Context, req Request, validate ConfigValidator, reader func(context.Context, Request, ConfigValidator, *Report) error) (Report, error) {
	if ctx == nil || ValidateDirectory(req.DataDirectory) != nil || req.Catalog.ApplicationID == 0 || req.Catalog.MinReaderSchema < 1 || req.Catalog.MaxReaderSchema < req.Catalog.MinReaderSchema {
		return Report{}, ErrArguments
	}
	if !platformSupported() {
		return Report{}, ErrPlatform
	}
	if validate == nil || reader == nil {
		return Report{}, ErrIncomplete
	}
	work, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r := newReport(req.DataDirectory)
	err := work.Err()
	if err == nil {
		err = reader(work, req, validate, &r)
	}
	err = errors.Join(err, work.Err())
	r.FinishedAt = observedTime()
	if err != nil {
		r.Status = "incomplete"
		err = errors.Join(ErrIncomplete, err, r.attentionError)
	} else {
		r.Status = "checks_finished"
		for _, c := range r.Checks {
			if c.Attention {
				r.Status = "attention_required"
				err = errors.Join(ErrAttention, r.attentionError)
				break
			}
		}
	}
	r.Guidance = guidance(r)
	return r, err
}

func checkStatus(c *Check, status, reason, qualification string, attention bool) {
	c.Status, c.Reason, c.Qualification, c.Attention = status, reason, qualification, attention
	c.ObservedAt = observedTime()
}

func headerObservation(body []byte, catalog HeaderCatalog) (*Header, string) {
	if len(body) != HeaderBytes {
		return nil, "incomplete_header"
	}
	read32 := func(at int) uint32 {
		return uint32(body[at])<<24 | uint32(body[at+1])<<16 | uint32(body[at+2])<<8 | uint32(body[at+3])
	}
	h := &Header{MagicMatches: string(body[:16]) == "SQLite format 3\x00", PageSize: uint32(body[16])<<8 | uint32(body[17]), WriteVersion: body[18], ReadVersion: body[19], UserVersion: read32(60), ApplicationID: read32(68)}
	if h.PageSize == 1 {
		h.PageSize = 65536
	}
	if !h.MagicMatches || h.PageSize < 512 || h.PageSize > 65536 || h.PageSize&(h.PageSize-1) != 0 || (h.ReadVersion != 1 && h.ReadVersion != 2) || (h.WriteVersion != 1 && h.WriteVersion != 2) {
		h.Declaration = "malformed_header"
		return h, "malformed_header"
	}
	switch {
	case h.ApplicationID != catalog.ApplicationID:
		h.Declaration = "foreign_header"
	case h.UserVersion >= catalog.MinReaderSchema && h.UserVersion <= catalog.MaxReaderSchema:
		h.Declaration = "declared_reader_range"
	case h.UserVersion >= 1 && h.UserVersion < catalog.MinReaderSchema:
		h.Declaration = "legacy_declared"
	default:
		h.Declaration = "outside_declared_range"
	}
	return h, h.Declaration
}

func guidance(r Report) []Guidance {
	items := []Guidance{
		{ID: "check_build", Inferred: true, Message: "Compare declared binary metadata before choosing another command.", Argv: []string{"rydd", "--json", "--version"}, Effect: "No configuration or state opening; declared build provenance is not authentication.", Basis: []string{"state_header"}, Preconditions: []string{}},
		{ID: "discover_supported_commands", Inferred: true, Message: "Inspect implemented commands and their effects.", Argv: []string{"rydd", "capabilities", "--json"}, Effect: "No instance initialization or configuration/state reads.", Basis: []string{"directory", "configuration", "state_header"}, Preconditions: []string{}},
	}
	if len(r.Checks) != len(checkIDs) || r.Checks[0].Status != "present" {
		return items
	}
	cfg, header := r.Checks[1], r.Checks[2]
	prefix := []string{"rydd", "--data-dir", r.DataDirectory}
	argv := func(tail ...string) []string { return append(append([]string{}, prefix...), tail...) }
	if cfg.Metadata != nil && cfg.Metadata.Suitable != nil && *cfg.Metadata.Suitable && cfg.Metadata.IdentityRechecked && cfg.Status != "changed" && cfg.Status != "absent" {
		items = append(items, Guidance{ID: "check_selected_configuration", Inferred: true, Basis: []string{"configuration"}, Message: "Request detailed validation of this selected configuration explicitly.", Argv: argv("config", "check", "--json"), Effect: "Reads existing configuration and may disclose configured paths/errors; it does not inspect scan roots.", Preconditions: []string{}})
	}
	if r.Status != "incomplete" && cfg.Status == "valid_config" && cfg.Metadata != nil && cfg.Metadata.IdentityRechecked && header.Header != nil && header.Header.Declaration != "foreign_header" && header.Header.Declaration != "malformed_header" && header.Metadata != nil && header.Metadata.IdentityRechecked {
		items = append(items, Guidance{ID: "inspect_saved_and_worker_status", Inferred: true, Basis: []string{"configuration", "state_header", "writer_lock"}, Message: "Inspect saved state and request worker status as a separate operation.", Argv: argv("status", "--json"), Effect: "Opens existing read-only SQLite, queries saved inventory/accounting and attempts worker IPC. Reader sidecars/access times may change.", Preconditions: []string{"This is separate from doctor and can still refuse; it grants no repair permission."}})
	}
	canInit := cfg.Status == "valid_config" && cfg.Metadata != nil && cfg.Metadata.IdentityRechecked && r.Checks[0].Metadata != nil && r.Checks[0].Metadata.IdentityRechecked && (header.Status == "absent" || (header.Status == "legacy_declared" && header.Metadata != nil && header.Metadata.IdentityRechecked)) && r.Status != "incomplete"
	for _, index := range []int{3, 4, 5} {
		c := r.Checks[index]
		if c.Status != "absent" && (c.Status != "present" || c.Metadata == nil || !c.Metadata.IdentityRechecked || c.Metadata.LogicalBytes == nil || *c.Metadata.LogicalBytes != 0 || c.Metadata.Suitable == nil || !*c.Metadata.Suitable) {
			canInit = false
		}
	}
	if canInit {
		items = append(items, Guidance{ID: "explicit_state_initialization_or_migration", Inferred: true, Basis: []string{"directory", "configuration", "state_header", "wal", "shm", "journal"}, Message: "Only after the listed owner checks, choose explicit state initialization or migration.", Argv: argv("state", "init", "--json"), Effect: "MUTATING separate command: can initialize/migrate inventory and synchronize configured roots. Doctor never runs it.", Preconditions: []string{"Confirm this exact intended instance and preserve existing data.", "Stop all writers and wait for process exit; lock metadata cannot establish shutdown.", "Review configuration/roots and compatibility; header observations alone do not prove migration is valid."}})
	}
	return items
}
