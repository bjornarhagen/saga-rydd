//go:build darwin || linux

package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"
)

const LoginLinkContract = "service_linux_login_link_v1"

var (
	ErrLoginLinkConflict = errors.New("selected login link is foreign or differs from the exact target")
	ErrLoginLinkChanged  = errors.New("selected login link or its directory changed")
	ErrLoginLinkOutcome  = errors.New("selected login link change requires inspection with the exact scope")
)

// LoginLinkResult describes just one default.target dependency link. It never
// authenticates its creator or establishes global enablement, current target
// dependencies, runtime or next-login behavior. Manager preflight may LoadUnit;
// it sends no enable/disable/start/stop/reload request.
type LoginLinkResult struct {
	Contract                    string             `json:"contract"`
	Action                      string             `json:"action"`
	Scope                       string             `json:"scope"`
	Platform                    string             `json:"platform"`
	ManagerProfile              string             `json:"manager_profile"`
	Label                       string             `json:"label"`
	Directory                   string             `json:"directory"`
	DescriptorPath              string             `json:"descriptor_path"`
	DescriptorSHA256            string             `json:"descriptor_sha256"`
	Executable                  string             `json:"executable"`
	ConfigFile                  string             `json:"config_file"`
	StateDir                    string             `json:"state_dir"`
	RuntimeDir                  string             `json:"runtime_dir"`
	Argv                        []string           `json:"argv"`
	Env                         map[string]string  `json:"env"`
	Manager                     ManagerObservation `json:"manager"`
	ManagerUniqueName           string             `json:"manager_unique_name"`
	UnitObjectPath              string             `json:"unit_object_path"`
	UnitLoadAttempted           bool               `json:"unit_load_attempted"`
	LoadedBindingMatched        *bool              `json:"loaded_binding_matched"`
	BindingCheckedAt            *time.Time         `json:"binding_checked_at"`
	LinkPath                    string             `json:"link_path"`
	LinkTarget                  string             `json:"link_target"`
	LinkStatus                  string             `json:"link_status"`
	LinkPresent                 *bool              `json:"link_present"`
	LinkObservedAt              *time.Time         `json:"link_observed_at"`
	LinkOriginVerified          bool               `json:"link_origin_verified"`
	ChangeStatus                string             `json:"change_status"`
	ChangeAttempted             bool               `json:"change_attempted"`
	ChangeCompleted             bool               `json:"change_completed"`
	RemovalObserved             *bool              `json:"removal_observed"`
	SyncCompleted               bool               `json:"sync_completed"`
	DirectoriesCreated          []string           `json:"directories_created"`
	DirectorySyncCompleted      bool               `json:"directory_sync_completed"`
	Enabled                     *bool              `json:"enabled"`
	Running                     *bool              `json:"running"`
	Stopped                     *bool              `json:"stopped"`
	FutureLoginMayStart         *bool              `json:"future_login_may_start"`
	EffectiveEnablementVerified bool               `json:"effective_enablement_verified"`
	RuntimeStateVerified        bool               `json:"runtime_state_verified"`
	LoadedOriginVerified        bool               `json:"loaded_origin_verified"`
	ManagerEnablementRequested  bool               `json:"manager_enablement_requested"`
	StartRequested              bool               `json:"start_requested"`
	StopRequested               bool               `json:"stop_requested"`
	ReloadRequested             bool               `json:"reload_requested"`
	ScanningRequested           bool               `json:"scanning_requested"`
	DescriptorChanged           bool               `json:"descriptor_changed"`
	OtherLinksChanged           bool               `json:"other_links_changed"`
	DataRemoved                 bool               `json:"data_removed"`
	ExecutableRemoved           bool               `json:"executable_removed"`
}

// EnableLoginLink creates only the fixed selected .wants link. A matching
// existing link is a no-op regardless of creator; its origin remains unknown.
func EnableLoginLink(ctx context.Context, spec InstallSpec) (LoginLinkResult, error) {
	return nativeLoginLink(ctx, spec, "enable-login")
}

// DisableLoginLink removes only the fixed matching link, including a manually
// created identical link at that explicitly selected name. Other links remain.
// The exact descriptor must still exist; disable before descriptor removal.
func DisableLoginLink(ctx context.Context, spec InstallSpec) (LoginLinkResult, error) {
	return nativeLoginLink(ctx, spec, "disable-login")
}

func nativeLoginLink(ctx context.Context, spec InstallSpec, action string) (LoginLinkResult, error) {
	if runtime.GOOS != "linux" || spec.Service.GOOS != "linux" {
		return LoginLinkResult{}, fmt.Errorf("%w: fixed login link controls require native Linux", ErrSpec)
	}
	return controlLoginLink(ctx, spec, action, loginLinkHooks{})
}

type loginLinkHooks struct {
	manager      runtimeHooks
	beforeChange func()
	afterChange  func()
	symlink      func(string, int, string) error
	unlink       func(int, string) error
	syncParent   func(*os.File) error
}
