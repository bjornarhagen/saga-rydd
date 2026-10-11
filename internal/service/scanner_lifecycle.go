package service

const (
	ScannerLifecycleContract   = "service_scanner_artifact_lifecycle_v1"
	ScannerRuntimeContract     = "service_scanner_runtime_request_v1"
	ScannerLoginLinkContract   = "service_scanner_linux_login_link_v1"
	ScannerRemovalContract     = "service_scanner_descriptor_removal_v1"
	ScannerObservationContract = "service_scanner_runtime_observation_v1"
)

// ScannerLifecycleIntent describes the selected API scope, never an observed
// worker, loaded registration, configured-root permission or cleanup authority.
type ScannerLifecycleIntent struct {
	Contract                         string  `json:"contract"`
	WorkerProfile                    Profile `json:"worker_profile"`
	ActivationFlag                   string  `json:"activation_flag"`
	ConfigurationScope               string  `json:"configuration_scope"`
	DescriptorStartMayScan           bool    `json:"descriptor_start_may_scan"`
	LifecycleSupported               bool    `json:"lifecycle_supported"`
	NativeLifecycleVerified          bool    `json:"native_lifecycle_verified"`
	ConfiguredRootsVerified          bool    `json:"configured_roots_verified"`
	CurrentSourcePermissionEvaluated bool    `json:"current_source_permission_evaluated"`
	RepresentativeResourcesVerified  bool    `json:"representative_resources_verified"`
	CleanupAuthority                 bool    `json:"cleanup_authority"`
}

// buildLifecycle only admits the closed explicit profile. It neither examines
// an existing artifact nor infers a profile from a loaded manager registration.
func buildLifecycle(spec Spec) (Descriptor, error) {
	d, err := Build(spec)
	if err != nil {
		return Descriptor{}, err
	}
	if d.ScannerIntent != nil {
		d.ScannerIntent = nil
		d.ScannerLifecycleIntent = &ScannerLifecycleIntent{
			Contract:               "service_scanner_lifecycle_intent_v1",
			WorkerProfile:          ProfileExperimentalInventory,
			ActivationFlag:         "--experimental-scan",
			ConfigurationScope:     "configured_roots_at_future_worker_start",
			DescriptorStartMayScan: true, LifecycleSupported: true,
		}
	}
	return d, nil
}

func lifecycleContract(d Descriptor, idle, scanner string) string {
	if d.ScannerLifecycleIntent != nil {
		return scanner
	}
	return idle
}
