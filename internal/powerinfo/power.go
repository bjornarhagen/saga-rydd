// Package powerinfo makes bounded, sequential power-supply observations.
// It does not establish physical power state or authenticate a namespace.
package powerinfo

import (
	"context"
	"errors"
	"runtime"
	"time"
)

const (
	Contract                  = "power_observation_v1"
	LinuxProfile              = "linux_sysfs_power_v1"
	MaxSupplyEntries          = 32
	MaxAttributeBytes         = 64
	MaxAttributeAttempts      = 5 * MaxSupplyEntries
	MaxReturnedAttributeBytes = (MaxAttributeBytes + 1) * MaxAttributeAttempts
	ObservationTimeout        = 5 * time.Second
)

var (
	ErrNilContext        = errors.New("power observation requires a context")
	ErrWallClockRollback = errors.New("power observation wall clock moved backwards")
)

// Observation contains an existential witness, not an instantaneous or
// whole-machine power-state claim. SystemBatteryDischargingObserved is true
// only when a declared System battery was observed present and discharging.
// Its absence is unknown; this API never returns a false power-state value.
// SupplyEntriesObserved includes an overflow sentinel and is not a total.
type Observation struct {
	Contract                         string    `json:"contract"`
	Platform                         string    `json:"platform"`
	Profile                          string    `json:"profile"`
	Status                           string    `json:"status"`
	Reason                           string    `json:"reason"`
	StartedAt                        time.Time `json:"started_at"`
	FinishedAt                       time.Time `json:"finished_at"`
	SystemBatteryDischargingObserved *bool     `json:"system_battery_discharging_observed"`
	SupplyEntriesObserved            *int      `json:"supply_entries_observed"`
	ProvidersProcessed               int       `json:"providers_processed"`
	ConfirmedSystemBatteries         int       `json:"confirmed_system_batteries"`
	DischargingSystemBatteries       int       `json:"discharging_system_batteries"`
	AmbiguousProviders               int       `json:"ambiguous_providers"`
	AttributeAttempts                int       `json:"attribute_attempts"`
	ReturnedAttributeBytes           int       `json:"returned_attribute_bytes"`
	CoverageComplete                 bool      `json:"coverage_complete"`
	SequentialObservations           bool      `json:"sequential_observations"`
	NamespaceAuthenticated           bool      `json:"namespace_authenticated"`
	PhysicalPowerVerified            bool      `json:"physical_power_verified"`
}

// Observe uses only the supported platform's fixed power-supply scope. Its
// deadline is cooperative: a kernel provider callback can remain blocked.
// The function creates no goroutine and reads no model, serial or device name
// attribute. Unsupported/unavailable sources return a qualified unknown
// observation. Cancellation returns no usable observation.
func Observe(ctx context.Context) (Observation, error) {
	return observe(ctx, runtime.GOOS, platformOpen, time.Now)
}

type source interface {
	names(context.Context, int) ([]string, error)
	provider(context.Context, string) (supply, error)
	check(context.Context) error
	close() error
}

type supply interface {
	attribute(context.Context, string) ([]byte, error)
	close() error
}

type openSource func(context.Context) (source, error)

type sourceError struct{ reason string }

func (e sourceError) Error() string { return "power observation unavailable: " + e.reason }

func reason(err error) string {
	var known sourceError
	if errors.As(err, &known) {
		return known.reason
	}
	return "provider_unavailable"
}

type wallGuardKey struct{}
type wallGuard struct {
	now            func() time.Time
	high, deadline time.Time
	denied         error
}

// Observation is synchronous; this operation-local guard is never shared
// with a background sampler. Wall readings omit their monotonic component.
func canceled(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if guard, ok := ctx.Value(wallGuardKey{}).(*wallGuard); ok {
		return guard.accept(guard.now().Round(0).UTC())
	}
	return nil
}

func (guard *wallGuard) accept(stamp time.Time) error {
	if guard.denied != nil {
		return guard.denied
	}
	if stamp.Before(guard.high) {
		guard.denied = ErrWallClockRollback
		return guard.denied
	}
	guard.high = stamp
	if !stamp.Before(guard.deadline) {
		guard.denied = context.DeadlineExceeded
		return guard.denied
	}
	return nil
}

// Every exported timestamp participates in the same high-water/deadline
// check. A later restored clock cannot erase an invalid finish reading.
func checkedStamp(ctx context.Context, now func() time.Time) (time.Time, error) {
	if err := context.Cause(ctx); err != nil {
		return time.Time{}, err
	}
	stamp := now().Round(0).UTC()
	if guard, ok := ctx.Value(wallGuardKey{}).(*wallGuard); ok {
		if err := guard.accept(stamp); err != nil {
			return time.Time{}, err
		}
	}
	return stamp, nil
}

func observe(parent context.Context, platform string, open openSource, now func() time.Time) (Observation, error) {
	if parent == nil {
		return Observation{}, ErrNilContext
	}
	ctx, cancel := context.WithTimeout(parent, ObservationTimeout)
	defer cancel()
	started := now().Round(0).UTC()
	ctx = context.WithValue(ctx, wallGuardKey{}, &wallGuard{now: now, high: started, deadline: started.Add(ObservationTimeout)})
	if err := canceled(ctx); err != nil {
		return Observation{}, err
	}
	o := Observation{Contract: Contract, Platform: platform, Profile: LinuxProfile, Status: "unknown", Reason: "no_system_battery_discharge_witness", StartedAt: started, SequentialObservations: true}
	if platform != "linux" {
		o.Profile = "unsupported"
	}
	s, err := open(ctx)
	if err != nil {
		if err := canceled(ctx); err != nil {
			return Observation{}, err
		}
		o.Reason = reason(err)
		finished, err := checkedStamp(ctx, now)
		if err != nil {
			return Observation{}, err
		}
		o.FinishedAt = finished
		if err := canceled(ctx); err != nil {
			return Observation{}, err
		}
		return o, nil
	}
	closed := false
	defer func() {
		if !closed {
			_ = s.close()
		}
	}()
	if err := canceled(ctx); err != nil {
		return Observation{}, err
	}
	names, err := s.names(ctx, MaxSupplyEntries+1)
	if err != nil {
		o.Reason = "enumeration_unavailable"
	} else {
		n := len(names)
		if n > MaxSupplyEntries+1 {
			n = MaxSupplyEntries + 1
			names = names[:n]
		}
		o.SupplyEntriesObserved = &n
		o.CoverageComplete = n <= MaxSupplyEntries
		if !o.CoverageComplete {
			o.Reason = "supply_entry_limit"
			names = names[:MaxSupplyEntries]
		}
		for _, name := range names {
			if err := canceled(ctx); err != nil {
				return Observation{}, err
			}
			o.ProvidersProcessed++
			p, err := s.provider(ctx, name)
			if err != nil {
				o.ambiguous(reason(err))
				continue
			}
			classification, err := classify(ctx, p, &o)
			closeErr := p.close()
			if err := canceled(ctx); err != nil {
				return Observation{}, err
			}
			if err != nil || closeErr != nil {
				o.ambiguous("provider_unavailable")
				continue
			}
			if classification.ambiguous != "" {
				o.ambiguous(classification.ambiguous)
			}
			if classification.battery {
				o.ConfirmedSystemBatteries++
			}
			if classification.discharging {
				o.DischargingSystemBatteries++
			}
		}
	}
	if err := canceled(ctx); err != nil {
		return Observation{}, err
	}
	// Recheck held/named scope before publishing any witness. This is not
	// authentication against deliberate replacement between checks.
	if err := s.check(ctx); err != nil {
		o.CoverageComplete = false
		o.Reason = "scope_changed"
		o.DischargingSystemBatteries = 0
	}
	closeErr := s.close()
	closed = true
	if closeErr != nil {
		o.CoverageComplete = false
		o.Reason = "source_close_failed"
		o.DischargingSystemBatteries = 0
	}
	if err := canceled(ctx); err != nil {
		return Observation{}, err
	}
	if o.DischargingSystemBatteries > 0 {
		witness := true
		o.SystemBatteryDischargingObserved = &witness
		o.Status = "system_battery_discharging_observed"
		o.Reason = "confirmed_system_battery_discharge_witness"
	}
	finished, err := checkedStamp(ctx, now)
	if err != nil {
		return Observation{}, err
	}
	o.FinishedAt = finished
	if err := canceled(ctx); err != nil {
		return Observation{}, err
	}
	return o, nil
}

func (o *Observation) ambiguous(why string) {
	o.AmbiguousProviders++
	o.CoverageComplete = false
	if o.Reason == "no_system_battery_discharge_witness" {
		o.Reason = why
	}
}

type classification struct {
	battery, discharging bool
	ambiguous            string
}

func readToken(ctx context.Context, p supply, name string, o *Observation) (string, error) {
	if err := canceled(ctx); err != nil {
		return "", err
	}
	if o.AttributeAttempts >= MaxAttributeAttempts {
		return "", sourceError{"attribute_attempt_limit"}
	}
	o.AttributeAttempts++
	b, err := p.attribute(ctx, name)
	if len(b) > MaxAttributeBytes+1 {
		b = b[:MaxAttributeBytes+1]
		err = sourceError{"attribute_byte_limit"}
	}
	o.ReturnedAttributeBytes += len(b)
	if err != nil {
		return "", err
	}
	if len(b) == 0 || len(b) > MaxAttributeBytes {
		return "", sourceError{"malformed_attribute"}
	}
	if b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return "", sourceError{"malformed_attribute"}
	}
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return "", sourceError{"malformed_attribute"}
		}
	}
	return string(b), nil
}

func classify(ctx context.Context, p supply, o *Observation) (classification, error) {
	typ, typeErr := readToken(ctx, p, "type", o)
	scope, scopeErr := readToken(ctx, p, "scope", o)
	if err := canceled(ctx); err != nil {
		return classification{}, err
	}
	// A declared Device provider is outside this profile even if its type
	// property is absent. A missing/Unknown scope is never assumed System.
	if scopeErr == nil && scope == "Device" {
		return classification{}, nil
	}
	if scopeErr != nil || scope != "System" {
		return classification{ambiguous: "system_scope_unconfirmed"}, nil
	}
	if typeErr != nil {
		return classification{ambiguous: "supply_type_unconfirmed"}, nil
	}
	if typ != "Battery" {
		switch typ {
		case "Mains", "USB", "USB_DCP", "USB_CDP", "USB_ACA", "USB_C", "USB_PD", "USB_PD_DRP", "BrickID", "Wireless":
			online, err := readToken(ctx, p, "online", o)
			if err != nil || (online != "0" && online != "1" && online != "2") {
				return classification{ambiguous: "external_supply_unconfirmed"}, nil
			}
			return classification{}, nil
		default:
			return classification{ambiguous: "supply_type_outside_profile"}, nil
		}
	}
	present, err := readToken(ctx, p, "present", o)
	if err != nil || (present != "0" && present != "1") {
		// The kernel ABI assumes present when the property is omitted. This
		// deliberately narrower witness requires an explicit value of one.
		return classification{ambiguous: "battery_presence_unconfirmed"}, nil
	}
	if present == "0" {
		return classification{}, nil
	}
	status, err := readToken(ctx, p, "status", o)
	if err != nil {
		return classification{ambiguous: "battery_status_unconfirmed"}, nil
	}
	switch status {
	case "Discharging":
		return classification{battery: true, discharging: true}, nil
	case "Charging", "Not charging", "Full":
		return classification{battery: true}, nil
	default:
		return classification{ambiguous: "battery_status_unconfirmed"}, nil
	}
}
