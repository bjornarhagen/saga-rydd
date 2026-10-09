package powerinfo

import (
	"context"
	"testing"
)

func TestPowerObservationDarwinUnsupportedWithoutProbe(t *testing.T) {
	o, err := Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if o.Platform != "darwin" || o.Profile != "unsupported" || o.Status != "unknown" || o.Reason != "unsupported_platform" || o.SystemBatteryDischargingObserved != nil || o.SupplyEntriesObserved != nil || o.AttributeAttempts != 0 || o.ReturnedAttributeBytes != 0 || o.CoverageComplete || !o.SequentialObservations || o.PhysicalPowerVerified || o.NamespaceAuthenticated {
		t.Fatalf("unsupported platform fabricated state: %+v", o)
	}
}
