package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// Saved consent is historical evidence. A reader must neither evaluate current
// permission nor infer remaining quota from its recorded limits and clocks.
func printHashReadConsent(out io.Writer, consent *inventory.HashReadConsent) {
	if consent == nil {
		return
	}
	printResultBanner(out, "SAVED FULL-FILE READ CONSENT")
	fmt.Fprintf(out, "Read consent ID: %s\n", consent.ID)
	status := "Unknown saved state"
	switch consent.Status {
	case "recorded":
		status = "Consent recorded; current permission not checked"
	case "revoked":
		status = "Revocation recorded"
	case "expired_observed":
		status = "Expiry observed by a writer"
	}
	printField(out, "Saved consent state", status)
	printField(out, "Recorded at", consent.Approval.CreatedAt.UTC().Format(time.RFC3339Nano))
	printField(out, "Recorded expiry", consent.Approval.ExpiresAt.UTC().Format(time.RFC3339Nano))
	for _, field := range []struct {
		label string
		bytes int64
	}{
		{"Step reservation cap", consent.Approval.StepByteLimit},
		{"Day reservation cap", consent.Approval.DailyReservedByteLimit},
		{"Lifetime reservation cap", consent.Approval.LifetimeReservedByteLimit},
		{"Charges before approval", consent.Approval.InitialTotalReservedBytes},
	} {
		printField(out, field.label, fmt.Sprintf("%d bytes", field.bytes))
	}
	printField(out, "Consent clock observation", consent.ClockHighWater.UTC().Format(time.RFC3339Nano))
	if consent.Revocation != nil {
		printField(out, "Recorded revocation", consent.Revocation.RecordedAt.UTC().Format(time.RFC3339Nano))
	}
	if consent.ExpiredObserved {
		printWrapped(out, "A writer recorded that this approval expired. This view does not make a new expiry observation.", "")
	}
	printField(out, "Current read permission", "Not evaluated by this saved-only view")
	printWrapped(out, "These fixed caps cover the whole saved selection. Lifetime charges include earlier, canceled and unknown reservations. Day charges follow the saved reservation day; they do not measure reads per wall-clock day. This view does not calculate remaining quota or time to expiry, and it does not start or recover reads. Read consent does not authorize cleanup.", "")
}
