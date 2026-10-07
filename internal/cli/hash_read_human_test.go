package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

func TestHashCLISavedReadConsentViewsOfflineAndNoRecovery(t *testing.T) {
	ctx := context.Background()
	f := newHashProposalCLIFixture(t)
	code, raw, stderr := f.run(ctx, f.selectArgs()...)
	proposal := hashCLIProposal(t, code, raw, stderr)
	// A coherent reservation supplies earlier charges without reading source
	// bytes. Saved-only views must preserve its running/null-usage state.
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	for _, query := range []struct {
		text string
		args []any
	}{
		{"UPDATE hash_work SET status='running' WHERE id=1", nil},
		{"INSERT INTO hash_attempt VALUES(1,?,0,0,64,?,'reserved',NULL,NULL,NULL)", []any{strings.Repeat("a", 64), now.Format(time.DateOnly)}},
		{"INSERT INTO hash_budget VALUES(1,?,?,64,0,0,0,64,0,0,0)", []any{now.Format(time.DateOnly), now.UnixNano()}},
	} {
		if _, err := tx.Exec(query.text, query.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.report); err != nil {
		t.Fatal(err)
	}
	writer, err := inventory.OpenHashSelectionWriter(ctx, f.base)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	consent, err := writer.ApproveRead(ctx, inventory.HashReadApprovalRequest{
		StoreID: proposal.StoreID, SelectionID: proposal.SelectionID, InventoryID: proposal.InventoryID,
		SourceLocator: *proposal.SourceLocator, DailyReservedByteLimit: 128, LifetimeReservedByteLimit: 512, ConfirmFullFileRead: true,
	})
	if err != nil || consent.Approval.InitialTotalReservedBytes != 64 {
		t.Fatal(consent, err)
	}
	for _, revoked := range []bool{false, true} {
		t.Run(fmt.Sprint(revoked), func(t *testing.T) {
			if revoked {
				var err error
				consent, err = writer.RevokeRead(ctx, consent.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
			code, raw, stderr := f.run(ctx, "hash", "--show", proposal.SelectionID, "--json")
			shown := hashCLIProposal(t, code, raw, stderr)
			if shown.ReadConsent == nil || !reflect.DeepEqual(*shown.ReadConsent, consent) {
				t.Fatal("proposal lost saved consent", raw)
			}
			var fields struct {
				Hash struct {
					ReadConsent map[string]any `json:"read_consent"`
				} `json:"hash"`
			}
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				t.Fatal(err)
			}
			if fields.Hash.ReadConsent["current_read_permission_evaluated"] != false || fields.Hash.ReadConsent["executable"] != false || fields.Hash.ReadConsent["estimated_reclaimable_bytes"] != nil {
				t.Fatal("saved consent inferred permission", raw)
			}
			code, raw, stderr = f.run(ctx, "hashes", "--work", "1", "--json")
			snapshot := hashCLIReport(t, code, raw, stderr)
			if snapshot.ReadConsent == nil || !reflect.DeepEqual(*snapshot.ReadConsent, consent) || len(snapshot.Work) != 1 || snapshot.Work[0].Status != "running" || snapshot.Work[0].Sequence != 0 || snapshot.Work[0].LatestAttempt == nil || snapshot.Work[0].LatestAttempt.Status != "reserved" || snapshot.Work[0].LatestAttempt.ReadBytes != nil || snapshot.Budget == nil || snapshot.Budget.TotalReservedBytes != 64 || snapshot.Budget.TotalUnknownReservedBytes != 0 {
				t.Fatal("saved consent view recovered or filtered scope", raw)
			}
			for _, args := range [][]string{{"hash", "--show", proposal.SelectionID}, {"hashes", "--work", "1"}} {
				code, human, stderr := f.run(ctx, args...)
				flat := strings.Join(strings.Fields(human), " ")
				if code != 0 || stderr != "" || strings.Contains(human, "UNAPPROVED") || !strings.Contains(human, "Read consent ID: "+consent.ID) {
					t.Fatal(code, human, stderr)
				}
				for _, want := range []string{"SAVED FULL-FILE READ CONSENT", "Recorded expiry", consent.Approval.ExpiresAt.UTC().Format(time.RFC3339Nano), "Step reservation cap", "1048576 bytes", "Day reservation cap", "128 bytes", "Lifetime reservation cap", "512 bytes", "Charges before approval", "64 bytes", "Not evaluated by this saved-only view"} {
					if !strings.Contains(flat, want) {
						t.Fatal("saved consent display omitted decision data", want, human)
					}
				}
				if revoked && !strings.Contains(flat, "Revocation recorded") {
					t.Fatal("revoked consent called active", human)
				}
				if !revoked && !strings.Contains(flat, "Consent recorded; current permission not checked") {
					t.Fatal("recorded consent evaluated current permission", human)
				}
			}
			stillSaved, err := writer.Approval(ctx, consent.ID)
			if err != nil || !reflect.DeepEqual(stillSaved, consent) {
				t.Fatal("reader advanced lifecycle clock", stillSaved, err)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) {
				t.Fatal("saved consent view changed records or source bytes")
			}
			for _, args := range [][]string{{"hash", "--show", proposal.SelectionID}, {"hashes"}} {
				var diagnostic bytes.Buffer
				if code := Run(ctx, append([]string{"--data-dir", f.base}, args...), hashFailWriter{short: true}, &diagnostic); code != 1 || diagnostic.Len() == 0 {
					t.Fatal("saved consent output failure returned success", code, diagnostic.String())
				}
			}
		})
	}
}

func TestHashSavedConsentHumanUsesRecordedLifecycleOnly(t *testing.T) {
	// An old recorded expiry must remain unevaluated. Expiry is reported only
	// when the stored writer observation says so; no wall clock is consulted.
	created := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, status := range []string{"recorded", "expired_observed", "revoked"} {
		t.Run(status, func(t *testing.T) {
			consent := &inventory.HashReadConsent{
				ID: strings.Repeat("a", 64), Status: status, ClockHighWater: created,
				Approval: inventory.HashReadApproval{CreatedAt: created, ExpiresAt: created.Add(24 * time.Hour), StepByteLimit: 1 << 20, DailyReservedByteLimit: 256, LifetimeReservedByteLimit: 1024, InitialTotalReservedBytes: 128},
			}
			if status == "expired_observed" {
				consent.ExpiredObserved = true
				consent.ClockHighWater = consent.Approval.ExpiresAt
			}
			if status == "revoked" {
				consent.Revocation = &inventory.HashReadRevocation{RecordedAt: created}
			}
			before := *consent
			var out bytes.Buffer
			if err := printHashProposal(&out, inventory.HashProposal{ReadConsent: consent}); err != nil {
				t.Fatal(err)
			}
			flat := strings.Join(strings.Fields(out.String()), " ")
			if strings.Contains(flat, "UNAPPROVED") || !strings.Contains(flat, "SAVED HASH SELECTION - READ CONSENT RECORDED") || !strings.Contains(flat, "Not evaluated by this saved-only view") || strings.Contains(flat, "Remaining quota:") || strings.Contains(flat, "Time to expiry:") || !reflect.DeepEqual(before, *consent) {
				t.Fatal("human view evaluated or changed saved lifecycle", out.String())
			}
			if (status == "expired_observed") != strings.Contains(flat, "A writer recorded that this approval expired") {
				t.Fatal("human view inferred expiry from date", out.String())
			}
		})
	}
}
