package state

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

var manualRefreshTestPath = []byte("/generated/manual/node_modules")

func manualRefreshFixture(t *testing.T, compact bool) (*Store, string, *ManualRefreshSession) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	session, receipt, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, &compact)
	if err != nil || receipt != nil {
		t.Fatal(receipt, err)
	}
	if _, err = s.ConfigureCompact(context.Background(), &compact); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(context.Background(), []string{string(manualRefreshTestPath)}); err != nil {
		t.Fatal(err)
	}
	return s, dir, session
}

func manualRefreshStart(t *testing.T, s *Store, session *ManualRefreshSession) *ManualRefreshActivation {
	t.Helper()
	r, err := s.RequestManualRefresh(context.Background(), session)
	if err != nil || r == nil || r.Phase != "pending" {
		t.Fatal(r, err)
	}
	r, activation, err := s.TryActivateManualRefresh(context.Background(), session, time.Now())
	if err != nil || activation == nil || r.Phase != "active" {
		t.Fatal(r, activation, err)
	}
	return activation
}

func manualRefreshRootBatch(j Job, complete bool) ScanBatch {
	return ScanBatch{Identity: "generated-manual-volume", Generation: 71, Directory: Entry{Path: append([]byte(nil), j.Path...), Kind: "directory"}, Cursor: []byte{0, 255, 7}, Complete: complete}
}

func manualRefreshSaved(t *testing.T, s *Store) *manualRefreshSlot {
	t.Helper()
	var raw []byte
	err := s.db.QueryRow("SELECT value FROM settings WHERE key=?", manualRefreshKey).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := manualRefreshDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return slot
}

func manualRefreshRaw(t *testing.T, s *Store) []byte {
	t.Helper()
	var raw []byte
	if err := s.db.QueryRow("SELECT value FROM settings WHERE key=?", manualRefreshKey).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

func manualRefreshDrain(t *testing.T, s *Store) {
	t.Helper()
	for n := 0; n < 100; n++ {
		worked, err := compactMaintenanceStep(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if !worked {
			return
		}
	}
	t.Fatal("generated maintenance did not drain")
}

func TestManualRefreshLifecycleAndHistoricalCompletion(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint("compact=", compact), func(t *testing.T) {
			ctx := context.Background()
			s, dir, session := manualRefreshFixture(t, compact)
			activation := manualRefreshStart(t, s, session)
			j := scanJob(t, s)
			b := manualRefreshRootBatch(j, false)
			b.Entries = []Entry{{Path: []byte("child"), Kind: "directory"}, compactFile("file", "shared", 12), compactFile("alias", "shared", 12)}
			r, err := s.CommitManualRefreshScan(ctx, activation, j, b)
			if err != nil || r.ObservedRootGeneration != 71 || r.RootListingEOFWitness {
				t.Fatal(r, err)
			}
			j = scanJob(t, s)
			if !bytes.Equal(j.Cursor, b.Cursor) {
				t.Fatal("ordinary durable cursor changed", j)
			}
			r, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true))
			if err != nil || !r.RootListingEOFWitness || r.Phase != "active" {
				t.Fatal(r, err)
			}
			r, err = s.FinalizeManualRefresh(ctx, session)
			if err != nil || r.Phase != "active" {
				t.Fatal("EOF completed despite child and maintenance", r, err)
			}
			_, nextActivation, err := s.TryActivateManualRefresh(ctx, session, time.Now())
			if err != nil || nextActivation != nil {
				t.Fatal("sealed proof was reissued", err)
			}
			j = scanJob(t, s)
			if string(j.Path) != "child" {
				t.Fatal(j)
			}
			if err = s.CommitScan(ctx, j, manualRefreshRootBatch(j, true)); err != nil {
				t.Fatal(err)
			}
			manualRefreshDrain(t, s)
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			reopened, r, err := w.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil || !r.RootListingEOFWitness {
				t.Fatal(r, err)
			}
			r, err = w.RequestManualRefresh(ctx, reopened)
			if err != nil || r.Phase != "active" {
				t.Fatal(r, err)
			}
			_, proof, err := w.TryActivateManualRefresh(ctx, reopened, time.Now())
			if err != nil || proof != nil {
				t.Fatal("historical evidence created a fresh proof", err)
			}
			r, err = w.FinalizeManualRefresh(ctx, reopened)
			if err != nil || r.Phase != "completed" || r.TerminalQualification != "historical_pass_completed" {
				t.Fatal(r, err)
			}
			if countRows(t, w, "SELECT count(*) FROM jobs") != 0 {
				t.Fatal("completion seeded another pass")
			}
			if compact {
				report, err := w.MeasureDirectory(ctx, string(manualRefreshTestPath))
				if err != nil {
					t.Fatal(err)
				}
				if report.LogicalBytes == nil || *report.LogicalBytes != 24 || report.AllocatedBytes == nil || *report.AllocatedBytes != 4096 || report.RepeatedInodes != 1 {
					t.Fatal(report)
				}
			} else if countRows(t, w, "SELECT count(*) FROM entries WHERE kind='file' AND size=12") != 2 {
				t.Fatal("detailed observations changed")
			}
			later, _, err := w.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err = w.RequestManualRefresh(ctx, later)
			if err != nil || r.RequestGeneration != 2 || r.Phase != "pending" {
				t.Fatal(r, err)
			}
		})
	}
}

func TestManualRefreshAllWorkBlocksActivation(t *testing.T) {
	cases := []struct{ name, setup, cleanup string }{
		{"future_retry", "INSERT INTO jobs(root_id,kind,path,due_at_ns,cursor,attempts,last_error) VALUES(1,'inventory',X'2e',9223372036854775807,X'00ff',4,'retained')", "DELETE FROM jobs"},
		{"running", "INSERT INTO jobs(root_id,kind,path,status,due_at_ns,lease_token,lease_until_ns) VALUES(1,'inventory',X'2e','running',1,'retained',9223372036854775807)", "DELETE FROM jobs"},
		{"other_kind", "INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'unhandled',X'2e',1)", "DELETE FROM jobs"},
		{"compact_retirement", "INSERT INTO compact_retirement VALUES(1,X'2e',7)", "DELETE FROM compact_retirement"},
		{"subtree_reconcile", "INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(1,X'2e',7)", "DELETE FROM subtree_reconcile"},
		{"subtree_retirement", "INSERT INTO subtree_retirement(root_id,path,scan_revision,preserve_entry) VALUES(1,X'2e',7,1)", "DELETE FROM subtree_retirement"},
		{"missing_allocation_revision", "INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'done')", "DELETE FROM allocation_cache"},
		{"allocation_revision_mismatch", "INSERT INTO allocation_revisions(root_id,revision) VALUES(1,8); INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'done')", "DELETE FROM allocation_cache"},
		{"allocation_phase", "INSERT INTO allocation_revisions(root_id,revision) VALUES(1,7); INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'entries')", "DELETE FROM allocation_cache"},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			s, _, session := manualRefreshFixture(t, false)
			ctx := context.Background()
			if _, err := s.db.Exec(fixture.setup); err != nil {
				t.Fatal(err)
			}
			r, err := s.RequestManualRefresh(ctx, session)
			if err != nil || r.Phase != "pending" {
				t.Fatal(r, err)
			}
			before := manualRefreshRaw(t, s)
			for i := 0; i < 2; i++ {
				r, proof, err := s.TryActivateManualRefresh(ctx, session, time.Now())
				if err != nil || proof != nil || r.Phase != "pending" {
					t.Fatal(r, proof, err)
				}
			}
			if !bytes.Equal(before, manualRefreshRaw(t, s)) {
				t.Fatal("blocked work replaced the request")
			}
			if fixture.name == "future_retry" && countRows(t, s, "SELECT count(*) FROM jobs WHERE due_at_ns=9223372036854775807 AND cursor=X'00ff' AND attempts=4 AND last_error='retained'") != 1 {
				t.Fatal("future retry changed")
			}
			if fixture.name == "running" && countRows(t, s, "SELECT count(*) FROM jobs WHERE status='running' AND lease_token='retained' AND lease_until_ns=9223372036854775807") != 1 {
				t.Fatal("running lease changed")
			}
			if _, err = s.db.Exec(fixture.cleanup); err != nil {
				t.Fatal(err)
			}
			r, proof, err := s.TryActivateManualRefresh(ctx, session, time.Now())
			if err != nil || proof == nil || r.Phase != "active" || countRows(t, s, "SELECT count(*) FROM jobs") != 1 {
				t.Fatal(r, proof, err)
			}
		})
	}
}

func TestManualRefreshCopiesCoalescingAndProofRefusal(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	s, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	path := append([]byte(nil), manualRefreshTestPath...)
	mode := false
	session, _, err := s.PreflightManualRefresh(ctx, path, &mode)
	if err != nil {
		t.Fatal(err)
	}
	path[1], mode = 'X', true
	if _, err = s.ConfigureCompact(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(ctx, []string{string(manualRefreshTestPath)}); err != nil {
		t.Fatal(err)
	}
	r, err := s.RequestManualRefresh(ctx, session)
	if err != nil || r.CompactMode {
		t.Fatal(r, err)
	}
	original := *r
	r.RootID, r.RequestGeneration, r.Phase = 99, 99, "unknown"
	r, err = s.RequestManualRefresh(ctx, session)
	if err != nil || *r != original {
		t.Fatal("receipt aliases authoritative state", r, err)
	}
	copiedSession := *session
	if _, err = s.RequestManualRefresh(ctx, &copiedSession); !errors.Is(err, ErrManualRefreshScope) {
		t.Fatal("copied session accepted", err)
	}
	r, activation, err := s.TryActivateManualRefresh(ctx, session, time.Now())
	if err != nil || activation == nil {
		t.Fatal(r, err)
	}
	before := manualRefreshRaw(t, s)
	coalesced, err := s.RequestManualRefresh(ctx, session)
	if err != nil || *coalesced != *r || !bytes.Equal(before, manualRefreshRaw(t, s)) || countRows(t, s, "SELECT count(*) FROM jobs") != 1 {
		t.Fatal(coalesced, err)
	}
	j := scanJob(t, s)
	copyActivation := *activation
	for _, proof := range []*ManualRefreshActivation{{}, &copyActivation} {
		if _, err = s.CommitManualRefreshScan(ctx, proof, j, manualRefreshRootBatch(j, true)); err == nil {
			t.Fatal("forged or copied proof accepted")
		}
	}
	foreign, _, _ := manualRefreshFixture(t, false)
	if _, err = foreign.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); !errors.Is(err, ErrManualRefreshScope) {
		t.Fatal("foreign Store accepted proof", err)
	}
	if !bytes.Equal(before, manualRefreshRaw(t, s)) || countRows(t, s, "SELECT count(*) FROM jobs WHERE status='running'") != 1 {
		t.Fatal("refused proof changed durable work")
	}
	if _, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); !errors.Is(err, ErrManualRefreshScope) {
		t.Fatal("sealed proof accepted", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.FinalizeManualRefresh(ctx, session); err == nil {
		t.Fatal("closed Store accepted session")
	}
	if _, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err == nil {
		t.Fatal("closed Store accepted proof")
	}
}

func TestManualRefreshReopenedIncompleteAlwaysUnknown(t *testing.T) {
	for _, firstGeneration := range []bool{false, true} {
		t.Run(fmt.Sprint("first_generation=", firstGeneration), func(t *testing.T) {
			ctx := context.Background()
			s, dir, session := manualRefreshFixture(t, false)
			activation := manualRefreshStart(t, s, session)
			if firstGeneration {
				j := scanJob(t, s)
				if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, false)); err != nil {
					t.Fatal(err)
				}
			}
			before := *manualRefreshSaved(t, s)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			w, err := OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			reopened, _, err := w.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := w.RequestManualRefresh(ctx, reopened)
			if !errors.Is(err, ErrManualRefreshUnknown) || r.Phase != "unknown" || r.ActivatedRootJobID != before.ActivatedRootJobID || r.ObservedRootGeneration != before.ObservedRootGeneration || r.RootListingEOFWitness {
				t.Fatal(r, err)
			}
			if countRows(t, w, "SELECT count(*) FROM jobs") != 1 {
				t.Fatal("unknown discarded or added work")
			}
			// Serve the inherited work with the unchanged ordinary API. Even after
			// draining it, this discovery invocation cannot supersede its unknown.
			j := scanJob(t, w)
			if err = w.CommitScan(ctx, j, manualRefreshRootBatch(j, true)); err != nil {
				t.Fatal(err)
			}
			manualRefreshDrain(t, w)
			if _, err = w.RequestManualRefresh(ctx, reopened); !errors.Is(err, ErrManualRefreshUnknown) {
				t.Fatal("same invocation superseded unknown", err)
			}
			if _, proof, err := w.TryActivateManualRefresh(ctx, reopened, time.Now()); proof != nil || !errors.Is(err, ErrManualRefreshUnknown) {
				t.Fatal(proof, err)
			}
			if _, err = w.FinalizeManualRefresh(ctx, reopened); !errors.Is(err, ErrManualRefreshUnknown) {
				t.Fatal(err)
			}
			later, _, err := w.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err = w.RequestManualRefresh(ctx, later)
			if err != nil || r.RequestGeneration != 2 || r.Phase != "pending" {
				t.Fatal(r, err)
			}
		})
	}
}

func TestManualRefreshPendingCoalescerCannotInheritActivation(t *testing.T) {
	ctx := context.Background()
	for _, transition := range []string{"request", "activate", "finalize"} {
		t.Run(transition, func(t *testing.T) {
			s, _, owner := manualRefreshFixture(t, false)
			if _, err := s.RequestManualRefresh(ctx, owner); err != nil {
				t.Fatal(err)
			}
			coalescer, _, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.RequestManualRefresh(ctx, coalescer); err != nil {
				t.Fatal(err)
			}
			_, activation, err := s.TryActivateManualRefresh(ctx, owner, time.Now())
			if err != nil || activation == nil {
				t.Fatal(err)
			}
			var receipt *ManualRefreshReceipt
			switch transition {
			case "request":
				receipt, err = s.RequestManualRefresh(ctx, coalescer)
			case "activate":
				var proof *ManualRefreshActivation
				receipt, proof, err = s.TryActivateManualRefresh(ctx, coalescer, time.Now())
				if proof != nil {
					t.Fatal("coalescer acquired another invocation's proof")
				}
			case "finalize":
				receipt, err = s.FinalizeManualRefresh(ctx, coalescer)
			}
			if !errors.Is(err, ErrManualRefreshUnknown) || receipt.Phase != "unknown" || countRows(t, s, "SELECT count(*) FROM jobs") != 1 {
				t.Fatal(receipt, err)
			}
			j := scanJob(t, s)
			if _, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); !errors.Is(err, ErrManualRefreshUnknown) {
				t.Fatal("invalidated owner proof created EOF", err)
			}
			if manualRefreshSaved(t, s).RootListingEOFWitness || countRows(t, s, "SELECT count(*) FROM jobs WHERE status='running'") != 1 {
				t.Fatal("unknown changed work or witnesses")
			}
		})
	}
}

func TestManualRefreshStrictSlotAndScalarBounds(t *testing.T) {
	s, _, session := manualRefreshFixture(t, false)
	manualRefreshStart(t, s, session)
	valid := manualRefreshRaw(t, s)
	bad := map[string][]byte{
		"duplicate":              bytes.Replace(valid, []byte(`"root_id":1`), []byte(`"root_id":1,"root_id":1`), 1),
		"unknown":                append(append([]byte(nil), valid[:len(valid)-1]...), []byte(`,"extra":1}`)...),
		"missing":                bytes.Replace(valid, []byte(`"root_listing_eof_witness":false,`), nil, 1),
		"type":                   bytes.Replace(valid, []byte(`"root_id":1`), []byte(`"root_id":"1"`), 1),
		"overflow":               bytes.Replace(valid, []byte(`"request_generation":1`), []byte(`"request_generation":9223372036854775808`), 1),
		"negative_generation":    bytes.Replace(valid, []byte(`"observed_root_generation":0`), []byte(`"observed_root_generation":-1`), 1),
		"zero_request":           bytes.Replace(valid, []byte(`"request_generation":1`), []byte(`"request_generation":0`), 1),
		"zero_root":              bytes.Replace(valid, []byte(`"root_id":1`), []byte(`"root_id":0`), 1),
		"zero_job":               bytes.Replace(valid, []byte(`"activated_root_job_id":1`), []byte(`"activated_root_job_id":0`), 1),
		"unsupported_phase":      bytes.Replace(valid, []byte(`"phase":"active"`), []byte(`"phase":"future"`), 1),
		"eof_without_generation": bytes.Replace(valid, []byte(`"root_listing_eof_witness":false`), []byte(`"root_listing_eof_witness":true`), 1),
		"missing_path":           bytes.Replace(valid, []byte(`"raw_root_path":"`), []byte(`"not_raw_root_path":"`), 1),
		"wrong_contract":         bytes.Replace(valid, []byte(manualRefreshContract), []byte("other_contract"), 1),
		"oversize":               bytes.Repeat([]byte("x"), manualRefreshSlotLimit+1),
		"whitespace":             append([]byte(" "), valid...),
	}
	for name, raw := range bad {
		t.Run(name, func(t *testing.T) {
			if bytes.Equal(raw, valid) {
				t.Fatal("negative fixture did not alter its intended field")
			}
			if _, err := manualRefreshDecode(raw); !errors.Is(err, ErrManualRefreshInvalid) {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE settings SET value=? WHERE key=?", raw, manualRefreshKey); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil); !errors.Is(err, ErrManualRefreshInvalid) {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, manualRefreshRaw(t, s)) {
				t.Fatal("malformed evidence repaired")
			}
		})
	}
	if _, err := s.db.Exec("UPDATE settings SET value=? WHERE key=?", string(valid), manualRefreshKey); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil); !errors.Is(err, ErrManualRefreshInvalid) {
		t.Fatal("TEXT slot accepted", err)
	}
	if _, err := s.db.Exec("UPDATE settings SET value=? WHERE key=?", valid, manualRefreshKey); err != nil {
		t.Fatal(err)
	}
	projection, err := json.Marshal(manualRefreshSaved(t, s).ManualRefreshReceipt)
	if err != nil || len(projection) > 1024 {
		t.Fatal(len(projection), err)
	}
	for _, path := range [][]byte{nil, []byte("relative"), []byte("/a/../b"), bytes.Repeat([]byte("/"), 4097), {'/', 0, 'a'}} {
		if _, _, err := s.PreflightManualRefresh(context.Background(), path, nil); !errors.Is(err, ErrManualRefreshInvalid) {
			t.Fatal("invalid path accepted", err)
		}
	}
}

func TestManualRefreshPreflightScopeBeforeSetup(t *testing.T) {
	cases := []struct {
		name, change string
		mode         *bool
	}{
		{"other_enabled_root", "INSERT INTO roots(path) VALUES(X'2f6f74686572')", nil},
		{"other_disabled_root", "INSERT INTO roots(path,enabled) VALUES(X'2f6f74686572',0)", nil},
		{"selected_disabled", "UPDATE roots SET enabled=0", nil},
		{"selected_replaced_id", "UPDATE roots SET id=2", nil},
		{"selected_replaced_path", "UPDATE roots SET path=X'2f6f74686572'", nil},
		{"malformed_mode", "INSERT INTO settings(key,value) VALUES('inventory.compact',X'3031')", nil},
		{"text_mode", "INSERT INTO settings(key,value) VALUES('inventory.compact','0')", nil},
	}
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			s, _, session := manualRefreshFixture(t, false)
			var err error
			session, _, err = s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			var boundMarker []byte
			if fixture.name == "selected_replaced_id" {
				// A fresh preflight needs saved evidence of the previous ID.
				// Pending binds that ID without creating foreign-key job work.
				if _, err := s.RequestManualRefresh(context.Background(), session); err != nil {
					t.Fatal(err)
				}
				boundMarker = manualRefreshRaw(t, s)
			}
			if _, err := s.db.Exec(fixture.change); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, fixture.mode); err == nil || boundMarker != nil && !errors.Is(err, ErrManualRefreshScope) {
				t.Fatal("mismatched scope admitted before setup", err)
			}
			if _, err := s.RequestManualRefresh(context.Background(), session); err == nil || boundMarker != nil && !errors.Is(err, ErrManualRefreshScope) {
				t.Fatal("stale preflight admitted changed scope", err)
			}
			if boundMarker != nil {
				if !bytes.Equal(boundMarker, manualRefreshRaw(t, s)) || countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
					t.Fatal("ID replacement refusal changed the pending binding or jobs")
				}
			} else if countRows(t, s, "SELECT count(*) FROM settings WHERE key='inventory.manual_refresh.v1'") != 0 {
				t.Fatal("scope refusal wrote a request")
			}
		})
	}
	s, _, session := manualRefreshFixture(t, false)
	if _, err := s.RequestManualRefresh(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	before := manualRefreshRaw(t, s)
	on := true
	if _, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, &on); !errors.Is(err, ErrManualRefreshScope) {
		t.Fatal("pending mode switch admitted", err)
	}
	if !bytes.Equal(before, manualRefreshRaw(t, s)) {
		t.Fatal("preflight mode refusal changed marker")
	}
}

func TestManualRefreshCommitWitnessAtomicity(t *testing.T) {
	ctx := context.Background()
	s, _, session := manualRefreshFixture(t, false)
	activation := manualRefreshStart(t, s, session)
	j := scanJob(t, s)
	before := manualRefreshRaw(t, s)
	if _, err := s.db.Exec(`CREATE TRIGGER fixture_refresh_refusal BEFORE UPDATE ON settings WHEN NEW.key='inventory.manual_refresh.v1' BEGIN SELECT RAISE(ABORT,'generated refusal'); END`); err != nil {
		t.Fatal(err)
	}
	b := manualRefreshRootBatch(j, true)
	b.Entries = []Entry{compactFile("file", "inode", 12)}
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, b); err == nil {
		t.Fatal("witness write refusal ignored")
	}
	if !bytes.Equal(before, manualRefreshRaw(t, s)) || countRows(t, s, "SELECT count(*) FROM entries") != 0 || countRows(t, s, "SELECT count(*) FROM allocation_revisions") != 0 || countRows(t, s, "SELECT count(*) FROM jobs WHERE status='running'") != 1 || countRows(t, s, "SELECT count(*) FROM subtree_reconcile") != 0 {
		t.Fatal("witness and scan did not roll back together")
	}
	if _, err := s.db.Exec("DROP TRIGGER fixture_refresh_refusal"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, b); err != nil {
		t.Fatal(err)
	}
	if !manualRefreshSaved(t, s).RootListingEOFWitness {
		t.Fatal("successful scan lacked atomic EOF")
	}
	manualRefreshDrain(t, s)
	if _, err := s.db.Exec("UPDATE directories SET generation=72 WHERE path=X'2e'"); err != nil {
		t.Fatal(err)
	}
	r, err := s.FinalizeManualRefresh(ctx, session)
	if !errors.Is(err, ErrManualRefreshUnknown) || r.Phase != "unknown" || !r.RootListingEOFWitness || r.ObservedRootGeneration != 71 {
		t.Fatal("changed generation claimed completion or erased history", r, err)
	}
}

func TestManualRefreshTerminalModeChangeAndNonterminalRefusal(t *testing.T) {
	ctx := context.Background()
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint("unknown=", unknown), func(t *testing.T) {
			s, _, session := manualRefreshFixture(t, false)
			activation := manualRefreshStart(t, s, session)
			j := scanJob(t, s)
			if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
				t.Fatal(err)
			}
			manualRefreshDrain(t, s)
			if unknown {
				if _, err := s.db.Exec("UPDATE directories SET generation=72 WHERE path=X'2e'"); err != nil {
					t.Fatal(err)
				}
			}
			_, err := s.FinalizeManualRefresh(ctx, session)
			if unknown && !errors.Is(err, ErrManualRefreshUnknown) || !unknown && err != nil {
				t.Fatal(err)
			}
			on := true
			if _, err = s.ConfigureCompact(ctx, &on); err != nil {
				t.Fatal(err)
			}
			before := manualRefreshRaw(t, s)
			later, historical, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
			if err != nil || historical.CompactMode || !bytes.Equal(before, manualRefreshRaw(t, s)) {
				t.Fatal("terminal history blocked ordinary mode change", historical, err)
			}
			r, err := s.RequestManualRefresh(ctx, later)
			if err != nil || !r.CompactMode || r.RequestGeneration != 2 || r.Phase != "pending" {
				t.Fatal(r, err)
			}
			_, proof, err := s.TryActivateManualRefresh(ctx, later, time.Now())
			if err != nil || proof == nil {
				t.Fatal(proof, err)
			}
		})
	}
	s, _, session := manualRefreshFixture(t, false)
	if _, err := s.RequestManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	on := true
	if _, err := s.ConfigureCompact(ctx, &on); err != nil {
		t.Fatal(err)
	}
	before := manualRefreshRaw(t, s)
	if _, _, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, nil); !errors.Is(err, ErrManualRefreshScope) {
		t.Fatal("pending saved mode mismatch admitted", err)
	}
	if !bytes.Equal(before, manualRefreshRaw(t, s)) {
		t.Fatal("mode mismatch repaired nonterminal history")
	}
}

func manualRefreshWorkSnapshot(t *testing.T, s *Store) []byte {
	t.Helper()
	var snapshot []any
	for _, query := range []string{
		"SELECT * FROM roots ORDER BY id", "SELECT * FROM settings ORDER BY key",
		"SELECT * FROM jobs ORDER BY id", "SELECT * FROM compact_retirement ORDER BY root_id,path,generation",
		"SELECT * FROM subtree_reconcile ORDER BY root_id,path", "SELECT * FROM subtree_retirement ORDER BY root_id,path",
		"SELECT * FROM allocation_cache ORDER BY root_id,path", "SELECT * FROM allocation_revisions ORDER BY root_id",
	} {
		rows, err := s.db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var bodies [][]byte
		for rows.Next() {
			if len(bodies) >= 4 {
				rows.Close()
				t.Fatal("generated snapshot row bound exceeded")
			}
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			body, err := json.Marshal(values)
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			bodies = append(bodies, body)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if err = rows.Close(); err != nil {
			t.Fatal(err)
		}
		snapshot = append(snapshot, bodies)
	}
	body, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestManualRefreshPreflightModeChangeRetainsAllWork(t *testing.T) {
	ctx := context.Background()
	blockers := []struct{ name, setup string }{
		{"future_retry", "INSERT INTO jobs(root_id,kind,path,due_at_ns,cursor,attempts,last_error) VALUES(1,'inventory',X'2e',9223372036854775807,X'00ff',4,'retained')"},
		{"running", "INSERT INTO jobs(root_id,kind,path,status,due_at_ns,lease_token,lease_until_ns) VALUES(1,'inventory',X'2e','running',1,'retained',9223372036854775807)"},
		{"other_kind", "INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'unhandled',X'2e',1)"},
		{"compact_retirement", "INSERT INTO compact_retirement VALUES(1,X'2e',7)"},
		{"subtree_reconcile", "INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(1,X'2e',7)"},
		{"subtree_retirement", "INSERT INTO subtree_retirement(root_id,path,scan_revision,preserve_entry) VALUES(1,X'2e',7,1)"},
		{"missing_allocation_revision", "DELETE FROM allocation_revisions; INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'done')"},
		{"allocation_revision_mismatch", "INSERT INTO allocation_revisions(root_id,revision) VALUES(1,8) ON CONFLICT(root_id) DO UPDATE SET revision=8; INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'done')"},
		{"allocation_phase", "INSERT INTO allocation_revisions(root_id,revision) VALUES(1,7) ON CONFLICT(root_id) DO UPDATE SET revision=7; INSERT INTO allocation_cache(root_id,path,revision,phase) VALUES(1,X'2e',7,'entries')"},
	}
	for _, phase := range []string{"absent", "completed", "unknown"} {
		t.Run(phase, func(t *testing.T) {
			for _, blocker := range blockers {
				t.Run(blocker.name, func(t *testing.T) {
					s, _, owner := manualRefreshFixture(t, false)
					if phase != "absent" {
						activation := manualRefreshStart(t, s, owner)
						j := scanJob(t, s)
						if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
							t.Fatal(err)
						}
						manualRefreshDrain(t, s)
						if phase == "unknown" {
							if _, err := s.db.Exec("UPDATE directories SET generation=72 WHERE path=X'2e'"); err != nil {
								t.Fatal(err)
							}
						}
						_, err := s.FinalizeManualRefresh(ctx, owner)
						if phase == "unknown" && !errors.Is(err, ErrManualRefreshUnknown) || phase == "completed" && err != nil {
							t.Fatal(err)
						}
					}
					if _, err := s.db.Exec(blocker.setup); err != nil {
						t.Fatal(err)
					}
					before := manualRefreshWorkSnapshot(t, s)
					on := true
					session, receipt, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, &on)
					if !errors.Is(err, ErrManualRefreshScope) || session != nil {
						t.Fatal("blocked requested mode admitted before setup", session, receipt, err)
					}
					if phase == "absent" && receipt != nil || phase != "absent" && (receipt == nil || receipt.Phase != phase || receipt.CompactMode) {
						t.Fatal("preflight lost historical receipt", receipt)
					}
					if !bytes.Equal(before, manualRefreshWorkSnapshot(t, s)) {
						t.Fatal("mode admission changed prior root/mode/slot/work bytes")
					}
				})
			}
		})
	}
	for _, phase := range []string{"absent", "completed", "unknown"} {
		t.Run("drained_"+phase, func(t *testing.T) {
			s, _, owner := manualRefreshFixture(t, false)
			expectedGeneration := int64(1)
			if phase != "absent" {
				activation := manualRefreshStart(t, s, owner)
				j := scanJob(t, s)
				if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
					t.Fatal(err)
				}
				manualRefreshDrain(t, s)
				if phase == "unknown" {
					if _, err := s.db.Exec("UPDATE directories SET generation=72 WHERE path=X'2e'"); err != nil {
						t.Fatal(err)
					}
				}
				_, err := s.FinalizeManualRefresh(ctx, owner)
				if phase == "unknown" && !errors.Is(err, ErrManualRefreshUnknown) || phase == "completed" && err != nil {
					t.Fatal(err)
				}
				expectedGeneration = 2
			}
			on := true
			before := manualRefreshWorkSnapshot(t, s)
			selected, historical, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, &on)
			if err != nil || selected == nil || selected.rootID != 1 || !bytes.Equal(before, manualRefreshWorkSnapshot(t, s)) {
				t.Fatal("drained requested mode refused or mutated scope", selected, historical, err)
			}
			if phase == "absent" && historical != nil || phase != "absent" && (historical == nil || historical.Phase != phase || historical.CompactMode) {
				t.Fatal("preflight replaced historical receipt", historical)
			}
			if _, err = s.ConfigureCompact(ctx, &on); err != nil {
				t.Fatal(err)
			}
			if err = s.SyncRoots(ctx, []string{string(manualRefreshTestPath)}); err != nil {
				t.Fatal(err)
			}
			r, err := s.RequestManualRefresh(ctx, selected)
			if err != nil || r.RootID != selected.rootID || r.InventoryIncarnationID != owner.incarnation || r.RequestGeneration != expectedGeneration || !r.CompactMode || r.Phase != "pending" || !bytes.Equal(manualRefreshSaved(t, s).RawRootPath, manualRefreshTestPath) || countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
				t.Fatal("drained mode setup lost binding or next generation", r, err)
			}
		})
	}
}

func TestManualRefreshAdditionalSQLAndLegacyParity(t *testing.T) {
	ctx := context.Background()
	s, _, session := manualRefreshFixture(t, false)
	statements := 0
	session.hooks = &manualRefreshHooks{statement: func() { statements++ }}
	assert := func(want int) {
		t.Helper()
		if statements != want || statements > manualRefreshStatementLimit {
			t.Fatalf("selected query/Exec increment %d, want %d", statements, want)
		}
		statements = 0
	}
	if _, err := s.RequestManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	assert(5)
	if _, err := s.RequestManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	assert(4)
	_, activation, err := s.TryActivateManualRefresh(ctx, session, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	assert(7)
	j := scanJob(t, s)
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, false)); err != nil {
		t.Fatal(err)
	}
	assert(5)
	j = scanJob(t, s)
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
		t.Fatal(err)
	}
	assert(5)
	if _, err := s.FinalizeManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	assert(5)
	manualRefreshDrain(t, s)
	if _, err := s.FinalizeManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	assert(7)
	before := manualRefreshRaw(t, s)
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j = scanJob(t, s)
	if err := s.CommitScan(ctx, j, manualRefreshRootBatch(j, true)); err != nil {
		t.Fatal(err)
	}
	assert(0)
	if !bytes.Equal(before, manualRefreshRaw(t, s)) {
		t.Fatal("legacy commit changed refresh evidence")
	}
	// Malformed refresh evidence cannot add a lookup or change the original
	// public/five-argument CommitScan route.
	manualRefreshDrain(t, s)
	if _, err := s.db.Exec("UPDATE settings SET value=X'ff' WHERE key=?", manualRefreshKey); err != nil {
		t.Fatal(err)
	}
	if err := s.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	j = scanJob(t, s)
	if err := s.commitScan(ctx, j, manualRefreshRootBatch(j, true), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	assert(0)
}

func TestManualRefreshRequestGenerationOverflow(t *testing.T) {
	ctx := context.Background()
	s, _, session := manualRefreshFixture(t, false)
	activation := manualRefreshStart(t, s, session)
	j := scanJob(t, s)
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err != nil {
		t.Fatal(err)
	}
	manualRefreshDrain(t, s)
	if _, err := s.FinalizeManualRefresh(ctx, session); err != nil {
		t.Fatal(err)
	}
	slot := *manualRefreshSaved(t, s)
	slot.RequestGeneration = math.MaxInt64
	raw, err := manualRefreshEncode(slot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE settings SET value=? WHERE key=?", raw, manualRefreshKey); err != nil {
		t.Fatal(err)
	}
	later, _, err := s.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.RequestManualRefresh(ctx, later)
	if !errors.Is(err, ErrManualRefreshInvalid) || r.RequestGeneration != math.MaxInt64 || !bytes.Equal(raw, manualRefreshRaw(t, s)) || countRows(t, s, "SELECT count(*) FROM jobs") != 0 {
		t.Fatal(r, err)
	}
}

func TestManualRefreshRebuiltStoreAndRawPathBounds(t *testing.T) {
	ctx := context.Background()
	s, _, session := manualRefreshFixture(t, false)
	manualRefreshStart(t, s, session)
	original := manualRefreshRaw(t, s)
	foreign, _, _ := manualRefreshFixture(t, false)
	if _, err := foreign.db.Exec("INSERT INTO settings(key,value) VALUES(?,?)", manualRefreshKey, original); err != nil {
		t.Fatal(err)
	}
	_, receipt, err := foreign.PreflightManualRefresh(ctx, manualRefreshTestPath, nil)
	if !errors.Is(err, ErrManualRefreshScope) || receipt.InventoryIncarnationID != session.incarnation || !bytes.Equal(original, manualRefreshRaw(t, foreign)) || countRows(t, foreign, "SELECT count(*) FROM jobs") != 0 {
		t.Fatal("rebuilt store retargeted old evidence", receipt, err)
	}
	for _, path := range [][]byte{{'/', 'r', 255}, []byte("/" + strings.Repeat("r", 4095))} {
		w, err := OpenWriter(ctx, privateDir(t))
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		selected, _, err := w.PreflightManualRefresh(ctx, path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.SyncRoots(ctx, []string{string(path)}); err != nil {
			t.Fatal(err)
		}
		r, err := w.RequestManualRefresh(ctx, selected)
		if err != nil {
			t.Fatal(r, err)
		}
		saved := manualRefreshSaved(t, w)
		if !bytes.Equal(saved.RawRootPath, path) || len(manualRefreshRaw(t, w)) > manualRefreshSlotLimit {
			t.Fatal("raw path failed bounded round trip")
		}
		projection, err := json.Marshal(r)
		if err != nil || len(projection) > 1024 || bytes.Contains(projection, []byte("raw_root_path")) {
			t.Fatal(len(projection), err)
		}
	}
}

func TestManualRefreshClosedStoreCannotUseLiveProof(t *testing.T) {
	ctx := context.Background()
	s, dir, session := manualRefreshFixture(t, false)
	activation := manualRefreshStart(t, s, session)
	j := scanJob(t, s)
	before := manualRefreshRaw(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.db == nil || s.lock == nil || activation.sealed {
		t.Fatal("fixture did not retain original closed pointers and fresh proof")
	}
	if _, err := s.RequestManualRefresh(ctx, session); err == nil {
		t.Fatal("closed Store accepted selected request")
	}
	if _, _, err := s.TryActivateManualRefresh(ctx, session, time.Now()); err == nil {
		t.Fatal("closed Store accepted activation")
	}
	if _, err := s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true)); err == nil {
		t.Fatal("closed Store used live activation")
	}
	if _, err := s.FinalizeManualRefresh(ctx, session); err == nil {
		t.Fatal("closed Store finalized")
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if !bytes.Equal(before, manualRefreshRaw(t, w)) || countRows(t, w, "SELECT count(*) FROM entries") != 0 || countRows(t, w, "SELECT count(*) FROM jobs WHERE status='running'") != 1 {
		t.Fatal("closed operations changed durable work")
	}
}

func TestManualRefreshUnknownAcknowledgmentCancellation(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint("after=", after), func(t *testing.T) {
			s, _, owner := manualRefreshFixture(t, false)
			manualRefreshStart(t, s, owner)
			reopened, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reopened.hooks = &manualRefreshHooks{}
			if after {
				reopened.hooks.afterCommit = manualRefreshBoundaryHook("request_unknown", cancel)
			} else {
				reopened.hooks.beforeCommit = manualRefreshBoundaryHook("request_unknown", cancel)
			}
			r, err := s.RequestManualRefresh(ctx, reopened)
			if !errors.Is(err, context.Canceled) || r == nil || (r.Phase == "unknown") != after || *r != manualRefreshSaved(t, s).ManualRefreshReceipt {
				t.Fatal("unknown publication lost cancellation or durable identity", r, err)
			}
			if countRows(t, s, "SELECT count(*) FROM jobs") != 1 {
				t.Fatal("unknown cancellation changed work")
			}
		})
	}
}

func TestManualRefreshTransitionDeadlineAndStatementAdmission(t *testing.T) {
	parent, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	parentDeadline, _ := parent.Deadline()
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.Equal(parentDeadline) {
		t.Fatal("selected transition restarted caller deadline")
	}
	before := time.Now()
	bounded, boundedCancel, err := manualRefreshContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer boundedCancel()
	deadline, ok = bounded.Deadline()
	if !ok || deadline.Before(before.Add(5*time.Second)) || deadline.After(time.Now().Add(5*time.Second)) {
		t.Fatal("selected cooperative window changed")
	}
	if _, _, err := manualRefreshContext(nil); !errors.Is(err, ErrManualRefreshInvalid) {
		t.Fatal(err)
	}
	expired, expiredCancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer expiredCancel()
	if _, _, err := manualRefreshContext(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	admission := manualRefreshTx{ctx: context.Background()}
	for i := 0; i < manualRefreshStatementLimit; i++ {
		if err := admission.admit(); err != nil {
			t.Fatal(err)
		}
	}
	if err := admission.admit(); !errors.Is(err, ErrManualRefreshInvalid) || admission.statements != manualRefreshStatementLimit {
		t.Fatal("statement limit silently widened", err)
	}
}

func manualRefreshAtBoundary(t *testing.T, s *Store, session *ManualRefreshSession, target string, ctx context.Context) (*ManualRefreshReceipt, error) {
	t.Helper()
	r, err := s.RequestManualRefresh(ctx, session)
	if target == "request" || err != nil {
		return r, err
	}
	r, activation, err := s.TryActivateManualRefresh(ctx, session, time.Now())
	if target == "activate" || err != nil {
		return r, err
	}
	j := scanJob(t, s)
	r, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, false))
	if target == "first_generation" || err != nil {
		return r, err
	}
	j = scanJob(t, s)
	r, err = s.CommitManualRefreshScan(ctx, activation, j, manualRefreshRootBatch(j, true))
	if target == "eof" || err != nil {
		return r, err
	}
	manualRefreshDrain(t, s)
	return s.FinalizeManualRefresh(ctx, session)
}

func manualRefreshBoundaryHook(target string, callback func()) func(string) {
	witnesses := 0
	return func(operation string) {
		if operation == "witness" {
			witnesses++
		}
		if operation == target || operation == "witness" && (target == "first_generation" && witnesses == 1 || target == "eof" && witnesses == 2) {
			callback()
		}
	}
}

func TestManualRefreshCommitAcknowledgmentCancellation(t *testing.T) {
	for _, target := range []string{"request", "activate", "first_generation", "eof", "finalize"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprint(target, "/after=", after), func(t *testing.T) {
				s, _, session := manualRefreshFixture(t, false)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				session.hooks = &manualRefreshHooks{}
				if after {
					session.hooks.afterCommit = manualRefreshBoundaryHook(target, cancel)
				} else {
					session.hooks.beforeCommit = manualRefreshBoundaryHook(target, cancel)
				}
				r, err := manualRefreshAtBoundary(t, s, session, target, ctx)
				if !errors.Is(err, context.Canceled) {
					t.Fatal(r, err)
				}
				if target == "request" && !after {
					if r != nil || countRows(t, s, "SELECT count(*) FROM settings WHERE key='inventory.manual_refresh.v1'") != 0 {
						t.Fatal("tentative request presented as durable", r)
					}
					return
				}
				saved := manualRefreshSaved(t, s)
				if r == nil || *r != saved.ManualRefreshReceipt {
					t.Fatal("canceled reply lost durable identity", r, saved)
				}
				if target == "activate" && countRows(t, s, "SELECT count(*) FROM jobs") != func() int {
					if after {
						return 1
					}
					return 0
				}() {
					t.Fatal("activation atomicity")
				}
				if target == "first_generation" && (saved.ObservedRootGeneration == 71) != after {
					t.Fatal("generation atomicity", saved)
				}
				if target == "eof" && saved.RootListingEOFWitness != after {
					t.Fatal("EOF atomicity", saved)
				}
				if target == "finalize" && (saved.Phase == "completed") != after {
					t.Fatal("completion atomicity", saved)
				}
			})
		}
	}
}

type manualRefreshCrashOutput struct {
	body     []byte
	overflow bool
}

func (b *manualRefreshCrashOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > 8192-len(b.body) {
		b.overflow = true
		p = p[:8192-len(b.body)]
	}
	b.body = append(b.body, p...)
	return n, nil
}

func TestManualRefreshProcessLoss(t *testing.T) {
	for _, target := range []string{"request", "activate", "first_generation", "eof", "finalize"} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprint(target, "/after=", after), func(t *testing.T) {
				dir := privateDir(t)
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, executable, "-test.run=^TestManualRefreshProcessLossChild$")
				cmd.Env = append(os.Environ(), "RYDD_MANUAL_REFRESH_CHILD_DIR="+dir, "RYDD_MANUAL_REFRESH_CHILD_TARGET="+target, fmt.Sprint("RYDD_MANUAL_REFRESH_CHILD_AFTER=", after))
				var stderr manualRefreshCrashOutput
				cmd.Stderr = &stderr
				stdout, err := cmd.StdoutPipe()
				if err != nil {
					t.Fatal(err)
				}
				if err = cmd.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = cmd.Process.Kill() }()
				line := bufio.NewScanner(stdout)
				line.Buffer(make([]byte, 256), 4096)
				ready := line.Scan() && line.Text() == "manual-refresh-ready"
				if ready {
					if err = cmd.Process.Kill(); err != nil {
						t.Fatal(err)
					}
				}
				waitErr := cmd.Wait()
				if !ready || waitErr == nil || ctx.Err() != nil || stderr.overflow {
					t.Fatalf("child did not reach bounded crash point: ready=%v err=%v stderr=%s", ready, waitErr, stderr.body)
				}
				status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatal("child was not reaped after owned SIGKILL", cmd.ProcessState)
				}
				w, err := OpenWriter(context.Background(), dir)
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				if target == "request" && !after {
					if countRows(t, w, "SELECT count(*) FROM settings WHERE key='inventory.manual_refresh.v1'") != 0 || countRows(t, w, "SELECT count(*) FROM jobs") != 0 {
						t.Fatal("precommit crash left request or job")
					}
					return
				}
				original := *manualRefreshSaved(t, w)
				if target == "activate" && countRows(t, w, "SELECT count(*) FROM jobs") != func() int {
					if after {
						return 1
					}
					return 0
				}() {
					t.Fatal("crash separated activation and job")
				}
				if target == "first_generation" && (original.ObservedRootGeneration == 71) != after {
					t.Fatal(original)
				}
				if target == "eof" && original.RootListingEOFWitness != after {
					t.Fatal(original)
				}
				if target == "finalize" && (original.Phase == "completed") != after {
					t.Fatal(original)
				}
				reopened, _, err := w.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil)
				if err != nil {
					t.Fatal(err)
				}
				r, err := w.RequestManualRefresh(context.Background(), reopened)
				if original.Phase == "active" && !original.RootListingEOFWitness {
					if !errors.Is(err, ErrManualRefreshUnknown) || r.Phase != "unknown" || r.RequestGeneration != original.RequestGeneration || r.ActivatedRootJobID != original.ActivatedRootJobID || r.ObservedRootGeneration != original.ObservedRootGeneration || r.RootListingEOFWitness {
						t.Fatal(r, err)
					}
					if _, err = w.RequestManualRefresh(context.Background(), reopened); !errors.Is(err, ErrManualRefreshUnknown) {
						t.Fatal("crash discovery superseded itself", err)
					}
				} else if original.Phase == "active" {
					if err != nil {
						t.Fatal(err)
					}
					manualRefreshDrain(t, w)
					r, err = w.FinalizeManualRefresh(context.Background(), reopened)
					if err != nil || r.Phase != "completed" {
						t.Fatal("durable historical EOF lost", r, err)
					}
				} else if err != nil {
					t.Fatal(r, err)
				}
			})
		}
	}
}

func TestManualRefreshProcessLossChild(t *testing.T) {
	dir := os.Getenv("RYDD_MANUAL_REFRESH_CHILD_DIR")
	if dir == "" {
		t.Skip("generated subprocess only")
	}
	target := os.Getenv("RYDD_MANUAL_REFRESH_CHILD_TARGET")
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	session, _, err := s.PreflightManualRefresh(context.Background(), manualRefreshTestPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ConfigureCompact(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err = s.SyncRoots(context.Background(), []string{string(manualRefreshTestPath)}); err != nil {
		t.Fatal(err)
	}
	pause := manualRefreshBoundaryHook(target, func() {
		fmt.Println("manual-refresh-ready")
		for {
			time.Sleep(time.Hour)
		}
	})
	session.hooks = &manualRefreshHooks{}
	if os.Getenv("RYDD_MANUAL_REFRESH_CHILD_AFTER") == "true" {
		session.hooks.afterCommit = pause
	} else {
		session.hooks.beforeCommit = pause
	}
	if _, err = manualRefreshAtBoundary(t, s, session, target, context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Fatal("generated crash boundary was not reached")
}
