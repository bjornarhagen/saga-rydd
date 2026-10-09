package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func dismissalSelectionFixture(t *testing.T) (*Store, SelectionSnapshot) {
	t.Helper()
	s, id := savedSelectionFixture(t)
	if _, err := s.db.Exec(`UPDATE roots SET volume_id='generated-root'; INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1)`); err != nil {
		t.Fatal(err)
	}
	saved, err := s.SnapshotSelection(context.Background(), []string{id}, FindingAgeDays)
	if err != nil {
		t.Fatal(err)
	}
	return s, saved
}

func TestDismissalSelectionPartialUnknownAndCanonicalClones(t *testing.T) {
	s, saved := dismissalSelectionFixture(t)
	if saved.Evidence.Findings[0].Measurement.Status != "partial" {
		t.Fatal("fixture lost genuine partial evidence", saved)
	}
	canonical, err := CanonicalDismissalSelection(saved)
	if err != nil || !canonical.Evidence.GeneratedAt.IsZero() || !canonical.Evidence.Findings[0].Measurement.GeneratedAt.IsZero() || canonical.Evidence.Findings[0].Path != "" || canonical.Evidence.Notes != nil || !reflect.DeepEqual(canonical.Evidence.Findings[0].Measurement.LogicalBytes, saved.Evidence.Findings[0].Measurement.LogicalBytes) {
		t.Fatal("partial measurements were refused, normalized away or promoted", canonical, err)
	}
	if err = s.CheckDismissalSelection(context.Background(), canonical); err != nil {
		t.Fatal("exact partial evidence did not match", err)
	}
	again, err := CanonicalDismissalSelection(canonical)
	if err != nil || !reflect.DeepEqual(again, canonical) {
		t.Fatal("canonical selection is not stable", again, err)
	}
	canonical.Roots[0].PathBytes[1] ^= 1
	canonical.Evidence.Findings[0].PathBytes[1] ^= 1
	canonical.Evidence.Findings[0].ManifestPathBytes[1] ^= 1
	canonical.Evidence.Findings[0].Measurement.PathBytes[1] ^= 1
	*canonical.Evidence.Findings[0].Measurement.LogicalBytes = 999
	*canonical.Evidence.Findings[0].Measurement.OldestObservation = time.Unix(0, 999).UTC()
	canonical.Evidence.Diagnostics[0].Count = 999
	if string(saved.Roots[0].PathBytes) != "/fixture" || string(saved.Evidence.Findings[0].PathBytes) != "/fixture/a/node_modules" || *saved.Evidence.Findings[0].Measurement.LogicalBytes != 3 || saved.Evidence.Findings[0].Measurement.OldestObservation.UnixNano() != 10 || saved.Evidence.Diagnostics[0].Count != 0 {
		t.Fatal("canonical output aliases its input", saved)
	}
	// An unknown size is qualified evidence, not an unknown target identity.
	if _, err = s.db.Exec("UPDATE entries SET kind='file' WHERE path=X'61'"); err != nil {
		t.Fatal(err)
	}
	unknown, err := s.SnapshotSelection(context.Background(), []string{saved.Targets[0].FindingID}, FindingAgeDays)
	if err != nil || unknown.Evidence.Findings[0].Measurement.Status != "unknown" || unknown.Evidence.Findings[0].Measurement.LogicalBytes != nil || unknown.Evidence.Findings[0].Measurement.AllocatedBytes != nil {
		t.Fatal("fixture did not retain genuine unknown size", unknown, err)
	}
	if _, err = CanonicalDismissalSelection(unknown); err != nil {
		t.Fatal("unknown measured size refused despite exact identities", err)
	}
	if err = s.CheckDismissalSelection(context.Background(), unknown); err != nil {
		t.Fatal("exact unknown size was treated as live verification", err)
	}
}

func TestDismissalSelectionDefiniteChangesAndIncarnationFirst(t *testing.T) {
	for _, tc := range []struct{ name, query string }{
		{"root_identity", "UPDATE roots SET volume_id='replacement'"},
		{"root_revision", "UPDATE allocation_revisions SET revision=2"},
		{"root_path", "UPDATE roots SET path=CAST('/other' AS BLOB)"},
		{"root_disabled", "UPDATE roots SET enabled=0"},
		{"target_identity", "UPDATE entries SET inode='replacement' WHERE path=CAST('a/node_modules' AS BLOB)"},
		{"target_ctime", "UPDATE entries SET ctime_ns=3 WHERE path=CAST('a/node_modules' AS BLOB)"},
		{"target_observation", "UPDATE entries SET observed_at_ns=11 WHERE path=CAST('a/node_modules' AS BLOB)"},
		{"manifest_identity", "UPDATE entries SET inode='replacement' WHERE path=CAST('a/package.json' AS BLOB)"},
		{"manifest_ctime", "UPDATE entries SET ctime_ns=3 WHERE path=CAST('a/package.json' AS BLOB)"},
		{"manifest_observation", "UPDATE entries SET observed_at_ns=11 WHERE path=CAST('a/package.json' AS BLOB)"},
		{"new_scan_generation", "UPDATE entries SET generation=3 WHERE parent=CAST('a' AS BLOB); UPDATE directories SET generation=3 WHERE path=CAST('a' AS BLOB)"},
		{"measurement_bytes", "UPDATE entries SET size=4 WHERE path=CAST('a/node_modules/file' AS BLOB)"},
		{"measurement_qualification", "UPDATE directories SET complete=1 WHERE path=CAST('a/node_modules' AS BLOB)"},
		{"target_missing", "DELETE FROM entries WHERE path=CAST('a/node_modules' AS BLOB)"},
		{"unknown_now", "UPDATE entries SET inode='' WHERE path=CAST('a/node_modules' AS BLOB)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, saved := dismissalSelectionFixture(t)
			if _, err := s.db.Exec(tc.query); err != nil {
				t.Fatal(err)
			}
			if err := s.CheckDismissalSelection(context.Background(), saved); !errors.Is(err, ErrDismissalChanged) {
				t.Fatal("changed evidence did not require a new preview", err)
			}
		})
	}
	_, saved := dismissalSelectionFixture(t)
	other, _ := dismissalSelectionFixture(t)
	// Recreated IDs must not be resolved, even if their table is unavailable.
	if _, err := other.db.Exec("DROP TABLE entries"); err != nil {
		t.Fatal(err)
	}
	if err := other.CheckDismissalSelection(context.Background(), saved); !errors.Is(err, ErrDismissalChanged) {
		t.Fatal("incarnation was checked after reusable finding IDs", err)
	}
}

func TestDismissalSelectionInvalidScopeAndFalseClaims(t *testing.T) {
	_, original := dismissalSelectionFixture(t)
	for name, alter := range map[string]func(*SelectionSnapshot){
		"extra_finding":           func(s *SelectionSnapshot) { s.Evidence.Findings = append(s.Evidence.Findings, s.Evidence.Findings[0]) },
		"root_unknown":            func(s *SelectionSnapshot) { s.Roots[0].Fingerprint = "" },
		"revision_unknown":        func(s *SelectionSnapshot) { s.Roots[0].Revision = 0 },
		"target_unknown":          func(s *SelectionSnapshot) { s.Targets[0].Target.Inode = "0" },
		"manifest_unknown":        func(s *SelectionSnapshot) { s.Targets[0].Manifest.Device = "" },
		"generation_unknown":      func(s *SelectionSnapshot) { s.Targets[0].Manifest.Generation = 0 },
		"ctime_unknown":           func(s *SelectionSnapshot) { s.Targets[0].Target.ChangedNS = 0 },
		"identity_conflict":       func(s *SelectionSnapshot) { s.Evidence.Findings[0].Device = "other" },
		"outside_root":            func(s *SelectionSnapshot) { s.Roots[0].PathBytes = []byte("/unrelated") },
		"noncanonical_path":       func(s *SelectionSnapshot) { s.Evidence.Findings[0].PathBytes = []byte("/fixture/a/../a/node_modules") },
		"false_live_claim":        func(s *SelectionSnapshot) { s.Evidence.CurrentStateVerified = true },
		"false_measurement_claim": func(s *SelectionSnapshot) { s.Evidence.Findings[0].Measurement.CurrentStateVerified = true },
		"action":                  func(s *SelectionSnapshot) { s.Evidence.Findings[0].Actions = []string{"delete"} },
		"rule":                    func(s *SelectionSnapshot) { s.Evidence.Findings[0].RuleVersion++ },
		"negative_count":          func(s *SelectionSnapshot) { s.Evidence.Findings[0].Measurement.UnknownInodes = -1 },
		"negative_size":           func(s *SelectionSnapshot) { *s.Evidence.Findings[0].Measurement.LogicalBytes = -1 },
		"wrong_diagnostic":        func(s *SelectionSnapshot) { s.Evidence.Diagnostics[0].Count = 1 },
		"cursor":                  func(s *SelectionSnapshot) { s.Evidence.NextCursor = "nm1:1" },
		"wrong_inventory":         func(s *SelectionSnapshot) { s.InventoryID = strings.Repeat("A", 64) },
		"non_utf8_fingerprint":    func(s *SelectionSnapshot) { s.Roots[0].Fingerprint = "root\xff" },
		"non_utf8_target_device": func(s *SelectionSnapshot) {
			s.Targets[0].Target.Device, s.Evidence.Findings[0].Device = "dev\xff", "dev\xff"
		},
		"non_utf8_target_inode": func(s *SelectionSnapshot) {
			s.Targets[0].Target.Inode, s.Evidence.Findings[0].Inode = "ino\xff", "ino\xff"
		},
		"non_utf8_manifest_device": func(s *SelectionSnapshot) { s.Targets[0].Manifest.Device = "dev\xff" },
		"non_utf8_manifest_inode":  func(s *SelectionSnapshot) { s.Targets[0].Manifest.Inode = "ino\xff" },
		"non_utf8_root_error":      func(s *SelectionSnapshot) { s.Evidence.Findings[0].Measurement.RootError = "error\xff" },
		"non_utf8_unknown_reason":  func(s *SelectionSnapshot) { s.Evidence.Findings[0].Measurement.UnknownReason = "unknown\xff" },
	} {
		t.Run(name, func(t *testing.T) {
			saved, err := CanonicalDismissalSelection(original)
			if err != nil {
				t.Fatal(err)
			}
			alter(&saved)
			if _, err = CanonicalDismissalSelection(saved); !errors.Is(err, ErrDismissalSelection) {
				t.Fatal("invalid scope or authority claim accepted", err)
			}
		})
	}
}

func TestDismissalSelectionRawPathsAndDisplayNormalization(t *testing.T) {
	s, saved := dismissalSelectionFixture(t)
	rawRoot := []byte("/fixture/quoted\"\n雪\xff")
	if _, err := s.db.Exec("UPDATE roots SET path=?", rawRoot); err != nil {
		t.Fatal(err)
	}
	saved, err := s.SnapshotSelection(context.Background(), []string{saved.Targets[0].FindingID}, FindingAgeDays)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	var decoded SelectionSnapshot
	if err = json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	decoded.Evidence.GeneratedAt = time.Now().Add(time.Hour)
	decoded.Evidence.Notes = []string{"new explanatory prose"}
	decoded.Evidence.Findings[0].Measurement.GeneratedAt = time.Now().Add(time.Hour)
	decoded.Evidence.Findings[0].Measurement.Notes = []string{"new size prose"}
	decoded.Evidence.Diagnostics[0].Explanation = "new wording"
	one, e1 := CanonicalDismissalSelection(saved)
	two, e2 := CanonicalDismissalSelection(decoded)
	if e1 != nil || e2 != nil || !reflect.DeepEqual(one, two) || string(two.Roots[0].PathBytes) != string(rawRoot) {
		t.Fatal("raw paths or non-observational normalization changed equality", one, two, e1, e2)
	}
	if err = s.CheckDismissalSelection(context.Background(), decoded); err != nil {
		t.Fatal("JSON display replacement changed authoritative byte evidence", err)
	}
}

func TestDismissalSelectionNonUTF8EvidenceRemainsVisible(t *testing.T) {
	s, saved := dismissalSelectionFixture(t)
	rawRoot, rawError := []byte("/fixture/raw\xff"), "generated error\xff"
	if _, err := s.db.Exec("UPDATE roots SET path=?,last_error=?", rawRoot, rawError); err != nil {
		t.Fatal(err)
	}
	page, err := s.NodeModulesFindingPage(context.Background(), "", FindingAgeDays)
	if err != nil || len(page.Evidence.Findings) != 1 || page.Evidence.Findings[0].Measurement.RootError != rawError || !reflect.DeepEqual(page.Roots[0].PathBytes, rawRoot) {
		t.Fatal("non-UTF8 evidence hid or changed the ordinary candidate", page, err)
	}
	if _, err = SingleDismissalSelection(page, saved.Targets[0].FindingID); !errors.Is(err, ErrDismissalSelection) || len(page.Evidence.Findings) != 1 {
		t.Fatal("lossy text became dismissible or changed the frozen page", page, err)
	}
	payload, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	var decoded SelectionSnapshot
	if err = json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Roots[0].PathBytes, rawRoot) || !utf8.ValidString(decoded.Evidence.Findings[0].Measurement.RootError) || decoded.Evidence.Findings[0].Measurement.RootError == rawError {
		t.Fatal("fixture did not preserve raw paths and demonstrate JSON text replacement", decoded)
	}
	// A request made from replaced display text cannot match the inventory's
	// original non-UTF8 evidence. Filtering must leave that candidate visible.
	selection, err := SingleDismissalSelection(decoded, saved.Targets[0].FindingID)
	if err != nil {
		t.Fatal("valid decoded text could not form its own exact scope", err)
	}
	if err = s.CheckDismissalSelection(context.Background(), selection); !errors.Is(err, ErrDismissalChanged) {
		t.Fatal("JSON replacement matched different authoritative evidence", err)
	}
}

func dismissalPageFixture(t *testing.T, noise int) *Store {
	t.Helper()
	s := directoryFixture(t)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec("UPDATE roots SET volume_id='generated-root'; INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < noise; i++ {
		if _, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,X'2e','file',1,0,1,2,'dev',?,1,10)`, []byte(fmt.Sprintf("noise%d", i)), fmt.Sprintf("noise%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 21; i++ {
		p := fmt.Sprintf("project%d", i)
		for _, e := range []struct{ path, parent, kind string }{{p, ".", "directory"}, {p + "/package.json", p, "file"}, {p + "/node_modules", p, "directory"}} {
			if _, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns) VALUES(1,?,?,?,0,0,1,2,'dev',?,1,10)`, []byte(e.path), []byte(e.parent), e.kind, e.path); err != nil {
				t.Fatal(err)
			}
		}
		for _, path := range []string{p, p + "/node_modules"} {
			if _, err = tx.Exec("INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,20)", []byte(path)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDismissalFindingPageRawBoundsAndSingleExtraction(t *testing.T) {
	for _, noise := range []int{0, FindingEntryLimit + 1} {
		t.Run(fmt.Sprint(noise), func(t *testing.T) {
			s := dismissalPageFixture(t, noise)
			ctx := context.Background()
			page, err := s.NodeModulesFindingPage(ctx, "", FindingAgeDays)
			old, oldErr := s.NodeModulesFindings(ctx, "", FindingAgeDays)
			if err != nil || oldErr != nil || page.Evidence.EntriesExamined != old.EntriesExamined || page.Evidence.NextCursor != old.NextCursor || len(page.Evidence.Findings) != len(old.Findings) || !reflect.DeepEqual(page.Evidence.Diagnostics, old.Diagnostics) || len(page.Targets) != len(page.Evidence.Findings) || page.Evidence.EntryLimit != FindingEntryLimit {
				t.Fatal("binding page changed raw bounds, diagnostics or cursor", page, old, err, oldErr)
			}
			if noise != 0 {
				if len(page.Targets) != 0 || page.Evidence.EntriesExamined != FindingEntryLimit || page.Evidence.NextCursor == "" {
					t.Fatal("empty raw page was refilled", page)
				}
				page, err = s.NodeModulesFindingPage(ctx, page.Evidence.NextCursor, FindingAgeDays)
			}
			if err != nil || len(page.Targets) != 20 || len(page.Roots) != 1 || page.Evidence.NextCursor == "" {
				t.Fatal("candidate cap changed", page, err)
			}
			for _, f := range page.Evidence.Findings {
				single, err := SingleDismissalSelection(page, f.ID)
				if err != nil || single.Evidence.EntriesExamined != 1 || single.Evidence.EntryLimit != 1 || single.Evidence.NextCursor != "" || len(single.Targets) != 1 || !dismissalDiagnostics(single.Evidence.Diagnostics) {
					t.Fatal("one-finding extraction broadened or remeasured the page", single, err)
				}
				if err = s.CheckDismissalSelection(ctx, single); err != nil {
					t.Fatal("page binding or measurement disagrees with its exact selection", err)
				}
			}
			next, err := s.NodeModulesFindingPage(ctx, page.Evidence.NextCursor, FindingAgeDays)
			if err != nil || len(next.Targets) != 1 || next.Evidence.NextCursor != "" {
				t.Fatal("raw continuation lost the remaining eligible row", next, err)
			}
			if _, err = SingleDismissalSelection(page, "node-modules-v1:1:99999999"); !errors.Is(err, ErrDismissalSelection) {
				t.Fatal("missing selected ID accepted", err)
			}
		})
	}
}

func TestDismissalFindingPageOneSnapshotAndUnknownBindingsRemainVisible(t *testing.T) {
	w, saved := dismissalSelectionFixture(t)
	ctx := context.Background()
	r, err := OpenReader(ctx, filepath.Dir(w.path))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var identity string
	if err = tx.QueryRow("SELECT token FROM inventory_identity").Scan(&identity); err != nil {
		t.Fatal(err)
	}
	if _, err = w.db.Exec("UPDATE roots SET volume_id='replacement'; UPDATE allocation_revisions SET revision=2; UPDATE entries SET inode='replacement',ctime_ns=999 WHERE path=CAST('a/node_modules' AS BLOB); UPDATE entries SET size=999 WHERE path=CAST('a/node_modules/file' AS BLOB)"); err != nil {
		t.Fatal(err)
	}
	page, err := r.nodeModulesFindingPage(ctx, "", FindingAgeDays, tx)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := SingleDismissalSelection(page, saved.Targets[0].FindingID)
	want, wantErr := CanonicalDismissalSelection(saved)
	if err != nil || wantErr != nil || !reflect.DeepEqual(frozen, want) {
		t.Fatal("binding or measurement crossed the page snapshot", frozen, want, err, wantErr)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = r.CheckDismissalSelection(ctx, saved); !errors.Is(err, ErrDismissalChanged) {
		t.Fatal("subsequent snapshot missed committed changes", err)
	}
	if _, err = w.db.Exec("UPDATE entries SET inode='' WHERE path=CAST('a/node_modules' AS BLOB)"); err != nil {
		t.Fatal(err)
	}
	page, err = r.NodeModulesFindingPage(ctx, "", FindingAgeDays)
	if err != nil || len(page.Evidence.Findings) != 1 || page.Targets[0].Target.Inode != "" {
		t.Fatal("unknown identity hid an ordinary candidate", page, err)
	}
	if _, err = SingleDismissalSelection(page, saved.Targets[0].FindingID); !errors.Is(err, ErrDismissalSelection) {
		t.Fatal("unknown object identity became dismissible", err)
	}
}

func TestDismissalSelectionSchemaCursorAndCancellation(t *testing.T) {
	s, saved := dismissalSelectionFixture(t)
	ctx := context.Background()
	for _, cursor := range []string{"bad", "nm1:0", "nm2:30:1"} {
		if _, err := s.NodeModulesFindingPage(ctx, cursor, FindingAgeDays); !errors.Is(err, ErrReportCursor) {
			t.Fatal(cursor, err)
		}
	}
	if _, err := s.NodeModulesFindingPage(ctx, "", 0); !errors.Is(err, ErrFindingAge) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.CheckDismissalSelection(canceled, saved); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if page, err := s.NodeModulesFindingPage(canceled, "", FindingAgeDays); !errors.Is(err, context.Canceled) || len(page.Evidence.Findings) != 0 {
		t.Fatal("cancellation returned a positive page", page, err)
	}
	s.schema = 8
	if err := s.CheckDismissalSelection(ctx, saved); !errors.Is(err, ErrPlanSchema) {
		t.Fatal(err)
	}
	if _, err := s.NodeModulesFindingPage(ctx, "", FindingAgeDays); !errors.Is(err, ErrPlanSchema) {
		t.Fatal(err)
	}
	if _, err := s.NodeModulesFindings(ctx, "", FindingAgeDays); err != nil {
		t.Fatal("legacy ordinary report stopped working", err)
	}
}
