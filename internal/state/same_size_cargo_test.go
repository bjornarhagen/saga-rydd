package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func sameSizeCargoFixture(t *testing.T, profile string) *Store {
	t.Helper()
	s := newBuildOutputFixture(t)
	addBuildOutputProject(t, s, "project", profile)
	parent := filepath.Join("project", "target", profile, "deps")
	buildOutputPut(t, s.db, parent+"/manual-a", parent, "file", "a", 5, 2<<20, 4096)
	buildOutputPut(t, s.db, parent+"/manual-b", parent, "file", "b", 5, 2<<20, 4096)
	buildOutputPut(t, s.db, "outside-a", ".", "file", "c", 1, 2<<20, 4096)
	buildOutputPut(t, s.db, "outside-b", ".", "file", "d", 1, 2<<20, 4096)
	// The existing layout fixture uses arbitrary identity labels. This filter
	// requires the actual canonical decimal scanner representation, including
	// a valid zero device. No filesystem/toolchain/manifest body is inspected.
	if _, err := s.db.Exec("UPDATE entries SET device='0',inode=CAST(id AS TEXT)"); err != nil {
		t.Fatal(err)
	}
	return s
}

func sameSizeCargoReport(t *testing.T, s *Store, limit int, cursor string) SameSizeReport {
	t.Helper()
	r, err := s.SameSizeCandidatesWithOptions(context.Background(), limit, cursor, 1, SameSizeOptions{true})
	if err != nil {
		t.Fatal(err)
	}
	c := r.CargoTargetExclusion
	if c == nil || c.Policy != SameSizeCargoPolicy || c.Recognition != "saved_layout_metadata_only" || r.CurrentStateVerified || r.ContentVerified || r.EstimatedReclaimableBytes != nil || r.EntriesExamined > limit || len(r.Diagnostics) != 7 {
		t.Fatal("filter acquired authority or changed its raw bound", r)
	}
	if c.RowsWithTargetAncestor != c.ExcludedRows+c.RetainedUnrecognizedRows+c.RetainedUnknownRows+c.RetainedAncestorLimitRows || c.RowsWithTargetAncestor > c.EligibleRows || c.EligibleRows > r.EntriesExamined || r.Diagnostics[5].Count != c.EligibleRows-c.ExcludedRows || r.Diagnostics[6].Count != c.ExcludedRows {
		t.Fatal("first-match or refusal accounting changed", r)
	}
	calls := c.RootQueryCalls + c.MembershipBatchQueryCalls + c.LayoutBatchQueryCalls + c.MarkerQualificationBatchQueryCalls
	if calls > 6*r.EntriesExamined || calls > 55200 {
		t.Fatal("added SQL-query-call budget exceeded", c)
	}
	body, err := json.Marshal(c)
	if err != nil || len(body) > 4096 {
		t.Fatal("unbounded optional summary", len(body), err)
	}
	return r
}

func sameSizeCargoExec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

// This wrapper always owns real transaction rows. It changes only test
// observations after QueryContext succeeds; it cannot replace query accounting.
type sameSizeCargoObservedRows struct {
	rows                                                              sameSizeCargoRows
	stage                                                             sameSizeCargoStage
	expectedRows                                                      int
	nextCalls, scanCalls, headerCalls, presentHeaderCalls, closeCalls int
	row                                                               int
	closed                                                            bool
	afterCloseCalls                                                   int
	header                                                            func(int, *any, *any)
	headerInput                                                       func(int, string, any) any
	inputOrdinal, inputPresence                                       any
	next                                                              func(*sameSizeCargoObservedRows, bool) bool
	beforeScan                                                        func()
	afterScan                                                         func([]any)
	scanResult                                                        func(error) error
	err                                                               error
	closeErr                                                          error
	lastErr, lastCloseErr                                             error
	onClose                                                           func()
}

// Decorate only the actual strict transport destinations. The real Rows.Scan
// still controls column order and stops before payloads after a header error.
type sameSizeCargoObservedHeaderScanner struct {
	destination sql.Scanner
	before      func(any) any
}

func (s *sameSizeCargoObservedHeaderScanner) Scan(value any) error {
	if s.before != nil {
		value = s.before(value)
	}
	return s.destination.Scan(value)
}

func sameSizeCargoTestOwnersEmpty(reader *sameSizeCargoEntryReader) bool {
	if reader.active != nil || reader.proof != nil {
		return false
	}
	for _, stmt := range reader.statements {
		if stmt != nil {
			return false
		}
	}
	return true
}

func (r *sameSizeCargoObservedRows) Next() bool {
	r.nextCalls++
	if r.closed {
		r.afterCloseCalls++
	}
	ok := r.rows.Next()
	if ok {
		r.row++
	}
	if r.next != nil {
		return r.next(r, ok)
	}
	return ok
}

func (r *sameSizeCargoObservedRows) Scan(dest ...any) error {
	r.scanCalls++
	if r.closed {
		r.afterCloseCalls++
	}
	if r.beforeScan != nil {
		r.beforeScan()
	}
	actualDest := dest
	var strictOrdinal *sameSizeCargoOrdinalScanner
	var strictPresence *sameSizeCargoPresenceScanner
	if len(dest) >= 2 {
		ordinal, ordinalOK := dest[0].(*sameSizeCargoOrdinalScanner)
		presence, presenceOK := dest[1].(*sameSizeCargoPresenceScanner)
		if ordinalOK && presenceOK {
			strictOrdinal, strictPresence = ordinal, presence
			r.headerCalls++
			actualDest = append([]any(nil), dest...)
			actualDest[0] = &sameSizeCargoObservedHeaderScanner{destination: ordinal, before: func(value any) any {
				if r.headerInput != nil {
					value = r.headerInput(r.row, "ordinal", value)
				}
				r.inputOrdinal = value
				return value
			}}
			actualDest[1] = &sameSizeCargoObservedHeaderScanner{destination: presence, before: func(value any) any {
				if r.headerInput != nil {
					value = r.headerInput(r.row, "presence", value)
				}
				r.inputPresence = value
				return value
			}}
		}
	}
	err := r.rows.Scan(actualDest...)
	if strictOrdinal != nil && strictOrdinal.admitted && strictPresence.admitted && !strictPresence.absent {
		r.presentHeaderCalls++
	}
	if r.scanResult != nil {
		err = r.scanResult(err)
	}
	if err != nil {
		return err
	}
	if r.afterScan != nil {
		r.afterScan(dest)
	}
	if len(dest) >= 2 {
		ordinal, ordinalOK := dest[0].(*any)
		presence, presenceOK := dest[1].(*any)
		if ordinalOK && presenceOK {
			r.headerCalls++
			if r.header != nil {
				r.header(r.row, ordinal, presence)
			}
			if *ordinal == int64(r.row-1) && *presence == int64(1) {
				r.presentHeaderCalls++
			}
		}
	}
	return nil
}

func (r *sameSizeCargoObservedRows) Err() error {
	r.lastErr = errors.Join(r.rows.Err(), r.err)
	return r.lastErr
}

func (r *sameSizeCargoObservedRows) Close() error {
	r.closeCalls++
	r.closed = true
	if r.onClose != nil {
		r.onClose()
	}
	r.lastCloseErr = errors.Join(r.rows.Close(), r.closeErr)
	return r.lastCloseErr
}

func TestSameSizeCargoCursorStagesAndActualAccounting(t *testing.T) {
	for _, profile := range []string{"debug", "release"} {
		t.Run(profile, func(t *testing.T) {
			s := sameSizeCargoFixture(t, profile)
			opened := map[sameSizeCargoStage]int{}
			var cursors []*sameSizeCargoObservedRows
			active, observations := 0, 0
			r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{
				disableProofReuse: true,
				wrapCursor: func(stage sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
					if active != 0 {
						t.Fatal("a cursor survived into the next stage", stage)
					}
					opened[stage]++
					active++
					cursor := &sameSizeCargoObservedRows{rows: rows, stage: stage, onClose: func() { active-- }}
					cursor.expectedRows = map[sameSizeCargoStage]int{sameSizeCargoRootStage: 1, sameSizeCargoMembershipStage: 6, sameSizeCargoCommonStage: 3, sameSizeCargoDebugStage: 4, sameSizeCargoReleaseStage: 4, sameSizeCargoQualificationStage: 7}[stage]
					cursors = append(cursors, cursor)
					return cursor
				},
				beforeExcluded: func(*sameSizeCargoIdentities, *BuildOutputFinding) {
					observations++
					if active != 0 {
						t.Fatal("cursor retained at the heap checkpoint")
					}
				},
			})
			if err != nil || active != 0 || observations != 2 || r.CargoTargetExclusion == nil {
				t.Fatal(r, active, observations, err)
			}
			want := map[sameSizeCargoStage]int{sameSizeCargoRootStage: 2, sameSizeCargoMembershipStage: 2, sameSizeCargoCommonStage: 2, sameSizeCargoDebugStage: 2, sameSizeCargoQualificationStage: 2}
			if profile == "release" {
				want[sameSizeCargoReleaseStage] = 2
			}
			if !reflect.DeepEqual(opened, want) {
				t.Fatal("profile branch opened the wrong physical stages", opened, want)
			}
			for _, cursor := range cursors {
				if cursor.closeCalls != 1 || cursor.afterCloseCalls != 0 {
					t.Fatal("cursor did not close exactly once before reuse", cursor.closeCalls, cursor.afterCloseCalls)
				}
				count := cursor.expectedRows
				wantScans := cursor.headerCalls + cursor.presentHeaderCalls
				if cursor.stage == sameSizeCargoMembershipStage || cursor.stage == sameSizeCargoQualificationStage {
					wantScans = cursor.headerCalls
				}
				if cursor.row != count || cursor.headerCalls != count || cursor.scanCalls != wantScans || cursor.nextCalls != count+1 {
					t.Fatal("production Scan count changed", cursor.stage, cursor.row, cursor.headerCalls, cursor.presentHeaderCalls, cursor.scanCalls, cursor.nextCalls)
				}
			}
			c := r.CargoTargetExclusion
			layoutCalls := 4
			if profile == "release" {
				layoutCalls = 6
			}
			if c.RootQueryCalls != 2 || c.RootEvidenceRequests != 2 || c.ReusedProofRows != 0 || c.MembershipBatchQueryCalls != 2 || c.LayoutBatchQueryCalls != layoutCalls || c.MarkerQualificationBatchQueryCalls != 2 || c.ExcludedRows != 2 {
				t.Fatal("physical query attempts do not match actual cursor opens", c, opened)
			}
		})
	}
}

func TestSameSizeCargoTransportHeadersRefuseWholeReport(t *testing.T) {
	stages := []sameSizeCargoStage{sameSizeCargoRootStage, sameSizeCargoMembershipStage, sameSizeCargoCommonStage, sameSizeCargoDebugStage, sameSizeCargoReleaseStage, sameSizeCargoQualificationStage}
	for _, stage := range stages {
		for _, field := range []string{"ordinal", "presence"} {
			for _, value := range []struct {
				name  string
				value any
			}{{"text", "1"}, {"real", float64(1)}, {"null", nil}, {"boolean", true}, {"negative", int64(-1)}, {"out_of_range", int64(1000)}} {
				t.Run(fmt.Sprintf("stage=%d/%s/%s", stage, field, value.name), func(t *testing.T) {
					s := sameSizeCargoFixture(t, "release")
					var changed *sameSizeCargoObservedRows
					committed := false
					r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{
						beforeCommit: func() { committed = true },
						wrapCursor: func(current sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
							if current != stage || changed != nil {
								return rows
							}
							changed = &sameSizeCargoObservedRows{rows: rows}
							changed.headerInput = func(row int, column string, raw any) any {
								if row == 1 && column == field {
									return value.value
								}
								return raw
							}
							if current == sameSizeCargoMembershipStage || current == sameSizeCargoQualificationStage {
								changed.afterScan = func([]any) { t.Fatal("invalid header completed a member payload Scan") }
							}
							changed.header = func(row int, ordinal, presence *any) {
								if row != 1 {
									return
								}
								if field == "ordinal" {
									*ordinal = value.value
								} else {
									*presence = value.value
								}
							}
							return changed
						},
					})
					if changed == nil || changed.headerCalls != 1 || changed.scanCalls != 1 || changed.closeCalls != 1 || changed.afterCloseCalls != 0 || err == nil || committed || !reflect.DeepEqual(r, SameSizeReport{}) {
						t.Fatal("malformed transport reached payload or escaped whole-report refusal", changed, committed, r, err)
					}
				})
			}
		}
	}
}

func TestSameSizeCargoBatchQueryIndexedPlans(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	for _, stage := range []sameSizeCargoStage{sameSizeCargoRootStage, sameSizeCargoMembershipStage, sameSizeCargoCommonStage, sameSizeCargoDebugStage, sameSizeCargoReleaseStage, sameSizeCargoQualificationStage} {
		t.Run(fmt.Sprintf("stage=%d", stage), func(t *testing.T) {
			query, err := sameSizeCargoBatchQuery(stage)
			if err != nil {
				t.Fatal(err)
			}
			var args []any
			switch stage {
			case sameSizeCargoRootStage:
				args = []any{[]byte("project/Cargo.toml"), []byte("project/Cargo.lock"), int64(1)}
			case sameSizeCargoMembershipStage:
				args = make([]any, 3+2*257)
				args[0], args[1], args[2] = int64(1), []byte("project/target"), 1
				for i := 0; i < 257; i++ {
					args[3+2*i], args[4+2*i] = 0, ""
				}
				args[3], args[4] = len("project/target"), "project"
			default:
				args = make([]any, 2+2*sameSizeCargoStageSize(stage))
				args[0], args[1] = int64(1), 1
				for i := 0; i < sameSizeCargoStageSize(stage); i++ {
					args[2+2*i], args[3+2*i] = []byte{}, ""
				}
				args[2], args[3] = []byte("project/target"), "project"
			}
			if len(args) >= 999 {
				t.Fatal("static shape exceeded its portable bind bound", stage, len(args))
			}
			rows, err := s.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
			if err != nil {
				t.Fatal("actual static shape did not accept its exact argument array", stage, len(args), err)
			}
			var details []string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				details = append(details, detail)
				if len(details) > 1024 {
					rows.Close()
					t.Fatal("query plan exceeded its fixed observation bound")
				}
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				t.Fatal(err)
			}
			aliases := []string{"e", "p", "o"}
			if stage == sameSizeCargoRootStage {
				aliases = []string{"m", "l"}
			}
			for _, alias := range aliases {
				seeks := 0
				for _, detail := range details {
					if strings.HasPrefix(detail, "SCAN "+alias+" ") {
						t.Fatal("saved table became an unbounded scan", stage, alias, details)
					}
					if strings.HasPrefix(detail, "SEARCH "+alias+" ") {
						if !strings.Contains(detail, "INDEX") || !strings.Contains(detail, "root_id=?") || !strings.Contains(detail, "path=?") {
							t.Fatal("saved-key lookup lost either indexed equality", stage, alias, detail)
						}
						seeks++
					}
				}
				if seeks != 1 {
					t.Fatal("saved relation lacks one exact indexed seek", stage, alias, seeks, details)
				}
			}
			if stage == sameSizeCargoRootStage {
				seeks := 0
				for _, detail := range details {
					if strings.HasPrefix(detail, "SCAN r ") {
						t.Fatal("root key became a table scan", details)
					}
					if strings.HasPrefix(detail, "SEARCH r ") {
						if !strings.Contains(detail, "PRIMARY KEY") || !strings.Contains(detail, "rowid=?") {
							t.Fatal("root lost its exact integer key seek", detail)
						}
						seeks++
					}
				}
				if seeks != 1 {
					t.Fatal("root relation lacks its exact primary-key seek", seeks, details)
				}
			}
		})
	}
}

func TestSameSizeCargoSealedRequestBoundsAndCorrespondence(t *testing.T) {
	for _, ancestors := range []int{0, 1, 256, 257} {
		t.Run(fmt.Sprintf("ancestors=%d", ancestors), func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			leaf := strings.Repeat("a/", ancestors) + "leaf"
			requests := make([]sameSizeCargoRequest, 0, ancestors+1)
			for path := leaf; ; path = filepath.Dir(path) {
				requests = append(requests, sameSizeCargoRequest{path: path, parent: sameSizeCargoExpectedParent(path), role: sameSizeCargoMemberRole})
				if len(requests) == ancestors+1 {
					break
				}
			}
			reader := sameSizeCargoEntryReader{tx: tx}
			err = reader.open(context.Background(), sameSizeCargoMembershipStage, 1, []byte(leaf), requests)
			if ancestors == 257 {
				if err == nil || !sameSizeCargoTestOwnersEmpty(&reader) {
					t.Fatal("258-request plan prepared an owner", err)
				}
				return
			}
			if err != nil {
				t.Fatal("bounded membership plan refused", ancestors, err)
			}
			for _, request := range requests {
				if _, exists, err := reader.next(request.path, request.role, request.parent); err != nil || exists {
					t.Fatal("bounded missing key changed request correspondence", request, exists, err)
				}
			}
			if err := reader.finish(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := reader.close(); err != nil || !sameSizeCargoTestOwnersEmpty(&reader) {
				t.Fatal(err)
			}
		})
	}
	for _, change := range []string{"key", "role", "parent", "copied_plan", "root_id"} {
		t.Run(change, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			requests := []sameSizeCargoRequest{{path: "project/target", parent: "project", role: sameSizeCargoMemberRole}}
			reader := sameSizeCargoEntryReader{tx: tx}
			var observed *sameSizeCargoObservedRows
			reader.wrapCursor = func(_ sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
				observed = &sameSizeCargoObservedRows{rows: rows}
				return observed
			}
			if err := reader.open(context.Background(), sameSizeCargoMembershipStage, 1, []byte(requests[0].path), requests); err != nil {
				t.Fatal(err)
			}
			path, parent, role := requests[0].path, requests[0].parent, requests[0].role
			switch change {
			case "key":
				path = "project/Cargo.toml"
			case "role":
				role = sameSizeCargoLayoutRole
			case "parent":
				parent = "."
			case "copied_plan":
				requests[0].path = "changed-after-open"
			}
			if change == "root_id" {
				if _, _, err := readSameSizeCargoEntry(context.Background(), &reader, 2, path); err == nil || observed.nextCalls != 0 || observed.scanCalls != 0 {
					t.Fatal("helper consumed another root's sealed row", err, observed.nextCalls, observed.scanCalls)
				}
			} else {
				_, exists, err := reader.next(path, role, parent)
				if change == "copied_plan" {
					if err != nil || !exists {
						t.Fatal("caller mutation changed the sealed plan", exists, err)
					}
					if err := reader.finish(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else if err == nil || exists || observed.nextCalls != 0 || observed.scanCalls != 0 {
					t.Fatal("callback mismatch reached transport bytes", change, exists, err, observed.nextCalls, observed.scanCalls)
				}
			}
			if err := reader.close(); err != nil || observed.closeCalls != 1 || observed.afterCloseCalls != 0 {
				t.Fatal("sealed request did not release its rows", change, err)
			}
			// The combined member path has its own before-Next admission. Exercise
			// the same correspondence cases without replacing query dispatch.
			combined := sameSizeCargoEntryReader{tx: tx}
			var combinedRows *sameSizeCargoObservedRows
			combined.wrapCursor = func(_ sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
				combinedRows = &sameSizeCargoObservedRows{rows: rows}
				return combinedRows
			}
			stage := sameSizeCargoMembershipStage
			plan := []sameSizeCargoRequest{{path: "project/target", parent: "project", role: sameSizeCargoMemberRole}}
			if change == "role" {
				stage = sameSizeCargoCommonStage
				plan[0].role = sameSizeCargoLayoutRole
			}
			if err := combined.open(context.Background(), stage, 1, []byte("project/target"), plan); err != nil {
				t.Fatal(err)
			}
			row := sameSizeCargoMemberRow{reader: &combined, path: "project/target", parent: "project"}
			switch change {
			case "key":
				row.path = "project/Cargo.toml"
			case "parent":
				row.parent = "."
			case "copied_plan":
				plan[0].path = "changed-after-open"
			}
			var entry sameSizeCargoEntry
			var combinedErr error
			if change == "root_id" {
				_, _, combinedErr = readSameSizeCargoEntry(context.Background(), &combined, 2, row.path)
			} else {
				entry, combinedErr = scanSameSizeCargoEntry(&row)
			}
			if change == "copied_plan" {
				if combinedErr != nil || entry.id <= 0 || combinedRows.nextCalls != 1 || combinedRows.scanCalls != 1 {
					t.Fatal("combined Scan did not use the sealed caller plan", entry, combinedErr)
				}
				if err := combined.finish(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if combinedErr == nil || combinedRows.nextCalls != 0 || combinedRows.scanCalls != 0 {
				t.Fatal("combined callback mismatch consumed transport", change, combinedErr, combinedRows.nextCalls, combinedRows.scanCalls)
			}
			if err := combined.close(); err != nil || combinedRows.closeCalls != 1 || combinedRows.afterCloseCalls != 0 {
				t.Fatal("combined refusal left rows open", change, err)
			}
		})
	}
	for _, invalid := range []string{"zero_count", "invalid_stage", "overlong_leaf", "overlong_key"} {
		t.Run(invalid, func(t *testing.T) {
			reader := sameSizeCargoEntryReader{}
			stage, leaf := sameSizeCargoMembershipStage, "leaf"
			requests := []sameSizeCargoRequest{{path: leaf, parent: ".", role: sameSizeCargoMemberRole}}
			switch invalid {
			case "zero_count":
				requests = nil
			case "invalid_stage":
				stage = sameSizeCargoStage(255)
			case "overlong_leaf":
				leaf = "leaf" + strings.Repeat("a", 4093)
			case "overlong_key":
				leaf = strings.Repeat("a", 4097)
				requests[0].path = leaf
			}
			if err := reader.open(context.Background(), stage, 1, []byte(leaf), requests); err == nil || !sameSizeCargoTestOwnersEmpty(&reader) || reader.sealedCount != 0 {
				t.Fatal("invalid plan reached preparation", invalid, err)
			}
		})
	}
	for _, path := range []string{".", "raw-\xff/leaf", strings.Repeat("a", 4096)} {
		name := "dot"
		if path != "." {
			name = "raw_bytes"
			if len(path) == 4096 {
				name = "inclusive_4096"
			}
		}
		t.Run(name, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			if path != "." {
				buildOutputPut(t, s.db, path, sameSizeCargoExpectedParent(path), "file", "30000", 1, 1, 1)
			}
			tx, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			reader := sameSizeCargoEntryReader{tx: tx}
			var observed *sameSizeCargoObservedRows
			reader.wrapCursor = func(_ sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
				observed = &sameSizeCargoObservedRows{rows: rows}
				return observed
			}
			entry, exists, err := readSameSizeCargoEntry(context.Background(), &reader, 1, path)
			if err != nil || !exists || entry.id <= 0 || observed == nil || observed.nextCalls != 2 || observed.headerCalls != 1 || observed.scanCalls != 1 || observed.closeCalls != 1 || reader.active != nil || reader.sealedCount != 0 {
				t.Fatal("bounded exact byte key lost singleton correspondence", name, entry, exists, observed, err)
			}
			if err := reader.close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSameSizeCargoMissingExtraDuplicateAndReorderedRows(t *testing.T) {
	for _, stage := range []sameSizeCargoStage{sameSizeCargoRootStage, sameSizeCargoMembershipStage, sameSizeCargoCommonStage, sameSizeCargoDebugStage, sameSizeCargoReleaseStage, sameSizeCargoQualificationStage} {
		for _, fault := range []string{"missing_first", "missing_last", "extra", "reordered_first", "duplicate_second"} {
			if stage == sameSizeCargoRootStage && fault == "duplicate_second" {
				continue
			}
			t.Run(fmt.Sprintf("stage=%d/%s", stage, fault), func(t *testing.T) {
				s := sameSizeCargoFixture(t, "release")
				var changed *sameSizeCargoObservedRows
				injected := false
				last := map[sameSizeCargoStage]int{sameSizeCargoRootStage: 1, sameSizeCargoMembershipStage: 6, sameSizeCargoCommonStage: 3, sameSizeCargoDebugStage: 4, sameSizeCargoReleaseStage: 4, sameSizeCargoQualificationStage: 7}[stage]
				var firstOrdinal any
				committed := false
				r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{beforeCommit: func() { committed = true }, wrapCursor: func(current sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
					if current != stage || changed != nil {
						return rows
					}
					changed = &sameSizeCargoObservedRows{rows: rows}
					changed.next = func(cursor *sameSizeCargoObservedRows, ok bool) bool {
						if fault == "missing_first" && ok && cursor.row == 1 || fault == "missing_last" && ok && cursor.row == last {
							injected = true
							return false
						}
						if fault == "extra" && !ok && !injected {
							injected = true
							return true
						}
						return ok
					}
					changed.header = func(row int, ordinal, _ *any) {
						if row == 1 {
							firstOrdinal = *ordinal
						}
						if fault == "reordered_first" && row == 1 {
							*ordinal = (*ordinal).(int64) + 1
							injected = true
						}
						if fault == "duplicate_second" && row == 2 {
							*ordinal = firstOrdinal
							injected = true
						}
					}
					changed.headerInput = func(row int, column string, value any) any {
						if column != "ordinal" {
							return value
						}
						if row == 1 {
							firstOrdinal = value
						}
						if fault == "reordered_first" && row == 1 {
							injected = true
							return value.(int64) + 1
						}
						if fault == "duplicate_second" && row == 2 {
							injected = true
							return firstOrdinal
						}
						return value
					}
					return changed
				}})
				if changed == nil || !injected || changed.closeCalls != 1 || changed.afterCloseCalls != 0 || err == nil || committed || !reflect.DeepEqual(r, SameSizeReport{}) {
					t.Fatal("transport cardinality/order fault escaped", stage, fault, changed, injected, committed, r, err)
				}
			})
		}
	}
}

func TestSameSizeCargoLogicalRefusalChecksCloseWithoutLaterEvidence(t *testing.T) {
	for _, stage := range []sameSizeCargoStage{sameSizeCargoMembershipStage, sameSizeCargoQualificationStage} {
		for _, terminal := range []string{"clean", "Err", "Close", "cancel_close"} {
			t.Run(fmt.Sprintf("stage=%d/%s", stage, terminal), func(t *testing.T) {
				s := sameSizeCargoFixture(t, "release")
				if stage == sameSizeCargoMembershipStage {
					sameSizeCargoExec(t, s, "UPDATE directories SET complete=0 WHERE path=?", []byte("project/target/release/deps"))
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				terminalErr := errors.New("generated logical-refusal terminal error")
				var cursors []*sameSizeCargoObservedRows
				committed := false
				r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, sameSizeCargoHooks{beforeCommit: func() { committed = true }, wrapCursor: func(current sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
					if current != stage {
						return rows
					}
					cursor := &sameSizeCargoObservedRows{rows: rows}
					switch terminal {
					case "Err":
						cursor.err = terminalErr
					case "Close":
						cursor.closeErr = terminalErr
					case "cancel_close":
						cursor.onClose = cancel
					}
					cursor.header = func(row int, ordinal, _ *any) {
						if row > 1 {
							*ordinal = "unreached corrupt header"
						}
					}
					cursor.headerInput = func(row int, column string, value any) any {
						if row > 1 && column == "ordinal" {
							return "unreached corrupt header"
						}
						return value
					}
					if stage == sameSizeCargoQualificationStage {
						cursor.afterScan = func(dest []any) {
							if _, header := dest[0].(*any); header {
								return
							}
							if len(dest) != 28 {
								t.Fatal("qualification payload width changed")
							}
							kind, ok := dest[16].(*[]byte)
							if !ok {
								t.Fatal("qualification kind decoder destination changed")
							}
							*kind = []byte("file")
						}
					}
					cursors = append(cursors, cursor)
					return cursor
				}})
				if terminal == "clean" {
					if err != nil || len(cursors) != 2 || !committed || r.CargoTargetExclusion == nil || r.CargoTargetExclusion.RetainedUnknownRows != 2 || r.CargoTargetExclusion.ExcludedRows != 0 {
						t.Fatal("later corrupt transport became observed after logical refusal", r, err, len(cursors))
					}
				} else {
					cause := terminalErr
					if terminal == "cancel_close" {
						cause = context.Canceled
					}
					if !errors.Is(err, cause) || len(cursors) != 1 || committed || !reflect.DeepEqual(r, SameSizeReport{}) {
						t.Fatal("early-refusal close cause did not veto the whole report", terminal, r, err, len(cursors))
					}
				}
				for _, cursor := range cursors {
					if cursor.nextCalls != 1 || cursor.headerCalls != 1 || cursor.scanCalls != 1 || cursor.closeCalls != 1 || cursor.afterCloseCalls != 0 {
						t.Fatal("refusal drained or decoded unused evidence", cursor.nextCalls, cursor.headerCalls, cursor.scanCalls, cursor.closeCalls, cursor.afterCloseCalls)
					}
				}
			})
		}
	}
}

func TestSameSizeCargoCursorCancellationFirstMiddleLast(t *testing.T) {
	for _, stage := range []sameSizeCargoStage{sameSizeCargoRootStage, sameSizeCargoMembershipStage, sameSizeCargoCommonStage, sameSizeCargoDebugStage, sameSizeCargoReleaseStage, sameSizeCargoQualificationStage} {
		maximum := map[sameSizeCargoStage]int{sameSizeCargoRootStage: 1, sameSizeCargoMembershipStage: 6, sameSizeCargoCommonStage: 3, sameSizeCargoDebugStage: 4, sameSizeCargoReleaseStage: 4, sameSizeCargoQualificationStage: 7}[stage]
		for _, position := range []struct {
			name string
			row  int
		}{{"first", 1}, {"middle", (maximum + 1) / 2}, {"last", maximum}} {
			if maximum == 1 && position.name != "first" {
				continue
			}
			for _, operation := range []string{"Next", "Scan", "Close"} {
				if operation == "Close" && position.name != "last" && maximum != 1 {
					continue
				}
				t.Run(fmt.Sprintf("stage=%d/%s/%s", stage, position.name, operation), func(t *testing.T) {
					s := sameSizeCargoFixture(t, "release")
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var changed *sameSizeCargoObservedRows
					injected, committed := false, false
					r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, sameSizeCargoHooks{beforeCommit: func() { committed = true }, wrapCursor: func(current sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
						if current != stage || changed != nil {
							return rows
						}
						changed = &sameSizeCargoObservedRows{rows: rows}
						changed.next = func(cursor *sameSizeCargoObservedRows, ok bool) bool {
							if operation == "Next" && ok && cursor.row == position.row && !injected {
								injected = true
								cancel()
							}
							return ok
						}
						changed.beforeScan = func() {
							if operation == "Scan" && changed.row == position.row && !injected {
								injected = true
								cancel()
							}
						}
						changed.onClose = func() {
							if operation == "Close" && !injected {
								injected = true
								cancel()
							}
						}
						return changed
					}})
					if changed == nil || !injected || changed.closeCalls != 1 || changed.afterCloseCalls != 0 || committed || !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, SameSizeReport{}) {
						t.Fatal("cursor cancellation escaped checked closure", stage, position, operation, changed, injected, committed, r, err)
					}
				})
			}
		}
	}
}

func TestSameSizeCargoCursorErrAndCloseVetoWholeReport(t *testing.T) {
	for _, stage := range []sameSizeCargoStage{sameSizeCargoRootStage, sameSizeCargoMembershipStage, sameSizeCargoCommonStage, sameSizeCargoDebugStage, sameSizeCargoReleaseStage, sameSizeCargoQualificationStage} {
		for _, operation := range []string{"Err", "Close", "both"} {
			t.Run(fmt.Sprintf("stage=%d/%s", stage, operation), func(t *testing.T) {
				s := sameSizeCargoFixture(t, "release")
				rowErr, closeErr := errors.New("generated cursor Err"), errors.New("generated cursor Close")
				var changed *sameSizeCargoObservedRows
				committed := false
				r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{beforeCommit: func() { committed = true }, wrapCursor: func(current sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
					if current != stage || changed != nil {
						return rows
					}
					changed = &sameSizeCargoObservedRows{rows: rows}
					if operation != "Close" {
						changed.err = rowErr
					}
					if operation != "Err" {
						changed.closeErr = closeErr
					}
					return changed
				}})
				if changed == nil || changed.closeCalls != 1 || changed.afterCloseCalls != 0 || committed || !reflect.DeepEqual(r, SameSizeReport{}) || operation != "Close" && !errors.Is(err, rowErr) || operation != "Err" && !errors.Is(err, closeErr) {
					t.Fatal("observed cursor terminal cause was lost", stage, operation, changed, committed, r, err)
				}
			})
		}
	}
}

type sameSizeCargoCycleError struct{ marker byte }

func (*sameSizeCargoCycleError) Error() string   { return "generated cyclic wrapping error" }
func (e *sameSizeCargoCycleError) Unwrap() error { return e }

func TestSameSizeCargoAbsenceStopNormalization(t *testing.T) {
	stop, foreign := new(sameSizeCargoAbsenceStop), new(sameSizeCargoAbsenceStop)
	if stop == foreign {
		t.Fatal("distinct nonzero-sized rows shared a stop identity")
	}
	other := errors.New("generated unrelated Scan error")
	wrap := func(err error, count int) error {
		for i := 0; i < count; i++ {
			err = fmt.Errorf("generated wrapping %d: %w", i, err)
		}
		return err
	}
	for _, tc := range []struct {
		name string
		err  error
		stop *sameSizeCargoAbsenceStop
		want bool
	}{
		{"own", stop, stop, true}, {"single_wrap", wrap(stop, 1), stop, true}, {"fifteen_links", wrap(stop, 15), stop, true},
		{"sixteen_links", wrap(stop, 16), stop, false}, {"nil_error", nil, stop, false}, {"nil_stop", stop, nil, false},
		{"bare_ErrNoRows", sql.ErrNoRows, stop, false}, {"wrapped_ErrNoRows", wrap(sql.ErrNoRows, 1), stop, false},
		{"joined_ErrNoRows", errors.Join(sql.ErrNoRows, other), stop, false}, {"own_plus_other", errors.Join(stop, other), stop, false},
		{"singleton_join", errors.Join(stop), stop, false}, {"wrapped_join", wrap(errors.Join(stop, other), 1), stop, false},
		{"foreign", foreign, stop, false}, {"wrapped_foreign", wrap(foreign, 1), stop, false},
		{"unrelated_terminal", wrap(other, 2), stop, false}, {"cyclic", new(sameSizeCargoCycleError), stop, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameSizeCargoOnlyAbsenceStop(tc.err, tc.stop); got != tc.want {
				t.Fatal("absence normalization lost exact cause identity", tc.name, got, tc.want)
			}
		})
	}
}

func TestSameSizeCargoMemberAbsenceAndScanErrors(t *testing.T) {
	for _, stage := range []sameSizeCargoStage{sameSizeCargoMembershipStage, sameSizeCargoQualificationStage} {
		for _, name := range []string{"present", "genuine_missing", "sole_wrapped_stop", "bare_present", "joined_present", "bare_absent", "joined_absent", "stop_plus_error", "stop_singleton_join", "foreign_stop", "unrelated_chain", "own_stop_without_absence"} {
			t.Run(fmt.Sprintf("stage=%d/%s", stage, name), func(t *testing.T) {
				s := sameSizeCargoFixture(t, "debug")
				tx, err := s.db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				path := "not-saved"
				if name == "present" || name == "bare_present" || name == "joined_present" || name == "own_stop_without_absence" {
					path = "project/target"
				}
				reader := sameSizeCargoEntryReader{tx: tx}
				var observed *sameSizeCargoObservedRows
				var originalScanErr error
				other := errors.New("generated additional Scan cause")
				foreign := new(sameSizeCargoAbsenceStop)
				row := sameSizeCargoMemberRow{reader: &reader, path: path, parent: sameSizeCargoExpectedParent(path)}
				reader.wrapCursor = func(_ sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
					observed = &sameSizeCargoObservedRows{rows: rows}
					observed.scanResult = func(scanErr error) error {
						originalScanErr = scanErr
						switch name {
						case "sole_wrapped_stop":
							return fmt.Errorf("generated sole wrapping: %w", scanErr)
						case "bare_present", "bare_absent":
							return sql.ErrNoRows
						case "joined_present", "joined_absent":
							return errors.Join(sql.ErrNoRows, other)
						case "stop_plus_error":
							return errors.Join(scanErr, other)
						case "stop_singleton_join":
							return errors.Join(scanErr)
						case "foreign_stop":
							return fmt.Errorf("generated foreign wrapping: %w", foreign)
						case "unrelated_chain":
							return fmt.Errorf("generated unrelated wrapping: %w", other)
						case "own_stop_without_absence":
							return &row.stop
						}
						return scanErr
					}
					return observed
				}
				request := sameSizeCargoRequest{path: path, parent: sameSizeCargoExpectedParent(path), role: sameSizeCargoMemberRole}
				if err := reader.open(context.Background(), stage, 1, []byte(path), []sameSizeCargoRequest{request}); err != nil {
					t.Fatal(err)
				}
				var entry sameSizeCargoEntry
				var exists bool
				if name == "own_stop_without_absence" {
					entry, err = scanSameSizeCargoEntry(&row)
					if row.normalizedAbsence || err != &row.stop {
						t.Fatal("own stop without admitted zero became absence", row.normalizedAbsence, err)
					}
				} else {
					entry, exists, err = readSameSizeCargoEntry(context.Background(), &reader, 1, path)
				}
				positive := name == "present" || name == "genuine_missing" || name == "sole_wrapped_stop"
				if positive {
					if err != nil || exists != (name == "present") || name == "present" && entry.id <= 0 || name != "present" && !reflect.DeepEqual(entry, sameSizeCargoEntry{}) {
						t.Fatal("validated presence lost original member behavior", name, entry, exists, err)
					}
				} else {
					if err == nil || exists {
						t.Fatal("observed Scan error became logical missing", name, entry, exists, err)
					}
					if strings.HasPrefix(name, "bare_") || strings.HasPrefix(name, "joined_") {
						if !errors.Is(err, sql.ErrNoRows) {
							t.Fatal("generic Scan cause lost", name, err)
						}
					}
					if strings.HasPrefix(name, "joined_") || name == "stop_plus_error" || name == "unrelated_chain" {
						if !errors.Is(err, other) {
							t.Fatal("additional Scan cause was hidden", name, err)
						}
					}
					if name == "stop_plus_error" || name == "stop_singleton_join" {
						if originalScanErr == nil || !errors.Is(err, originalScanErr) {
							t.Fatal("private Scan stop cause was hidden", name, originalScanErr, err)
						}
					}
					if name == "foreign_stop" && !errors.Is(err, foreign) {
						t.Fatal("foreign row cause lost", err)
					}
				}
				wantNext := 1
				if name == "present" {
					wantNext = 2
					if err := reader.finish(context.Background()); err != nil {
						t.Fatal(err)
					}
				} else if err := reader.closeCursor(context.Background()); err != nil {
					t.Fatal(err)
				}
				if observed == nil || observed.nextCalls != wantNext || observed.headerCalls != 1 || observed.scanCalls != 1 || observed.closeCalls != 1 || observed.afterCloseCalls != 0 {
					t.Fatal("absence/error path touched later evidence", name, observed)
				}
				if err := reader.close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func sameSizeCargoComparableProofReport(r SameSizeReport) SameSizeReport {
	r.GeneratedAt = time.Time{}
	if r.CargoTargetExclusion != nil {
		c := *r.CargoTargetExclusion
		c.RootQueryCalls, c.MembershipBatchQueryCalls, c.LayoutBatchQueryCalls, c.MarkerQualificationBatchQueryCalls, c.ReusedProofRows = 0, 0, 0, 0, 0
		r.CargoTargetExclusion = &c
	}
	return r
}

func TestSameSizeCargoProofReuseDecisionsAndLogicalPrefixes(t *testing.T) {
	for _, profile := range []string{"debug", "release"} {
		for _, name := range []string{"ordinary", "cold_marker_alias", "matching_marker_alias", "conflicting_marker_alias", "ancestor_alias", "shared_ancestor_alias", "stale_leaf", "raw_id_mismatch", "wrong_kind"} {
			t.Run(profile+"/"+name, func(t *testing.T) {
				s := sameSizeCargoFixture(t, profile)
				// Equal sizes use ID descending: manual-b is cold, manual-a is the hit.
				leaf := filepath.Join("project", "target", profile, "deps", "manual-a")
				if name == "cold_marker_alias" {
					leaf = filepath.Join("project", "target", profile, "deps", "manual-b")
				}
				alias := func(path, other string) {
					sameSizeCargoExec(t, s, `UPDATE entries SET device=(SELECT device FROM entries WHERE path=?),inode=(SELECT inode FROM entries WHERE path=?),ctime_ns=(SELECT ctime_ns FROM entries WHERE path=?),mtime_ns=(SELECT mtime_ns FROM entries WHERE path=?),allocated=(SELECT allocated FROM entries WHERE path=?) WHERE path=?`, []byte(other), []byte(other), []byte(other), []byte(other), []byte(other), []byte(path))
				}
				switch name {
				case "cold_marker_alias", "matching_marker_alias", "conflicting_marker_alias":
					alias(leaf, "project/Cargo.toml")
				case "ancestor_alias":
					alias(leaf, filepath.Join("project", "target", profile, "deps"))
				case "shared_ancestor_alias":
					alias("project/target", filepath.Join("project", "target", profile, "deps"))
				case "stale_leaf":
					sameSizeCargoExec(t, s, "UPDATE entries SET generation=0 WHERE path=?", []byte(leaf))
				}
				if name == "conflicting_marker_alias" {
					sameSizeCargoExec(t, s, "UPDATE entries SET ctime_ns=ctime_ns+1 WHERE path=?", []byte(leaf))
				}
				run := func(disabled bool) SameSizeReport {
					memberCalls := 0
					h := sameSizeCargoHooks{disableProofReuse: disabled}
					if name == "raw_id_mismatch" || name == "wrong_kind" {
						h.wrapCursor = func(stage sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
							if stage != sameSizeCargoMembershipStage {
								return rows
							}
							memberCalls++
							if memberCalls != 2 {
								return rows
							}
							wrapped := &sameSizeCargoObservedRows{rows: rows}
							wrapped.afterScan = func(dest []any) {
								if wrapped.row != 1 {
									return
								}
								if name == "raw_id_mismatch" {
									id := dest[2].(*any)
									*id = (*id).(int64) + 10000
								} else {
									*dest[16].(*[]byte) = []byte("directory")
								}
							}
							return wrapped
						}
					}
					r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, h)
					if err != nil {
						t.Fatal(disabled, err)
					}
					return r
				}
				cold, hit := run(true), run(false)
				if !reflect.DeepEqual(sameSizeCargoComparableProofReport(cold), sameSizeCargoComparableProofReport(hit)) {
					t.Fatal("reused proof changed decisions or first refusal demand", name, cold.CargoTargetExclusion, hit.CargoTargetExclusion)
				}
				c := hit.CargoTargetExclusion
				positive := name == "ordinary" || name == "cold_marker_alias" || name == "matching_marker_alias" || name == "shared_ancestor_alias"
				wantExcluded := 1
				if positive {
					wantExcluded = 2
				}
				wantReuse := 0
				if positive && name != "cold_marker_alias" {
					wantReuse = 1
				}
				if c.ExcludedRows != wantExcluded || c.RetainedUnknownRows != 2-wantExcluded || c.ReusedProofRows != wantReuse || c.RootEvidenceRequests != 2 || c.RootQueryCalls != map[bool]int{false: 1, true: 2}[name == "cold_marker_alias"] {
					t.Fatal("proof admission/refusal branch was not exercised", name, c)
				}
			})
		}
	}
}

func TestSameSizeCargoProofReuseActualStages(t *testing.T) {
	for _, profile := range []string{"debug", "release"} {
		t.Run(profile, func(t *testing.T) {
			s := sameSizeCargoFixture(t, profile)
			opened := map[sameSizeCargoStage]int{}
			active, checkpoints := 0, 0
			r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{wrapCursor: func(stage sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
				if active != 0 {
					t.Fatal("proof hit overlapped a cursor")
				}
				active++
				opened[stage]++
				cursor := &sameSizeCargoObservedRows{rows: rows, onClose: func() { active-- }}
				if stage == sameSizeCargoMembershipStage && opened[stage] == 2 {
					cursor.onClose = func() {
						active--
						if cursor.row != 1 || cursor.headerCalls != 1 || cursor.scanCalls != 1 || cursor.nextCalls != 2 {
							t.Fatal("hit did not read and finish exactly one real leaf", cursor)
						}
					}
				}
				return cursor
			}, beforeExcluded: func(*sameSizeCargoIdentities, *BuildOutputFinding) {
				checkpoints++
				if active != 0 {
					t.Fatal("proof was observed before cursor close")
				}
			}})
			want := map[sameSizeCargoStage]int{sameSizeCargoRootStage: 1, sameSizeCargoMembershipStage: 2, sameSizeCargoCommonStage: 1, sameSizeCargoDebugStage: 1, sameSizeCargoQualificationStage: 1}
			if profile == "release" {
				want[sameSizeCargoReleaseStage] = 1
			}
			if err != nil || active != 0 || checkpoints != 2 || !reflect.DeepEqual(opened, want) || r.CargoTargetExclusion.ReusedProofRows != 1 {
				t.Fatal("proof hit queried shared stages or skipped a checkpoint", opened, want, checkpoints, r, err)
			}
		})
	}
}

func TestSameSizeCargoProofReuseHitFailures(t *testing.T) {
	for _, fault := range []string{"missing", "extra", "ordinal", "presence", "Scan", "Err", "Close", "cancel_Next", "cancel_Scan", "cancel_Close"} {
		t.Run(fault, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "release")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			memberCalls := 0
			committed := false
			cause := errors.New("generated proof-hit cursor failure")
			var changed *sameSizeCargoObservedRows
			r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, sameSizeCargoHooks{beforeCommit: func() { committed = true }, wrapCursor: func(stage sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
				if stage != sameSizeCargoMembershipStage {
					return rows
				}
				memberCalls++
				if memberCalls != 2 {
					return rows
				}
				changed = &sameSizeCargoObservedRows{rows: rows}
				changed.next = func(_ *sameSizeCargoObservedRows, ok bool) bool {
					if fault == "missing" && ok {
						return false
					}
					if fault == "extra" && !ok {
						return true
					}
					if fault == "cancel_Next" && ok {
						cancel()
					}
					return ok
				}
				changed.headerInput = func(_ int, column string, value any) any {
					if fault == column {
						return "generated invalid hit header"
					}
					return value
				}
				if fault == "Scan" {
					changed.scanResult = func(err error) error { return errors.Join(err, cause) }
				}
				if fault == "Err" {
					changed.err = cause
				}
				if fault == "Close" {
					changed.closeErr = cause
				}
				if fault == "cancel_Scan" {
					changed.beforeScan = cancel
				}
				if fault == "cancel_Close" {
					changed.onClose = cancel
				}
				return changed
			}})
			if changed == nil || memberCalls != 2 || changed.closeCalls != 1 || changed.afterCloseCalls != 0 || committed || err == nil || !reflect.DeepEqual(r, SameSizeReport{}) {
				t.Fatal("hit cursor fault escaped whole-report veto", fault, changed, r, err)
			}
			if fault == "Scan" || fault == "Err" || fault == "Close" {
				if !errors.Is(err, cause) {
					t.Fatal("hit failure cause lost", fault, err)
				}
			}
			if strings.HasPrefix(fault, "cancel_") && !errors.Is(err, context.Canceled) {
				t.Fatal("hit cancellation cause lost", fault, err)
			}
		})
	}
}

func TestSameSizeCargoProofReuseScopeSnapshotAndObserverIsolation(t *testing.T) {
	t.Run("most_recent_exact_scope", func(t *testing.T) {
		s := sameSizeCargoFixture(t, "debug")
		addBuildOutputProject(t, s, "other", "debug")
		p := "other/target/debug/deps"
		buildOutputPut(t, s.db, p+"/a", p, "file", "new", 5, 2<<20, 4096)
		deep := "project/target/debug/deps/deep"
		buildOutputPut(t, s.db, deep, filepath.Dir(deep), "directory", "deep", 5, 0, 0)
		buildOutputDirectory(t, s.db, deep, 6)
		buildOutputPut(t, s.db, deep+"/a", deep, "file", "deep-file", 6, 2<<20, 4096)
		sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		raw, err := readSameSizeCargoRows(context.Background(), tx, sameSizeInitialQuery, 1, 200)
		if err != nil {
			t.Fatal(err)
		}
		files := map[string]sameSizeRawFile{}
		for _, f := range raw {
			files[string(f.relative)] = f
		}
		c := SameSizeCargoTargetExclusion{}
		reader := sameSizeCargoEntryReader{tx: tx, counters: &c}
		defer reader.close()
		apply := func(f sameSizeRawFile, want bool) {
			excluded, err := sameSizeCargoExcluded(context.Background(), tx, &reader, f, &c, sameSizeCargoHooks{})
			if err != nil || excluded != want {
				t.Fatal(string(f.relative), excluded, want, err)
			}
		}
		a, b := files["project/target/debug/deps/manual-a"], files["project/target/debug/deps/manual-b"]
		apply(a, true)
		first := reader.proof
		if first == nil {
			t.Fatal("cold success did not publish proof")
		}
		apply(files["outside-a"], false)
		wrongRoot := a
		wrongRoot.file.RootID = 999
		apply(wrongRoot, false)
		wrongPath := a
		wrongPath.root = []byte("/different-saved-root")
		apply(wrongPath, false)
		if reader.proof != first {
			t.Fatal("refusal replaced positive proof")
		}
		apply(b, true)
		apply(files[deep+"/a"], true)
		if reader.proof == first || reader.proof.parent != deep || reader.proof.target != "project/target" {
			t.Fatal("same target with a different parent reused the old proof")
		}
		apply(a, true)
		apply(files[p+"/a"], true)
		if reader.proof.target != "other/target" {
			t.Fatal("successful target change did not replace the sole proof")
		}
		apply(a, true)
		apply(b, true)
		if c.ReusedProofRows != 2 || c.RootQueryCalls != 7 {
			t.Fatal("scope changes reused an unrelated positive proof", c)
		}
		if err := reader.close(); err != nil || !sameSizeCargoTestOwnersEmpty(&reader) {
			t.Fatal("reader retained proof after close", err)
		}
	})
	t.Run("saved_snapshot", func(t *testing.T) {
		s := sameSizeCargoFixture(t, "debug")
		writer, err := connect(context.Background(), s.path, false)
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		observations := 0
		r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{beforeExcluded: func(*sameSizeCargoIdentities, *BuildOutputFinding) {
			observations++
			if observations == 1 {
				if _, err := writer.db.Exec("UPDATE entries SET inode='' WHERE path=?", []byte("project/Cargo.toml")); err != nil {
					t.Fatal(err)
				}
			}
		}})
		if err != nil || observations != 2 || r.CargoTargetExclusion.ExcludedRows != 2 || r.CargoTargetExclusion.ReusedProofRows != 1 {
			t.Fatal("hit mixed another committed snapshot", r, err)
		}
		if next := sameSizeCargoReport(t, s, 200, ""); next.CargoTargetExclusion.ExcludedRows != 0 {
			t.Fatal("proof survived into another transaction", next)
		}
	})
	t.Run("observer_copies", func(t *testing.T) {
		s := sameSizeCargoFixture(t, "debug")
		observations := 0
		r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{beforeExcluded: func(seen *sameSizeCargoIdentities, f *BuildOutputFinding) {
			observations++
			if seen.count < 2 || len(f.Markers) != 7 {
				t.Fatal("observer did not receive complete proof scratch")
			}
			if seen.values[1].kind != "directory" || f.Profile != "debug" || f.Path != "/cargo-fixture/project/target" || !bytes.Equal(f.PathBytes, []byte(f.Path)) || f.Markers[0].Role != "target" || f.Markers[0].Path != f.Path || !bytes.Equal(f.Markers[0].PathBytes, []byte(f.Markers[0].Path)) {
				t.Fatal("observer received evidence changed by an earlier callback", observations, seen.values[1], f)
			}
			seen.values[1].kind = "observer conflict"
			f.PathBytes[0] = 'X'
			f.Markers[0].PathBytes[0] = 'X'
			f.Markers[0].Role = "observer conflict"
			f.Profile = "observer conflict"
		}})
		if err != nil || observations != 2 || r.CargoTargetExclusion.ExcludedRows != 2 || r.CargoTargetExclusion.ReusedProofRows != 1 {
			t.Fatal("working observer data mutated reused proof", r, err)
		}
	})
	t.Run("observer_raw_path_copies", func(t *testing.T) {
		for _, profile := range []string{"debug", "release"} {
			t.Run(profile, func(t *testing.T) {
				s := newBuildOutputFixture(t)
				root, project := "/cargo-fixture/root-\xfe", "project-\xff"
				sameSizeCargoExec(t, s, "UPDATE roots SET path=? WHERE id=1", []byte(root))
				addBuildOutputProject(t, s, project, profile)
				parent := filepath.Join(project, "target", profile, "deps")
				for _, name := range []string{"a", "b", "c"} {
					buildOutputPut(t, s.db, filepath.Join(parent, name), parent, "file", name, 5, 2<<20, 4096)
				}
				sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
				observations := 0
				var want BuildOutputFinding
				r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{beforeExcluded: func(seen *sameSizeCargoIdentities, f *BuildOutputFinding) {
					observations++
					if seen.count < 2 || len(f.Markers) != 7 || f.Profile != profile || f.Path != filepath.Join(root, project, "target") || !bytes.Equal(f.PathBytes, []byte(f.Path)) || f.Actions == nil || len(f.Actions) != 0 {
						t.Fatal("raw-path observer did not receive the complete original finding", observations, f)
					}
					for _, marker := range f.Markers {
						if !bytes.Equal(marker.PathBytes, []byte(marker.Path)) || !bytes.Contains(marker.PathBytes, []byte{0xfe}) || !bytes.Contains(marker.PathBytes, []byte{0xff}) {
							t.Fatal("observer path bytes were changed or text-encoded", observations, marker)
						}
					}
					if observations == 1 {
						// Keep an independent cold observation, including every scalar,
						// marker and mutable body, for both later cache callbacks.
						want = *f
						want.PathBytes = append([]byte(nil), f.PathBytes...)
						want.Markers = append([]BuildOutputMarker(nil), f.Markers...)
						for i := range want.Markers {
							want.Markers[i].PathBytes = append([]byte(nil), f.Markers[i].PathBytes...)
						}
						want.Actions = make([]string, len(f.Actions))
						copy(want.Actions, f.Actions)
					} else if !reflect.DeepEqual(*f, want) {
						t.Fatal("a prior raw-path callback changed the cached finding", observations, f)
					}
					seen.values[1].kind = "observer conflict"
					f.PathBytes[0] = 'X'
					for i := range f.Markers {
						// Even the target finding and target marker must have
						// independent byte bodies inside this one callback.
						if !bytes.Equal(f.Markers[i].PathBytes, []byte(want.Markers[i].Path)) {
							t.Fatal("observer path bodies alias inside one callback", observations, i)
						}
						f.Markers[i].PathBytes[0] = 'Y'
						f.Markers[i] = BuildOutputMarker{Path: "observer conflict", PathBytes: f.Markers[i].PathBytes}
					}
					f.Markers = append(f.Markers, BuildOutputMarker{Role: "observer conflict"})
					f.Actions = append(f.Actions, "observer action")
					f.Actions[0] = "observer conflict"
					f.Path, f.Profile = "observer conflict", "observer conflict"
				}})
				c := r.CargoTargetExclusion
				layoutCalls := 2
				if profile == "release" {
					layoutCalls = 3
				}
				if err != nil || observations != 3 || c == nil || r.EntriesExamined != 3 || len(r.Bands) != 0 || c.ExcludedRows != 3 || c.ReusedProofRows != 2 || c.RootQueryCalls != 1 || c.MembershipBatchQueryCalls != 3 || c.LayoutBatchQueryCalls != layoutCalls || c.MarkerQualificationBatchQueryCalls != 1 {
					t.Fatal("raw-path mutations changed recognition or cache reuse", r, err)
				}
			})
		}
	})
}

func TestSameSizeCargoProofReuseIdentityReplayParity(t *testing.T) {
	// Keep the former nested loop as an independent parity oracle. The proof
	// owns unique shared pairs; each replay begins with one fresh file tuple.
	reference := func(seen *sameSizeCargoIdentities, proof *sameSizeCargoProof) (uint16, bool) {
		for index, identity := range proof.shared.values[:proof.shared.count] {
			found := false
			for _, prior := range seen.values[:seen.count] {
				if prior.device == identity.device && prior.inode == identity.inode {
					if prior != identity {
						return proof.firstDemand[index], false
					}
					found = true
					break
				}
			}
			if found {
				continue
			}
			if seen.count == len(seen.values) {
				return proof.firstDemand[index], false
			}
			seen.values[seen.count], seen.count = identity, seen.count+1
		}
		return 0, true
	}
	newProof := func(count int) sameSizeCargoProof {
		proof := sameSizeCargoProof{}
		proof.shared.count = count
		for i := 0; i < count; i++ {
			proof.shared.values[i] = sameSizeCargoIdentity{device: 7, inode: uint64(i + 1), changed: 11, modified: 13, allocated: 17, kind: "file"}
			proof.firstDemand[i] = uint16(i + 1)
		}
		return proof
	}
	check := func(t *testing.T, proof sameSizeCargoProof, leaf sameSizeCargoIdentity, demand uint16, matches bool) {
		t.Helper()
		retained, tags := proof.shared, proof.firstDemand
		want := sameSizeCargoIdentities{count: 1}
		want.values[0] = leaf
		got := want
		wantDemand, wantMatches := reference(&want, &proof)
		gotDemand, gotMatches := sameSizeCargoReplayIdentities(&got, &proof)
		if wantDemand != demand || wantMatches != matches || gotDemand != wantDemand || gotMatches != wantMatches || got != want || proof.shared != retained || proof.firstDemand != tags {
			t.Fatal("linear replay changed ordered demand, complete identity scratch or immutable proof", gotDemand, wantDemand, gotMatches, wantMatches, got.count, want.count)
		}
	}
	sharedCount := sameSizeCargoAncestorLimit + 4
	t.Run("no_alias", func(t *testing.T) {
		check(t, newProof(sharedCount), sameSizeCargoIdentity{device: 9, inode: 1, changed: 11, modified: 13, allocated: 17, kind: "file"}, 0, true)
	})
	t.Run("full_identity_capacity", func(t *testing.T) {
		check(t, newProof(len(sameSizeCargoIdentities{}.values)-1), sameSizeCargoIdentity{device: 9, inode: 1, changed: 11, modified: 13, allocated: 17, kind: "file"}, 0, true)
	})
	for _, position := range []struct {
		name  string
		index int
	}{{"first", 0}, {"middle", sharedCount / 2}, {"last", sharedCount - 1}} {
		for _, difference := range []string{"equal", "changed", "modified", "allocated", "kind"} {
			t.Run(position.name+"/"+difference, func(t *testing.T) {
				proof := newProof(sharedCount)
				leaf := proof.shared.values[position.index]
				switch difference {
				case "changed":
					leaf.changed++
				case "modified":
					leaf.modified++
				case "allocated":
					leaf.allocated++
				case "kind":
					proof.shared.values[position.index].kind = "directory"
				}
				demand := proof.firstDemand[position.index]
				if difference == "equal" {
					demand = 0
				}
				check(t, proof, leaf, demand, difference == "equal")
			})
		}
	}
}

func TestSameSizeCargoDefaultParity(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	before, err := s.SameSizeCandidates(context.Background(), 2, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"", before.NextCursor} {
		plain, e := s.SameSizeCandidates(context.Background(), 2, cursor, 1)
		other, e2 := s.SameSizeCandidatesWithOptions(context.Background(), 2, cursor, 1, SameSizeOptions{})
		other.GeneratedAt = plain.GeneratedAt
		left, _ := json.Marshal(plain)
		right, _ := json.Marshal(other)
		if e != nil || e2 != nil || !bytes.Equal(left, right) || other.CargoTargetExclusion != nil || strings.Contains(string(right), "cargo_target_exclusion") || len(other.Diagnostics) != 6 {
			t.Fatal("false changed original report/cursor/IDs/notes", string(left), string(right), e, e2)
		}
	}
}

func TestSameSizeCargoSupportedLayoutAndNoAge(t *testing.T) {
	for _, profile := range []string{"debug", "release"} {
		t.Run(profile, func(t *testing.T) {
			s := sameSizeCargoFixture(t, profile)
			sameSizeCargoExec(t, s, "UPDATE entries SET mtime_ns=?", int64(math.MaxInt64))
			r := sameSizeCargoReport(t, s, 200, "")
			c := r.CargoTargetExclusion
			if c.ExcludedRows != 2 || len(r.Bands) != 1 || len(r.Bands[0].Files) != 2 || c.LayoutMarkerEvidenceRequests != map[string]int{"debug": 14, "release": 22}[profile] || c.MarkerQualificationEvidenceRequests != 14 || c.RootQueryCalls != 1 || c.RootEvidenceRequests != 2 || c.ReusedProofRows != 1 || c.MembershipBatchQueryCalls != 2 || c.LayoutBatchQueryCalls != map[string]int{"debug": 2, "release": 3}[profile] || c.MarkerQualificationBatchQueryCalls != 1 {
				t.Fatal("supported positive future metadata layout was missed", r)
			}
			for _, file := range r.Bands[0].Files {
				if strings.Contains(file.Path, "/target/") {
					t.Fatal("unique manual file was not hidden by disclosed layout policy", file)
				}
			}
		})
	}
	s := sameSizeCargoFixture(t, "debug")
	addBuildOutputProject(t, s, "project-two", "release")
	sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
	// Both profiles exist. The existing recognizer's debug-first order is
	// preserved even when a later release has independently complete metadata.
	addBuildOutputProject(t, s, "both", "debug", "release")
	for _, name := range []string{"a", "b"} {
		buildOutputPut(t, s.db, "both/target/debug/deps/"+name, "both/target/debug/deps", "file", name, 5, 2<<20, 4096)
	}
	sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
	if r := sameSizeCargoReport(t, s, 200, ""); r.CargoTargetExclusion.ExcludedRows != 4 || r.CargoTargetExclusion.LayoutMarkerEvidenceRequests != 28 {
		t.Fatal("existing debug-first profile order changed", r)
	}
}

func TestSameSizeCargoTargetOnlyAndUnsupportedStayVisible(t *testing.T) {
	cases := []struct {
		name, query     string
		args            []any
		expectedUnknown bool
	}{
		{"manifest_missing", "DELETE FROM entries WHERE path=?", []any{[]byte("project/Cargo.toml")}, false},
		{"lock_wrong_kind", "UPDATE entries SET kind='symlink' WHERE path=?", []any{[]byte("project/Cargo.lock")}, false},
		{"profile_lock_missing", "DELETE FROM entries WHERE path=?", []any{[]byte("project/target/debug/.cargo-lock")}, false},
		// Renaming only the profile leaves stale ancestor and own-listing lineage.
		// This does not establish a supported cross-target layout.
		{"profile_cross_target", "UPDATE entries SET path=? WHERE path=?", []any{[]byte("project/target/triple/debug"), []byte("project/target/debug")}, true},
		{"target_only", "DELETE FROM entries WHERE path IN (?,?)", []any{[]byte("project/Cargo.toml"), []byte("project/Cargo.lock")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			sameSizeCargoExec(t, s, tc.query, tc.args...)
			r := sameSizeCargoReport(t, s, 200, "")
			expectedUnknown, expectedUnrecognized := 0, 2
			if tc.expectedUnknown {
				expectedUnknown, expectedUnrecognized = 2, 0
			}
			if c := r.CargoTargetExclusion; c.ExcludedRows != 0 || c.RetainedUnrecognizedRows != expectedUnrecognized || c.RetainedUnknownRows != expectedUnknown || len(r.Bands[0].Files) != 4 {
				t.Fatal("unsupported layout hid ordinary rows", r)
			}
		})
	}
	for _, name := range []string{"target", "target-ish/file", "dist/file", "ordinary/file"} {
		t.Run(name, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			buildOutputPut(t, s.db, name, filepath.Dir(name), "file", "10000", 1, 2<<20, 4096)
			r := sameSizeCargoReport(t, s, 200, "")
			found := false
			for _, b := range r.Bands {
				for _, f := range b.Files {
					found = found || bytes.Equal(f.PathBytes, []byte(filepath.Join("/cargo-fixture", name)))
				}
			}
			if !found || r.CargoTargetExclusion.ExcludedRows != 2 {
				t.Fatal("ordinary near-miss disappeared", r)
			}
		})
	}
	s := sameSizeCargoFixture(t, "debug")
	sameSizeCargoExec(t, s, "UPDATE roots SET path=? WHERE id=1", []byte("/already/target/root"))
	r := sameSizeCargoReport(t, s, 200, "")
	if r.CargoTargetExclusion.ExcludedRows != 0 || r.CargoTargetExclusion.RootQueryCalls != 0 {
		t.Fatal("read sibling evidence outside selected root", r)
	}
}

func TestSameSizeCargoIdentityAndExactMembership(t *testing.T) {
	cases := []struct{ name, assignment, path string }{
		{"device_noncanonical", "device='01'", "project/target/debug/deps/manual-a"},
		{"inode_unknown", "inode=''", "project/target/debug/.fingerprint"},
		{"inode_zero", "inode='0'", "project/target"},
		{"inode_oversized", "inode='18446744073709551616'", "."},
		{"ctime_unknown", "ctime_ns=0", "project/Cargo.toml"},
		{"generation_unknown", "generation=0", "project/target/debug/.cargo-lock"},
		{"false_leaf_parent", "parent=X'2e'", "project/target/debug/deps/manual-a"},
		{"false_ancestor_parent", "parent=X'2e'", "project/target/debug/deps"},
		{"conflicting_identity", "inode=(SELECT inode FROM entries WHERE path=X'2e'),ctime_ns=3", "project/Cargo.lock"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			sameSizeCargoExec(t, s, "UPDATE entries SET "+tc.assignment+" WHERE path=?", []byte(tc.path))
			r := sameSizeCargoReport(t, s, 200, "")
			want := 0
			if strings.HasSuffix(tc.path, "manual-a") {
				want = 1
			}
			if r.CargoTargetExclusion.ExcludedRows != want || r.CargoTargetExclusion.RetainedUnknownRows != 2-want {
				t.Fatal("unknown identity/membership supplied exclusion", r)
			}
		})
	}
	s := sameSizeCargoFixture(t, "debug")
	sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=(SELECT inode FROM entries WHERE path=?) WHERE path=?", []byte("project/target/debug/deps/manual-a"), []byte("outside-a"))
	r := sameSizeCargoReport(t, s, 200, "")
	if len(r.Bands) != 1 || len(r.Bands[0].Files) != 2 || r.CargoTargetExclusion.ExcludedRows != 2 {
		t.Fatal("saved alias propagated outside target path", r)
	}
	for _, scalar := range []string{"generation", "ctime_ns", "observed_at_ns", "mtime_ns", "allocated"} {
		t.Run("corrupt_"+scalar, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			// Nonnumeric text remains a corrupt SQLite scalar, rather than
			// a numeric string canonicalized by INTEGER affinity.
			sameSizeCargoExec(t, s, "UPDATE entries SET "+scalar+"='corrupt' WHERE path=?", []byte("project/target/debug/deps/manual-a"))
			if r, err := s.SameSizeCandidatesWithOptions(context.Background(), 200, "", 1, SameSizeOptions{true}); err == nil || !reflect.DeepEqual(r, SameSizeReport{}) {
				t.Fatal("corrupt SQLite scalar became report", r, err)
			}
		})
	}
}

func TestSameSizeCargoPartialStaleUnknownRefusals(t *testing.T) {
	cases := []struct {
		name, query string
		args        []any
	}{
		{"leaf_stale", "UPDATE entries SET generation=999 WHERE path=?", []any{[]byte("project/target/debug/deps/manual-a")}},
		{"root_error", "UPDATE roots SET last_error='saved error' WHERE id=1", nil},
		{"outer_partial", "UPDATE directories SET complete=0 WHERE path=?", []any{[]byte("project")}},
		{"outer_error", "UPDATE directories SET last_error='saved error' WHERE path=?", []any{[]byte(".")}},
		{"own_missing", "DELETE FROM directories WHERE path=?", []any{[]byte("project/target/debug/.fingerprint")}},
		{"own_checked_zero", "UPDATE directories SET checked_at_ns=0 WHERE path=?", []any{[]byte("project/target/debug/.fingerprint")}},
		{"marker_observed_late", "UPDATE entries SET observed_at_ns=? WHERE path=?", []any{int64(math.MaxInt64), []byte("project/Cargo.lock")}},
		{"directory_observed_late", "UPDATE entries SET observed_at_ns=? WHERE path=?", []any{int64(math.MaxInt64), []byte("project/target")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			sameSizeCargoExec(t, s, tc.query, tc.args...)
			r := sameSizeCargoReport(t, s, 200, "")
			want := 0
			if tc.name == "leaf_stale" {
				want = 1
			}
			if r.CargoTargetExclusion.ExcludedRows != want || r.CargoTargetExclusion.RetainedUnknownRows != 2-want {
				t.Fatal("partial/stale evidence became exclusion", r)
			}
		})
	}
	// An unrelated incomplete descendant is not claimed complete and does not
	// invalidate exact required marker membership or demand subtree reduction.
	s := sameSizeCargoFixture(t, "debug")
	buildOutputPut(t, s.db, "project/target/unrelated", "project/target", "directory", "20000", 3, 0, 0)
	if sameSizeCargoReport(t, s, 200, "").CargoTargetExclusion.ExcludedRows != 2 {
		t.Fatal("filter enumerated unrelated descendants")
	}
}

func TestSameSizeCargoRawBytesAndPathBounds(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	addBuildOutputProject(t, s, "raw-\xff", "debug")
	p := "raw-\xff/target/debug/deps"
	buildOutputPut(t, s.db, p+"/a", p, "file", "9000", 5, 2<<20, 4096)
	buildOutputPut(t, s.db, p+"/b", p, "file", "9001", 5, 2<<20, 4096)
	sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
	if sameSizeCargoReport(t, s, 200, "").CargoTargetExclusion.ExcludedRows != 4 {
		t.Fatal("byte path was normalized or refused")
	}
	sameSizeCargoExec(t, s, "UPDATE entries SET path=CAST(path AS TEXT) WHERE path=?", []byte("outside-a"))
	r := sameSizeCargoReport(t, s, 200, "")
	if len(r.Bands[0].Files) != 2 {
		t.Fatal("legacy text path lost ordinary visibility", r)
	}
	for _, length := range []int{4096, 4097} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			sameSizeCargoExec(t, s, "UPDATE entries SET path=CAST(CAST(path AS BLOB)||X'6161' AS BLOB) WHERE kind='file' AND size>0 AND parent=?", []byte("project/target/debug/deps"))
			root := "/" + strings.Repeat("r", length-len("/project/target/debug/deps/manual-aaa")-1)
			sameSizeCargoExec(t, s, "UPDATE roots SET path=? WHERE id=1", []byte(root))
			r := sameSizeCargoReport(t, s, 200, "")
			if length == 4096 && r.CargoTargetExclusion.ExcludedRows != 2 {
				t.Fatal("exact bound refused", r)
			}
			if length == 4097 && (r.CargoTargetExclusion.ExcludedRows != 0 || r.Diagnostics[4].Count != 2) {
				t.Fatal("over-bound path recognized", r)
			}
		})
	}
	sameSizeCargoExec(t, s, "UPDATE entries SET path=? WHERE path=?", []byte("bad\x00target/file"), []byte("outside-b"))
	r = sameSizeCargoReport(t, s, 200, "")
	if r.Diagnostics[4].Count != 1 {
		t.Fatal("NUL path reached optional recognition", r)
	}
}

func TestSameSizeCargoRawPageNoRefillAndSentinel(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	sameSizeCargoExec(t, s, "DELETE FROM entries WHERE kind='file' AND size>0")
	parent := "project/target/debug/deps"
	for i := 0; i < 201; i++ {
		buildOutputPut(t, s.db, fmt.Sprintf("%s/f-%03d", parent, i), parent, "file", fmt.Sprint(10000+i), 5, 2<<20, 4096)
	}
	sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
	r := sameSizeCargoReport(t, s, 200, "")
	c := r.CargoTargetExclusion
	if r.EntriesExamined != 200 || c.ExcludedRows != 200 || c.RootEvidenceRequests != 200 || c.LeafEvidenceRequests != 200 || len(r.Bands) != 0 || r.NextCursor == "" || r.PageCoverage != "more_saved_entries" {
		t.Fatal("page refilled or sentinel recognized", r)
	}
	position, err := decodeSameSizeCargoCursor(r.NextCursor)
	var secondID int64
	if err != nil || s.db.QueryRow("SELECT id FROM entries WHERE kind='file' AND size>0 ORDER BY id LIMIT 1 OFFSET 1").Scan(&secondID) != nil || position.ID != secondID {
		t.Fatal("cursor advanced by displayed rather than raw row", position, secondID, err)
	}
	last := sameSizeCargoReport(t, s, 200, r.NextCursor)
	if last.EntriesExamined != 1 || last.CargoTargetExclusion.ExcludedRows != 1 || last.NextCursor != "" {
		t.Fatal("empty page could not continue exactly", last)
	}
	// Original first-match precedence excludes node_modules before optional
	// Cargo queries, even with a syntactically present target ancestor.
	buildOutputPut(t, s.db, "node_modules/x/target/a", "node_modules/x/target", "file", "30000", 1, 4<<20, 0)
	r = sameSizeCargoReport(t, s, 1, "")
	if r.Diagnostics[2].Count != 1 || r.CargoTargetExclusion.EligibleRows != 0 || r.CargoTargetExclusion.RootQueryCalls != 0 {
		t.Fatal("old first-match order changed", r)
	}
}

func TestSameSizeCargoPolicyBoundCursors(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	r := sameSizeCargoReport(t, s, 1, "")
	cursor, err := decodeSameSizeCargoCursor(r.NextCursor)
	if err != nil || cursor.Version != 2 || cursor.ID <= 0 {
		t.Fatal(cursor, err)
	}
	old, err := s.SameSizeCandidates(context.Background(), 1, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SameSizeCandidatesWithOptions(context.Background(), 1, old.NextCursor, 1, SameSizeOptions{true}); !errors.Is(err, ErrReportCursor) {
		t.Fatal("V1 entered selected mode", err)
	}
	if _, err = s.SameSizeCandidates(context.Background(), 1, r.NextCursor, 1); !errors.Is(err, ErrReportCursor) {
		t.Fatal("V2 entered default mode", err)
	}
	body, _ := base64.RawURLEncoding.DecodeString(r.NextCursor)
	for _, bad := range []string{string(body) + "{}", strings.Replace(string(body), `"version":2`, `"version":2,"version":2`, 1), strings.Replace(string(body), `"policy":"`+SameSizeCargoPolicy+`"`, `"policy":"foreign"`, 1), strings.Replace(string(body), `"minimum_bytes":1`, `"minimum_bytes":1e0`, 1), strings.Replace(string(body), `"policy":"`+SameSizeCargoPolicy+`",`, "", 1)} {
		if _, err = decodeSameSizeCargoCursor(base64.RawURLEncoding.EncodeToString([]byte(bad))); !errors.Is(err, ErrReportCursor) {
			t.Fatal("noncanonical cursor accepted", bad, err)
		}
	}
	for _, bad := range []string{r.NextCursor + "=", strings.Repeat("x", 513)} {
		if _, err = decodeSameSizeCargoCursor(bad); !errors.Is(err, ErrReportCursor) {
			t.Fatal("cursor bound/padding accepted", err)
		}
	}
	if _, err = s.SameSizeCandidatesWithOptions(context.Background(), 1, r.NextCursor, 2, SameSizeOptions{true}); !errors.Is(err, ErrReportCursor) {
		t.Fatal("changed minimum accepted", err)
	}
	if _, err = s.SameSizeCandidatesWithOptions(context.Background(), 200, r.NextCursor, 1, SameSizeOptions{true}); err != nil {
		t.Fatal("limit was incorrectly cursor-bound", err)
	}
}

func TestSameSizeCargoOneWALSnapshot(t *testing.T) {
	s := sameSizeCargoFixture(t, "debug")
	writer, err := connect(context.Background(), s.path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	committed := false
	r, err := s.sameSizeCargoCandidates(context.Background(), 200, "", 1, sameSizeCargoHooks{afterSnapshot: func() {
		if _, writeErr := writer.db.Exec("UPDATE entries SET inode='' WHERE path=?", []byte("project/Cargo.toml")); writeErr != nil {
			t.Fatal(writeErr)
		}
		committed = true
	}})
	if err != nil || !committed || r.CargoTargetExclusion.ExcludedRows != 2 {
		t.Fatal("one read snapshot mixed committed writer evidence", r, err)
	}
	if next := sameSizeCargoReport(t, s, 200, ""); next.CargoTargetExclusion.ExcludedRows != 0 || next.CargoTargetExclusion.RetainedUnknownRows != 2 {
		t.Fatal("next snapshot ignored committed evidence", next)
	}
}

func TestSameSizeCargoAncestorAndMemoryBounds(t *testing.T) {
	for _, depth := range []int{251, 252} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			sameSizeCargoExec(t, s, "DELETE FROM entries WHERE kind='file' AND size>0")
			parent := "project/target/debug/deps"
			generation := int64(5)
			for i := 0; i < depth; i++ {
				rel := parent + "/a"
				buildOutputPut(t, s.db, rel, parent, "directory", fmt.Sprint(50000+i), generation, 0, 0)
				buildOutputDirectory(t, s.db, rel, generation+1)
				parent, generation = rel, generation+1
			}
			for _, name := range []string{"a", "b"} {
				buildOutputPut(t, s.db, parent+"/"+name, parent, "file", name, generation, 100, 4096)
			}
			sameSizeCargoExec(t, s, "UPDATE entries SET device='0',inode=CAST(id AS TEXT)")
			r := sameSizeCargoReport(t, s, 200, "")
			if depth == 251 && (r.CargoTargetExclusion.ExcludedRows != 2 || r.CargoTargetExclusion.AncestorEvidenceRequests != 512) {
				t.Fatal("256 completed directories not accepted", r)
			}
			if depth == 252 && (r.CargoTargetExclusion.ExcludedRows != 0 || r.CargoTargetExclusion.RetainedAncestorLimitRows != 2 || r.CargoTargetExclusion.AncestorEvidenceRequests != 512) {
				t.Fatal("257th directory bypassed cap", r)
			}
		})
	}
	// Fixed identity slots hold scalars only: no path retained per ancestor.
	// Measure the extra membership scratch independent of old raw-page/report
	// storage. Runtime allocation acceptance remains a focused gate.
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	seen := new(sameSizeCargoIdentities)
	for i := 0; i < sameSizeCargoAncestorLimit+8; i++ {
		e := sameSizeCargoEntry{device: []byte("18446744073709551615"), inode: []byte(fmt.Sprint(i + 1)), changed: 1, generation: 1, kind: []byte("directory")}
		if !seen.add(e) {
			t.Fatal("fixed identity scratch rejected bound", i)
		}
	}
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(seen)
	if after.HeapAlloc > before.HeapAlloc+256<<10 {
		t.Fatal("membership retained heap target exceeded", after.HeapAlloc-before.HeapAlloc)
	}
	s := sameSizeCargoFixture(t, "debug")
	rows, err := s.db.Query("EXPLAIN QUERY PLAN "+sameSizeCargoEntryQuery, 1, []byte("project/target"), sameSizeCargoMemberRole, "project")
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var a, b, c int
		var detail string
		if err = rows.Scan(&a, &b, &c, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil || len(plan) != 3 {
		t.Fatal("bounded joined plan changed", plan, err)
	}
	for _, line := range plan {
		if !strings.Contains(line, "SEARCH") || !strings.Contains(line, "INDEX") {
			t.Fatal("exact joined key became scan", line)
		}
	}
}

// Production timing and intrusive heap measurements each own a fresh process,
// including when this parent runs in the whole state suite. The same executable
// retains normal/race instrumentation, environment and the current CPU setting.
// No explicit garbage-collection setting is changed by this wrapper.
func TestSameSizeCargoFullLayoutRetainedHeap(t *testing.T) {
	const childEnv = "RYDD_CARGO_FULL_LAYOUT_HEAP_PROFILE"
	const modeEnv = "RYDD_CARGO_FULL_LAYOUT_RESOURCE_MODE"
	const testName = "TestSameSizeCargoFullLayoutRetainedHeap"
	if profile, child := os.LookupEnv(childEnv); child {
		if profile != "debug" && profile != "release" {
			t.Fatal("invalid isolated Cargo resource profile")
		}
		mode := os.Getenv(modeEnv)
		if mode != "timing" && mode != "heap" {
			t.Fatal("invalid isolated Cargo resource mode")
		}
		selector := flag.Lookup("test.run")
		if selector == nil || selector.Value.String() != "^"+testName+"$/^"+profile+"$/^"+mode+"$" {
			t.Fatal("isolated Cargo resource selector differs from its profile or mode")
		}
		// The fixed selector admits one profile and one independently owned mode.
		sameSizeCargoFullLayoutRetainedHeap(t, mode)
		return
	}
	for _, profile := range []string{"debug", "release"} {
		t.Run(profile, func(t *testing.T) {
			for _, mode := range []string{"timing", "heap"} {
				t.Run(mode, func(t *testing.T) {
					executable, err := os.Executable()
					if err != nil {
						t.Fatal("locate isolated Cargo resource executable", err)
					}
					// These unchanged process bounds include fixture setup. Modes
					// cannot warm each other's database or heap observations.
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, executable,
						"-test.run=^"+testName+"$/^"+profile+"$/^"+mode+"$", "-test.count=1", "-test.v", "-test.timeout=25s",
						fmt.Sprintf("-test.cpu=%d", runtime.GOMAXPROCS(0)))
					cmd.Env = append(os.Environ(), childEnv+"="+profile, modeEnv+"="+mode)
					cmd.WaitDelay = 2 * time.Second
					var output sameSizeCargoHeapOutput
					cmd.Stdout, cmd.Stderr = &output, &output
					err = cmd.Run()
					if output.body.Len() != 0 {
						t.Logf("isolated Cargo resource profile=%s mode=%s:\n%s", profile, mode, output.body.String())
					}
					if err = errors.Join(err, ctx.Err()); err != nil {
						t.Fatal("isolated Cargo resource child failed", err)
					}
					if output.truncated {
						t.Fatal("isolated Cargo resource output exceeded 64 KiB")
					}
				})
			}
		})
	}
}

// Drain both streams while retaining only their first bounded combined bytes.
// An overflow is a failed observation even if the child exits successfully.
type sameSizeCargoHeapOutput struct {
	mu        sync.Mutex
	body      bytes.Buffer
	truncated bool
}

func (output *sameSizeCargoHeapOutput) Write(body []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	count := len(body)
	remaining := (64 << 10) - output.body.Len()
	if len(body) > remaining {
		body = body[:remaining]
		output.truncated = true
	}
	_, _ = output.body.Write(body)
	return count, nil
}

func sameSizeCargoFullLayoutRetainedHeap(t *testing.T, mode string) {
	for _, profile := range []string{"debug", "release"} {
		t.Run(profile, func(t *testing.T) {
			t.Run(mode, func(t *testing.T) {
				s := newBuildOutputFixture(t)
				tx, err := s.db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				const root = "/cargo-fixture"
				const prefixCount = 252
				const targetBytes = 4075
				componentBytes := targetBytes - len(root) - 1 - (prefixCount - 1) - len("/target")
				width, longer := componentBytes/prefixCount, componentBytes%prefixCount
				parent, generation := ".", int64(1)
				for i := 0; i < prefixCount; i++ {
					length := width
					if i < longer {
						length++
					}
					component := fmt.Sprintf("d%03d", i) + strings.Repeat("x", length-4)
					if len(component) > 255 {
						t.Fatal("fixture component exceeds a native filename bound")
					}
					relative := filepath.Join(parent, component)
					buildOutputPut(t, tx, relative, parent, "directory", "prefix", generation, 0, 0)
					generation++
					buildOutputDirectory(t, tx, relative, generation)
					parent = relative
				}
				for _, name := range []string{"Cargo.toml", "Cargo.lock"} {
					buildOutputPut(t, tx, filepath.Join(parent, name), parent, "file", "manifest", generation, 0, 0)
				}
				target := filepath.Join(parent, "target")
				if len(filepath.Join(root, target)) != targetBytes {
					t.Fatal("fixture target path changed")
				}
				buildOutputPut(t, tx, target, parent, "directory", "target", generation, 0, 0)
				generation++
				buildOutputDirectory(t, tx, target, generation)
				for _, layout := range []string{"debug", "release"} {
					path := filepath.Join(target, layout)
					buildOutputPut(t, tx, path, target, "directory", "profile", generation, 0, 0)
					buildOutputDirectory(t, tx, path, generation+1)
					if profile == "debug" || layout == "release" {
						buildOutputPut(t, tx, filepath.Join(path, ".cargo-lock"), path, "file", "lock", generation+1, 0, 0)
					}
					for _, child := range []string{"deps", ".fingerprint"} {
						childPath := filepath.Join(path, child)
						buildOutputPut(t, tx, childPath, path, "directory", "child", generation+1, 0, 0)
						buildOutputDirectory(t, tx, childPath, generation+2)
					}
				}
				parent = filepath.Join(target, profile, "deps")
				for i := 0; i < 200; i++ {
					name := fmt.Sprintf("f%06d", i)
					if profile == "debug" {
						name = fmt.Sprintf("f%08d", i)
					}
					path := filepath.Join(parent, name)
					if len(filepath.Join(root, path)) != 4096 {
						t.Fatal("fixture ordinary path is not the admitted byte maximum")
					}
					buildOutputPut(t, tx, path, parent, "file", "ordinary", generation+2, 2<<20, 4096)
				}
				if _, err = tx.Exec("UPDATE entries SET device='18446744073709551615',inode='18446744073709'||printf('%06d',id)"); err != nil {
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}

				// Both profiles are preflighted. The longest admitted release marker
				// is 4096 bytes; its debug counterpart is necessarily 4094 bytes.
				// No actual project exists. Only the generated saved database is read.
				if len(filepath.Join(root, target, "release", ".fingerprint")) != 4096 {
					t.Fatal("both-profile preflight maximum changed")
				}
				checkReport := func(r SameSizeReport) (int, int) {
					c := r.CargoTargetExclusion
					if c == nil || r.EntriesExamined != 200 || c.ExcludedRows != 200 || c.EligibleRows != 200 || c.RowsWithTargetAncestor != 200 || c.AncestorEvidenceRequests != 200*256 || len(r.Bands) != 0 || r.NextCursor != "" || c.RetainedUnknownRows != 0 || c.RetainedUnrecognizedRows != 0 || c.RetainedAncestorLimitRows != 0 {
						t.Fatal("full raw page/ancestor/profile fixture was not actually observed", r)
					}
					layoutRequests := 200 * 7
					layoutBatchCalls := 2
					actualCalls := 204
					logicalRequests := 54400
					if profile == "release" {
						layoutRequests = 200 * 11
						layoutBatchCalls = 3
						actualCalls = 205
						logicalRequests = 55200
					}
					calls := c.RootQueryCalls + c.MembershipBatchQueryCalls + c.LayoutBatchQueryCalls + c.MarkerQualificationBatchQueryCalls
					requests := c.RootEvidenceRequests + c.LeafEvidenceRequests + c.AncestorEvidenceRequests + c.LayoutMarkerEvidenceRequests + c.MarkerQualificationEvidenceRequests
					if c.RootQueryCalls != 1 || c.RootEvidenceRequests != 200 || c.ReusedProofRows != 199 || c.MembershipBatchQueryCalls != 200 || c.LayoutBatchQueryCalls != layoutBatchCalls || c.MarkerQualificationBatchQueryCalls != 1 || calls != actualCalls || calls > 55200 || c.LeafEvidenceRequests != 200 || c.LayoutMarkerEvidenceRequests != layoutRequests || c.MarkerQualificationEvidenceRequests != 200*7 || requests != logicalRequests || r.ContentVerified || r.CurrentStateVerified || r.EstimatedReclaimableBytes != nil {
						t.Fatal("full-page query or authority contract changed", c)
					}
					body, err := json.Marshal(c)
					if err != nil || len(body) > 4096 {
						t.Fatal("full-page summary exceeded its byte cap", len(body), err)
					}
					return calls, requests
				}
				if mode == "timing" {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					started := time.Now()
					r, runErr := s.SameSizeCandidatesWithOptions(ctx, 200, "", 1, SameSizeOptions{ExcludeCargoDefaultTarget: true})
					elapsed := time.Since(started)
					contextErr := ctx.Err()
					t.Logf("Cargo production timing profile=%s production_elapsed_ns=%d deadline_seconds=5 api_error=%v context_error=%v", profile, elapsed.Nanoseconds(), runErr, contextErr)
					if runErr != nil || contextErr != nil || elapsed > 5*time.Second {
						t.Fatal("uninstrumented full-page production observation failed its five-second wall deadline", elapsed, runErr, contextErr)
					}
					calls, requests := checkReport(r)
					t.Logf("Cargo production timing profile=%s physical_query_calls=%d logical_evidence_requests=%d", profile, calls, requests)
					return
				}
				var baseline, peak uint64
				baselineCalls, observations, collections := 0, 0, 0
				var collectionElapsed time.Duration
				measurementStarted := time.Now()
				measurementPassed := false
				defer func() {
					t.Logf("Cargo heap measurement profile=%s passed=%t elapsed_ns=%d forced_gc_elapsed_ns=%d forced_gc_collections=%d baseline_calls=%d checkpoint_count=%d peak_bytes=%d", profile, measurementPassed, time.Since(measurementStarted).Nanoseconds(), collectionElapsed.Nanoseconds(), collections, baselineCalls, observations, peak)
				}()
				// This test-only context includes intrusive GC and observation work.
				// Production deadline acceptance belongs to the separate timing child.
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, sameSizeCargoHooks{
					afterRawPage: func() {
						collectionStarted := time.Now()
						runtime.GC()
						collectionElapsed += time.Since(collectionStarted)
						collections++
						var stats runtime.MemStats
						runtime.ReadMemStats(&stats)
						baseline = stats.HeapAlloc
						baselineCalls++
					},
					beforeExcluded: func(seen *sameSizeCargoIdentities, finding *BuildOutputFinding) {
						collectionStarted := time.Now()
						runtime.GC()
						collectionElapsed += time.Since(collectionStarted)
						collections++
						var stats runtime.MemStats
						runtime.ReadMemStats(&stats)
						runtime.KeepAlive(seen)
						runtime.KeepAlive(finding)
						if baselineCalls != 1 || seen.count != 261 || len(finding.Markers) != 7 || finding.Profile != profile {
							t.Fatal("actual full-layout scratch is not retained at the checkpoint")
						}
						longest := 0
						for _, marker := range finding.Markers {
							if len(marker.PathBytes) > 4096 || len(marker.Device) != 20 || len(marker.Inode) != 20 {
								t.Fatal("actual retained marker evidence is not bounded maximum-width evidence")
							}
							longest = max(longest, len(marker.PathBytes))
						}
						wantLongest := 4096
						if profile == "debug" {
							wantLongest = 4094
						}
						if longest != wantLongest {
							t.Fatal("actual selected marker path maximum changed", longest)
						}
						if stats.HeapAlloc > baseline {
							peak = max(peak, stats.HeapAlloc-baseline)
						}
						observations++
					},
				})
				if err != nil {
					t.Fatal("full-page resource observation failed, including intrusive test GC time", err)
				}
				if baselineCalls != 1 || observations != 200 || collections != 201 {
					t.Fatal("heap measurement did not complete the exact baseline and 200 checkpoints", baselineCalls, observations, collections)
				}
				calls, requests := checkReport(r)
				// Raw rows/report and the stable fixture DB are already held at the
				// baseline. Include conservative headroom for added fixed notes and
				// counters. This is live Go heap, not total allocations or SQLite/OS
				// memory. Intrusive GC has its own finite test-measurement context.
				const fixedReportAllowance = 16 << 10
				if peak+fixedReportAllowance > 256<<10 {
					t.Fatal("actual full-layout extra live heap target exceeded", peak)
				}
				measurementPassed = true
				t.Logf("Cargo resource profile=%s peak_bytes=%d fixed_allowance_bytes=%d physical_query_calls=%d logical_evidence_requests=%d", profile, peak, fixedReportAllowance, calls, requests)
			})
		})
	}
}

func TestSameSizeCargoCancellationAndCheckedClose(t *testing.T) {
	for _, when := range []string{"before", "after_snapshot", "before_commit"} {
		t.Run(when, func(t *testing.T) {
			s := sameSizeCargoFixture(t, "debug")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hooks := sameSizeCargoHooks{}
			switch when {
			case "before":
				cancel()
			case "after_snapshot":
				hooks.afterSnapshot = cancel
			case "before_commit":
				hooks.beforeCommit = cancel
			}
			r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, hooks)
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(r, SameSizeReport{}) {
				t.Fatal("canceled selected report escaped", r, err)
			}
		})
	}
	s := sameSizeCargoFixture(t, "debug")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SameSizeCandidatesWithOptions(context.Background(), 200, "", 1, SameSizeOptions{true}); err == nil {
		t.Fatal("closed reader became report")
	}

	// Exercise real transaction/statement ownership without new event names.
	ownerStore := sameSizeCargoFixture(t, "debug")
	tx, err := ownerStore.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var rootID int64
	if err := tx.QueryRowContext(context.Background(), "SELECT id FROM roots").Scan(&rootID); err != nil {
		t.Fatal(err)
	}
	// Each static shape owns its own lazy statement. Invalid plan data must
	// refuse before preparing or reusing any owner.
	for _, role := range []int64{-1, 2} {
		fresh := sameSizeCargoEntryReader{}
		err := fresh.open(context.Background(), sameSizeCargoMembershipStage, rootID, []byte("project/target"), []sameSizeCargoRequest{{path: "project/target", parent: strings.Repeat("p", 4097), role: role}})
		if err == nil || !sameSizeCargoTestOwnersEmpty(&fresh) {
			t.Fatal("invalid fresh role mutated ownership", role, err)
		}
	}
	for _, role := range []int64{sameSizeCargoMemberRole, sameSizeCargoLayoutRole} {
		stage := sameSizeCargoMembershipStage
		if role == sameSizeCargoLayoutRole {
			stage = sameSizeCargoCommonStage
		}
		fresh := sameSizeCargoEntryReader{}
		err := fresh.open(context.Background(), stage, rootID, []byte("project/target"), []sameSizeCargoRequest{{path: "project/target", parent: strings.Repeat("p", 4097), role: role}})
		if err == nil || !sameSizeCargoTestOwnersEmpty(&fresh) {
			t.Fatal("invalid expected parent prepared an owner", role, err)
		}
		admitted := sameSizeCargoEntryReader{tx: tx}
		if err := admitted.open(context.Background(), stage, rootID, []byte("project/target"), []sameSizeCargoRequest{{path: "project/target", parent: strings.Repeat("p", 4096), role: role}}); err != nil || admitted.statements[stage] == nil {
			t.Fatal("inclusive expected parent bound refused", role, err)
		}
		if err := admitted.close(); err != nil || !sameSizeCargoTestOwnersEmpty(&admitted) {
			t.Fatal("inclusive plan owner was not closed", role, err)
		}
	}
	closeCalls, rootCloses := 0, 0
	reader := sameSizeCargoEntryReader{tx: tx, closeStatement: func(stmt *sql.Stmt) error { closeCalls++; return stmt.Close() }, closeRootStatement: func(stmt *sql.Stmt) error { rootCloses++; return stmt.Close() }}
	if err := reader.close(); err != nil || closeCalls != 0 || rootCloses != 0 || !sameSizeCargoTestOwnersEmpty(&reader) {
		t.Fatal("unprepared reader closed an owner", err)
	}
	if _, exists, err := readSameSizeCargoEntry(context.Background(), &reader, rootID, "project/target"); err != nil || !exists || reader.statements[sameSizeCargoMembershipStage] == nil || reader.active != nil {
		t.Fatal("actual singleton member stage failed", exists, err)
	}
	memberStmt := reader.statements[sameSizeCargoMembershipStage]
	if _, exists, err := readSameSizeCargoEntry(context.Background(), &reader, rootID, "project/Cargo.toml"); err != nil || !exists || reader.statements[sameSizeCargoMembershipStage] != memberStmt || reader.active != nil {
		t.Fatal("member owner was not reused and closed", exists, err)
	}
	rootStmt, err := reader.rootStatement(context.Background())
	if err != nil || rootStmt == nil || reader.statements[sameSizeCargoRootStage] != rootStmt {
		t.Fatal("root stage did not own its statement", err)
	}
	owners := reader.statements
	for _, role := range []int64{-1, 2} {
		if err := reader.open(context.Background(), sameSizeCargoMembershipStage, rootID, []byte("project/target"), []sameSizeCargoRequest{{path: "project/target", parent: "project", role: role}}); err == nil || reader.statements != owners || reader.active != nil || closeCalls != 0 || rootCloses != 0 {
			t.Fatal("invalid cached role changed ownership", role, err)
		}
	}
	for i := 0; i < 2; i++ {
		current, err := reader.rootStatement(context.Background())
		var root []byte
		var ordinal, presence, enabled, bad, skipped any
		if err != nil || current != rootStmt {
			t.Fatal("root owner was not reused", err)
		}
		if err = current.QueryRowContext(context.Background(), []byte("project/Cargo.toml"), []byte("project/Cargo.lock"), rootID).Scan(&ordinal, &presence, &root, &enabled, &bad, &skipped); err != nil || ordinal != int64(0) || presence != int64(1) || string(root) != "/cargo-fixture" || enabled != int64(1) || bad != int64(0) || skipped != int64(0) {
			t.Fatal("root transport or original projection changed", ordinal, presence, string(root), enabled, bad, skipped, err)
		}
	}
	if _, exists, err := reader.readLayout(context.Background(), tx, rootID, "project/target"); err != nil || !exists || reader.active != nil {
		t.Fatal("actual singleton layout stage failed", exists, err)
	}
	layoutStmt := reader.statements[sameSizeCargoCommonStage]
	if layoutStmt == nil || layoutStmt == memberStmt {
		t.Fatal("distinct static roles shared one statement")
	}
	if _, exists, err := reader.readLayout(context.Background(), tx, rootID, "project/Cargo.toml"); err != nil || !exists || reader.statements[sameSizeCargoCommonStage] != layoutStmt || reader.active != nil {
		t.Fatal("layout owner was not reused and closed", exists, err)
	}
	if err := reader.close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.close(); err != nil || closeCalls != 2 || rootCloses != 1 || !sameSizeCargoTestOwnersEmpty(&reader) {
		t.Fatal("all prepared owners did not close independently once", closeCalls, rootCloses, err)
	}
	for _, owner := range []struct {
		stage sameSizeCargoStage
		stmt  *sql.Stmt
	}{{sameSizeCargoMembershipStage, memberStmt}, {sameSizeCargoCommonStage, layoutStmt}, {sameSizeCargoRootStage, rootStmt}} {
		var args []any
		switch owner.stage {
		case sameSizeCargoRootStage:
			args = []any{[]byte("project/Cargo.toml"), []byte("project/Cargo.lock"), rootID}
		case sameSizeCargoMembershipStage:
			args = make([]any, 3+2*257)
			args[0], args[1], args[2] = rootID, []byte("project/target"), 1
			for i := 0; i < 257; i++ {
				args[3+2*i], args[4+2*i] = 0, ""
			}
			args[3], args[4] = len("project/target"), "project"
		default:
			args = make([]any, 2+2*sameSizeCargoStageSize(owner.stage))
			args[0], args[1] = rootID, 1
			for i := 0; i < sameSizeCargoStageSize(owner.stage); i++ {
				args[2+2*i], args[3+2*i] = []byte{}, ""
			}
			args[2], args[3] = []byte("project/target"), "project"
		}
		if rows, err := owner.stmt.QueryContext(context.Background(), args...); err == nil {
			rows.Close()
			t.Fatal("closed owner remained queryable with valid shape arguments", owner.stage)
		}
	}
	for _, order := range []string{"layout_first", "member_first"} {
		shared := sameSizeCargoEntryReader{tx: tx}
		if order == "layout_first" {
			if _, exists, err := shared.readLayout(context.Background(), tx, rootID, "project/target"); err != nil || !exists {
				t.Fatal(order, err)
			}
		} else if _, exists, err := readSameSizeCargoEntry(context.Background(), &shared, rootID, "project/target"); err != nil || !exists {
			t.Fatal(order, err)
		}
		for i := 0; i < 2; i++ {
			if _, exists, err := shared.readLayout(context.Background(), tx, rootID, "project/Cargo.toml"); err != nil || !exists {
				t.Fatal(order, err)
			}
			if _, exists, err := readSameSizeCargoEntry(context.Background(), &shared, rootID, "project/Cargo.lock"); err != nil || !exists || shared.active != nil {
				t.Fatal(order, err)
			}
		}
		if shared.statements[sameSizeCargoMembershipStage] == nil || shared.statements[sameSizeCargoCommonStage] == nil {
			t.Fatal("alternating roles did not retain both owners")
		}
		if _, exists, err := shared.readLayout(context.Background(), tx, rootID, "not-saved"); err != nil || exists || shared.active != nil {
			t.Fatal("absent singleton changed layout ownership", order, err)
		}
		if err := shared.close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unprepared := sameSizeCargoEntryReader{tx: tx}
	if _, _, err := readSameSizeCargoEntry(ctx, &unprepared, rootID, "project/target"); !errors.Is(err, context.Canceled) || !sameSizeCargoTestOwnersEmpty(&unprepared) {
		t.Fatal("canceled member prepared an owner", err)
	}
	if _, err := unprepared.rootStatement(ctx); !errors.Is(err, context.Canceled) || !sameSizeCargoTestOwnersEmpty(&unprepared) {
		t.Fatal("canceled root prepared an owner", err)
	}
	if _, _, err := unprepared.readLayout(ctx, tx, rootID, "project/target"); !errors.Is(err, context.Canceled) || !sameSizeCargoTestOwnersEmpty(&unprepared) {
		t.Fatal("canceled layout prepared an owner", err)
	}
	if err := unprepared.close(); err != nil {
		t.Fatal(err)
	}
	counters := SameSizeCargoTargetExclusion{}
	refused := sameSizeCargoEntryReader{tx: tx, counters: &counters}
	excluded, err := sameSizeCargoExcluded(context.Background(), tx, &refused, sameSizeRawFile{root: []byte("/different-saved-root"), relative: []byte("project/target/debug/deps/manual-a"), file: SameSizeFile{ReportFile: ReportFile{RootID: rootID}}}, &counters, sameSizeCargoHooks{})
	if err != nil || excluded || counters.RetainedUnknownRows != 1 || counters.RootQueryCalls != 1 || refused.statements[sameSizeCargoRootStage] == nil || refused.statements[sameSizeCargoMembershipStage] != nil || refused.active != nil {
		t.Fatal("root refusal prepared a later role", counters, err)
	}
	if err := refused.close(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	noRows := sameSizeCargoFixture(t, "debug")
	unusedCloses := 0
	countUnused := func(stmt *sql.Stmt) error { unusedCloses++; return stmt.Close() }
	unusedHooks := sameSizeCargoHooks{closeEntryStatement: countUnused, closeRootStatement: countUnused}
	if r, err := noRows.sameSizeCargoCandidates(context.Background(), 200, "", math.MaxInt64, unusedHooks); err != nil || r.EntriesExamined != 0 || unusedCloses != 0 {
		t.Fatal("empty page prepared an owner", r, unusedCloses, err)
	}
	sameSizeCargoExec(t, noRows, "DELETE FROM entries WHERE path IN (?,?)", []byte("project/target/debug/deps/manual-a"), []byte("project/target/debug/deps/manual-b"))
	if r, err := noRows.sameSizeCargoCandidates(context.Background(), 200, "", 1, unusedHooks); err != nil || r.EntriesExamined != 2 || r.CargoTargetExclusion.RowsWithTargetAncestor != 0 || unusedCloses != 0 {
		t.Fatal("no-target page prepared an owner", r, unusedCloses, err)
	}

	for _, tc := range []struct {
		name, profile, project, update string
		args                           []any
		code                           int
	}{
		{name: "debug", profile: "debug", project: "project", code: buildSelected},
		{name: "release", profile: "release", project: "project", code: buildSelected},
		{name: "missing", profile: "debug", project: "project", update: "DELETE FROM entries WHERE path=?", args: []any{[]byte("project/Cargo.toml")}, code: buildManifestUnsupported},
		{name: "skipped", profile: "debug", project: "project", update: "UPDATE entries SET skip_reason='generated refusal' WHERE path=?", args: []any{[]byte("project/Cargo.toml")}, code: buildSkipped},
		{name: "stale", profile: "debug", project: "project", update: "UPDATE directories SET generation=generation+1 WHERE path=?", args: []any{[]byte("project/target/debug")}, code: buildParentUnconfirmed},
		{name: "bounded", profile: "debug", project: "project", update: "UPDATE entries SET device=? WHERE path=?", args: []any{strings.Repeat("x", 1025), []byte("project/target")}, code: buildUnsupportedPath},
		{name: "raw", profile: "debug", project: "project-\xff", code: buildSelected},
	} {
		store := newBuildOutputFixture(t)
		addBuildOutputProject(t, store, tc.project, tc.profile)
		if tc.update != "" {
			sameSizeCargoExec(t, store, tc.update, tc.args...)
		}
		tx, err := store.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		var entryID int64
		target := []byte(filepath.Join(tc.project, "target"))
		if err := tx.QueryRowContext(context.Background(), "SELECT id FROM entries WHERE root_id=? AND path=?", int64(1), target).Scan(&entryID); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		reader := sameSizeCargoEntryReader{tx: tx}
		direct, directCode, directErr := readBuildOutputFinding(context.Background(), tx, 1, entryID, []byte("/cargo-fixture"), target, math.MaxInt64)
		prepared, preparedCode, preparedErr := readBuildOutputFindingWithReader(context.Background(), tx, 1, entryID, []byte("/cargo-fixture"), target, math.MaxInt64, reader.readLayout)
		closeErr := reader.close()
		rollbackErr := tx.Rollback()
		if directErr != nil || preparedErr != nil || closeErr != nil || rollbackErr != nil || directCode != tc.code || preparedCode != directCode || !reflect.DeepEqual(direct, prepared) {
			t.Fatal("direct/prepared layout result changed", tc.name, direct, prepared, directCode, preparedCode, directErr, preparedErr, closeErr, rollbackErr)
		}
		if tc.code == buildSelected && (len(prepared.Markers) != 7 || prepared.Profile != tc.profile) {
			t.Fatal("layout parity did not retain the whole expected finding", tc.name, prepared)
		}
	}

	// Compare the common query against both unchanged original contracts.
	// Every case uses generated saved tables. No new test event names are added.
	const originalMemberQuery = `SELECT e.id,substr(CAST(e.parent AS BLOB),1,4097),substr(CAST(e.kind AS BLOB),1,33),
 substr(CAST(e.device AS BLOB),1,21),substr(CAST(e.inode AS BLOB),1,21),e.size,e.allocated,e.mtime_ns,e.ctime_ns,e.generation,e.observed_at_ns,
 CASE WHEN e.skip_reason='' THEN 0 ELSE 1 END,
 COALESCE(p.generation,0),COALESCE(p.complete,0),COALESCE(p.checked_at_ns,0),CASE WHEN COALESCE(p.last_error,'')='' THEN 0 ELSE 1 END,
 COALESCE(o.generation,0),COALESCE(o.complete,0),COALESCE(o.checked_at_ns,0),CASE WHEN COALESCE(o.last_error,'')='' THEN 0 ELSE 1 END
 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent
 LEFT JOIN directories o ON o.root_id=e.root_id AND o.path=e.path WHERE e.root_id=? AND e.path=?`
	// Derive the new outcome from the actual old CAST-to-BLOB bytes, not from
	// the stored value's Go formatting or the new SQL predicate.
	referenceParentOutcome := func(parent []byte, path string) int64 {
		if len(parent) > 4096 {
			return 0
		}
		expected := ""
		if path != "." {
			expected = filepath.Dir(path)
		}
		if string(parent) == expected {
			return 2
		}
		return 1
	}
	oldMember := func(tx *sql.Tx, path string) (sameSizeCargoEntry, []byte, bool, error) {
		var e sameSizeCargoEntry
		var parent []byte
		var values [16]any
		err := tx.QueryRowContext(context.Background(), originalMemberQuery, int64(1), []byte(path)).Scan(&values[0], &parent, &e.kind, &e.device, &e.inode,
			&values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7], &values[8], &values[9], &values[10], &values[11], &values[12], &values[13], &values[14], &values[15])
		if errors.Is(err, sql.ErrNoRows) {
			return e, parent, false, nil
		}
		if err != nil {
			return e, parent, false, err
		}
		fields := []*int64{&e.id, &e.size, &e.allocated, &e.modified, &e.changed, &e.generation, &e.observed, &e.skipped,
			&e.parentGeneration, &e.parentComplete, &e.parentChecked, &e.parentError, &e.ownGeneration, &e.complete, &e.checked, &e.ownError}
		for i, v := range values {
			if *fields[i], err = sameSizeCargoInt(v); err != nil {
				return e, parent, false, err
			}
		}
		e.parentOutcome = referenceParentOutcome(parent, path)
		return e, parent, true, nil
	}
	parityTx := func() *sql.Tx {
		store := sameSizeCargoFixture(t, "debug")
		tx, err := store.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		// No affinity on scalar columns: prove malformed stored types instead
		// of letting the real INTEGER affinity repair the test's input.
		for _, q := range []string{
			`CREATE TEMP TABLE entries (id,root_id,path,parent,kind,device,inode,size,allocated,mtime_ns,ctime_ns,generation,observed_at_ns,skip_reason,UNIQUE(root_id,path))`,
			`INSERT INTO temp.entries SELECT id,root_id,path,parent,kind,device,inode,size,allocated,mtime_ns,ctime_ns,generation,observed_at_ns,skip_reason FROM main.entries`,
			`CREATE TEMP TABLE directories (root_id,path,generation,complete,checked_at_ns,last_error,UNIQUE(root_id,path))`,
			`INSERT INTO temp.directories SELECT root_id,path,generation,complete,checked_at_ns,last_error FROM main.directories`,
		} {
			if _, err := tx.ExecContext(context.Background(), q); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
		}
		return tx
	}
	// Compare raw SQL storage classes and values before either typed decoder.
	// This test-only cursor checks bounded column metadata and closes on every path.
	gatedAliases := map[int]string{1: "c02", 2: "c03", 3: "c04", 4: "c05", 9: "c10", 12: "c13", 13: "c14", 14: "c15", 15: "c16", 16: "c17", 17: "c18", 18: "c19", 19: "c20", 20: "c21", 21: "c22", 22: "c23", 23: "c24", 24: "c25", 25: "c26"}
	memberColumns := []int{0, 13, 14, 15, 16, 17, 18, 5, 6, 7, 8, 19, 10, 11, 20, 21, 22, 23, 24, 25}
	readProjectedRaw := func(reader *sameSizeCargoEntryReader, path string, role int64) ([]any, error) {
		if len(path) > 4096 {
			// Finite private decoder probes retain the original scalar payload.
			// They do not send an overlong key through the production transport.
			rows, err := reader.tx.QueryContext(context.Background(), sameSizeCargoEntryQuery, int64(1), []byte(path), role, sameSizeCargoExpectedParent(path))
			if err != nil {
				return nil, err
			}
			columns, err := rows.Columns()
			if err != nil || len(columns) != 26 {
				return nil, errors.Join(err, errors.New("scalar parity width changed"), rows.Close())
			}
			for i, column := range columns {
				if len(column) > 64 || gatedAliases[i] != "" && column != gatedAliases[i] {
					return nil, errors.Join(errors.New("scalar parity alias changed"), rows.Close())
				}
			}
			if !rows.Next() {
				return nil, errors.Join(sql.ErrNoRows, rows.Err(), rows.Close())
			}
			values, destinations := make([]any, 26), make([]any, 26)
			for i := range values {
				destinations[i] = &values[i]
			}
			if err := rows.Scan(destinations...); err != nil {
				return nil, errors.Join(err, rows.Err(), rows.Close())
			}
			if rows.Next() {
				return nil, errors.Join(errors.New("extra scalar parity row"), rows.Err(), rows.Close())
			}
			if err := errors.Join(rows.Err(), rows.Close()); err != nil {
				return nil, err
			}
			return values, nil
		}
		var values []any
		var captureErr error
		var observed *sameSizeCargoObservedRows
		previous := reader.wrapCursor
		reader.wrapCursor = func(stage sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
			actual, ok := rows.(*sql.Rows)
			if !ok {
				t.Fatal("raw parity did not receive real SQL rows")
			}
			columns, err := actual.Columns()
			if err != nil {
				captureErr = err
			} else if len(columns) != 28 {
				captureErr = errors.New("transport plus payload column count changed")
			}
			for i, column := range columns {
				if len(column) > 64 || i >= 2 && gatedAliases[i-2] != "" && column != gatedAliases[i-2] {
					captureErr = errors.New("bounded payload alias changed")
				}
			}
			observed = &sameSizeCargoObservedRows{rows: rows}
			captureRaw := func(ordinal, presence any) {
				if captureErr != nil || presence == int64(0) {
					return
				}
				values = make([]any, 26)
				var headerOrdinal, headerPresence any
				destinations := make([]any, 28)
				destinations[0], destinations[1] = &headerOrdinal, &headerPresence
				for i := range values {
					destinations[i+2] = &values[i]
				}
				captureErr = actual.Scan(destinations...)
				if captureErr == nil && (headerOrdinal != ordinal || headerPresence != presence) {
					captureErr = errors.New("repeated Scan changed reached transport header")
				}
			}
			observed.header = func(_ int, ordinal, presence *any) { captureRaw(*ordinal, *presence) }
			observed.afterScan = func(dest []any) {
				if _, combined := dest[0].(*sameSizeCargoOrdinalScanner); combined {
					// Repeating Scan is solely a raw-value diagnostic after a valid
					// production Scan. It is absent from the one-Scan count/resource cases.
					captureRaw(observed.inputOrdinal, observed.inputPresence)
				}
			}
			return observed
		}
		defer func() { reader.wrapCursor = previous }()
		var exists bool
		var err error
		if role == sameSizeCargoLayoutRole {
			_, exists, err = reader.readLayout(context.Background(), reader.tx, 1, path)
		} else {
			_, exists, err = readSameSizeCargoEntry(context.Background(), reader, 1, path)
		}
		if captureErr != nil {
			return nil, captureErr
		}
		if observed == nil {
			return nil, err
		}
		if observed.lastErr != nil || observed.lastCloseErr != nil {
			return nil, errors.Join(observed.lastErr, observed.lastCloseErr)
		}
		if observed.closeCalls != 1 || observed.afterCloseCalls != 0 || reader.active != nil {
			return nil, errors.New("raw parity left a cursor open")
		}
		if values == nil {
			if !exists && err == nil {
				return nil, sql.ErrNoRows
			}
			return nil, err
		}
		// The adjacent typed parity check compares decoder errors. This check
		// compares the actual reached SQL payload before that decoder converts it.
		return values, nil
	}

	checkRawProjection := func(tx *sql.Tx, reader *sameSizeCargoEntryReader, name, path string) {
		for _, role := range []int64{sameSizeCargoLayoutRole, sameSizeCargoMemberRole, sameSizeCargoLayoutRole, sameSizeCargoMemberRole} {
			query, count := buildOutputEntryQuery, 13
			if role == sameSizeCargoMemberRole {
				query, count = originalMemberQuery, 20
			}
			original, destinations := make([]any, count), make([]any, count)
			for i := range original {
				destinations[i] = &original[i]
			}
			directErr := tx.QueryRowContext(context.Background(), query, int64(1), []byte(path)).Scan(destinations...)
			projected, projectedErr := readProjectedRaw(reader, path, role)
			if (directErr == nil) != (projectedErr == nil) || errors.Is(directErr, sql.ErrNoRows) != errors.Is(projectedErr, sql.ErrNoRows) {
				t.Fatal("raw role projection error or ownership changed", name, role, directErr, projectedErr)
			}
			if directErr != nil {
				if !errors.Is(directErr, sql.ErrNoRows) {
					t.Fatal("unexpected raw direct-reader error", name, role, directErr)
				}
				continue
			}
			used := [26]bool{}
			for i, value := range original {
				index := i
				if role == sameSizeCargoMemberRole {
					index = memberColumns[i]
				}
				used[index] = true
				if role == sameSizeCargoMemberRole && i == 1 {
					parent, ok := value.([]byte)
					if value != nil && !ok {
						t.Fatal("original parent was not bounded BLOB/NULL", name, value)
					}
					if projected[index] != referenceParentOutcome(parent, path) {
						t.Fatal("parent outcome changed byte semantics", name, parent, projected[index])
					}
					continue
				}
				if !reflect.DeepEqual(value, projected[index]) {
					t.Fatal("selected SQL value or storage class changed", name, role, i, value, projected[index])
				}
			}
			for i, value := range projected {
				if !used[i] && value != nil {
					t.Fatal("inactive SQL column materialized a value", name, role, i, value)
				}
			}
		}
	}
	checkParity := func(tx *sql.Tx, name, path string) {
		reader := sameSizeCargoEntryReader{tx: tx}
		direct, directExists, directErr := readBuildOutputEntry(context.Background(), tx, 1, path)
		common, commonExists, commonErr := reader.readLayout(context.Background(), tx, 1, path)
		if directExists != commonExists || (directErr == nil) != (commonErr == nil) || !reflect.DeepEqual(direct, common) {
			t.Fatal("typed layout projection changed", name, direct, common, directErr, commonErr)
		}
		checkRawProjection(tx, &reader, name, path)
		old, _, oldExists, oldErr := oldMember(tx, path)
		member, memberExists, memberErr := readSameSizeCargoEntry(context.Background(), &reader, 1, path)
		if oldExists != memberExists || (oldErr == nil) != (memberErr == nil) || !reflect.DeepEqual(old, member) {
			t.Fatal("strict membership projection changed", name, old, member, oldErr, memberErr)
		}
		if err := reader.close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"device", "inode"} {
		for _, value := range []string{"", "0", "01", "bad", "\xff", "18446744073709551615", strings.Repeat("1", 21), strings.Repeat("1", 521), strings.Repeat("1", 1025), strings.Repeat("é", 513)} {
			tx := parityTx()
			if _, err := tx.ExecContext(context.Background(), "UPDATE entries SET "+field+"=? WHERE path=?", value, []byte("project/target")); err != nil {
				t.Fatal(err)
			}
			checkParity(tx, field, "project/target")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, field := range []string{"parent", "kind", "skip_reason"} {
		for _, value := range []string{"", "bad\x00value", "raw-\xff", strings.Repeat("k", 33), strings.Repeat("x", 4097), strings.Repeat("é", 4097)} {
			tx := parityTx()
			if _, err := tx.ExecContext(context.Background(), "UPDATE entries SET "+field+"=? WHERE path=?", value, []byte("project/target")); err != nil {
				t.Fatal(err)
			}
			checkParity(tx, field, "project/target")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, value := range []string{"", "bad\x00value", "raw-\xff", strings.Repeat("x", 4097), strings.Repeat("é", 4097)} {
		tx := parityTx()
		if _, err := tx.ExecContext(context.Background(), "UPDATE directories SET last_error=? WHERE path=?", value, []byte("project")); err != nil {
			t.Fatal(err)
		}
		checkParity(tx, "parent_error", "project/target")
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	for _, column := range []struct{ table, field, path string }{
		{"entries", "id", "project/target"}, {"entries", "mtime_ns", "project/target"}, {"entries", "ctime_ns", "project/target"},
		{"entries", "generation", "project/target"}, {"entries", "observed_at_ns", "project/target"},
		{"directories", "generation", "project"}, {"directories", "complete", "project"},
		// These values are irrelevant to the direct layout decoder.
		{"entries", "size", "project/target"}, {"entries", "allocated", "project/target"},
		{"directories", "generation", "project/target"}, {"directories", "complete", "project/target"}, {"directories", "checked_at_ns", "project/target"},
	} {
		for _, scalar := range []struct {
			value any
			kind  string
		}{{int64(7), "integer"}, {"7", "text"}, {"corrupt", "text"}, {float64(7.5), "real"}, {nil, "null"}} {
			tx := parityTx()
			if _, err := tx.ExecContext(context.Background(), "UPDATE "+column.table+" SET "+column.field+"=? WHERE path=?", scalar.value, []byte(column.path)); err != nil {
				t.Fatal(err)
			}
			var storedKind string
			if err := tx.QueryRowContext(context.Background(), "SELECT typeof("+column.field+") FROM "+column.table+" WHERE path=?", []byte(column.path)).Scan(&storedKind); err != nil || storedKind != scalar.kind {
				t.Fatal("numeric corruption fixture was coerced", column, storedKind, scalar.kind, err)
			}
			checkParity(tx, column.table+"."+column.field, "project/target")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Transport headers shift wrapped driver column indices. Require the same
	// first payload label and typed NULL refusal, without error-string parity.
	for _, field := range []struct{ column, original, alias string }{
		{"parent", "substr(e.parent,1,4097)", "c02"},
		{"kind", "substr(e.kind,1,33)", "c03"},
		{"device", "substr(e.device,1,1025)", "c04"},
		{"inode", "substr(e.inode,1,1025)", "c05"},
		{"skip_reason", "substr(e.skip_reason,1,4097)", "c10"},
	} {
		tx := parityTx()
		if _, err := tx.ExecContext(context.Background(), "UPDATE entries SET "+field.column+"=NULL WHERE path=?", []byte("project/target")); err != nil {
			t.Fatal(err)
		}
		var stored string
		if err := tx.QueryRowContext(context.Background(), "SELECT typeof("+field.column+") FROM entries WHERE path=?", []byte("project/target")).Scan(&stored); err != nil || stored != "null" {
			t.Fatal("NULL projection fixture was coerced", field.column, stored, err)
		}
		reader := sameSizeCargoEntryReader{tx: tx}
		checkRawProjection(tx, &reader, "gated_null_"+field.column, "project/target")
		direct, directExists, directErr := readBuildOutputEntry(context.Background(), tx, 1, "project/target")
		projected, projectedExists, projectedErr := reader.readLayout(context.Background(), tx, 1, "project/target")
		closeErr, rollbackErr := reader.close(), tx.Rollback()
		if directExists || projectedExists || directErr == nil || projectedErr == nil || !reflect.DeepEqual(direct, projected) || closeErr != nil || rollbackErr != nil {
			t.Fatal("gated NULL conversion ceased refusing", field, direct, projected, directErr, projectedErr, closeErr, rollbackErr)
		}
		directLabel, projectedLabel := "name \""+field.original+"\"", "name \""+field.alias+"\""
		if !strings.Contains(directErr.Error(), directLabel) || !strings.Contains(projectedErr.Error(), projectedLabel) || strings.Count(projectedErr.Error(), projectedLabel) != 1 || !strings.Contains(directErr.Error(), "converting NULL to string is unsupported") || !strings.Contains(projectedErr.Error(), "converting NULL to string is unsupported") {
			t.Fatal("gated NULL decoder order or refusal changed", field, directErr, projectedErr)
		}
	}
	// Independent legacy membership retains its separate saved parent bytes.
	// Compare both the decision and every identity slot, including refusals.
	oldMembership := func(e sameSizeCargoEntry, parent []byte, relative, kind string, seen *sameSizeCargoIdentities) bool {
		if e.id <= 0 || string(e.kind) != kind || len(parent) > 4096 || e.skipped != 0 || e.observed <= 0 || !seen.add(e) {
			return false
		}
		if relative == "." {
			if len(parent) != 0 {
				return false
			}
		} else if string(parent) != filepath.Dir(relative) || e.parentGeneration <= 0 || e.generation != e.parentGeneration || e.parentComplete != 1 || e.parentError != 0 || e.parentChecked <= 0 || e.observed > e.parentChecked {
			return false
		}
		return kind != "directory" || e.ownGeneration > 0 && e.complete == 1 && e.ownError == 0 && e.checked > 0 && e.observed <= e.checked
	}
	// These generated private projections include paths outside public raw-page
	// admission. They prove byte/type boundaries, not new filesystem authority.
	for _, tc := range []struct {
		name, path, storedKind string
		value                  any
		emptyBlob              bool
		outcome                int64
	}{
		{"root_null", ".", "null", nil, false, 2},
		{"root_empty_text", ".", "text", "", false, 2},
		{"root_empty_blob", ".", "blob", nil, true, 2},
		{"nonroot_null", "project/target", "null", nil, false, 1},
		{"nonroot_empty_text", "project/target", "text", "", false, 1},
		{"nonroot_empty_blob", "project/target", "blob", nil, true, 1},
		{"text_equal", "project/target", "text", "project", false, 2},
		{"blob_equal", "project/target", "blob", []byte("project"), false, 2},
		{"text_unequal", "project/target", "text", "projects", false, 1},
		{"blob_same_prefix", "project/target", "blob", []byte("projectx"), false, 1},
		{"integer_equal", "1/leaf", "integer", int64(1), false, 2},
		{"real_equal", "1.5/leaf", "real", float64(1.5), false, 2},
		{"raw_text_equal", "raw-\xff/leaf", "text", "raw-\xff", false, 2},
		{"raw_blob_equal", "raw-\xff/leaf", "blob", []byte("raw-\xff"), false, 2},
		{"nul_text_equal", "raw-\x00/leaf", "text", "raw-\x00", false, 2},
		{"nul_blob_unequal", "raw-\x00/leaf", "blob", []byte("raw-\x00x"), false, 1},
		{"4096_equal", strings.Repeat("p", 4096) + "/leaf", "blob", []byte(strings.Repeat("p", 4096)), false, 2},
		{"4096_unequal", strings.Repeat("p", 4096) + "/leaf", "blob", []byte(strings.Repeat("p", 4095) + "q"), false, 1},
		{"4097_prefix", strings.Repeat("p", 4096) + "/leaf", "blob", []byte(strings.Repeat("p", 4097)), false, 0},
		{"long_text", "project/target", "text", strings.Repeat("p", 5000), false, 0},
		{"multibyte_4096_equal", strings.Repeat("é", 2048) + "/leaf", "text", strings.Repeat("é", 2048), false, 2},
		{"multibyte_4097", strings.Repeat("é", 2048) + "/leaf", "text", strings.Repeat("é", 2048) + "x", false, 0},
	} {
		tx := parityTx()
		if _, err := tx.ExecContext(context.Background(), "INSERT OR REPLACE INTO temp.entries SELECT id,root_id,?,parent,kind,device,inode,size,allocated,mtime_ns,ctime_ns,generation,observed_at_ns,skip_reason FROM main.entries WHERE path=?", []byte(tc.path), []byte("project/target")); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(context.Background(), "INSERT OR REPLACE INTO temp.directories SELECT root_id,?,generation,complete,checked_at_ns,last_error FROM main.directories WHERE path=?", []byte(tc.path), []byte("project/target")); err != nil {
			t.Fatal(err)
		}
		if tc.path != "." {
			if _, err := tx.ExecContext(context.Background(), "INSERT OR REPLACE INTO temp.directories SELECT root_id,?,generation,complete,checked_at_ns,last_error FROM main.directories WHERE path=?", []byte(filepath.Dir(tc.path)), []byte("project")); err != nil {
				t.Fatal(err)
			}
		}
		if tc.emptyBlob {
			if _, err := tx.ExecContext(context.Background(), "UPDATE entries SET parent=X'' WHERE path=?", []byte(tc.path)); err != nil {
				t.Fatal(err)
			}
		} else if _, err := tx.ExecContext(context.Background(), "UPDATE entries SET parent=? WHERE path=?", tc.value, []byte(tc.path)); err != nil {
			t.Fatal(err)
		}
		var storedKind string
		if err := tx.QueryRowContext(context.Background(), "SELECT typeof(parent) FROM entries WHERE path=?", []byte(tc.path)).Scan(&storedKind); err != nil || storedKind != tc.storedKind {
			t.Fatal("parent storage-class fixture was coerced", tc.name, storedKind, err)
		}
		reader := sameSizeCargoEntryReader{tx: tx}
		checkRawProjection(tx, &reader, tc.name, tc.path)
		original, parent, originalExists, originalErr := oldMember(tx, tc.path)
		var projected sameSizeCargoEntry
		var projectedExists bool
		var projectedErr error
		if len(tc.path) > 4096 {
			// These finite private SQL probes test the existing 4096-byte
			// parent projection. They do not relax the transport key bound.
			projected, projectedErr = scanSameSizeCargoEntry(tx.QueryRowContext(context.Background(), sameSizeCargoEntryQuery, int64(1), []byte(tc.path), sameSizeCargoMemberRole, filepath.Dir(tc.path)))
			projectedExists = projectedErr == nil
			fresh := sameSizeCargoEntryReader{}
			request := sameSizeCargoRequest{path: tc.path, parent: filepath.Dir(tc.path), role: sameSizeCargoMemberRole}
			if err := fresh.open(context.Background(), sameSizeCargoMembershipStage, 1, []byte(tc.path), []sameSizeCargoRequest{request}); err == nil || !sameSizeCargoTestOwnersEmpty(&fresh) {
				t.Fatal("private decoder probe relaxed the transport key bound", tc.name, err)
			}
		} else {
			projected, projectedExists, projectedErr = readSameSizeCargoEntry(context.Background(), &reader, 1, tc.path)
		}
		if originalErr != nil || projectedErr != nil || !originalExists || !projectedExists || original.parentOutcome != tc.outcome || projected.parentOutcome != tc.outcome || !reflect.DeepEqual(original, projected) {
			t.Fatal("typed parent outcome changed", tc.name, original, projected, originalErr, projectedErr)
		}
		oldSeen, newSeen := sameSizeCargoIdentities{}, sameSizeCargoIdentities{}
		oldDecision := oldMembership(original, parent, tc.path, "directory", &oldSeen)
		newDecision := sameSizeCargoMembership(projected, tc.path, "directory", &newSeen)
		if oldDecision != newDecision || !reflect.DeepEqual(oldSeen, newSeen) || tc.outcome == 0 && oldSeen.count != 0 || tc.outcome == 1 && oldSeen.count != 1 {
			t.Fatal("parent outcome changed membership or seen ordering", tc.name, oldDecision, newDecision, oldSeen.count, newSeen.count)
		}
		if closeErr, rollbackErr := reader.close(), tx.Rollback(); closeErr != nil || rollbackErr != nil {
			t.Fatal("parent projection ownership failed", tc.name, closeErr, rollbackErr)
		}
	}
	baseEntry := sameSizeCargoEntry{id: 1, size: 1, allocated: 1, modified: 1, changed: 1, generation: 1, observed: 1,
		parentGeneration: 1, parentComplete: 1, parentChecked: 1, ownGeneration: 1, complete: 1, checked: 1,
		kind: []byte("directory"), device: []byte("0"), inode: []byte("2")}
	for _, tc := range []struct {
		name   string
		parent []byte
		mutate func(*sameSizeCargoEntry)
	}{
		{"too_long", []byte(strings.Repeat("p", 4097)), nil},
		{"unequal", []byte("other"), nil},
		{"matched", []byte("project"), nil},
		{"bad_id", []byte("project"), func(e *sameSizeCargoEntry) { e.id = 0 }},
		{"bad_kind", []byte("project"), func(e *sameSizeCargoEntry) { e.kind = []byte("file") }},
		{"skipped", []byte("project"), func(e *sameSizeCargoEntry) { e.skipped = 1 }},
		{"unobserved", []byte("project"), func(e *sameSizeCargoEntry) { e.observed = 0 }},
		{"bad_identity", []byte("project"), func(e *sameSizeCargoEntry) { e.device = []byte("bad") }},
		{"bad_generation", []byte("project"), func(e *sameSizeCargoEntry) { e.generation = 0 }},
		{"bad_parent_pass", []byte("project"), func(e *sameSizeCargoEntry) { e.parentComplete = 0 }},
		{"bad_own_pass", []byte("project"), func(e *sameSizeCargoEntry) { e.complete = 0 }},
	} {
		e := baseEntry
		if tc.mutate != nil {
			tc.mutate(&e)
		}
		e.parentOutcome = referenceParentOutcome(tc.parent, "project/target")
		for _, prior := range []string{"empty", "conflict", "full"} {
			oldSeen := sameSizeCargoIdentities{}
			if prior == "conflict" {
				oldSeen.count = 1
				oldSeen.values[0] = sameSizeCargoIdentity{device: 0, inode: 2, changed: 9}
			} else if prior == "full" {
				oldSeen.count = len(oldSeen.values)
				for i := range oldSeen.values {
					oldSeen.values[i] = sameSizeCargoIdentity{device: 9, inode: uint64(i + 1)}
				}
			}
			newSeen := oldSeen
			oldDecision := oldMembership(e, tc.parent, "project/target", "directory", &oldSeen)
			newDecision := sameSizeCargoMembership(e, "project/target", "directory", &newSeen)
			if oldDecision != newDecision || !reflect.DeepEqual(oldSeen, newSeen) {
				t.Fatal("parent projection changed legacy short circuit", tc.name, prior, oldDecision, newDecision, oldSeen.count, newSeen.count)
			}
			if prior == "empty" && (tc.name == "too_long" && oldSeen.count != 0 || tc.name == "unequal" && oldSeen.count != 1 || tc.name == "matched" && (!oldDecision || oldSeen.count != 1)) {
				t.Fatal("parent ordering fixture did not reach its boundary", tc.name, oldDecision, oldSeen.count)
			}
		}
	}
	missingTx := parityTx()
	checkParity(missingTx, "no_row", "not-saved")
	if err := missingTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	for _, role := range []int64{sameSizeCargoMemberRole, sameSizeCargoLayoutRole} {
		presenceTx := parityTx()
		if _, err := presenceTx.ExecContext(context.Background(), "UPDATE entries SET id=NULL WHERE path=?", []byte("project/target")); err != nil {
			t.Fatal(err)
		}
		reader := sameSizeCargoEntryReader{tx: presenceTx}
		var flags []any
		reader.wrapCursor = func(_ sameSizeCargoStage, rows sameSizeCargoRows) sameSizeCargoRows {
			observed := &sameSizeCargoObservedRows{rows: rows}
			observed.header = func(_ int, _ *any, presence *any) { flags = append(flags, *presence) }
			observed.headerInput = func(_ int, column string, value any) any {
				if column == "presence" {
					flags = append(flags, value)
				}
				return value
			}
			return observed
		}
		if role == sameSizeCargoMemberRole {
			entry, exists, err := readSameSizeCargoEntry(context.Background(), &reader, 1, "project/target")
			if exists || err == nil || entry.id != 0 {
				t.Fatal("matched NULL id bypassed the strict member decoder", entry, exists, err)
			}
			if _, exists, err := readSameSizeCargoEntry(context.Background(), &reader, 1, "not-saved"); exists || err != nil {
				t.Fatal("missing member acquired a decoder error", exists, err)
			}
		} else {
			entry, exists, err := reader.readLayout(context.Background(), presenceTx, 1, "project/target")
			if exists || err == nil || entry.id != 0 {
				t.Fatal("matched NULL id bypassed the typed layout decoder", entry, exists, err)
			}
			if _, exists, err := reader.readLayout(context.Background(), presenceTx, 1, "not-saved"); exists || err != nil {
				t.Fatal("missing layout acquired a decoder error", exists, err)
			}
		}
		if !reflect.DeepEqual(flags, []any{int64(1), int64(0)}) {
			t.Fatal("presence used corrupt id instead of the matched equality key", role, flags)
		}
		if err := reader.close(); err != nil {
			t.Fatal(err)
		}
		if err := presenceTx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}

	for _, role := range []string{"entry", "root"} {
		for _, fault := range []string{"close_error", "close_cancel", "read_and_close_error"} {
			s := sameSizeCargoFixture(t, "debug")
			ctx, cancel := context.WithCancel(context.Background())
			closeErr := errors.New("injected checked statement close")
			closes, observations := [2]int{}, 0
			beforeCommit := false
			checkedClose := func(index int, name string) func(*sql.Stmt) error {
				return func(stmt *sql.Stmt) error {
					closes[index]++
					err := stmt.Close()
					if name != role {
						return err
					}
					if fault == "close_cancel" {
						cancel()
						return err
					}
					return errors.Join(err, closeErr)
				}
			}
			hooks := sameSizeCargoHooks{beforeCommit: func() { beforeCommit = true },
				closeEntryStatement: checkedClose(0, "entry"), closeRootStatement: checkedClose(1, "root")}
			if fault == "read_and_close_error" {
				hooks.beforeExcluded = func(*sameSizeCargoIdentities, *BuildOutputFinding) {
					observations++
					cancel()
				}
			}
			r, err := s.sameSizeCargoCandidates(ctx, 200, "", 1, hooks)
			cancel()
			if closes != [2]int{4, 1} || beforeCommit || !reflect.DeepEqual(r, SameSizeReport{}) {
				t.Fatal("statement terminal error escaped checked close", role, fault, closes, beforeCommit, r, err)
			}
			if fault != "close_cancel" && !errors.Is(err, closeErr) {
				t.Fatal("statement close cause was lost", fault, err)
			}
			if fault != "close_error" && !errors.Is(err, context.Canceled) {
				t.Fatal("statement cancellation cause was lost", fault, err)
			}
			if fault == "read_and_close_error" && observations != 1 {
				t.Fatal("read cancellation did not follow real saved evidence", observations)
			}
			if next := sameSizeCargoReport(t, s, 200, ""); next.CargoTargetExclusion.ExcludedRows != 2 {
				t.Fatal("failed statement lifecycle changed saved data", fault, next)
			}
		}
	}
	// Multiple terminal causes must survive independent closes of every role.
	allFaults := sameSizeCargoFixture(t, "debug")
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	causes := [2]error{errors.New("entry close"), errors.New("root close")}
	closes := [2]int{}
	closeFault := func(index int) func(*sql.Stmt) error {
		return func(stmt *sql.Stmt) error {
			closes[index]++
			return errors.Join(stmt.Close(), causes[index])
		}
	}
	committed := false
	r, err := allFaults.sameSizeCargoCandidates(ctx, 200, "", 1, sameSizeCargoHooks{
		beforeExcluded: func(*sameSizeCargoIdentities, *BuildOutputFinding) { cancel() },
		beforeCommit:   func() { committed = true }, closeEntryStatement: closeFault(0),
		closeRootStatement: closeFault(1),
	})
	if closes != [2]int{4, 1} || committed || !reflect.DeepEqual(r, SameSizeReport{}) || !errors.Is(err, context.Canceled) {
		t.Fatal("combined read/close cancellation escaped", closes, committed, r, err)
	}
	for _, cause := range causes {
		if !errors.Is(err, cause) {
			t.Fatal("independent close cause was lost", cause, err)
		}
	}
	if next := sameSizeCargoReport(t, allFaults, 200, ""); next.CargoTargetExclusion.ExcludedRows != 2 {
		t.Fatal("joined failure changed saved data", next)
	}
}
