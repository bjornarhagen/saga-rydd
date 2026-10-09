package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestTopologyLargeBoundsAndGeometryWithoutGeneration(t *testing.T) {
	for _, shape := range []string{"wide", "deep"} {
		o := options{Binary: "/selected/binary", Output: "/new/generated", Shape: shape, Files: 1000000, Seconds: 3600}
		if err := o.validate(); err != nil {
			t.Fatal(err)
		}
		top := topology{shape, 1000000}
		root := "/new/generated/node_modules"
		rel, err := filepath.Rel(root, top.path(root, 999999))
		if err != nil {
			t.Fatal(err)
		}
		max := 8
		if shape == "deep" {
			max = 260
		}
		if len(rel) != max || top.directoryCount() != top.levels() {
			t.Fatal(shape, rel, top.directoryCount())
		}
		sentinel := 0
		for n := 0; n < top.files; n++ {
			if len(top.body(n)) > 0 {
				sentinel++
			}
		}
		want := 977
		if shape == "deep" {
			want = 1040
		}
		if sentinel != want {
			t.Fatal(shape, sentinel)
		}
		for _, change := range []func(*options){func(o *options) { o.Files = 1000001 }, func(o *options) { o.Seconds = 3601 }, func(o *options) { o.Output = "/" + string(make([]byte, 513)) }} {
			bad := o
			change(&bad)
			if bad.validate() == nil {
				t.Fatal("large unsupported bound accepted")
			}
		}
	}
	for _, field := range reflect.VisibleFields(reflect.TypeOf(sample{})) {
		if field.Name == "Saved" || field.Type == reflect.TypeOf(savedView{}) || field.Type == reflect.TypeOf([]savedJob{}) {
			t.Fatal("samples retain saved jobs", field)
		}
	}
}
func TestTopologyInitialCapacityArithmeticAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		block, avail uint64
		want         int64
		bad          bool
	}{{4096, 1 << 22, 16 << 30, false}, {4096, (1 << 22) - 1, (16 << 30) - 4096, false}, {0, 1, 0, true}, {1 << 63, 1, 0, true}, {4096, math.MaxUint64, 0, true}} {
		got, err := outputCapacityBytes(tc.block, tc.avail)
		if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
			t.Fatal(tc, got, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := checkInitialOutputCapacity(ctx, "/path/that/is/not/queried"); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(got, err)
	}
}
func TestTopologySavedJobsSpoolBoundedPages(t *testing.T) {
	base := t.TempDir()
	v := savedView{Files: 1000000}
	for n := 0; n < maximumSavedJobs; n++ {
		v.Jobs = append(v.Jobs, savedJob{ID: int64(n + 1), Cursor: make([]byte, state.MaxCursorBytes), Path: []byte(".")})
	}
	if err := spoolSavedEvidence(base, 720, v); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(base, "sample-720-jobs-*.json"))
	if err != nil || len(files) != 9 {
		t.Fatal(files, err)
	}
	seen := 0
	for _, p := range files {
		b, e := os.ReadFile(p)
		if e != nil || len(b) > maximumJobPageBytes {
			t.Fatal(e, len(b))
		}
		var jobs []savedJob
		if e = json.Unmarshal(b, &jobs); e != nil || len(jobs) > savedJobsPerPage {
			t.Fatal(e, len(jobs))
		}
		seen += len(jobs)
	}
	if seen != maximumSavedJobs {
		t.Fatal(seen)
	}
	b, err := os.ReadFile(filepath.Join(base, "sample-720-summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary savedView
	if err = json.Unmarshal(b, &summary); err != nil || summary.Files != 1000000 || len(summary.Jobs) != 0 {
		t.Fatal(err, summary.Files, len(summary.Jobs))
	}
	if spoolSavedEvidence(base, 721, v) == nil {
		t.Fatal("unbounded sample accepted")
	}
	// A repeated sample cannot overwrite the already-published exact evidence.
	if spoolSavedEvidence(base, 720, v) == nil {
		t.Fatal("existing receipt overwritten")
	}
}
func TestTopologySavedJobProjectionRefusesOversizedFields(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	dir := filepath.Join(base, "state")
	primary := filepath.Join(base, "primary")
	healthy := filepath.Join(base, "healthy")
	st, err := state.OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SyncRoots(ctx, []string{primary, healthy}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2} {
		if err = st.EnqueueJob(ctx, id, "inventory", []byte("."), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	if got, e := readSaved(ctx, dir, primary, healthy); e != nil || len(got.Jobs) != 2 {
		t.Fatal(e, len(got.Jobs))
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("PRAGMA ignore_check_constraints=on"); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"cursor", "path", "attempts", "last_error", "lease_token"} {
		tx, e := db.Begin()
		if e != nil {
			t.Fatal(e)
		}
		if _, e = tx.Exec("UPDATE jobs SET " + field + "=zeroblob(2097152) WHERE id=(SELECT min(id) FROM jobs)"); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
		if _, e = readSaved(ctx, dir, primary, healthy); !errors.Is(e, errProfile) {
			t.Fatal(field, e)
		}
		reset := map[string]string{"cursor": "NULL", "path": "X'2e'", "attempts": "0", "last_error": "''", "lease_token": "''"}[field]
		if _, e = db.Exec("UPDATE jobs SET " + field + "=" + reset); e != nil {
			t.Fatal(e)
		}
	}
}

func TestTopologyCapacityRefusalBeforeGenerationOrChild(t *testing.T) {
	base := t.TempDir()
	binary := filepath.Join(base, "selected-binary")
	if err := os.WriteFile(binary, []byte("not an executable format; capacity must refuse first"), 0700); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(base, "new-output")
	low := minimumOutputCapacity - 4096
	o := options{Binary: binary, Output: output, Shape: "deep", Files: 1000000, Seconds: 3600}
	called := 0
	got, err := runWithCapacity(context.Background(), o, func(ctx context.Context, path string) (*int64, error) {
		called++
		if filepath.Base(path) != "new-output" {
			t.Fatal(path)
		}
		return &low, errCapacity
	})
	if !errors.Is(err, errCapacity) || called != 1 || got.Outcome != "capacity_refused_no_generation" || got.InitialAvailableOutputBytes == nil || *got.InitialAvailableOutputBytes != low {
		t.Fatal(err, called, got.Outcome)
	}
	files, e := os.ReadDir(output)
	if e != nil || len(files) != 1 || files[0].Name() != "aggregate.json" {
		t.Fatal(e, files)
	}
	b, e := os.ReadFile(filepath.Join(output, "aggregate.json"))
	if e != nil {
		t.Fatal(e)
	}
	var receipt result
	if e = json.Unmarshal(b, &receipt); e != nil || receipt.SourceRevision != nil || receipt.DeclaredBuildMetadata != nil || len(receipt.Workers) != 0 || receipt.TerminalEvidenceSaved {
		t.Fatal(e)
	}
}

func TestTopologyCanonicalOutputBoundBeforeCapacity(t *testing.T) {
	base := t.TempDir()
	deep := base
	for len(deep) <= 512 {
		deep = filepath.Join(deep, strings.Repeat("d", 80))
	}
	if err := os.MkdirAll(deep, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(deep, alias); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(base, "selected-binary")
	if err := os.WriteFile(binary, []byte("never executed"), 0700); err != nil {
		t.Fatal(err)
	}
	o := options{Binary: binary, Output: filepath.Join(alias, "new-output"), Shape: "wide", Files: 1000000, Seconds: 3600}
	called := 0
	if _, err := runWithCapacity(context.Background(), o, func(context.Context, string) (*int64, error) { called++; return nil, errCapacity }); !errors.Is(err, errProfile) || called != 0 {
		t.Fatal(err, called)
	}
	files, err := os.ReadDir(filepath.Join(deep, "new-output"))
	if err != nil || len(files) != 0 {
		t.Fatal("refused path started generation or children", err, files)
	}
}

func TestTopologyFutureJobsCensusIncludesEverySavedJob(t *testing.T) {
	for _, tc := range []struct {
		name, change string
		wantMatch    bool
	}{
		{"exact", "", true},
		{"orphan_job", `INSERT INTO jobs VALUES(3,999,'inventory',X'2e','pending',86400000000100,NULL,'','',0,0,0)`, false},
		{"missing_directory", `DELETE FROM directories WHERE root_id=1`, false},
		{"other_root", `INSERT INTO roots VALUES(3,X'6f74686572'); INSERT INTO directories VALUES(3,X'2e',100); UPDATE jobs SET root_id=3 WHERE id=2`, false},
		{"one_root_twice", `UPDATE jobs SET root_id=1 WHERE id=2`, false},
		{"wrong_due_type", `UPDATE jobs SET due_at_ns=zeroblob(2097152) WHERE id=1`, false},
		{"oversized_cursor", `UPDATE jobs SET cursor=zeroblob(2097152) WHERE id=1`, false},
		{"oversized_path", `UPDATE jobs SET path=zeroblob(2097152) WHERE id=1`, false},
		{"oversized_error", `UPDATE jobs SET last_error=zeroblob(2097152) WHERE id=1`, false},
		{"claimed", `UPDATE jobs SET inventory_claimed=1 WHERE id=1`, false},
		{"attempted", `UPDATE jobs SET attempts=1 WHERE id=1`, false},
		{"checked_at_overflow", `UPDATE directories SET checked_at_ns=9223372036854775807 WHERE root_id=1`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			_, err = db.Exec(`CREATE TABLE roots(id INTEGER PRIMARY KEY,path BLOB UNIQUE);
 CREATE TABLE directories(root_id INTEGER,path BLOB,checked_at_ns INTEGER);
 CREATE TABLE jobs(id INTEGER PRIMARY KEY,root_id INTEGER,kind TEXT,path BLOB,status TEXT,due_at_ns INTEGER,cursor BLOB,last_error TEXT,lease_token TEXT,lease_until_ns INTEGER,attempts INTEGER,inventory_claimed INTEGER);
 INSERT INTO roots VALUES(1,X'7072696d617279'),(2,X'6865616c746879');
 INSERT INTO directories VALUES(1,X'2e',100),(2,X'2e',100);
 INSERT INTO jobs VALUES(1,1,'inventory',X'2e','pending',86400000000100,NULL,'','',0,0,0),(2,2,'inventory',X'2e','pending',86400000000100,NULL,'','',0,0,0);`)
			if err != nil {
				t.Fatal(err)
			}
			if tc.change != "" {
				if _, err = db.Exec(tc.change); err != nil {
					t.Fatal(err)
				}
			}
			err = futureJobs(context.Background(), dir, "primary", "healthy")
			if tc.wantMatch {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errProfile) {
				t.Fatal("unexpected job census accepted", err)
			}
			if tc.name == "orphan_job" {
				// The earlier inner-join census omitted the third orphan row.
				var joined int
				if err = db.QueryRow(`SELECT count(*) FROM jobs j JOIN directories d ON d.root_id=j.root_id AND d.path=X'2e'`).Scan(&joined); err != nil || joined != 2 {
					t.Fatal("regression fixture does not reproduce the omitted row", joined, err)
				}
			}
		})
	}
}
