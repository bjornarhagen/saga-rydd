package state

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func seedScopeReduction(t *testing.T, s *Store) {
	t.Helper()
	on := true
	if _, err := s.ConfigureCompact(context.Background(), &on); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1);
 INSERT INTO allocation_cache(root_id,path) VALUES(1,X'2e')`); err != nil {
		t.Fatal(err)
	}
}

func TestScopeCoverageMatchesBoundedEvidence(t *testing.T) {
	cases := []string{
		"",
		"UPDATE directories SET complete=0 WHERE path=X'61'",
		"UPDATE directories SET last_error='fixture' WHERE path=X'61'",
		"UPDATE entries SET skip_reason='excluded' WHERE path=X'61'",
		"UPDATE entries SET generation=99 WHERE path=X'61'",
		"UPDATE entries SET kind='file' WHERE path=X'61'",
		"DELETE FROM entries WHERE path=X'61'",
		"UPDATE entries SET observed_at_ns=100 WHERE path=X'61'",
		"UPDATE entries SET inode='' WHERE kind='file'",
		"UPDATE entries SET allocated=8192 WHERE path=X'612f6c696e6b'",
		"UPDATE directories SET complete=0 WHERE path=X'2e'; UPDATE entries SET generation=99 WHERE path=X'61'",
	}
	for i, change := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s := directoryFixture(t)
			ctx := context.Background()
			if change != "" {
				if _, err := s.db.Exec(change); err != nil {
					t.Fatal(err)
				}
			}
			before, err := s.MeasureDirectory(ctx, "/fixture")
			if err != nil {
				t.Fatal(err)
			}
			seedScopeReduction(t, s)
			drainAllocations(t, s)
			after, err := s.MeasureDirectory(ctx, "/fixture")
			if err != nil {
				t.Fatal(err)
			}
			if after.CoverageSource != "cached_reduction" {
				t.Fatal(after)
			}
			normalize := func(r DirectoryReport) DirectoryReport {
				r.GeneratedAt = before.GeneratedAt
				r.Notes = nil
				r.EntryLimit = 0
				r.AllocatedSizeSource = ""
				r.CoverageSource = ""
				return r
			}
			if !reflect.DeepEqual(normalize(before), normalize(after)) {
				t.Fatalf("before %+v\nafter %+v", before, after)
			}
		})
	}
}

func TestScopeCoverageResumeAndRevisionReset(t *testing.T) {
	ctx := context.Background()
	s := directoryFixture(t)
	dir := filepath.Dir(s.path)
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 350; i++ {
		_, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,X'2e','file',1,4096,1,1,'dev','shared',1,10)`, []byte(fmt.Sprintf("z%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	seedScopeReduction(t, s)
	// Reset, then exactly one entry batch.
	for i := 0; i < 2; i++ {
		if worked, err := s.ReduceAllocations(ctx); err != nil || !worked {
			t.Fatal(worked, err)
		}
	}
	var examined int
	var coverage []byte
	if err = s.db.QueryRow("SELECT examined,coverage FROM allocation_cache").Scan(&examined, &coverage); err != nil || examined != MaxBatchEntries {
		t.Fatal(examined, err)
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_coverage BEFORE UPDATE ON allocation_cache BEGIN SELECT RAISE(ABORT,'fixture'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReduceAllocations(ctx); err == nil {
		t.Fatal("expected rollback")
	}
	var after []byte
	if err = s.db.QueryRow("SELECT coverage FROM allocation_cache").Scan(&after); err != nil || string(after) != string(coverage) {
		t.Fatal("coverage changed after rollback", err)
	}
	if _, err = s.db.Exec("DROP TRIGGER fail_coverage"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Simulate a committed revision change while partial work exists.
	if _, err = s.db.Exec(`UPDATE allocation_revisions SET revision=revision+1; UPDATE entries SET size=2 WHERE inode='shared'`); err != nil {
		t.Fatal(err)
	}
	drainAllocations(t, s)
	r, err := s.MeasureDirectory(ctx, "/fixture")
	if err != nil || r.CoverageSource != "cached_reduction" || *r.LogicalBytes != 2400 || *r.AllocatedBytes != 12288 || r.RepeatedInodes != 350 {
		t.Fatal(r, err)
	}
	// A newer revision hides the entire summary immediately.
	if _, err = s.db.Exec("UPDATE allocation_revisions SET revision=revision+1"); err != nil {
		t.Fatal(err)
	}
	r, err = s.MeasureDirectory(ctx, "/fixture")
	if err != nil || r.CoverageSource == "cached_reduction" {
		t.Fatal(r, err)
	}
}

func TestScopeCoverageMigrationV7(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			if err := os.WriteFile(filepath.Join(dir, Filename), nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, filepath.Join(dir, Filename), false)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 7; i++ {
				if _, err = s.db.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec(`PRAGMA application_id=0x52594444; PRAGMA user_version=7;
 INSERT INTO roots(path,enabled) VALUES(X'2f66697874757265',1);
 INSERT INTO allocation_revisions(root_id,revision) VALUES(1,4);
 INSERT INTO allocation_cache(root_id,path,revision,ready,phase) VALUES(1,X'2e',4,1,'done')`); err != nil {
				t.Fatal(err)
			}
			if fail {
				if _, err = s.db.Exec("ALTER TABLE allocation_members ADD COLUMN confirmed INTEGER"); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = r.MeasureDirectory(ctx, "/fixture"); err != nil {
				t.Fatal(err)
			}
			r.Close()
			w, err := OpenWriter(ctx, dir)
			if fail {
				if err == nil {
					w.Close()
					t.Fatal("conflicting migration succeeded")
				}
				r, err = OpenReader(ctx, dir)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				var columns int
				if err = r.db.QueryRow("SELECT count(*) FROM pragma_table_info('allocation_cache') WHERE name='coverage'").Scan(&columns); err != nil || columns != 0 {
					t.Fatal(columns, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer w.Close()
				var revision int
				var ready bool
				if err = w.db.QueryRow("SELECT revision,ready FROM allocation_cache").Scan(&revision, &ready); err != nil || revision != -1 || ready {
					t.Fatal(revision, ready, err)
				}
			}
		})
	}
}
