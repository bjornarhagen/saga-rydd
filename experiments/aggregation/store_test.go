package aggregation

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func database(t *testing.T, path string, schema string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL", "PRAGMA cache_size=-4096", schema} {
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func commit(t *testing.T, db *sql.DB, b Batch) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = Commit(context.Background(), tx, b); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func totals(t *testing.T, db *sql.DB) Totals {
	t.Helper()
	r, err := Measure(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestRestartReplacesOnlyInterruptedDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compact.db")
	db := database(t, path, Schema)
	a := File{"dev", "1", 10, 4096}
	b := File{"dev", "2", 20, 8192}
	commit(t, db, Batch{Directory: 1, Generation: 1, Complete: true, Files: []File{a}})
	commit(t, db, Batch{Directory: 2, Generation: 1, Files: []File{a, b}})
	if r := totals(t, db); r.Files != 3 || r.Logical != 40 || r.Allocated != 12288 || r.Complete {
		t.Fatal(r)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = database(t, path, "")
	// A restarted listing no longer contains b. Its old contribution must vanish
	// immediately, while the completed sibling and shared inode remain counted.
	commit(t, db, Batch{Directory: 2, Generation: 2, Complete: true, Files: []File{a}})
	if r := totals(t, db); r.Files != 2 || r.Logical != 20 || r.Allocated != 4096 || !r.Complete {
		t.Fatal(r)
	}
}
func TestAtomicReplayAndMutation(t *testing.T) {
	db := database(t, filepath.Join(t.TempDir(), "compact.db"), Schema)
	a := File{"dev", "1", 10, 4096}
	first := Batch{Directory: 1, Generation: 1, Files: []File{a}}
	commit(t, db, first)
	before := totals(t, db)
	tx, _ := db.Begin()
	if err := Commit(context.Background(), tx, first); err == nil {
		t.Fatal("replayed committed batch")
	}
	tx.Rollback()
	if got := totals(t, db); got != before {
		t.Fatal(got, before)
	}
	// Simulate a crash or failed lease update after aggregate SQL has run.
	next := Batch{Directory: 1, Generation: 1, Ordinal: 1, Complete: true, Files: []File{{"dev", "2", 20, 8192}}}
	tx, _ = db.Begin()
	if err := Commit(context.Background(), tx, next); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if got := totals(t, db); got != before {
		t.Fatal(got, before)
	}
	commit(t, db, next)
	commit(t, db, Batch{Directory: 2, Generation: 1, Complete: true, Files: []File{{"dev", "1", 11, 8192}}})
	if got := totals(t, db); !got.Conflicting || got.Allocated != 16384 {
		t.Fatal(got)
	}
}
func TestBoundsOverflowAndRetirement(t *testing.T) {
	db := database(t, filepath.Join(t.TempDir(), "compact.db"), Schema)
	files := make([]File, BatchLimit)
	for i := range files {
		files[i] = File{"dev", fmt.Sprint(i), 1, 4096}
	}
	commit(t, db, Batch{Directory: 1, Generation: 1, Files: files})
	commit(t, db, Batch{Directory: 1, Generation: 1, Ordinal: 1, Complete: true, Files: []File{{"dev", "last", 1, 4096}}})
	commit(t, db, Batch{Directory: 1, Generation: 2, Complete: true})
	for _, expected := range []int64{128, 1, 0} {
		tx, _ := db.Begin()
		n, err := Collect(context.Background(), tx, 1, 1)
		if err != nil || n != expected {
			t.Fatal(n, err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	for _, b := range []Batch{
		{Directory: 2, Generation: 1, Files: append(files, files[0])},
		{Directory: 2, Generation: 1, Files: []File{{"", "1", 1, 1}}},
		{Directory: 2, Generation: 1, Files: []File{{"dev", "1", math.MaxInt64, 1}, {"dev", "2", 1, 1}}},
	} {
		tx, _ := db.Begin()
		if err := Commit(context.Background(), tx, b); err == nil {
			t.Fatal("accepted invalid batch")
		}
		tx.Rollback()
	}
	if got := totals(t, db); got.Files != 0 || got.Logical != 0 || !got.Complete {
		t.Fatal(got)
	}
}

// The explicit opt-in avoids turning this data-volume experiment into every
// developer's test workload. All metadata is generated; no user files are read.
func TestStorageComparison(t *testing.T) {
	if os.Getenv("RYDD_AGGREGATE_EXPERIMENT") != "1" {
		t.Skip("set RYDD_AGGREGATE_EXPERIMENT=1")
	}
	const directories = 100
	const filesPerDirectory = 1000
	baselinePath := filepath.Join(t.TempDir(), "ordinary.db")
	compactPath := filepath.Join(t.TempDir(), "compact.db")
	baseline := database(t, baselinePath, baselineSchema)
	compact := database(t, compactPath, Schema)
	for d := 0; d < directories; d++ {
		for start := 0; start < filesPerDirectory; start += BatchLimit {
			tx, err := baseline.Begin()
			if err != nil {
				t.Fatal(err)
			}
			b := Batch{Directory: int64(d + 1), Generation: 1, Ordinal: int64(start / BatchLimit), Complete: start+BatchLimit >= filesPerDirectory}
			for i := start; i < min(start+BatchLimit, filesPerDirectory); i++ {
				inode := fmt.Sprint(d*filesPerDirectory + i)
				if i%10 == 0 {
					inode = fmt.Sprint(i)
				} // Shared across directories.
				f := File{"12345678", inode, 1024, 4096}
				b.Files = append(b.Files, f)
				parent := fmt.Sprintf("node_modules/@synthetic/package-%03d/dist", d)
				path := fmt.Sprintf("%s/generated-file-%05d.js", parent, i)
				_, err = tx.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns,skip_reason) VALUES(1,?,?,'file',1024,4096,1700000000000000000,1700000000000000000,?,?,1,1700000000000000000,'')`, []byte(path), []byte(parent), f.Device, f.Inode)
				if err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			commit(t, compact, b)
		}
	}
	got := totals(t, compact)
	var logical, allocated, unique int64
	if err := baseline.QueryRow("SELECT sum(size) FROM entries").Scan(&logical); err != nil {
		t.Fatal(err)
	}
	if err := baseline.QueryRow("SELECT sum(allocated),count(*) FROM (SELECT max(allocated) AS allocated FROM entries GROUP BY device,inode)").Scan(&allocated, &unique); err != nil {
		t.Fatal(err)
	}
	if got.Files != directories*filesPerDirectory || got.Logical != logical || got.Allocated != allocated || got.UniqueInodes != unique || !got.Complete || got.Conflicting {
		t.Fatal(got, logical, allocated, unique)
	}
	for _, db := range []*sql.DB{baseline, compact} {
		if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			t.Fatal(err)
		}
	}
	old, _ := os.Stat(baselinePath)
	new, _ := os.Stat(compactPath)
	t.Logf("synthetic files=%d; baseline entries+indexes=%d bytes; compact totals+inode ledger=%d bytes; ratio=%.3f; distinct inodes=%d; file paths retained in compact store=0", got.Files, old.Size(), new.Size(), float64(new.Size())/float64(old.Size()), got.UniqueInodes)
}

// Mirrors production entries and its three indexes, without unrelated tables.
// Excluding root/directory rows from both sides makes this a file-storage
// comparison, not a whole application database or scanner benchmark.
const baselineSchema = `CREATE TABLE entries (
 id INTEGER PRIMARY KEY,root_id INTEGER NOT NULL,path BLOB NOT NULL,parent BLOB NOT NULL,
 kind TEXT NOT NULL,size INTEGER NOT NULL,allocated INTEGER NOT NULL,
 mtime_ns INTEGER NOT NULL,ctime_ns INTEGER NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,
 generation INTEGER NOT NULL,observed_at_ns INTEGER NOT NULL,skip_reason TEXT NOT NULL,
 UNIQUE(root_id,path));
 CREATE INDEX entries_parent ON entries(root_id,parent);
 CREATE INDEX entries_size ON entries(size) WHERE kind='file';`
