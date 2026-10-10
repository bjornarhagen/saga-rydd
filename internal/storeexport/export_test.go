//go:build darwin || linux

package storeexport

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"golang.org/x/sys/unix"
)

// Canonicalization belongs only to this generated fixture setup. The library
// continues to refuse symlink components in every supplied path.
func privateTemp(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

type plansFixture struct {
	base, path string
	db         *sql.DB
	version    int64
}

func fixtureStore(t *testing.T, version, pageSize int64) *plansFixture {
	t.Helper()
	base := privateTemp(t)
	dir := filepath.Join(base, "plans")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"writer.lock", "plans.sqlite3"} {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "plans.sqlite3")
	db, err := sql.Open("sqlite", engineURI(path, true))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	f := &plansFixture{base, path, db, version}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err = db.ExecContext(ctx, fmt.Sprintf("PRAGMA page_size=%d", pageSize)); err != nil {
		t.Fatal(err)
	}
	app, objects, ok := plans.ExportProfile(version)
	if !ok {
		t.Fatal("invalid generated schema")
	}
	// This constructs a structural fixture, not a record-semantic oracle. The
	// plans package separately compares this profile with its original DDL.
	for _, kind := range []string{"table", "index", "trigger"} {
		for _, object := range objects {
			if object.Type == kind && object.SQL != "" {
				if _, err = db.ExecContext(ctx, object.SQL); err != nil {
					t.Fatal(object.Name, err)
				}
			}
		}
	}
	for _, query := range []string{fmt.Sprintf("PRAGMA application_id=%d", app), fmt.Sprintf("PRAGMA user_version=%d", version)} {
		if _, err = db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	payload := []byte("generated historical payload; no execution authority")
	inserts := []struct {
		minimum int64
		query   string
		args    []any
	}{
		{1, "INSERT INTO plans(id,payload) VALUES(?,?)", []any{"legacy-plan", payload}},
		{2, "INSERT INTO plan_store_identity(singleton,token) VALUES(1,?)", []any{strings.Repeat("0", 64)}},
		{2, "INSERT INTO review_approvals(plan_id,id,payload) VALUES(?,?,?)", []any{"legacy-plan", "approval", payload}},
		{2, "INSERT INTO review_revocations(plan_id,id,payload) VALUES(?,?,?)", []any{"legacy-plan", "revocation", payload}},
		{3, "INSERT INTO observations(id,plan_id,payload) VALUES(?,?,?)", []any{"observation", "legacy-plan", payload}},
		{4, "INSERT INTO journal_intents(id,request_key,kind,plan_id,observation_id,finding_id,original_intent_id,payload) VALUES(?,?,?,?,?,?,?,?)", []any{"intent", "request", "quarantine_preparation", "legacy-plan", "observation", "finding", "", payload}},
		{4, "INSERT INTO journal_events(intent_id,sequence,id,request_key,payload) VALUES(?,1,?,?,?)", []any{"intent", "event", "event-request", payload}},
		{4, "INSERT INTO journal_heads(intent_id,sequence,event_id) VALUES(?,1,?)", []any{"intent", "event"}},
		{5, "INSERT INTO dismissal_store_identity(singleton,token) VALUES(1,?)", []any{strings.Repeat("1", 64)}},
		{5, "INSERT INTO finding_dismissals(id,request_id,payload) VALUES(?,?,?)", []any{"dismissal", "dismiss-request", payload}},
		{5, "INSERT INTO finding_dismissal_undos(dismissal_id,id,payload) VALUES(?,?,?)", []any{"dismissal", "undo", payload}},
	}
	for _, insert := range inserts {
		if version >= insert.minimum {
			if _, err = db.ExecContext(ctx, insert.query, insert.args...); err != nil {
				t.Fatal(err)
			}
		}
	}
	var mode string
	if err = db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("mode=%s err=%v", mode, err)
	}
	if _, err = db.ExecContext(ctx, "PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatal(err)
	}
	// Keep the fixture pool alive so the read-only export is not the last WAL
	// connection. Only metadata is sampled while that source engine is open.
	if _, err = db.ExecContext(ctx, "INSERT INTO plans(id,payload) VALUES(?,?)", "wal-committed", payload); err != nil {
		t.Fatal(err)
	}
	return f
}

func historyRows(t *testing.T, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, version int64) map[string][][]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tables := []struct {
		minimum int64
		name    string
	}{{1, "plans"}, {2, "plan_store_identity"}, {2, "review_approvals"}, {2, "review_revocations"}, {3, "observations"}, {4, "journal_intents"}, {4, "journal_events"}, {4, "journal_heads"}, {5, "dismissal_store_identity"}, {5, "finding_dismissals"}, {5, "finding_dismissal_undos"}}
	result := map[string][][]any{}
	for _, table := range tables {
		if version >= table.minimum {
			rows, err := q.QueryContext(ctx, "SELECT * FROM "+table.name+" ORDER BY rowid LIMIT 33")
			if err != nil {
				t.Fatal(err)
			}
			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			var records [][]any
			for rows.Next() {
				if len(records) >= 32 || len(columns) > 8 {
					rows.Close()
					t.Fatal("generated history oracle exceeded bounds")
				}
				record := make([]any, len(columns))
				ptrs := make([]any, len(columns))
				for i := range record {
					ptrs[i] = &record[i]
				}
				if err = rows.Scan(ptrs...); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				for i, value := range record {
					if b, ok := value.([]byte); ok {
						if len(b) > 4096 {
							rows.Close()
							t.Fatal("generated payload oversized")
						}
						record[i] = append([]byte(nil), b...)
					}
				}
				records = append(records, record)
			}
			if err = errors.Join(rows.Err(), rows.Close()); err != nil {
				t.Fatal(err)
			}
			result[table.name] = records
		}
	}
	return result
}

func artifactHistory(t *testing.T, artifact string, version int64) map[string][][]any {
	t.Helper()
	g := newGuard(context.Background())
	input, err := openDir(artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := input.close(); err != nil {
			t.Error(err)
		}
	}()
	_, _, pin, err := streamFile(input, "plans.sqlite3", nil, maxDatabaseBytes, g, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := openDir(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := parent.close(); err != nil {
			t.Error(err)
		}
	}()
	scratch, err := createStage(parent, g)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := scratch.close(); err != nil {
			t.Error(err)
		}
	}()
	_, scratchPin, err := copyFile(input, "plans.sqlite3", pin, scratch, g)
	if err != nil {
		t.Fatal(err)
	}
	e := engineEnv{guard: g}
	db, conn, err := openEngine(filepath.Join(scratch.path, "plans.sqlite3"), e)
	if err != nil {
		t.Fatal(err)
	}
	rows := historyRows(t, conn, version)
	closed, err := consumeConnection(conn, db, func(raw any) error { return readOnly(raw, e) })
	if !closed || err != nil {
		t.Fatal(closed, err)
	}
	if err = removeScratch(scratch, scratchPin, g); err != nil {
		t.Fatal(err)
	}
	return rows
}

// diagnoseFailedFixture reads only this generated operation's closed,
// retained candidate and fixed scratch. It removes nothing and claims no
// current ownership or repair. Names are bounded to five per selected folder;
// one regular-file read requests only the first twenty header bytes.
func diagnoseFailedFixture(t *testing.T, destination string, result ExportResult) {
	t.Helper()
	name := filepath.Base(result.Candidate)
	token := strings.TrimPrefix(name, ".rydd-export-")
	if result.Published || !result.CandidateRetained || !result.CandidateIdentityObserved || !result.EngineClosureKnown || filepath.Dir(result.Candidate) != filepath.Dir(destination) || !strings.HasPrefix(name, ".rydd-export-") || len(token) != 16 || strings.Trim(token, "0123456789abcdef") != "" {
		t.Log("diagnostic retained generated candidate/body scope unconfirmed; no directory or header read")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g := newGuard(ctx)
	candidate, err := openDir(result.Candidate)
	if err != nil {
		t.Logf("diagnostic candidate open error=%v", err)
		return
	}
	defer func() {
		if err := candidate.close(); err != nil {
			t.Logf("diagnostic candidate checked close error=%v", err)
		}
	}()
	names, err := candidate.names(g, 5)
	for _, name := range names {
		if len(name) > 256 {
			t.Log("diagnostic candidate name exceeds256 bytes; details omitted")
			return
		}
	}
	t.Logf("diagnostic candidate members=%q listing_error=%v", names, err)
	scratch, openErr := candidate.child(".verify")
	if openErr != nil {
		t.Logf("diagnostic scratch open error=%v", openErr)
	} else {
		defer func() {
			if err := scratch.close(); err != nil {
				t.Logf("diagnostic scratch checked close error=%v", err)
			}
		}()
		names, err := scratch.names(g, 5)
		for _, name := range names {
			if len(name) > 256 {
				t.Log("diagnostic scratch name exceeds256 bytes; details omitted")
				return
			}
		}
		t.Logf("diagnostic scratch members=%q listing_error=%v", names, err)
	}
	if err = g.check(); err != nil {
		t.Logf("diagnostic header guard error=%v", err)
		return
	}
	f, pin, err := openFile(candidate, "plans.sqlite3", nil)
	if err != nil {
		t.Logf("diagnostic candidate header open error=%v", err)
		return
	}
	if err = g.check(); err != nil {
		t.Logf("diagnostic header guard error=%v close_error=%v", err, f.Close())
		return
	}
	header := make([]byte, 20)
	n, readErr := f.Read(header)
	guardErr := g.check()
	var actual unix.Stat_t
	statErr := unix.Fstat(int(f.Fd()), &actual)
	named, namedErr := candidate.stat("plans.sqlite3")
	closeErr := f.Close()
	guardErr = errors.Join(guardErr, g.check())
	if n != 20 || readErr != nil || guardErr != nil || statErr != nil || namedErr != nil || closeErr != nil || !sameFile(pin, actual) || !sameFile(actual, named) {
		t.Logf("diagnostic candidate header unavailable: read_bytes=%d read_error=%v guard_error=%v stat_error=%v named_error=%v close_error=%v", n, readErr, guardErr, statErr, namedErr, closeErr)
		return
	}
	t.Logf("diagnostic candidate header bytes18/19=%d/%d read_bytes=%d held_named_metadata_matched=true", header[18], header[19], n)
}

func exportFixture(t *testing.T, f *plansFixture) (string, ExportResult) {
	t.Helper()
	destination := filepath.Join(privateTemp(t), "export")
	result, err := ExportPlanStore(context.Background(), f.base, destination)
	if err != nil {
		diagnoseFailedFixture(t, destination, result)
		t.Fatalf("export=%+v err=%v", result, err)
	}
	if !result.Published || !result.Verified || !result.SyncCompleted || !result.EngineClosureKnown || result.CandidateRetained {
		t.Fatalf("incomplete result %+v", result)
	}
	if result.FullInstanceBackup || result.RestoreAvailable || result.CleanupAuthority || result.CurrentPermissionEvaluated || result.RecordSemanticsChecked || result.SourceBytesEqual != nil || result.SourceStorageEffectsAuthenticated {
		t.Fatal("authority or source-byte claim invented")
	}
	return destination, result
}

func TestCommittedWALSnapshotPreservesEverySupportedHistoricalTable(t *testing.T) {
	for version := int64(1); version <= 6; version++ {
		t.Run(fmt.Sprintf("schema-%d", version), func(t *testing.T) {
			f := fixtureStore(t, version, 4096)
			want := historyRows(t, f.db, version)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := f.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.ExecContext(ctx, "INSERT INTO plans(id,payload) VALUES(?,?)", "uncommitted", []byte("must not be exported")); err != nil {
				t.Fatal(err)
			}
			source, err := openDir(filepath.Dir(f.path))
			if err != nil {
				t.Fatal(err)
			}
			before, err := sourceMetadata(source)
			if err != nil {
				source.close()
				t.Fatal(err)
			}
			if before.wal == nil || before.wal.Size <= 0 {
				source.close()
				t.Fatal("fixture has no committed WAL")
			}
			var shmBefore unix.Stat_t
			if err = unix.Lstat(f.path+"-shm", &shmBefore); err != nil {
				source.close()
				t.Fatal(err)
			}
			destination, _ := exportFixture(t, f)
			after, err := sourceMetadata(source)
			closeErr := source.close()
			if err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			var shmAfter unix.Stat_t
			if err = unix.Lstat(f.path+"-shm", &shmAfter); err != nil {
				t.Fatal(err)
			}
			t.Logf("metadata only: main same=%v WAL same=%v SHM stamp same=%v; SHM/storage effects and source-byte equality are not authenticated", sameFile(before.main, after.main), sameFile(*before.wal, *after.wal), sameFile(shmBefore, shmAfter))
			if !unchangedSource(before, after) {
				t.Fatal("source main/WAL metadata changed")
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if got := artifactHistory(t, destination, version); !reflect.DeepEqual(got, want) {
				t.Fatalf("historical rows differ: got=%v want=%v", got, want)
			}
			entries, err := os.ReadDir(destination)
			if err != nil || len(entries) != 2 {
				t.Fatal(entries, err)
			}
			for _, entry := range entries {
				if entry.Name() != "plans.sqlite3" && entry.Name() != "manifest.json" {
					t.Fatal("sidecar published", entry.Name())
				}
			}
			file, err := os.Open(filepath.Join(destination, "plans.sqlite3"))
			if err != nil {
				t.Fatal(err)
			}
			header := make([]byte, 100)
			n, readErr := file.Read(header)
			fileErr := file.Close()
			if n != 100 || readErr != nil || fileErr != nil {
				t.Fatal(n, readErr, fileErr)
			}
			if !bytes.Equal(header[:16], []byte("SQLite format 3\x00")) || header[18] != 1 || header[19] != 1 {
				t.Fatalf("standalone destination header versions=%d/%d", header[18], header[19])
			}
			checked, err := VerifyPlanStoreExport(context.Background(), destination, privateTemp(t))
			if err != nil || !checked.Verified || !checked.EngineClosureKnown || checked.ScratchRetained {
				t.Fatal(checked, err)
			}
		})
	}
}

func TestPositivePageSizesAndNoReplacePublication(t *testing.T) {
	for _, size := range []int64{512, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			f := fixtureStore(t, 6, size)
			want := historyRows(t, f.db, 6)
			destination, _ := exportFixture(t, f)
			if got := artifactHistory(t, destination, 6); !reflect.DeepEqual(got, want) {
				t.Fatal("history lost")
			}
			original, err := os.ReadFile(filepath.Join(destination, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			refused, err := ExportPlanStore(context.Background(), f.base, destination)
			if !errors.Is(err, unix.EEXIST) || refused.Published || refused.Candidate != "" {
				t.Fatal(refused, err)
			}
			after, err := os.ReadFile(filepath.Join(destination, "manifest.json"))
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("existing destination changed", err)
			}
		})
	}
}
