package state

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCargoBuildOutputsNativeReaderCompatibilityOfflineAndNoMutation(t *testing.T) {
	for version := 4; version <= schemaVersion; version++ {
		t.Run(fmt.Sprintf("schema%d", version), func(t *testing.T) {
			ctx := context.Background()
			dir := privateDir(t)
			path := filepath.Join(dir, Filename)
			if err := os.WriteFile(path, nil, 0600); err != nil {
				t.Fatal(err)
			}
			s, err := connect(ctx, path, false)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for i := 0; i < version; i++ {
				if _, err = s.db.Exec(migrations[i].sql); err != nil {
					t.Fatal(err)
				}
				if _, err = s.db.Exec("INSERT INTO schema_migrations VALUES(?,?,0)", i+1, migrations[i].name); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = s.db.Exec(fmt.Sprintf("PRAGMA application_id=0x52594444; PRAGMA user_version=%d", version)); err != nil {
				t.Fatal(err)
			}
			s.schema = version
			// This source path deliberately does not exist. Saved views must not
			// stat it, validate bodies or consult any project/configuration file.
			root := filepath.Join(t.TempDir(), "never-created-source")
			if err = s.SyncRoots(ctx, []string{root}); err != nil {
				t.Fatal(err)
			}
			buildOutputPut(t, s.db, ".", "", "directory", "root", 1, 0, 0)
			buildOutputDirectory(t, s.db, ".", 1)
			addBuildOutputProject(t, s, "project", "debug")
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			r, err := OpenReader(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			report, err := r.CargoBuildOutputs(ctx, "", 90)
			if err != nil || len(report.Findings) != 1 || report.Findings[0].Path != filepath.Join(root, "project", "target") {
				t.Fatal(report, err)
			}
			assertBuildOutputQualified(t, report)
			var gotVersion, ledger int
			if err = r.db.QueryRow("PRAGMA user_version").Scan(&gotVersion); err != nil {
				t.Fatal(err)
			}
			if err = r.db.QueryRow("SELECT count(*) FROM schema_migrations").Scan(&ledger); err != nil {
				t.Fatal(err)
			}
			if gotVersion != version || ledger != version {
				t.Fatal("reader migrated legacy storage", gotVersion, ledger)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("saved-only reader changed database bytes", err)
			}
			if _, err = os.Lstat(root); !os.IsNotExist(err) {
				t.Fatal("report accessed/created source namespace", err)
			}
		})
	}
}

func TestCargoBuildOutputsSingleSnapshotWhileWriterHeld(t *testing.T) {
	writer := newBuildOutputFixture(t)
	addBuildOutputProject(t, writer, "project", "debug")
	ctx := context.Background()
	reader, err := OpenReader(ctx, filepath.Dir(writer.path))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	before, err := reader.CargoBuildOutputs(ctx, "", 90)
	if err != nil || len(before.Findings) != 1 {
		t.Fatal(before, err)
	}
	// WAL permits a genuine concurrent inventory update after the captured
	// findings and measurements. The completed historical view remains bound
	// to its original transaction and must not claim current freshness.
	captured, err := reader.cargoBuildOutputs(ctx, "", 90, buildOutputHooks{beforeCommit: func() {
		if _, e := writer.db.Exec("UPDATE entries SET mtime_ns=9223372036854775807 WHERE path=?", []byte("project/Cargo.lock")); e != nil {
			t.Fatal(e)
		}
	}})
	// Measurement generation time describes this call, not its saved evidence.
	// All marker timestamps, identities, sizes and qualifications stay exact.
	for _, findings := range [][]BuildOutputFinding{before.Findings, captured.Findings} {
		for i := range findings {
			findings[i].Measurement.GeneratedAt = time.Time{}
		}
	}
	if err != nil || len(captured.Findings) != 1 || !reflect.DeepEqual(captured.Findings, before.Findings) {
		t.Fatal("report combined snapshots", captured, err)
	}
	assertBuildOutputQualified(t, captured)
	next, err := reader.CargoBuildOutputs(ctx, "", 90)
	if err != nil || len(next.Findings) != 0 || buildOutputCount(next, "age_not_met") != 1 {
		t.Fatal("later view ignored committed update", next, err)
	}
	assertBuildOutputQualified(t, next)
}
