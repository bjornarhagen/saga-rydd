package plans

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExportProfileMatchesOriginalSchemaHistory(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, release, err := openWithMigration(ctx, base, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Use the existing initializer and original migration constants. This test
	// does not create its schema from the new export helper's duplicated DDL.
	migrations := []string{"", reviewSchema, observationSchema, journalSchema, dismissalSchema, admissionSchema}
	for version := int64(1); version <= 6; version++ {
		if version > 1 {
			if _, err = db.ExecContext(ctx, migrations[version-1]); err != nil {
				t.Fatal(version, err)
			}
		}
		app, objects, ok := ExportProfile(version)
		if !ok || app != applicationID {
			t.Fatal(version, app, ok)
		}
		var actualVersion int64
		if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&actualVersion); err != nil || actualVersion != version {
			t.Fatal(actualVersion, err)
		}
		rows, err := db.QueryContext(ctx, "SELECT name,type,tbl_name,sql FROM sqlite_schema ORDER BY name LIMIT 65")
		if err != nil {
			t.Fatal(err)
		}
		i := 0
		for rows.Next() {
			if i >= 64 || i >= len(objects) {
				rows.Close()
				t.Fatal("unexpected schema object")
			}
			var name, kind, table string
			var ddl sql.NullString
			if err = rows.Scan(&name, &kind, &table, &ddl); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			want := objects[i]
			if name != want.Name || kind != want.Type || table != want.Table || ddl.Valid != (want.SQL != "") || strings.TrimSpace(ddl.String) != strings.TrimSpace(want.SQL) {
				rows.Close()
				t.Fatalf("schema %d row %d got=%s/%s/%s/%q want=%+v", version, i, name, kind, table, ddl.String, want)
			}
			i++
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil || closeErr != nil || i != len(objects) {
			t.Fatal(version, i, len(objects), rowErr, closeErr)
		}
		objects[0].SQL = "generated mutation"
		_, fresh, ok := ExportProfile(version)
		if !ok || fresh[0].SQL == "generated mutation" {
			t.Fatal("caller mutation changed future profile")
		}
	}
	for _, version := range []int64{-1, 0, 7} {
		if _, _, ok := ExportProfile(version); ok {
			t.Fatal("unknown schema accepted", version)
		}
	}
}
