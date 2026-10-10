//go:build darwin || linux

package storeexport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func replaceGeneratedFile(t *testing.T, path string, body []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := f.Write(body)
	closeErr := f.Close()
	if n != len(body) || writeErr != nil || closeErr != nil {
		t.Fatal(n, writeErr, closeErr)
	}
}

func rewriteGeneratedManifest(t *testing.T, artifact string, change func(*manifest)) {
	t.Helper()
	path := filepath.Join(artifact, "manifest.json")
	body, err := os.ReadFile(path)
	if err != nil || len(body) > maxManifestBytes {
		t.Fatal(err)
	}
	m, err := parseManifest(body)
	if err != nil {
		t.Fatal(err)
	}
	change(&m)
	m.ExportID, err = manifestID(m)
	if err != nil {
		t.Fatal(err)
	}
	body, err = encodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	replaceGeneratedFile(t, path, body)
}

func TestOfflineVerifierRefusesTamperingAndRetainsOnlyItsScratch(t *testing.T) {
	for _, kind := range []string{"hash", "coherent-corrupt-database", "coherent-wal-header", "coherent-profile-token", "unknown-member", "unsafe-mode", "manifest-authority"} {
		t.Run(kind, func(t *testing.T) {
			f := fixtureStore(t, 6, 4096)
			artifact, _ := exportFixture(t, f)
			dataPath := filepath.Join(artifact, "plans.sqlite3")
			manifestPath := filepath.Join(artifact, "manifest.json")
			switch kind {
			case "hash", "coherent-corrupt-database", "coherent-wal-header":
				body, err := os.ReadFile(dataPath)
				if err != nil || len(body) > 8<<20 {
					t.Fatal(err)
				}
				if kind == "coherent-wal-header" {
					body[18] = 2
					body[19] = 2
				} else {
					body[0] ^= 0xff
				}
				replaceGeneratedFile(t, dataPath, body)
				if kind != "hash" {
					sum := sha256.Sum256(body)
					rewriteGeneratedManifest(t, artifact, func(m *manifest) { m.DataSHA256 = hex.EncodeToString(sum[:]) })
				}
			case "coherent-profile-token":
				rewriteGeneratedManifest(t, artifact, func(m *manifest) {
					m.Profile.StoreToken = "2222222222222222222222222222222222222222222222222222222222222222"
				})
			case "unknown-member":
				privateBytes(t, filepath.Join(artifact, "unknown"), []byte("must remain"))
			case "unsafe-mode":
				if err := os.Chmod(dataPath, 0644); err != nil {
					t.Fatal(err)
				}
			case "manifest-authority":
				body, err := os.ReadFile(manifestPath)
				if err != nil {
					t.Fatal(err)
				}
				body = bytes.Replace(body, []byte(`"cleanup_authority":false`), []byte(`"cleanup_authority":true`), 1)
				replaceGeneratedFile(t, manifestPath, body)
			}
			beforeData, err := os.ReadFile(dataPath)
			if err != nil {
				t.Fatal(err)
			}
			beforeManifest, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			parent := privateTemp(t)
			result, err := VerifyPlanStoreExport(context.Background(), artifact, parent)
			if err == nil || result.Verified || result.CleanupAuthority || result.RestoreAvailable || result.CurrentPermissionEvaluated || result.RecordSemanticsChecked {
				t.Fatal(result, err)
			}
			afterData, e := os.ReadFile(dataPath)
			if e != nil || !bytes.Equal(beforeData, afterData) {
				t.Fatal("artifact database changed", e)
			}
			afterManifest, e := os.ReadFile(manifestPath)
			if e != nil || !bytes.Equal(beforeManifest, afterManifest) {
				t.Fatal("artifact manifest changed", e)
			}
			entries, e := os.ReadDir(parent)
			if e != nil {
				t.Fatal(e)
			}
			late := kind == "coherent-corrupt-database" || kind == "coherent-wal-header" || kind == "coherent-profile-token"
			if late {
				if !result.ScratchRetained || result.Scratch == "" || len(entries) != 1 {
					t.Fatal("late refusal lost its scratch path", result, len(entries))
				}
				if _, e = os.Stat(filepath.Join(result.Scratch, "plans.sqlite3")); e != nil {
					t.Fatal("failed scratch removed", e)
				}
				if kind == "coherent-wal-header" {
					members, e := os.ReadDir(result.Scratch)
					if !errors.Is(err, ErrProfile) || !result.EngineClosureKnown || e != nil || len(members) != 1 || members[0].Name() != "plans.sqlite3" {
						t.Fatal("WAL header opened an engine or changed scratch scope", result, members, err, e)
					}
				}
			} else if result.Scratch != "" || result.ScratchRetained || len(entries) != 0 {
				t.Fatal("early refusal created scratch", result, len(entries))
			}
			if kind == "unknown-member" {
				if body, e := os.ReadFile(filepath.Join(artifact, "unknown")); e != nil || string(body) != "must remain" {
					t.Fatal("unknown artifact member changed", e)
				}
			}
		})
	}
}

func TestExportForeignKeyIntegrityRefusalRetainsItsCandidate(t *testing.T) {
	f := fixtureStore(t, 5, 4096)
	// The fixture has a deliberate structurally invalid reference. The export
	// must reject it in the copied scratch even when its schema census matches.
	if _, err := f.db.Exec("INSERT INTO finding_dismissal_undos(dismissal_id,id,payload) VALUES(?,?,?)", "missing-dismissal", "orphan-undo", []byte("generated orphan")); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(privateTemp(t), "refused")
	result, err := ExportPlanStore(context.Background(), f.base, destination)
	if !errors.Is(err, ErrBounds) || !strings.HasPrefix(err.Error(), "scratch integrity:") || result.Published || result.Verified || !result.CandidateRetained || result.Candidate == "" {
		t.Fatal(result, err)
	}
	if _, e := os.Lstat(destination); !os.IsNotExist(e) {
		t.Fatal("refused destination published", e)
	}
	if _, e := os.Stat(filepath.Join(result.Candidate, "plans.sqlite3")); e != nil {
		t.Fatal("refused candidate removed", e)
	}
	if _, e := os.Stat(filepath.Join(result.Candidate, ".verify", "plans.sqlite3")); e != nil {
		t.Fatal("failed verification scratch removed", e)
	}
}

func TestExistingSourceUnknownProfilesAreNotMigrated(t *testing.T) {
	for _, kind := range []string{"application", "future-schema", "unexpected-table"} {
		t.Run(kind, func(t *testing.T) {
			f := fixtureStore(t, 1, 4096)
			query := "PRAGMA application_id=123"
			if kind == "future-schema" {
				query = "PRAGMA user_version=7"
			}
			if kind == "unexpected-table" {
				query = "CREATE TABLE unknown_object(id INTEGER)"
			}
			if _, err := f.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(privateTemp(t), "refused")
			result, err := ExportPlanStore(context.Background(), f.base, destination)
			if err == nil || result.Published || result.Verified {
				t.Fatal(result, err)
			}
			var version, app int64
			if e := f.db.QueryRow("PRAGMA user_version").Scan(&version); e != nil {
				t.Fatal(e)
			}
			if e := f.db.QueryRow("PRAGMA application_id").Scan(&app); e != nil {
				t.Fatal(e)
			}
			if (kind == "future-schema" && version != 7) || (kind != "future-schema" && version != 1) || (kind == "application" && app != 123) {
				t.Fatal("source profile migrated", version, app)
			}
		})
	}
}
