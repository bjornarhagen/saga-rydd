//go:build darwin || linux

package storeexport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func privateBytes(t *testing.T, path string, body []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := f.Write(body)
	closeErr := f.Close()
	if n != len(body) || writeErr != nil || closeErr != nil {
		t.Fatal(n, writeErr, closeErr)
	}
}

func heldTemp(t *testing.T) *heldDir {
	t.Helper()
	d, err := openDir(privateTemp(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.close(); err != nil {
			t.Error(err)
		}
	})
	return d
}

func TestOwnedDirectoriesRefuseSymlinksAndReplacement(t *testing.T) {
	parent := heldTemp(t)
	childPath := filepath.Join(parent.path, "owned")
	if err := os.Mkdir(childPath, 0700); err != nil {
		t.Fatal(err)
	}
	child, err := parent.child("owned")
	if err != nil {
		t.Fatal(err)
	}
	defer child.close()
	if err = os.Symlink(childPath, filepath.Join(parent.path, "alias")); err != nil {
		t.Fatal(err)
	}
	if d, err := openDir(filepath.Join(parent.path, "alias")); err == nil {
		d.close()
		t.Fatal("symlink directory admitted")
	}
	if err = os.Rename(childPath, filepath.Join(parent.path, "retained-original")); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(childPath, 0700); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(child.check(), ErrChanged) {
		t.Fatal("held directory replacement admitted")
	}
	if _, err = os.Stat(filepath.Join(parent.path, "retained-original")); err != nil {
		t.Fatal("original directory removed", err)
	}
}

func TestExclusiveStageHasFiniteParentCapacity(t *testing.T) {
	for _, count := range []int{255, 256} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			parent := heldTemp(t)
			for i := 0; i < count; i++ {
				privateBytes(t, filepath.Join(parent.path, fmt.Sprintf("generated-%03d", i)), nil)
			}
			stage, err := createStage(parent, newGuard(context.Background()))
			entries, readErr := os.ReadDir(parent.path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if count == 255 {
				if err != nil || stage == nil || len(entries) != 256 {
					t.Fatal(stage, err, len(entries))
				}
				defer stage.close()
			} else if !errors.Is(err, ErrBounds) || stage != nil || len(entries) != 256 {
				t.Fatal(stage, err, len(entries))
			}
		})
	}
}

func TestStageAttemptRefusalRetainsInspectionPathWithoutIdentity(t *testing.T) {
	for _, kind := range []string{"opener-refusal", "nil-stage", "private-mode", "closed-handle", "held-stage"} {
		t.Run(kind, func(t *testing.T) {
			parent := heldTemp(t)
			calls := 0
			inspectionPath := ""
			cause := errors.New("generated post-mkdir refusal")
			opener := func(d *heldDir, name string) (*heldDir, error) {
				calls++
				inspectionPath = filepath.Join(d.path, name)
				// This generated witness observes only its own freshly created child.
				// Production failure metadata does not make this observation.
				if st, err := os.Lstat(inspectionPath); err != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
					t.Fatal("mkdir not completed before opener", st, err)
				}
				if kind == "opener-refusal" {
					return nil, errors.Join(ErrChanged, cause)
				}
				if kind == "nil-stage" {
					return nil, nil
				}
				if kind == "private-mode" {
					if err := os.Chmod(inspectionPath, 0755); err != nil {
						t.Fatal(err)
					}
				}
				stage, err := d.child(name)
				if err != nil {
					return nil, err
				}
				if kind == "closed-handle" {
					if err = stage.file.Close(); err != nil {
						t.Fatal(err)
					}
				}
				return stage, nil
			}
			stage, err := createStageWithOpener(parent, newGuard(context.Background()), opener)
			exported := ExportResult{Contract: Contract}
			verified := VerificationResult{Contract: Contract}
			// These are the same result-routing functions called immediately after
			// createStage by ExportPlanStore and VerifyPlanStoreExport.
			retainCandidateStage(&exported, stage, err)
			retainScratchStage(&verified, stage, err)
			if calls != 1 || inspectionPath == "" || exported.Candidate != inspectionPath || verified.Scratch != inspectionPath || !exported.CandidateRetained || !verified.ScratchRetained {
				t.Fatal("created inspection path lost", calls, exported, verified, err)
			}
			if kind == "held-stage" {
				if err != nil || stage == nil || !exported.CandidateIdentityObserved || !verified.ScratchIdentityObserved {
					t.Fatal("successful held stage observation lost", stage, err, exported, verified)
				}
				if err = stage.close(); err != nil {
					t.Fatal(err)
				}
			} else {
				var attempt *stageAttemptError
				if err == nil || stage != nil || !errors.As(err, &attempt) || attempt.inspectionPath != inspectionPath || exported.CandidateIdentityObserved || verified.ScratchIdentityObserved {
					t.Fatal("unproved stage admitted or omitted", stage, err, exported, verified)
				}
				if kind == "opener-refusal" && (!errors.Is(err, cause) || !errors.Is(err, ErrChanged)) {
					t.Fatal("opener cause lost", err)
				}
				if kind == "nil-stage" && !errors.Is(err, ErrChanged) {
					t.Fatal("nil stage not refused", err)
				}
				if kind == "private-mode" && !errors.Is(err, ErrProfile) {
					t.Fatal("private mode not refused", err)
				}
				if kind == "closed-handle" && !errors.Is(err, os.ErrClosed) {
					t.Fatal("checked close failure lost", err)
				}
				for _, item := range []struct {
					value any
					field string
				}{{exported, "candidate_identity_observed"}, {verified, "scratch_identity_observed"}} {
					body, e := json.Marshal(item.value)
					if e != nil || !strings.Contains(string(body), `"`+item.field+`":false`) {
						t.Fatal("explicit false identity metadata missing", string(body), e)
					}
				}
			}
			if exported.Published || exported.Verified || exported.EngineClosureKnown || exported.CleanupAuthority || verified.Verified || verified.EngineClosureKnown || verified.CleanupAuthority {
				t.Fatal("stage metadata granted engine/publication/cleanup result", exported, verified)
			}
			entries, readErr := os.ReadDir(parent.path)
			if readErr != nil || len(entries) != 1 || filepath.Join(parent.path, entries[0].Name()) != inspectionPath {
				t.Fatal("unproved name removed, replaced or reused", entries, readErr)
			}
			// t.TempDir may remove this generated fixture after the witness. The
			// production path remains inspection-only and is never fallback-unlinked.
		})
	}
	parent := heldTemp(t)
	stage, err := createStageWithOpener(parent, newGuard(context.Background()), nil)
	var attempt *stageAttemptError
	exported := ExportResult{}
	verified := VerificationResult{}
	retainCandidateStage(&exported, stage, err)
	retainScratchStage(&verified, stage, err)
	entries, readErr := os.ReadDir(parent.path)
	if !errors.Is(err, ErrProfile) || stage != nil || errors.As(err, &attempt) || exported.Candidate != "" || verified.Scratch != "" || exported.CandidateRetained || verified.ScratchRetained || len(entries) != 0 || readErr != nil {
		t.Fatal("pre-mkdir refusal invented retained stage", stage, err, exported, verified, entries, readErr)
	}
}

func TestScratchCleanupRetainsUnprovedMembersAndIdentities(t *testing.T) {
	for _, kind := range []string{"exact", "unknown-member", "replacement", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			parent := heldTemp(t)
			g := newGuard(context.Background())
			scratch, err := createStage(parent, g)
			if err != nil {
				t.Fatal(err)
			}
			defer scratch.close()
			path := filepath.Join(scratch.path, "plans.sqlite3")
			privateBytes(t, path, []byte("generated exact copy"))
			pin, err := scratch.stat("plans.sqlite3")
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown-member":
				privateBytes(t, filepath.Join(scratch.path, "unknown"), []byte("retain"))
			case "replacement":
				if err = os.Rename(path, filepath.Join(parent.path, "retained-original")); err != nil {
					t.Fatal(err)
				}
				privateBytes(t, path, []byte("replacement"))
			case "hardlink":
				if err = os.Link(path, filepath.Join(parent.path, "linked-copy")); err != nil {
					t.Fatal(err)
				}
			}
			err = removeScratch(scratch, pin, g)
			if kind == "exact" {
				if err != nil {
					t.Fatal(err)
				}
				if _, e := os.Lstat(scratch.path); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("empty owned scratch not removed", e)
				}
			} else {
				if err == nil {
					t.Fatal("unproved cleanup admitted")
				}
				if _, e := os.Lstat(path); e != nil {
					t.Fatal("unproved copied member removed", e)
				}
				if _, e := os.Lstat(scratch.path); e != nil {
					t.Fatal("unproved child removed", e)
				}
				if kind == "unknown-member" {
					if b, e := os.ReadFile(filepath.Join(scratch.path, "unknown")); e != nil || string(b) != "retain" {
						t.Fatal("unknown member changed", e)
					}
				}
			}
		})
	}
}

func TestCandidateStreamUsesAdmittedLengthAndEOFIdentity(t *testing.T) {
	for _, kind := range []string{"rollback", "wal", "mixed", "signature", "short", "mode", "changed-after-read"} {
		t.Run("standalone-header-"+kind, func(t *testing.T) {
			d := heldTemp(t)
			path := filepath.Join(d.path, "plans.sqlite3")
			body := make([]byte, 20)
			copy(body, []byte("SQLite format 3\x00"))
			body[18] = 1
			body[19] = 1
			switch kind {
			case "wal":
				body[18] = 2
				body[19] = 2
			case "mixed":
				body[19] = 2
			case "signature":
				body[0] ^= 0xff
			case "short":
				body = body[:19]
			}
			privateBytes(t, path, body)
			if kind == "mode" {
				if err := os.Chmod(path, 0400); err != nil {
					t.Fatal(err)
				}
			}
			pin, err := d.stat("plans.sqlite3")
			if err != nil {
				t.Fatal(err)
			}
			g := newGuard(context.Background())
			changed := false
			if kind == "changed-after-read" {
				calls := 0
				wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				g = guardWithClocks(context.Background(), func() time.Time {
					calls++
					if calls == 4 {
						if err := os.Chmod(path, 0400); err != nil {
							t.Fatal(err)
						}
						changed = true
					}
					return wall
				}, func() time.Duration { return 0 })
			}
			err = rollbackHeader(d, pin, g)
			if kind == "rollback" {
				if err != nil {
					t.Fatal("standalone rollback header refused", err)
				}
			} else if err == nil {
				t.Fatal("nonstandalone header accepted", kind)
			}
			if kind == "changed-after-read" && (!changed || !errors.Is(err, ErrChanged)) {
				t.Fatal("held/named post-read metadata change accepted", changed, err)
			}
			retained, e := os.ReadFile(path)
			if e != nil || string(retained) != string(body) {
				t.Fatal("header gate modified output", e)
			}
			entries, e := os.ReadDir(d.path)
			if e != nil || len(entries) != 1 || entries[0].Name() != "plans.sqlite3" {
				t.Fatal("header gate removed/replaced or created members", entries, e)
			}
		})
	}
	for _, kind := range []string{"exact", "too-large", "growth", "hardlink", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			d := heldTemp(t)
			path := filepath.Join(d.path, "candidate")
			body := []byte("generated fixed candidate")
			privateBytes(t, path, body)
			g := newGuard(context.Background())
			maximum := int64(len(body))
			switch kind {
			case "too-large":
				maximum--
			case "hardlink":
				if err := os.Link(path, filepath.Join(d.path, "alias")); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(path, filepath.Join(d.path, "original")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(d.path, "original"), path); err != nil {
					t.Fatal(err)
				}
			case "growth":
				calls := 0
				wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				g = guardWithClocks(context.Background(), func() time.Time {
					calls++
					if calls == 4 {
						f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
						if e != nil {
							t.Fatal(e)
						}
						n, e := f.Write([]byte("x"))
						c := f.Close()
						if n != 1 || e != nil || c != nil {
							t.Fatal(n, e, c)
						}
					}
					return wall
				}, func() time.Duration { return 0 })
			}
			digest, got, _, err := streamFile(d, "candidate", nil, maximum, g, nil, true)
			if kind == "exact" {
				sum := sha256.Sum256(body)
				if err != nil || string(got) != string(body) || digest != hex.EncodeToString(sum[:]) {
					t.Fatal(digest, string(got), err)
				}
			} else {
				if err == nil || digest != "" {
					t.Fatal("invalid stream accepted", digest, err)
				}
				if kind == "too-large" && (!errors.Is(err, ErrBounds) || len(got) != 0) {
					t.Fatal("body allocated before size refusal", len(got), err)
				}
				if kind == "growth" && !errors.Is(err, ErrChanged) {
					t.Fatal("EOF growth was not refused", err)
				}
			}
		})
	}
	// Generated created-output witnesses retain this existing parent selection.
	// Clock callbacks alter only known disposable outputs after exclusive create;
	// they are not a public hook or evidence of a production incident.
	for _, operation := range []string{"write-manifest", "copy-scratch"} {
		for _, mode := range []os.FileMode{0600, 0400, 0700} {
			t.Run(fmt.Sprintf("%s-created-mode-%04o", operation, mode), func(t *testing.T) {
				from, to := heldTemp(t), heldTemp(t)
				body := []byte("generated fixed output bytes")
				privateBytes(t, filepath.Join(from.path, "source"), body)
				sourcePin, err := from.stat("source")
				if err != nil {
					t.Fatal(err)
				}
				name := "manifest.json"
				if operation == "copy-scratch" {
					name = "plans.sqlite3"
				}
				path := filepath.Join(to.path, name)
				changed := false
				var before unix.Stat_t
				wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				g := guardWithClocks(context.Background(), func() time.Time {
					if !changed {
						var st unix.Stat_t
						e := unix.Lstat(path, &st)
						if e == nil {
							before = st
							if st.Mode&0777 != 0600 || st.Nlink != 1 {
								t.Fatal("exclusive create mode/links invalid", st)
							}
							if e = os.Chmod(path, mode); e != nil {
								t.Fatal(e)
							}
							changed = true
						} else if !errors.Is(e, unix.ENOENT) {
							t.Fatal(e)
						}
					}
					return wall
				}, func() time.Duration { return 0 })
				var pin unix.Stat_t
				if operation == "write-manifest" {
					pin, err = writeFile(to, name, body, g)
				} else {
					_, pin, err = copyFile(from, "source", sourcePin, to, g)
				}
				if !changed {
					t.Fatal("created-mode witness did not enter")
				}
				if mode == 0600 {
					if err != nil || pin.Mode&0777 != 0600 || !sameObject(before, pin) {
						t.Fatal("exact created output refused", pin, err)
					}
				} else if !errors.Is(err, ErrChanged) {
					t.Fatal("changed owner-only mode accepted", pin, err)
				}
				var retained unix.Stat_t
				if e := unix.Lstat(path, &retained); e != nil || retained.Dev != before.Dev || retained.Ino != before.Ino || os.FileMode(retained.Mode&0777) != mode || retained.Nlink != 1 {
					t.Fatal("changed output removed, replaced or repaired", retained, e)
				}
				entries, e := os.ReadDir(to.path)
				if e != nil || len(entries) != 1 || entries[0].Name() != name {
					t.Fatal("unproved output cleanup/reuse", entries, e)
				}
				// The generated fixture may be removed by t.TempDir after this witness;
				// these helpers perform no cleanup, publication or engine operation.
			})
		}
	}
	for _, operation := range []string{"write-manifest", "copy-scratch"} {
		for _, kind := range []string{"append", "shorten", "named-replacement", "hardlink"} {
			t.Run(operation+"-created-output-"+kind, func(t *testing.T) {
				from, to := heldTemp(t), heldTemp(t)
				body := []byte("generated complete output")
				privateBytes(t, filepath.Join(from.path, "source"), body)
				sourcePin, err := from.stat("source")
				if err != nil {
					t.Fatal(err)
				}
				name := "manifest.json"
				if operation == "copy-scratch" {
					name = "plans.sqlite3"
				}
				path := filepath.Join(to.path, name)
				changed := false
				var before unix.Stat_t
				wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				g := guardWithClocks(context.Background(), func() time.Time {
					if !changed {
						var st unix.Stat_t
						e := unix.Lstat(path, &st)
						if e == nil && st.Size == int64(len(body)) {
							before = st
							changed = true
							if st.Mode&0777 != 0600 || st.Nlink != 1 {
								t.Fatal("exclusive output invalid before generated change", st)
							}
							switch kind {
							case "append":
								f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
								if e != nil {
									t.Fatal(e)
								}
								n, e := f.Write([]byte("x"))
								c := f.Close()
								if n != 1 || e != nil || c != nil {
									t.Fatal(n, e, c)
								}
							case "shorten":
								if e = os.Truncate(path, int64(len(body)-1)); e != nil {
									t.Fatal(e)
								}
							case "named-replacement":
								if e = os.Rename(path, filepath.Join(to.path, "retained-created")); e != nil {
									t.Fatal(e)
								}
								privateBytes(t, path, body)
							case "hardlink":
								if e = os.Link(path, filepath.Join(to.path, "retained-alias")); e != nil {
									t.Fatal(e)
								}
							}
						} else if e != nil && !errors.Is(e, unix.ENOENT) {
							t.Fatal(e)
						}
					}
					return wall
				}, func() time.Duration { return 0 })
				if operation == "write-manifest" {
					_, err = writeFile(to, name, body, g)
				} else {
					_, _, err = copyFile(from, "source", sourcePin, to, g)
				}
				if !changed || !errors.Is(err, ErrChanged) {
					t.Fatal("created output length/identity change accepted", changed, err)
				}
				var named unix.Stat_t
				if e := unix.Lstat(path, &named); e != nil {
					t.Fatal("unproved output removed", e)
				}
				if kind == "named-replacement" {
					var original unix.Stat_t
					if e := unix.Lstat(filepath.Join(to.path, "retained-created"), &original); e != nil || !sameObject(before, original) || named.Ino == before.Ino {
						t.Fatal("original or replacement repaired/removed", original, named, e)
					}
				} else if named.Dev != before.Dev || named.Ino != before.Ino {
					t.Fatal("original output replaced", before, named)
				}
				wantNames := 1
				switch kind {
				case "append":
					if named.Size != int64(len(body)+1) {
						t.Fatal("appended bytes repaired", named.Size)
					}
				case "shorten":
					if named.Size != int64(len(body)-1) {
						t.Fatal("shortened output repaired", named.Size)
					}
				case "hardlink":
					wantNames = 2
					var alias unix.Stat_t
					if e := unix.Lstat(filepath.Join(to.path, "retained-alias"), &alias); e != nil || alias.Ino != before.Ino || named.Nlink != 2 {
						t.Fatal("unproved alias removed", alias, named, e)
					}
				case "named-replacement":
					wantNames = 2
				}
				entries, e := os.ReadDir(to.path)
				if e != nil || len(entries) != wantNames {
					t.Fatal("created-output cleanup/reuse occurred", entries, e)
				}
			})
		}
	}
	t.Run("created-output-invalid-original-pin", func(t *testing.T) {
		d := heldTemp(t)
		f, original, err := createFile(d, "manifest.json")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if e := f.Close(); e != nil {
				t.Error(e)
			}
		}()
		// These synthetic metadata arguments test the shared final predicate;
		// they do not change owner UID or claim a real chown operation.
		for _, kind := range []string{"uid", "mode", "links", "object"} {
			bad := original
			switch kind {
			case "uid":
				bad.Uid ^= 1
			case "mode":
				bad.Mode = (bad.Mode &^ 0777) | 0700
			case "links":
				bad.Nlink = 2
			case "object":
				bad.Ino ^= 1
			}
			if _, e := createdOutputPin(d, "manifest.json", f, bad, 0); !errors.Is(e, ErrChanged) {
				t.Fatal("invalid original stamp admitted", kind, e)
			}
		}
		if _, e := createdOutputPin(d, "manifest.json", nil, original, 0); !errors.Is(e, ErrChanged) {
			t.Fatal("missing held output admitted", e)
		}
		if _, e := createdOutputPin(d, "manifest.json", f, original, maxDatabaseBytes+1); !errors.Is(e, ErrBounds) {
			t.Fatal("output expected-size bound admitted", e)
		}
		if current, e := d.stat("manifest.json"); e != nil || !sameFile(original, current) {
			t.Fatal("synthetic predicate changed output", current, e)
		}
	})
}

func sparseMetadataFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(size); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSourceMetadataCapsAreSampledWithoutBodyReads(t *testing.T) {
	for _, kind := range []string{"exact-main", "large-main", "exact-WAL", "large-WAL", "exact-SHM", "large-SHM", "aggregate", "journal", "hardlinked-main"} {
		t.Run(kind, func(t *testing.T) {
			d := heldTemp(t)
			main := int64(512)
			if kind == "exact-main" {
				main = maxDatabaseBytes
			}
			if kind == "large-main" {
				main = maxDatabaseBytes + 1
			}
			if kind == "aggregate" {
				main = maxDatabaseBytes
			}
			sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3"), main)
			switch kind {
			case "exact-WAL":
				sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3-wal"), maxSidecarBytes)
			case "large-WAL":
				sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3-wal"), maxSidecarBytes+1)
			case "exact-SHM":
				sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3-shm"), maxSidecarBytes)
			case "large-SHM":
				sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3-shm"), maxSidecarBytes+1)
			case "aggregate":
				sparseMetadataFile(t, filepath.Join(d.path, "plans.sqlite3-wal"), 1)
			case "journal":
				privateBytes(t, filepath.Join(d.path, "plans.sqlite3-journal"), nil)
			case "hardlinked-main":
				if err := os.Link(filepath.Join(d.path, "plans.sqlite3"), filepath.Join(d.path, "alias")); err != nil {
					t.Fatal(err)
				}
			}
			_, err := sourceMetadata(d)
			positive := kind == "exact-main" || kind == "exact-WAL" || kind == "exact-SHM"
			if positive && err != nil {
				t.Fatal(err)
			}
			if !positive && err == nil {
				t.Fatal("sampled source bound/profile admitted")
			}
			// Truncate creates generated sparse metadata fixtures. This test does
			// not read their bodies or claim a hard limit on SQLite engine writes.
			var st unix.Stat_t
			if err = unix.Lstat(filepath.Join(d.path, "plans.sqlite3"), &st); err != nil || st.Size != main {
				t.Fatal(st.Size, err)
			}
		})
	}
}
