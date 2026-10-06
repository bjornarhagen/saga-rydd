package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func recoveryFixture(t *testing.T) RecoveryRequest {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "project", "node_modules")
	destination := filepath.Join(root, "quarantine", "slot")
	for _, path := range []string{source, filepath.Dir(destination)} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "retained"), []byte("original dependency contents"), 0000); err != nil {
		t.Fatal(err)
	}
	return RecoveryRequest{SourcePathBytes: []byte(source), DestinationPathBytes: []byte(destination), SourceParent: recoveryFixtureIdentity(t, filepath.Dir(source)), SourceObject: recoveryFixtureIdentity(t, source), DestinationParent: recoveryFixtureIdentity(t, filepath.Dir(destination))}
}

func recoveryFixtureIdentity(t *testing.T, path string) RecoveryIdentity {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		t.Fatal(err)
	}
	_, ctime := timestamps(&st)
	return RecoveryIdentity{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), ChangedNS: ctime, Generation: 42}
}

func requireRecoveryUnknown(t *testing.T, report RecoveryReport, err error) {
	t.Helper()
	if err != nil || report.Status != "outcome_unknown" || report.Source != "live_recovery_metadata" || report.CheckedAt.IsZero() || report.CurrentStateVerified || report.Executable || report.HistoricalMountVerified || report.ScopeVerified {
		t.Fatalf("metadata observation overclaimed or failed: %+v error=%v", report, err)
	}
}

func TestRecoveryMetadataLocations(t *testing.T) {
	request := recoveryFixture(t)
	report, err := ObserveRecovery(context.Background(), request)
	requireRecoveryUnknown(t, report, err)
	if report.SourceLocation.Status != "observed_present" || report.SourceLocation.IdentityRelation != "metadata_matches_reference" || !report.SourceLocation.DeviceInodeMatch || !report.SourceLocation.CtimeMatch || report.SourceLocation.ObjectIdentity == nil || report.SourceLocation.ObjectIdentity.Kind != "directory" || report.DestinationLocation.Status != "observed_absent" || report.DestinationLocation.ObjectIdentity != nil || report.DestinationLocation.ParentIdentity == nil {
		t.Fatalf("unexpected location evidence: %+v", report)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "generation") || strings.Contains(string(encoded), "retained") || strings.Contains(string(encoded), "original dependency contents") {
		t.Fatal("live observation invented generation or included child names/contents")
	}
	// The fixture body's permissions prevent ordinary reading. No body or
	// directory listing is needed to observe either exact directory location.
	if err := os.Chmod(filepath.Join(string(request.SourcePathBytes), "retained"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(string(request.SourcePathBytes), "retained"), []byte("local changes remain unverified"), 0600); err != nil {
		t.Fatal(err)
	}
	report, err = ObserveRecovery(context.Background(), request)
	requireRecoveryUnknown(t, report, err)
	if report.SourceLocation.IdentityRelation != "metadata_matches_reference" {
		t.Fatal("ordinary file contents affected directory-only observation", report)
	}
}

func TestRecoveryPotentialIdentityAndParentCtime(t *testing.T) {
	request := recoveryFixture(t)
	request.SourceParent.ChangedNS--
	request.DestinationParent.ChangedNS--
	request.SourceObject.ChangedNS--
	request.SourceObject.Generation = 987654321 // Deliberately not a kernel value.
	report, err := ObserveRecovery(context.Background(), request)
	requireRecoveryUnknown(t, report, err)
	if report.SourceLocation.IdentityRelation != "potential_identity" || !report.SourceLocation.DeviceInodeMatch || report.SourceLocation.CtimeMatch || !report.SourceLocation.ObjectCtimeChanged || !report.SourceLocation.ParentCtimeChanged || !report.DestinationLocation.ParentCtimeChanged || report.DestinationLocation.Status != "observed_absent" {
		t.Fatal("ctime difference was treated as verified identity or rejected parent", report)
	}
}

func TestRecoveryRelocationIsNotVerifiedMovement(t *testing.T) {
	request := recoveryFixture(t)
	if err := os.Rename(string(request.SourcePathBytes), string(request.DestinationPathBytes)); err != nil {
		t.Fatal(err)
	}
	report, err := ObserveRecovery(context.Background(), request)
	requireRecoveryUnknown(t, report, err)
	if report.SourceLocation.Status != "observed_absent" || report.DestinationLocation.Status != "observed_present" || !report.DestinationLocation.DeviceInodeMatch || !report.SourceLocation.ParentCtimeChanged || !report.DestinationLocation.ParentCtimeChanged {
		t.Fatal("unexpected relocated observation", report)
	}
	if report.DestinationLocation.IdentityRelation != "potential_identity" && report.DestinationLocation.IdentityRelation != "metadata_matches_reference" {
		t.Fatal(report)
	}
}

func TestRecoveryDifferentDirectory(t *testing.T) {
	request := recoveryFixture(t)
	if err := os.Mkdir(string(request.DestinationPathBytes), 0700); err != nil {
		t.Fatal(err)
	}
	report, err := ObserveRecovery(context.Background(), request)
	requireRecoveryUnknown(t, report, err)
	if report.DestinationLocation.Status != "observed_present" || report.DestinationLocation.IdentityRelation != "different_identity" || report.DestinationLocation.DeviceInodeMatch {
		t.Fatal("collision observation overclaimed identity", report)
	}
}

func TestRecoveryUnknownLocations(t *testing.T) {
	for _, kind := range []string{"missing_ancestor", "replacement_parent", "ancestor_symlink", "child_symlink", "child_file", "child_fifo", "permission"} {
		t.Run(kind, func(t *testing.T) {
			request := recoveryFixture(t)
			source, parent := string(request.SourcePathBytes), filepath.Dir(string(request.SourcePathBytes))
			var err error
			switch kind {
			case "missing_ancestor", "replacement_parent", "ancestor_symlink":
				if err = os.Rename(parent, parent+".old"); err != nil {
					t.Fatal(err)
				}
				if kind == "replacement_parent" {
					err = os.Mkdir(parent, 0700)
				} else if kind == "ancestor_symlink" {
					err = os.Symlink(parent+".old", parent)
				}
			case "child_symlink", "child_file", "child_fifo":
				if err = os.Rename(source, source+".old"); err != nil {
					t.Fatal(err)
				}
				if kind == "child_symlink" {
					err = os.Symlink(source+".old", source)
				} else if kind == "child_file" {
					err = os.WriteFile(source, []byte("replacement"), 0600)
				} else {
					err = unix.Mkfifo(source, 0600)
				}
			case "permission":
				if os.Geteuid() == 0 {
					t.Skip("root bypasses fixture permissions")
				}
				err = os.Chmod(parent, 0000)
				defer os.Chmod(parent, 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			report, err := ObserveRecovery(context.Background(), request)
			requireRecoveryUnknown(t, report, err)
			if report.SourceLocation.Status != "blocked" && report.SourceLocation.Status != "unavailable" {
				t.Fatal("missing/unsupported path was mistaken for a usable final location", report)
			}
			if report.SourceLocation.Code == "" || report.SourceLocation.ObjectIdentity != nil || report.SourceLocation.ParentIdentity != nil || report.SourceLocation.IdentityRelation != "unknown" {
				t.Fatal("blocked path leaked positive identity evidence", report)
			}
			if kind == "replacement_parent" && report.SourceLocation.Code != "parent_identity_changed" {
				t.Fatal(report)
			}
		})
	}
}

func TestRecoveryRacesStayUnknown(t *testing.T) {
	for _, kind := range []string{"source_removed", "source_replaced", "destination_created", "source_chmod", "parent_replaced", "ancestor_link_swap", "parent_sibling_changed", "final_child_chmod", "final_child_entry"} {
		t.Run(kind, func(t *testing.T) {
			request := recoveryFixture(t)
			source, parent := string(request.SourcePathBytes), filepath.Dir(string(request.SourcePathBytes))
			if kind == "source_chmod" || kind == "final_child_chmod" {
				t.Cleanup(func() { _ = os.Chmod(source, 0700) })
			}
			change := func() {
				var err error
				switch kind {
				case "source_removed", "source_replaced":
					if err = os.Rename(source, source+".old"); err != nil {
						t.Fatal(err)
					}
					if kind == "source_replaced" {
						err = os.Mkdir(source, 0700)
					}
				case "destination_created":
					err = os.Mkdir(string(request.DestinationPathBytes), 0700)
				case "source_chmod", "final_child_chmod":
					err = os.Chmod(source, 0500)
				case "final_child_entry":
					err = os.WriteFile(filepath.Join(source, "new-child"), []byte("new child"), 0600)
				case "parent_replaced", "ancestor_link_swap":
					if err = os.Rename(parent, parent+".old"); err != nil {
						t.Fatal(err)
					}
					if kind == "parent_replaced" {
						err = os.Mkdir(parent, 0700)
					} else {
						err = os.Symlink(parent+".old", parent)
					}
				case "parent_sibling_changed":
					err = os.WriteFile(filepath.Join(parent, "sibling"), []byte("new sibling"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			var beforeSecond, beforeFinal func()
			if kind == "final_child_chmod" || kind == "ancestor_link_swap" || kind == "final_child_entry" {
				beforeFinal = change
			} else {
				beforeSecond = change
			}
			report, err := observeRecovery(context.Background(), request, beforeSecond, beforeFinal)
			requireRecoveryUnknown(t, report, err)
			location := report.SourceLocation
			if kind == "destination_created" {
				location = report.DestinationLocation
			}
			if location.Status != "blocked" || location.Code != "path_changed_during_check" || location.ObjectIdentity != nil || location.ParentIdentity != nil {
				t.Fatalf("race was treated as an observation: %+v", report)
			}
		})
	}
}

func TestRecoveryOpeningStampBoundaries(t *testing.T) {
	for _, exactParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("exact_parent_%t", exactParent), func(t *testing.T) {
			request := recoveryFixture(t)
			source := string(request.SourcePathBytes)
			parent := filepath.Dir(source)
			mutationPath := filepath.Dir(parent)
			if exactParent {
				mutationPath = parent
			}
			scanner, err := New(nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer scanner.Close()
			mutated := false
			chain, err := scanner.openRecoveryChainWithHook(context.Background(), source, request.SourceParent, func(path string) {
				if path != mutationPath {
					return
				}
				mutated = true
				if err := os.WriteFile(filepath.Join(path, "unrelated-sibling"), []byte("fixture mutation"), 0600); err != nil {
					t.Fatal(err)
				}
			})
			if !mutated {
				t.Fatal("between-stat/open fixture hook did not run")
			}
			if exactParent {
				var failure liveError
				if err == nil || !errors.As(err, &failure) || failure.code != "path_changed_during_check" || chain != nil {
					t.Fatalf("exact-parent mutation passed opening: chain=%v error=%v", chain, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unrelated outer-ancestor sibling mutation blocked opening: %v", err)
			}
			defer chain.close()
			if err := scanner.recheckRecoveryChain(context.Background(), chain); err != nil {
				t.Fatalf("outer ancestor identity/link checks failed after sibling mutation: %v", err)
			}
			object, err := scanner.probeRecoveryChild(context.Background(), chain)
			if err != nil || object == nil || fmt.Sprint(object.Ino) != request.SourceObject.Inode {
				t.Fatalf("outer sibling mutation changed exact child observation: %v", err)
			}
		})
	}
}

func TestRecoveryBuiltInProtections(t *testing.T) {
	for _, target := range []string{"source", "destination", "ancestor"} {
		t.Run(target, func(t *testing.T) {
			request := recoveryFixture(t)
			source := string(request.SourcePathBytes)
			switch target {
			case "source":
				protected := filepath.Join(filepath.Dir(source), ".git")
				if err := os.Rename(source, protected); err != nil {
					t.Fatal(err)
				}
				request.SourcePathBytes = []byte(protected)
			case "destination":
				request.DestinationPathBytes = []byte(filepath.Join(filepath.Dir(string(request.DestinationPathBytes)), ".Trash"))
			case "ancestor":
				parent := filepath.Dir(source)
				protected := filepath.Join(filepath.Dir(parent), ".git")
				if err := os.Rename(parent, protected); err != nil {
					t.Fatal(err)
				}
				request.SourcePathBytes = []byte(filepath.Join(protected, "node_modules"))
			}
			report, err := ObserveRecovery(context.Background(), request)
			requireRecoveryUnknown(t, report, err)
			location := report.SourceLocation
			if target == "destination" {
				location = report.DestinationLocation
			}
			if location.Status != "blocked" || location.Code != "scope_excluded" || location.ObjectIdentity != nil {
				t.Fatal("built-in protected path was probed", report)
			}
		})
	}
	t.Run("protected_identity_after_rename", func(t *testing.T) {
		request := recoveryFixture(t)
		source := string(request.SourcePathBytes)
		// Simulate an identity already known to the standard scanner protection
		// map using a disposable protected fixture, then give it another name.
		scanner, err := New(nil, nil, []string{source})
		if err != nil {
			t.Fatal(err)
		}
		defer scanner.Close()
		alias := filepath.Join(filepath.Dir(source), "alias")
		if err := os.Rename(source, alias); err != nil {
			t.Fatal(err)
		}
		chain, err := scanner.openRecoveryChain(context.Background(), alias, request.SourceParent)
		if err != nil {
			t.Fatal(err)
		}
		defer chain.close()
		if _, err := scanner.probeRecoveryChild(context.Background(), chain); err == nil {
			t.Fatal("renaming a protected object bypassed its saved identity guard")
		} else {
			var failure liveError
			if !errors.As(err, &failure) || failure.code != "scope_excluded" {
				t.Fatal(err)
			}
		}
	})
}

func TestRecoveryRequestBoundsAndCancellation(t *testing.T) {
	request := recoveryFixture(t)
	for _, kind := range []string{"relative", "root", "nul", "nonclean", "bytes", "depth", "same_path", "unknown_parent", "invalid_generation", "noncanonical_identity"} {
		t.Run(kind, func(t *testing.T) {
			invalid := request
			switch kind {
			case "relative":
				invalid.SourcePathBytes = []byte("relative/node_modules")
			case "root":
				invalid.SourcePathBytes = []byte("/")
			case "nul":
				invalid.SourcePathBytes = []byte("/invalid\x00name")
			case "nonclean":
				invalid.SourcePathBytes = []byte("/some/../node_modules")
			case "bytes":
				invalid.SourcePathBytes = []byte("/" + strings.Repeat("x", 4096))
			case "depth":
				invalid.SourcePathBytes = []byte(strings.Repeat("/x", 257))
			case "same_path":
				invalid.DestinationPathBytes = invalid.SourcePathBytes
			case "unknown_parent":
				invalid.SourceParent.Inode = "0"
			case "invalid_generation":
				invalid.SourceObject.Generation = -1
			case "noncanonical_identity":
				invalid.SourceObject.Device = "00"
			}
			if _, err := ObserveRecovery(context.Background(), invalid); !errors.Is(err, ErrRecoveryRequest) {
				t.Fatalf("invalid request accepted: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ObserveRecovery(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if _, err := observeRecovery(ctx, request, cancel, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
