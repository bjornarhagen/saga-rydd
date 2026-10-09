package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func requireSampleFixtureFilename(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	// Some Darwin filesystems refuse invalid UTF-8 during fixture creation.
	// Gate only that exact case; ordinary permissions and Linux errors fail.
	if runtime.GOOS == "darwin" && !utf8.ValidString(name) && (errors.Is(err, unix.EILSEQ) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EPERM)) {
		t.Skipf("Darwin filesystem refused this synthetic invalid UTF-8 filename: %v; valid UTF-8/control-name cases run separately", err)
	}
	t.Fatal(err)
}

func sampleFixture(t *testing.T, contents ...[]byte) (*Scanner, []SavedFileTarget) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(root, "project"), 0700); err != nil {
		t.Fatal(err)
	}
	paths := make([]string, len(contents))
	for i, data := range contents {
		paths[i] = filepath.Join(root, "project", fmt.Sprintf("file-%02d", i))
		if err = os.WriteFile(paths[i], data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := New([]string{root}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, captureSampleTargets(t, s, root, paths)
}

func captureSampleTargets(t *testing.T, s *Scanner, root string, paths []string) []SavedFileTarget {
	t.Helper()
	stamp := func(path string) unix.Stat_t {
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	rootStamp := stamp(root)
	f, err := s.openAbsolute(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	volume, _, err := s.filesystem(int(f.Fd()))
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	binding := state.RootBinding{ID: 1, PathBytes: []byte(root), Fingerprint: rootFingerprint(root, volume, rootStamp), Revision: 1}
	var targets []SavedFileTarget
	for i, path := range paths {
		st := stamp(path)
		m, c := timestamps(&st)
		target := SavedFileTarget{InventoryID: strings.Repeat("a", 64), Root: binding, File: state.SameSizeFile{
			ReportFile: state.ReportFile{ID: int64(i + 1), RootID: 1, Path: path, PathBytes: []byte(path), Size: st.Size, Allocated: max(st.Blocks, 0) * 512, ObservedAt: time.Now().UTC(), ModifiedAt: time.Unix(0, m).UTC(), ParentPass: "observed_in_completed_parent_pass"},
			Device:     fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), ChangedNS: c, Generation: 1,
		}}
		target.Ancestors = append(target.Ancestors, observation(".", rootStamp))
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		if rel != "." {
			parts := strings.Split(rel, string(filepath.Separator))
			for j := range parts {
				parent := filepath.Join(parts[:j+1]...)
				target.Ancestors = append(target.Ancestors, observation(parent, stamp(filepath.Join(root, parent))))
			}
		}
		targets = append(targets, target)
	}
	return targets
}

func requireSampleBlocked(t *testing.T, report FileSampleReport, err error) FileSampleResult {
	t.Helper()
	if err != nil || report.Status != "blocked" || len(report.Targets) != 1 || report.Targets[0].Status != "blocked" || report.Targets[0].SHA256 != "" {
		t.Fatalf("expected blocked sample with no digest: %+v, %v", report, err)
	}
	return report.Targets[0]
}

func TestFileSampleFramingAndClaims(t *testing.T) {
	s, targets := sampleFixture(t, []byte("abc"), []byte("abc"), nil)
	r, err := s.ObserveFileSamples(context.Background(), targets)
	if err != nil || r.Status != "samples_observed" || len(r.Targets) != 3 {
		t.Fatal(r, err)
	}
	// This explicit frame checks the documented wire contract independently of
	// the range planner. Identity and path never enter the comparison digest.
	var frame bytes.Buffer
	frame.WriteString("saga-rydd:file_samples_v1\x00")
	for _, value := range []uint64{3, 1, 0, 3} {
		if err := binary.Write(&frame, binary.BigEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	frame.WriteString("abc")
	want := sha256.Sum256(frame.Bytes())
	if r.Targets[0].SHA256 != hex.EncodeToString(want[:]) || r.Targets[0].SHA256 != r.Targets[1].SHA256 || r.Targets[2].SHA256 == "" {
		t.Fatal("digest framing or path-independent equality changed", r)
	}
	if r.RequestedBytes != 6 || r.ReadBytes != 6 || r.Targets[2].ReadBytes != 0 || len(r.Targets[2].Ranges) != 0 {
		t.Fatal("small or empty files were read more than once", r)
	}
	if r.Contract != "file_samples_v1" || r.Source != "live_file_samples" || r.EvidenceSource != "caller_supplied_saved_records" || r.ProvenanceVerified || r.ContentVerified || r.CurrentStateVerified || r.DuplicatesVerified || r.Executable || r.EstimatedReclaimableBytes != nil {
		t.Fatal("sample evidence made a stronger claim", r)
	}
	for _, result := range r.Targets {
		if result.LiveLinkCount != 1 || result.RepeatedSavedIdentity {
			t.Fatal("incorrect link evidence", result)
		}
	}
	serialized, err := json.Marshal(r)
	if err != nil || bytes.Contains(serialized, []byte("\"abc\"")) {
		t.Fatal("source contents appeared in report", string(serialized), err)
	}
}

func TestFileSamplesEqualDespiteUnsampledDifferences(t *testing.T) {
	first := bytes.Repeat([]byte{'x'}, 256*1024)
	second := bytes.Clone(first)
	second[48*1024] = 'y' // outside all three ranges
	s, targets := sampleFixture(t, first, second, bytes.Repeat([]byte{'x'}, 257*1024))
	r, err := s.ObserveFileSamples(context.Background(), targets)
	if err != nil || r.Status != "samples_observed" {
		t.Fatal(r, err)
	}
	if r.Targets[0].SHA256 != r.Targets[1].SHA256 || r.Targets[0].SHA256 == r.Targets[2].SHA256 || r.ContentVerified || r.DuplicatesVerified {
		t.Fatal("samples were represented as a full-content comparison or size was not bound", r)
	}
	for _, result := range r.Targets {
		if result.ReadBytes != FileSampleFileByteLimit || result.RequestedBytes != FileSampleFileByteLimit {
			t.Fatal("unexpected sampled byte count", result)
		}
	}
}

func TestFileSampleRangeBounds(t *testing.T) {
	for _, size := range []int64{0, 1, 32767, 32768, 32769, 65535, 65536, 65537, 98303, 98304, 98305, 1024 * 1024, math.MaxInt64} {
		ranges, total, previousEnd := fileSampleRanges(size), int64(0), int64(-1)
		for _, sample := range ranges {
			if sample.Offset < 0 || sample.Length <= 0 || sample.Offset <= previousEnd || sample.Offset > size || sample.Length > size-sample.Offset {
				t.Fatalf("size %d produced invalid ranges %+v", size, ranges)
			}
			total += sample.Length
			previousEnd = sample.Offset + sample.Length
		}
		if total > FileSampleFileByteLimit || total > size {
			t.Fatalf("size %d exceeded byte bounds: %+v", size, ranges)
		}
		if size <= FileSampleFileByteLimit && total != size {
			t.Fatalf("small file left gaps: %d %+v", size, ranges)
		}
	}
}

func TestFileSampleActualCallBudget(t *testing.T) {
	contents := make([][]byte, FileSampleTargetLimit)
	for i := range contents {
		contents[i] = bytes.Repeat([]byte{byte(i)}, 256*1024)
	}
	s, targets := sampleFixture(t, contents...)
	actual, reads := int64(0), 0
	r, err := s.observeFileSamples(context.Background(), targets, fileSampleHooks{afterRead: func(n int) {
		actual += int64(n)
		reads++
		if n > 32*1024 {
			t.Fatal("read buffer exceeded 32 KiB")
		}
	}})
	if err != nil || r.Status != "samples_observed" || r.RequestedBytes != FileSampleCallByteLimit || r.ReadBytes != FileSampleCallByteLimit || actual != FileSampleCallByteLimit || reads != 60 {
		t.Fatal("actual reads exceeded or failed to reach the fixed call cap", actual, reads, r, err)
	}
	for _, item := range r.Targets {
		if item.ReadBytes != FileSampleFileByteLimit || item.SHA256 == "" {
			t.Fatal(item)
		}
	}
}

func TestFileSampleInvalidEvidence(t *testing.T) {
	cases := map[string]func(*SavedFileTarget){
		"inventory":          func(t *SavedFileTarget) { t.InventoryID = strings.Repeat("A", 64) },
		"root_revision":      func(t *SavedFileTarget) { t.Root.Revision = 0 },
		"root_fingerprint":   func(t *SavedFileTarget) { t.Root.Fingerprint = "v1:" + strings.Repeat("a", 64) },
		"root_id":            func(t *SavedFileTarget) { t.File.RootID = 2 },
		"file_id":            func(t *SavedFileTarget) { t.File.ID = 0 },
		"inode":              func(t *SavedFileTarget) { t.File.Inode = "01" },
		"device":             func(t *SavedFileTarget) { t.File.Device = "" },
		"ctime":              func(t *SavedFileTarget) { t.File.ChangedNS = 0 },
		"mtime":              func(t *SavedFileTarget) { t.File.ModifiedAt = time.Time{} },
		"generation":         func(t *SavedFileTarget) { t.File.Generation = 0 },
		"unconfirmed":        func(t *SavedFileTarget) { t.File.ParentPass = "unconfirmed" },
		"skip":               func(t *SavedFileTarget) { t.File.SkipReason = "unknown" },
		"size":               func(t *SavedFileTarget) { t.File.Size++ },
		"missing_ancestor":   func(t *SavedFileTarget) { t.Ancestors = t.Ancestors[1:] },
		"unknown_ancestor":   func(t *SavedFileTarget) { t.Ancestors[0].Inode = "" },
		"duplicate_ancestor": func(t *SavedFileTarget) { t.Ancestors[1] = t.Ancestors[0] },
		"ancestor_kind":      func(t *SavedFileTarget) { t.Ancestors[1].Kind = "file" },
		"ancestor_changed":   func(t *SavedFileTarget) { t.Ancestors[1].CtimeNS++ },
		"nul": func(t *SavedFileTarget) {
			t.File.PathBytes = append(t.File.PathBytes, 0)
			t.File.Path = string(t.File.PathBytes)
		},
		"outside": func(t *SavedFileTarget) {
			t.File.PathBytes = []byte(filepath.Join(filepath.Dir(string(t.Root.PathBytes)), "outside"))
			t.File.Path = string(t.File.PathBytes)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("private fixture"))
			change(&targets[0])
			reads := 0
			r, err := s.observeFileSamples(context.Background(), targets, fileSampleHooks{afterRead: func(int) { reads++ }})
			item := requireSampleBlocked(t, r, err)
			if reads != 0 || item.RequestedBytes != 0 || item.ReadBytes != 0 {
				t.Fatal("uncertain evidence caused content reads", item, reads)
			}
		})
	}
}

func TestFileSampleRequestBoundsAndConsistency(t *testing.T) {
	for _, kind := range []string{"empty", "too_many", "duplicate_path", "duplicate_id", "different_inventory", "root_conflict", "root_path_conflict", "ancestor_conflict", "long_path", "many_ancestors", "evidence_bytes", "depth"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("first"), []byte("second"))
			expectReport := false
			switch kind {
			case "empty":
				targets = nil
			case "too_many":
				targets = make([]SavedFileTarget, 21)
			case "duplicate_path":
				targets[1].File.PathBytes = targets[0].File.PathBytes
			case "duplicate_id":
				targets[1].File.ID = targets[0].File.ID
			case "different_inventory":
				targets[1].InventoryID = strings.Repeat("b", 64)
			case "root_conflict":
				targets[1].Root.Revision++
			case "root_path_conflict":
				targets[1].Root.ID++
				targets[1].File.RootID++
			case "ancestor_conflict":
				targets[1].Ancestors[1].CtimeNS++
			case "long_path":
				targets[0].File.PathBytes = bytes.Repeat([]byte{'x'}, 4097)
			case "many_ancestors":
				targets[0].Ancestors = make([]state.Entry, 258)
			case "evidence_bytes":
				targets = make([]SavedFileTarget, 20)
				for i := range targets {
					targets[i].Ancestors = make([]state.Entry, 30)
					for j := range targets[i].Ancestors {
						targets[i].Ancestors[j].Path = bytes.Repeat([]byte{'x'}, 4096)
					}
				}
			case "depth":
				targets = targets[:1]
				targets[0].File.PathBytes = []byte(string(targets[0].Root.PathBytes) + "/" + strings.Repeat("d/", 257) + "file")
				targets[0].File.Path = string(targets[0].File.PathBytes)
				expectReport = true
			}
			reads := 0
			r, err := s.observeFileSamples(context.Background(), targets, fileSampleHooks{afterRead: func(int) { reads++ }})
			if expectReport {
				requireSampleBlocked(t, r, err)
			} else if err == nil || len(r.Targets) != 0 {
				t.Fatal("invalid request accepted", r, err)
			}
			if reads != 0 {
				t.Fatal("invalid request caused content reads")
			}
		})
	}
}

func TestFileSampleScopeAndGeneratedExclusions(t *testing.T) {
	for _, kind := range []string{"file_excluded", "ancestor_excluded", "scope_removed", "protected_alias", "generated_parent", "generated_root", "ordinary_node_modules_name", "unix_name", "invalid_utf8_name"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("original"))
			target := targets[0]
			root, path := string(target.Root.PathBytes), string(target.File.PathBytes)
			want := "blocked"
			switch kind {
			case "file_excluded":
				s.excludes = append(s.excludes, path)
			case "ancestor_excluded":
				s.excludes = append(s.excludes, filepath.Dir(path))
			case "scope_removed":
				s.roots = map[string]bool{}
			case "protected_alias":
				s.protectedIDs[target.File.Device+":"+target.File.Inode] = true
			case "generated_parent", "generated_root":
				parent := filepath.Join(root, "node_modules")
				if err := os.Rename(filepath.Dir(path), parent); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(parent, filepath.Base(path))
				if kind == "generated_root" {
					root = parent
					s.roots = map[string]bool{root: true}
				}
				targets = captureSampleTargets(t, s, root, []string{path})
			case "ordinary_node_modules_name", "unix_name", "invalid_utf8_name":
				name := "node_modules"
				if kind == "unix_name" {
					name = "line\n\"quoted\tø"
				}
				if kind == "invalid_utf8_name" {
					name = "line\n\xff"
				}
				newPath := filepath.Join(filepath.Dir(path), name)
				requireSampleFixtureFilename(t, name, os.Rename(path, newPath))
				targets = captureSampleTargets(t, s, root, []string{newPath})
				if kind == "unix_name" || kind == "invalid_utf8_name" {
					encoded, err := json.Marshal(targets)
					if err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(encoded, &targets); err != nil {
						t.Fatal(err)
					}
				} else {
					targets[0].File.Path = "/ignored/display/path"
				}
				want = "samples_observed"
			}
			r, err := s.ObserveFileSamples(context.Background(), targets)
			if err != nil || r.Status != want {
				t.Fatal(r, err)
			}
			if want == "blocked" {
				item := requireSampleBlocked(t, r, err)
				if item.RequestedBytes != 0 {
					t.Fatal("excluded file read", item)
				}
			} else if !bytes.Equal(r.Targets[0].Target.File.PathBytes, targets[0].File.PathBytes) || r.Targets[0].SHA256 == "" {
				t.Fatal("exact Unix path was not preserved", r)
			}
		})
	}
}

func TestFileSampleChangesBeforeRead(t *testing.T) {
	for _, kind := range []string{"edit", "symlink", "fifo", "missing"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("original"))
			path := string(targets[0].File.PathBytes)
			if kind == "edit" {
				if err := os.WriteFile(path, []byte("modified"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if kind == "symlink" {
					if err := os.Symlink(path+".old", path); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "fifo" {
					if err := unix.Mkfifo(path, 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := s.ObserveFileSamples(context.Background(), targets)
			item := requireSampleBlocked(t, r, err)
			if item.ReadBytes != 0 || item.RequestedBytes != 0 {
				t.Fatal("changed path was read", item)
			}
		})
	}
}

func TestFileSampleLongUnixNameJSONRoundTrip(t *testing.T) {
	for _, fixture := range []struct{ name, component string }{
		{"valid_control_name", strings.Repeat("ø", 98) + "\n\""},
		{"invalid_utf8_name", strings.Repeat("\xff", 200)},
	} {
		t.Run(fixture.name, func(t *testing.T) { testFileSampleLongNameJSONRoundTrip(t, fixture.component) })
	}
}

func testFileSampleLongNameJSONRoundTrip(t *testing.T, component string) {
	t.Helper()
	s, targets := sampleFixture(t, []byte("fixture"))
	root := string(targets[0].Root.PathBytes)
	rootFD, err := unix.Open(root, openFlags, 0)
	if err != nil {
		t.Fatal(err)
	}
	directories := []int{rootFD}
	relatives := []string{"."}
	defer func() {
		for _, fd := range directories {
			_ = unix.Close(fd)
		}
	}()
	// Create and capture relative to descriptors. The complete path exceeds
	// macOS PATH_MAX, although each component and this API's byte path fit.
	for i := 0; i < 12; i++ {
		name := component
		parentFD := directories[len(directories)-1]
		requireSampleFixtureFilename(t, name, unix.Mkdirat(parentFD, name, 0700))
		next, err := unix.Openat(parentFD, name, openFlags, 0)
		requireSampleFixtureFilename(t, name, err)
		directories = append(directories, next)
		relatives = append(relatives, filepath.Join(relatives[len(relatives)-1], name))
	}
	parentFD := directories[len(directories)-1]
	fileFD, err := unix.Openat(parentFD, "ordinary", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	n, writeErr := unix.Write(fileFD, []byte("fixture"))
	_ = unix.Close(fileFD)
	if writeErr != nil || n != 7 {
		t.Fatal("fixture write", n, writeErr)
	}
	var fileStamp unix.Stat_t
	if err := unix.Fstatat(parentFD, "ordinary", &fileStamp, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		t.Fatal(err)
	}
	volume, _, err := s.filesystem(rootFD)
	if err != nil {
		t.Fatal(err)
	}
	targets[0].Ancestors = nil
	for i, fd := range directories {
		var stamp unix.Stat_t
		if err := unix.Fstat(fd, &stamp); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			targets[0].Root.Fingerprint = rootFingerprint(root, volume, stamp)
		}
		targets[0].Ancestors = append(targets[0].Ancestors, observation(relatives[i], stamp))
	}
	path := filepath.Join(root, relatives[len(relatives)-1], "ordinary")
	m, c := timestamps(&fileStamp)
	targets[0].File.Path, targets[0].File.PathBytes = path, []byte(path)
	targets[0].File.Size, targets[0].File.Allocated = fileStamp.Size, max(fileStamp.Blocks, 0)*512
	targets[0].File.Device, targets[0].File.Inode = fmt.Sprint(fileStamp.Dev), fmt.Sprint(fileStamp.Ino)
	targets[0].File.ModifiedAt, targets[0].File.ChangedNS = time.Unix(0, m).UTC(), c
	for _, fd := range directories {
		if err := unix.Close(fd); err != nil {
			t.Fatal(err)
		}
	}
	directories = nil
	encoded, err := json.Marshal(targets)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &targets); err != nil {
		t.Fatal(err)
	}
	if len(path) <= 1024 || !bytes.Equal(targets[0].File.PathBytes, []byte(path)) || (!utf8.ValidString(component) && len(targets[0].File.Path) <= 4096) {
		t.Fatal("fixture did not preserve its long byte path or required display expansion")
	}
	r, err := s.ObserveFileSamples(context.Background(), targets)
	if err != nil || r.Status != "samples_observed" || r.Targets[0].ReadBytes != 7 || !bytes.Equal(r.Targets[0].Target.File.PathBytes, []byte(path)) {
		t.Fatal("display expansion rejected an exact byte path", r, err)
	}
}

func TestFileSampleEditBetweenRanges(t *testing.T) {
	s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
	reads := 0
	r, err := s.observeFileSamples(context.Background(), targets, fileSampleHooks{afterRead: func(int) {
		reads++
		if reads != 1 {
			return
		}
		path := string(targets[0].File.PathBytes)
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteAt([]byte{'y'}, 128*1024)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		// Restoring the saved mtime cannot hide the changed ctime.
		if err := os.Chtimes(path, time.Now(), targets[0].File.ModifiedAt); err != nil {
			t.Fatal(err)
		}
	}})
	item := requireSampleBlocked(t, r, err)
	if item.ReadBytes != FileSampleFileByteLimit || reads != 3 {
		t.Fatal("fixture did not exercise a writer between ranges", item, reads)
	}
}

func TestFileSampleMutationsAndPathSwapsDuringRead(t *testing.T) {
	for _, kind := range []string{"same_size_edit", "grow", "truncate", "mode", "file_symlink", "file_fifo", "file_replace", "ancestor_symlink", "ancestor_detached", "root_symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
			path, root := string(targets[0].File.PathBytes), string(targets[0].Root.PathBytes)
			hooks := fileSampleHooks{beforeFinalCheck: func() {
				p := path
				switch kind {
				case "same_size_edit":
					f, err := os.OpenFile(path, os.O_WRONLY, 0)
					if err != nil {
						t.Fatal(err)
					}
					_, err = f.WriteAt([]byte{'y'}, 48*1024)
					_ = f.Close()
					if err != nil {
						t.Fatal(err)
					}
					return
				case "grow":
					if err := os.Truncate(path, 256*1024+1); err != nil {
						t.Fatal(err)
					}
					return
				case "truncate":
					if err := os.Truncate(path, 0); err != nil {
						t.Fatal(err)
					}
					return
				case "mode":
					if err := os.Chmod(path, 0400); err != nil {
						t.Fatal(err)
					}
					return
				case "ancestor_symlink", "ancestor_detached":
					p = filepath.Dir(path)
				case "root_symlink":
					p = root
				}
				if err := os.Rename(p, p+".old"); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "file_fifo":
					if err := unix.Mkfifo(p, 0600); err != nil {
						t.Fatal(err)
					}
				case "file_replace":
					if err := os.WriteFile(p, bytes.Repeat([]byte{'x'}, 256*1024), 0600); err != nil {
						t.Fatal(err)
					}
				case "ancestor_detached":
					if err := os.Mkdir(p, 0700); err != nil {
						t.Fatal(err)
					}
				default:
					if err := os.Symlink(p+".old", p); err != nil {
						t.Fatal(err)
					}
				}
			}}
			r, err := s.observeFileSamples(context.Background(), targets, hooks)
			item := requireSampleBlocked(t, r, err)
			if item.ReadBytes != FileSampleFileByteLimit || item.RequestedBytes != FileSampleFileByteLimit {
				t.Fatal("mutation test did not read its bounded samples", item)
			}
		})
	}
}

func TestFileSampleShortReadIsChargedAndHasNoDigest(t *testing.T) {
	s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024))
	r, err := s.observeFileSamples(context.Background(), targets, fileSampleHooks{afterOpen: func() {
		if err := os.Truncate(string(targets[0].File.PathBytes), 0); err != nil {
			t.Fatal(err)
		}
	}})
	item := requireSampleBlocked(t, r, err)
	if item.Code != "sample_incomplete" || item.RequestedBytes != FileSampleWindowBytes || item.ReadBytes != 0 || r.RequestedBytes != FileSampleWindowBytes || r.ReadBytes != 0 {
		t.Fatal("short reads were not charged by requested bytes", r)
	}
}

func TestFileSampleCancellationDiscardsEarlierDigests(t *testing.T) {
	for _, kind := range []string{"before", "during", "after_first", "deadline", "scanner_closed"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, bytes.Repeat([]byte{'x'}, 256*1024), bytes.Repeat([]byte{'y'}, 256*1024))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			hooks := fileSampleHooks{afterRead: func(int) {
				reads++
				if (kind == "during" && reads == 1) || (kind == "after_first" && reads == 4) {
					cancel()
				}
			}}
			if kind == "before" {
				cancel()
			}
			if kind == "deadline" {
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			if kind == "scanner_closed" {
				s.Close()
			}
			r, err := s.observeFileSamples(ctx, targets, hooks)
			if err == nil || len(r.Targets) != 0 || r.RequestedBytes != 0 || r.ReadBytes != 0 {
				t.Fatal("cancellation retained samples", r, err)
			}
			if kind == "deadline" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			} else if kind != "scanner_closed" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if kind == "before" || kind == "deadline" || kind == "scanner_closed" {
				if reads != 0 {
					t.Fatal("read after initial cancellation")
				}
			}
			if kind == "during" && reads != 1 {
				t.Fatal("continued reads after cancellation", reads)
			}
			if kind == "after_first" && reads != 4 {
				t.Fatal("continued reads after cancellation", reads)
			}
		})
	}
}

func TestFileSampleSavedHardlinkEvidence(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("same object"), []byte("replace with alias"))
			first, second := string(targets[0].File.PathBytes), string(targets[1].File.PathBytes)
			if err := os.Remove(second); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(first, second); err != nil {
				t.Fatal(err)
			}
			targets = captureSampleTargets(t, s, string(targets[0].Root.PathBytes), []string{first, second})
			if conflict {
				targets[1].File.ChangedNS++
			}
			r, err := s.ObserveFileSamples(context.Background(), targets)
			if err != nil || len(r.Targets) != 2 {
				t.Fatal(r, err)
			}
			for _, item := range r.Targets {
				if conflict {
					if item.Status != "blocked" || item.Code != "identity_conflict" || item.ReadBytes != 0 || item.SHA256 != "" {
						t.Fatal("conflicting saved aliases were read", item)
					}
				} else if item.Status != "samples_observed" || item.LiveLinkCount != 2 || !item.RepeatedSavedIdentity || item.SHA256 != r.Targets[0].SHA256 {
					t.Fatal("hardlink alias treated as an independent copy", item)
				}
			}
		})
	}
}

func TestFileSamplesFromProductionInventory(t *testing.T) {
	for _, kind := range []string{"ordinary", "control_name", "invalid_utf8_name"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, fixture := sampleFixture(t, bytes.Repeat([]byte{'x'}, 1024*1024), bytes.Repeat([]byte{'x'}, 1024*1024))
			root := string(fixture[0].Root.PathBytes)
			parent := filepath.Join(root, "project", "nested")
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			for i, target := range fixture {
				name := fmt.Sprintf("ordinary-%d", i)
				if kind == "control_name" && i == 0 {
					name = "ordinary-\n\"ø\t"
				}
				if kind == "invalid_utf8_name" && i == 0 {
					name = "ordinary-\xff"
				}
				requireSampleFixtureFilename(t, name, os.Rename(string(target.File.PathBytes), filepath.Join(parent, name)))
			}
			stateDir := filepath.Join(t.TempDir(), "state")
			writer, err := state.OpenWriter(ctx, stateDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = writer.Close() })
			if err := writer.SyncRoots(ctx, []string{root}); err != nil {
				t.Fatal(err)
			}
			compact := false
			if _, err := writer.ConfigureCompact(ctx, &compact); err != nil {
				t.Fatal(err)
			}
			if err := writer.SeedInventory(ctx); err != nil {
				t.Fatal(err)
			}
			finished := false
			for chunk := 0; chunk < 20; chunk++ {
				job, err := writer.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if job == nil {
					finished = true
					break
				}
				batch, err := s.Next(ctx, *job)
				if err != nil || batch.Fault != "" {
					t.Fatal(batch, err)
				}
				if err := writer.CommitScan(ctx, *job, batch); err != nil {
					t.Fatal(err)
				}
			}
			if !finished {
				t.Fatal("bounded production fixture did not finish")
			}
			finishSelectionFixtureMaintenance(t, ctx, writer, s)
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			reader, err := state.OpenReader(ctx, stateDir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			saved, err := reader.SameSizeCandidates(ctx, 20, "", state.SameSizeMinimumBytes)
			if err != nil || len(saved.Bands) != 1 || len(saved.Bands[0].Files) != 2 {
				t.Fatal(saved, err)
			}
			selected := saved.Bands[0].Files
			encoded, err := json.Marshal(selected)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &selected); err != nil {
				t.Fatal(err)
			}
			captured, err := reader.PrepareFileSampleSelection(ctx, saved.InventoryID, selected)
			if err != nil || len(captured) != 2 {
				t.Fatal(captured, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			// The alias allows captured bindings to pass directly to the sampler.
			// All database transactions/connections are closed before source reads.
			report, err := s.ObserveFileSamples(ctx, captured)
			if err != nil || report.Status != "samples_observed" || len(report.Targets) != 2 || report.ReadBytes != 2*FileSampleFileByteLimit || report.ProvenanceVerified || report.ContentVerified || report.CurrentStateVerified || report.DuplicatesVerified || report.Executable {
				t.Fatal(report, err)
			}
			for i, item := range report.Targets {
				if item.SHA256 == "" || item.SHA256 != report.Targets[0].SHA256 || !bytes.Equal(item.Target.File.PathBytes, selected[i].PathBytes) || len(item.Target.Ancestors) != 3 || item.Target.Root.Revision <= 0 || item.Target.Root.Fingerprint == "" {
					t.Fatal("production root/ancestor bindings were unusable", item)
				}
			}
		})
	}
}
