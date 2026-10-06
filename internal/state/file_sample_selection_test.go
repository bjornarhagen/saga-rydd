package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fileSampleFixture(t *testing.T) (*Store, string, string, []SameSizeFile) {
	t.Helper()
	s, dir := sameSizeFixture(t)
	if _, err := s.db.Exec(`UPDATE roots SET volume_id=?;
 INSERT INTO allocation_revisions(root_id,revision) VALUES(1,1);
 INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,X'2e',X'','directory',0,0,1,2,'1','100',1,10),
 (1,X'61',X'2e','directory',0,0,1,2,'1','101',1,10);
 INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,X'61',3,1,20)`, "v1:"+strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a/one", "a/two"} {
		id := addSameSizeFile(t, s, name, SameSizeMinimumBytes, fmt.Sprint(len(name)))
		if _, err := s.db.Exec("UPDATE entries SET generation=3 WHERE id=?", id); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || len(r.Bands) != 1 || len(r.Bands[0].Files) != 2 {
		t.Fatal(r, err)
	}
	return s, dir, r.InventoryID, r.Bands[0].Files
}

func TestPrepareFileSampleSelectionOfflineSnapshotAndNoWrites(t *testing.T) {
	w, dir, token, files := fileSampleFixture(t)
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	before, err := os.ReadFile(w.path)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve exact caller ordering; display text is deliberately lossy and
	// does not identify the selected byte path.
	files[0], files[1] = files[1], files[0]
	files[0].Path = "display text only"
	targets, err := r.PrepareFileSampleSelection(context.Background(), token, files)
	if err != nil || len(targets) != 2 {
		t.Fatal(targets, err)
	}
	for i, target := range targets {
		if target.InventoryID != token || target.Root.ID != 1 || string(target.Root.PathBytes) != "/offline-size-fixture" || target.Root.Fingerprint != "v1:"+strings.Repeat("a", 64) || target.Root.Revision != 1 || !sameFileSampleEvidence(target.File, files[i]) || target.File.Path != string(files[i].PathBytes) || len(target.Ancestors) != 2 || string(target.Ancestors[0].Path) != "." || string(target.Ancestors[1].Path) != "a" {
			t.Fatal(target)
		}
		for _, e := range target.Ancestors {
			if e.Kind != "directory" || e.Device != "1" || e.MtimeNS != 1 || e.CtimeNS != 2 || e.Inode == "" {
				t.Fatal(e)
			}
		}
	}
	after, err := os.ReadFile(w.path)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("capture changed the inventory database", err)
	}
	// The returned evidence owns its byte slices rather than aliasing caller
	// selection data that can change after this request.
	files[0].PathBytes[0] = 'X'
	if !bytes.HasPrefix(targets[0].File.PathBytes, []byte("/offline")) {
		t.Fatal("returned target aliases caller bytes")
	}
}

func TestPrepareFileSampleSelectionRefusesChangedOrUncertainEvidence(t *testing.T) {
	cases := map[string]string{
		"missing file":                           "DELETE FROM entries WHERE kind='file'",
		"file kind":                              "UPDATE entries SET kind='symlink' WHERE kind='file'",
		"file path":                              "UPDATE entries SET path=X'612f6d6f766564' WHERE id=4",
		"file root":                              "UPDATE entries SET root_id=2 WHERE kind='file'",
		"file size":                              "UPDATE entries SET size=size+1 WHERE kind='file'",
		"file allocated":                         "UPDATE entries SET allocated=allocated+1 WHERE kind='file'",
		"file observation":                       "UPDATE entries SET observed_at_ns=11 WHERE kind='file'",
		"file modification":                      "UPDATE entries SET mtime_ns=3 WHERE kind='file'",
		"file change time":                       "UPDATE entries SET ctime_ns=4 WHERE kind='file'",
		"file device":                            "UPDATE entries SET device='2' WHERE kind='file'",
		"file inode":                             "UPDATE entries SET inode='999' WHERE kind='file'",
		"file generation":                        "UPDATE entries SET generation=4 WHERE kind='file'",
		"file skip":                              "UPDATE entries SET skip_reason='excluded' WHERE kind='file'",
		"malformed file parent":                  "UPDATE entries SET parent=X'2e' WHERE kind='file'",
		"file observed after parent pass":        "UPDATE entries SET observed_at_ns=21 WHERE kind='file'",
		"disabled root":                          "UPDATE roots SET enabled=0 WHERE id=1",
		"changed root path":                      "UPDATE roots SET path=X'2f7265706c61636564' WHERE id=1",
		"unknown root fingerprint":               "UPDATE roots SET volume_id='' WHERE id=1",
		"unsupported root fingerprint":           "UPDATE roots SET volume_id='fixture' WHERE id=1",
		"root error":                             "UPDATE roots SET last_error='offline' WHERE id=1",
		"unknown root revision":                  "DELETE FROM allocation_revisions WHERE root_id=1",
		"zero root revision":                     "UPDATE allocation_revisions SET revision=0 WHERE root_id=1",
		"missing ancestor":                       "DELETE FROM entries WHERE path=X'61'",
		"ancestor replaced by file":              "UPDATE entries SET kind='file' WHERE path=X'61'",
		"ancestor missing identity":              "UPDATE entries SET inode='' WHERE path=X'61'",
		"ancestor zero inode":                    "UPDATE entries SET inode='0' WHERE path=X'61'",
		"ancestor noncanonical inode":            "UPDATE entries SET inode='0101' WHERE path=X'61'",
		"ancestor zero ctime":                    "UPDATE entries SET ctime_ns=0 WHERE path=X'61'",
		"ancestor zero mtime":                    "UPDATE entries SET mtime_ns=0 WHERE path=X'61'",
		"ancestor skip":                          "UPDATE entries SET skip_reason='excluded' WHERE path=X'61'",
		"ancestor filesystem differs":            "UPDATE entries SET device='2' WHERE path=X'61'",
		"ancestor absent from parent":            "UPDATE entries SET generation=2 WHERE path=X'61'",
		"ancestor observed after own listing":    "UPDATE entries SET observed_at_ns=21 WHERE path=X'61'",
		"ancestor observed after parent listing": "UPDATE entries SET observed_at_ns=21 WHERE path=X'61'; UPDATE directories SET checked_at_ns=30 WHERE path=X'61'",
		"ancestor parent link":                   "UPDATE entries SET parent=X'626164' WHERE path=X'61'",
		"root generation differs":                "UPDATE entries SET generation=2 WHERE path=X'2e'",
		"root parent link":                       "UPDATE entries SET parent=X'2e' WHERE path=X'2e'",
		"missing root listing":                   "DELETE FROM directories WHERE path=X'2e'",
		"partial root listing":                   "UPDATE directories SET complete=0 WHERE path=X'2e'",
		"partial direct parent":                  "UPDATE directories SET complete=0 WHERE path=X'61'",
		"ancestor listing error":                 "UPDATE directories SET last_error='error' WHERE path=X'2e'",
		"direct parent listing error":            "UPDATE directories SET last_error='error' WHERE path=X'61'",
		"missing listing timestamp":              "UPDATE directories SET checked_at_ns=0 WHERE path=X'61'",
		"unknown listing generation":             "UPDATE directories SET generation=0 WHERE path=X'61'",
		"compacted ancestor":                     "INSERT INTO compact_dirs VALUES(1,X'2e',1,0,0,0,0)",
		"compacted direct parent":                "INSERT INTO compact_dirs VALUES(1,X'61',3,0,0,0,0)",
		"pending ancestor scan":                  "INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'inventory',X'2e',1)",
		"pending direct parent scan":             "INSERT INTO jobs(root_id,kind,path,due_at_ns) VALUES(1,'inventory',X'61',1)",
		"pending reconcile":                      "INSERT INTO subtree_reconcile(root_id,path,generation) VALUES(1,X'6f74686572',1)",
		"pending retirement":                     "INSERT INTO subtree_retirement(root_id,path,scan_revision,preserve_entry) VALUES(1,X'6f74686572',1,0)",
		"pending compact retirement":             "INSERT INTO compact_retirement VALUES(1,X'6f74686572',1)",
		"oversized identity":                     "UPDATE entries SET device='1'||char(0)||hex(zeroblob(1048576)) WHERE kind='file'",
		"oversized ancestor identity":            "UPDATE entries SET inode='1'||char(0)||hex(zeroblob(1048576)) WHERE path=X'61'",
		"oversized root identity":                "UPDATE roots SET volume_id='v1:'||char(0)||hex(zeroblob(1048576)) WHERE id=1",
		"oversized text relative path":           "UPDATE entries SET path=CAST(hex(zeroblob(5000)) AS TEXT) WHERE id=4",
	}
	for name, query := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, token, files := fileSampleFixture(t)
			if err := s.SyncRoots(context.Background(), []string{"/offline-size-fixture", "/offline-second-fixture"}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			if targets, err := s.PrepareFileSampleSelection(context.Background(), token, files); !errors.Is(err, ErrFileSampleEvidence) || len(targets) != 0 {
				t.Fatal(targets, err)
			}
		})
	}
}

func TestPrepareFileSampleSelectionCapturesCurrentRootAndAncestorBaseline(t *testing.T) {
	s, _, token, files := fileSampleFixture(t)
	// The displayed file does not contain a root fingerprint, revision or
	// ancestor stamp. Capture a usable current baseline without pretending to
	// compare it to undisclosed earlier root/ancestor evidence.
	if _, err := s.db.Exec(`UPDATE roots SET volume_id=?;
 UPDATE allocation_revisions SET revision=2;
 UPDATE entries SET inode='200',mtime_ns=3,ctime_ns=4 WHERE path=X'61'`, "v1:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	targets, err := s.PrepareFileSampleSelection(context.Background(), token, files)
	if err != nil || targets[0].Root.Revision != 2 || targets[0].Root.Fingerprint != "v1:"+strings.Repeat("b", 64) || targets[0].Ancestors[1].Inode != "200" {
		t.Fatal(targets, err)
	}
}

func TestPrepareFileSampleSelectionInventoryBoundsAndCancellation(t *testing.T) {
	s, _, token, files := fileSampleFixture(t)
	other, _, _, _ := fileSampleFixture(t)
	if targets, err := other.PrepareFileSampleSelection(context.Background(), token, files); !errors.Is(err, ErrFileSampleEvidence) || len(targets) != 0 {
		t.Fatal(targets, err)
	}
	for _, tc := range []struct {
		token string
		files []SameSizeFile
	}{
		{"", files}, {strings.ToUpper(token), files}, {token, nil}, {token, append(files, files[0])}, {token, make([]SameSizeFile, 21)},
	} {
		if targets, err := s.PrepareFileSampleSelection(context.Background(), tc.token, tc.files); !errors.Is(err, ErrFileSampleSelection) || len(targets) != 0 {
			t.Fatal(targets, err)
		}
	}
	for _, change := range []func(*SameSizeFile){
		func(f *SameSizeFile) { f.PathBytes = []byte("relative") }, func(f *SameSizeFile) { f.PathBytes = []byte("/path/../bad") }, func(f *SameSizeFile) { f.PathBytes = []byte("/zero\x00") },
		func(f *SameSizeFile) { f.PathBytes = []byte("/" + strings.Repeat("x", 4096)) }, func(f *SameSizeFile) { f.Inode = "0" }, func(f *SameSizeFile) { f.Device = "01" },
		func(f *SameSizeFile) { f.ParentPass = "partial" }, func(f *SameSizeFile) { f.Generation = 0 }, func(f *SameSizeFile) { f.ChangedNS = 0 },
	} {
		modified := append([]SameSizeFile(nil), files...)
		change(&modified[0])
		if targets, err := s.PrepareFileSampleSelection(context.Background(), token, modified); !errors.Is(err, ErrFileSampleSelection) || len(targets) != 0 {
			t.Fatal(targets, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PrepareFileSampleSelection(ctx, token, files); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.schema = 8
	if _, err := s.PrepareFileSampleSelection(context.Background(), token, files); !errors.Is(err, ErrFileSampleSchema) {
		t.Fatal(err)
	}
}

func TestPrepareFileSampleSelectionTwentyTargets(t *testing.T) {
	s, _, token, _ := fileSampleFixture(t)
	for i := 0; i < 18; i++ {
		id := addSameSizeFile(t, s, fmt.Sprintf("a/file-%d", i), SameSizeMinimumBytes, fmt.Sprint(i+500))
		if _, err := s.db.Exec("UPDATE entries SET generation=3 WHERE id=?", id); err != nil {
			t.Fatal(err)
		}
	}
	r, err := s.SameSizeCandidates(context.Background(), 20, "", 1)
	if err != nil || len(r.Bands) != 1 || len(r.Bands[0].Files) != 20 {
		t.Fatal(r, err)
	}
	targets, err := s.PrepareFileSampleSelection(context.Background(), token, r.Bands[0].Files)
	if err != nil || len(targets) != 20 {
		t.Fatal(targets, err)
	}
	for i, target := range targets {
		if target.File.ID != r.Bands[0].Files[i].ID {
			t.Fatal("caller ordering changed")
		}
	}
}

func TestPrepareFileSampleSelectionRawPathsAndGeneratedRoots(t *testing.T) {
	for _, root := range []string{"/offline-size-fixture", "/offline-size-fixture/node_modules", "/offline-size-fixture/node_modules/pkg"} {
		t.Run(root, func(t *testing.T) {
			s, _, token, files := fileSampleFixture(t)
			if _, err := s.db.Exec("UPDATE roots SET path=? WHERE id=1", []byte(root)); err != nil {
				t.Fatal(err)
			}
			for i := range files {
				rel := filepath.Base(files[i].Path)
				files[i].Path = filepath.Join(root, "a", rel)
				files[i].PathBytes = []byte(files[i].Path)
			}
			targets, err := s.PrepareFileSampleSelection(context.Background(), token, files)
			if strings.Contains(root, "node_modules") {
				if !errors.Is(err, ErrFileSampleEvidence) || len(targets) != 0 {
					t.Fatal(targets, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("non UTF8", func(t *testing.T) {
		s, _, token, files := fileSampleFixture(t)
		path := []byte("a/\xff")
		if _, err := s.db.Exec("UPDATE entries SET path=? WHERE id=?", path, files[0].ID); err != nil {
			t.Fatal(err)
		}
		files[0].PathBytes = []byte("/offline-size-fixture/a/\xff")
		files[0].Path = "display only"
		targets, err := s.PrepareFileSampleSelection(context.Background(), token, files)
		if err != nil || !bytes.Equal(targets[0].File.PathBytes, files[0].PathBytes) {
			t.Fatal(targets, err)
		}
	})
	t.Run("generated ancestor", func(t *testing.T) {
		s, _, token, files := fileSampleFixture(t)
		if _, err := s.db.Exec("UPDATE entries SET path=? WHERE id=?", []byte("node_modules/pkg/file"), files[0].ID); err != nil {
			t.Fatal(err)
		}
		files[0].PathBytes = []byte("/offline-size-fixture/node_modules/pkg/file")
		if targets, err := s.PrepareFileSampleSelection(context.Background(), token, files); !errors.Is(err, ErrFileSampleEvidence) || len(targets) != 0 {
			t.Fatal(targets, err)
		}
	})
}

func TestPrepareFileSampleSelectionSingleSnapshot(t *testing.T) {
	w, dir, token, files := fileSampleFixture(t)
	r, err := OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	tx, err := r.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var pinned string
	if err = tx.QueryRow("SELECT token FROM inventory_identity").Scan(&pinned); err != nil {
		t.Fatal(err)
	}
	before, err := r.prepareFileSampleSelection(context.Background(), token, files, tx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.db.Exec("UPDATE entries SET ctime_ns=999; UPDATE allocation_revisions SET revision=2"); err != nil {
		t.Fatal(err)
	}
	after, err := r.prepareFileSampleSelection(context.Background(), token, files, tx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal(before, after, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if targets, err := r.PrepareFileSampleSelection(context.Background(), token, files); !errors.Is(err, ErrFileSampleEvidence) || len(targets) != 0 {
		t.Fatal(targets, err)
	}
}

func TestPrepareFileSampleSelectionDepthAndAggregateLimits(t *testing.T) {
	for _, parts := range []int{255, 257} {
		t.Run(fmt.Sprint(parts), func(t *testing.T) {
			s, _, token, _ := fileSampleFixture(t)
			fixture, err := s.db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer fixture.Rollback()
			var parent string
			for i := 0; i < parts; i++ {
				path := filepath.Join(parent, "abcdefghij")
				if parent == "" {
					parent = "."
				}
				if _, err := fixture.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,'directory',0,0,1,2,'1',?,1,10);
 INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,20)`, []byte(path), []byte(parent), fmt.Sprint(1000+i), []byte(path)); err != nil {
					t.Fatal(err)
				}
				parent = path
			}
			for i := 0; i < 20; i++ {
				if _, err := fixture.Exec(`INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,'file',?,4096,1,2,'1',?,1,10)`, []byte(filepath.Join(parent, fmt.Sprint(i))), []byte(parent), 2<<20, fmt.Sprint(i+2000)); err != nil {
					t.Fatal(err)
				}
			}
			if err = fixture.Commit(); err != nil {
				t.Fatal(err)
			}
			r, err := s.SameSizeCandidates(context.Background(), 20, "", 2<<20)
			if err != nil || len(r.Bands) != 1 || len(r.Bands[0].Files) != 20 {
				t.Fatal(r, err)
			}
			if parts == 255 {
				// Test structural/evidence limits independently of runtime speed.
				// Race instrumentation may legitimately exhaust the public API's
				// unchanged five-second deadline before completing a deep chain.
				tx, err := s.db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				one, err := s.prepareFileSampleSelection(context.Background(), token, r.Bands[0].Files[:1], tx)
				if err != nil || len(one) != 1 || len(one[0].Ancestors) != LivePathDepthLimit {
					t.Fatal("exact depth bound was refused", one, err)
				}
				if targets, err := s.prepareFileSampleSelection(context.Background(), token, r.Bands[0].Files, tx); !errors.Is(err, ErrFileSampleLimit) || len(targets) != 0 {
					t.Fatal(targets, err)
				}
				return
			}
			if targets, err := s.PrepareFileSampleSelection(context.Background(), token, r.Bands[0].Files); !errors.Is(err, ErrFileSampleLimit) || len(targets) != 0 {
				t.Fatal(targets, err)
			}
		})
	}
}
