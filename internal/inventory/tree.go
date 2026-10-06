package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/regeneration"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const (
	TreeEntryLimit     = 10000
	TreeDepthLimit     = 64
	TreePathBytesLimit = 4 * 1024 * 1024
)

type TreeEvidence struct {
	Status           string `json:"status"`
	Entries          int    `json:"entries"`
	Directories      int    `json:"directories"`
	RegularFiles     int    `json:"regular_files"`
	InternalBinLinks int    `json:"internal_bin_links"`
	MetadataSHA256   string `json:"metadata_sha256"`
}

type treeRecord struct {
	stamp unix.Stat_t
	link  string
	id    int
}

type treeReader struct {
	scanner          *Scanner
	ctx              context.Context
	rootFD           int // borrowed from the enclosing live-path check
	path, mount      string
	packages, scopes map[string]bool
	records          map[string]treeRecord
	baseline         map[string]treeRecord
	seen             []bool
	evidence         TreeEvidence
	pathBytes        int
	absoluteDepth    int
	device           uint64
}

func newTreeReader(s *Scanner, ctx context.Context, rootFD int, path, mount string, layout regeneration.Layout) *treeReader {
	w := &treeReader{scanner: s, ctx: ctx, rootFD: rootFD, path: path, mount: mount, packages: map[string]bool{}, scopes: map[string]bool{}}
	w.absoluteDepth = len(strings.Split(strings.TrimPrefix(path, "/"), "/"))
	for _, packagePath := range layout.PackagePaths {
		rel := strings.TrimPrefix(packagePath, "node_modules/")
		w.packages[rel] = true
		parent := filepath.Dir(rel)
		if strings.HasPrefix(filepath.Base(parent), "@") {
			w.scopes[parent] = true
		}
	}
	return w
}

func (w *treeReader) observe() (TreeEvidence, error) {
	w.records = map[string]treeRecord{}
	if err := w.walk(); err != nil {
		return TreeEvidence{}, err
	}
	if err := w.validateLinks(); err != nil {
		return TreeEvidence{}, err
	}
	paths := make([]string, 0, len(w.records))
	for path := range w.records {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	digest := sha256.New()
	_, _ = digest.Write([]byte("rydd-tree-metadata-v1\x00"))
	for _, path := range paths {
		if err := w.ctx.Err(); err != nil {
			return TreeEvidence{}, err
		}
		writeTreeRecord(digest, path, w.records[path])
	}
	w.evidence.Status, w.evidence.MetadataSHA256 = "metadata_observed", hex.EncodeToString(digest.Sum(nil))
	return w.evidence, nil
}

func (w *treeReader) recheck() error {
	w.baseline, w.records = w.records, nil
	w.seen = make([]bool, len(w.baseline))
	w.evidence, w.pathBytes = TreeEvidence{}, 0
	if err := w.walk(); err != nil {
		return err
	}
	if w.evidence.Entries+1 != len(w.baseline) {
		return blocked("tree_changed_during_check", "The dependency tree changed between metadata passes. Review it and retry.")
	}
	return nil
}

func (w *treeReader) walk() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	// Opening '.' creates an independent directory stream; it does not share
	// listing offsets with the live verifier or a previous metadata pass.
	fd, err := w.scanner.openat(w.rootFD, ".")
	if err != nil {
		return err
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		_ = unix.Close(fd)
		return err
	}
	w.device = uint64(st.Dev)
	return w.directory(fd, ".", "boundary", 0, st)
}

func (w *treeReader) add(path string, st unix.Stat_t, link string) error {
	if path != "." {
		w.evidence.Entries++
		if w.evidence.Entries > TreeEntryLimit {
			return blocked("tree_limit", "The dependency tree exceeds the 10,000-entry inspection limit. Review it separately.")
		}
		switch st.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			w.evidence.Directories++
		case unix.S_IFREG:
			w.evidence.RegularFiles++
		case unix.S_IFLNK:
			w.evidence.InternalBinLinks++
		}
	}
	w.pathBytes += len(path) + len(link)
	if w.pathBytes > TreePathBytesLimit {
		return blocked("tree_limit", "The dependency tree exceeds the retained path/link byte limit. Review it separately.")
	}
	record := treeRecord{stamp: st, link: link}
	if w.baseline != nil {
		saved, exists := w.baseline[path]
		if !exists || !sameInputStamp(saved.stamp, st) || saved.link != link {
			return blocked("tree_changed_during_check", "A dependency object or link changed between metadata passes. Review it and retry.")
		}
		if w.seen[saved.id] {
			return blocked("tree_changed_during_check", "The directory listing repeated an object during the second metadata pass.")
		}
		w.seen[saved.id] = true
	} else {
		if _, duplicate := w.records[path]; duplicate {
			return blocked("tree_changed_during_check", "The directory listing repeated an object during inspection. Review it and retry.")
		}
		record.id = len(w.records)
		w.records[path] = record
	}
	return nil
}

func (w *treeReader) directory(fd int, path, role string, depth int, before unix.Stat_t) error {
	file := os.NewFile(uintptr(fd), "dependency directory")
	defer file.Close()
	if depth > TreeDepthLimit || w.absoluteDepth+depth > state.LivePathDepthLimit {
		return blocked("tree_limit", "The dependency tree exceeds the bounded directory depth. Review it separately.")
	}
	if err := w.supported(before); err != nil {
		return err
	}
	v, m, err := w.scanner.filesystem(fd)
	if err != nil || v == "" || m != w.mount {
		return blocked("mount_boundary", "A dependency directory crosses a mount boundary or lacks filesystem evidence.")
	}
	if err = w.add(path, before, ""); err != nil {
		return err
	}
	for {
		if err = w.ctx.Err(); err != nil {
			return err
		}
		names, readErr := file.Readdirnames(128)
		for _, name := range names {
			if err = w.ctx.Err(); err != nil {
				return err
			}
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
				return blocked("tree_unsupported", "A dependency directory returned an unsupported entry name.")
			}
			rel := filepath.Join(path, name)
			if len(filepath.Join(w.path, rel)) > 4096 {
				return blocked("tree_limit", "A dependency path exceeds the 4096-byte inspection limit.")
			}
			if w.scanner.excluded(filepath.Join(w.path, rel)) {
				return blocked("scope_excluded", "The dependency tree contains an excluded or protected object.")
			}
			var st unix.Stat_t
			if err = unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return err
			}
			if err = w.supported(st); err != nil {
				return err
			}
			if err = verifyNamedMount(fd, name, w.mount); err != nil {
				return blocked("mount_boundary", "A dependency object crosses a mount boundary or lacks mount evidence.")
			}
			childRole, err := w.classify(path, role, name, rel, st)
			if err != nil {
				return err
			}
			switch st.Mode & unix.S_IFMT {
			case unix.S_IFDIR:
				if depth+1 > TreeDepthLimit || w.absoluteDepth+depth+1 > state.LivePathDepthLimit {
					return blocked("tree_limit", "The dependency tree exceeds the bounded directory depth. Review it separately.")
				}
				child, err := w.scanner.openat(fd, name)
				if err != nil {
					return err
				}
				var opened unix.Stat_t
				if err = unix.Fstat(child, &opened); err != nil || !sameInputStamp(st, opened) {
					_ = unix.Close(child)
					return blocked("tree_changed_during_check", "A dependency directory changed while opening.")
				}
				if err = w.directory(child, rel, childRole, depth+1, opened); err != nil {
					return err
				}
				if err = w.namedStamp(fd, name, st); err != nil {
					return err
				}
			case unix.S_IFREG:
				if err = w.add(rel, st, ""); err != nil {
					return err
				}
			case unix.S_IFLNK:
				var text [4097]byte
				n, err := unix.Readlinkat(fd, name, text[:])
				if err != nil {
					return err
				}
				if n > 4096 {
					return blocked("tree_limit", "An executable link exceeds the 4096-byte inspection limit.")
				}
				if err = w.add(rel, st, string(text[:n])); err != nil {
					return err
				}
				if err = w.namedStamp(fd, name, st); err != nil {
					return err
				}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	var after unix.Stat_t
	if err = unix.Fstat(fd, &after); err != nil {
		return err
	}
	if !sameInputStamp(before, after) {
		return blocked("tree_changed_during_check", "A dependency directory changed while its entries were inspected.")
	}
	return w.ctx.Err()
}

func (w *treeReader) supported(st unix.Stat_t) error {
	_, changed := timestamps(&st)
	if dataless(st) || w.scanner.protectedIDs[objectID(st)] || st.Ino == 0 || changed <= 0 {
		return blocked("tree_unsupported", "A dependency object is protected, a placeholder or lacks usable identity evidence.")
	}
	if uint64(st.Dev) != w.device {
		return blocked("mount_boundary", "A dependency object is on a different filesystem.")
	}
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFDIR, unix.S_IFLNK:
		return nil
	case unix.S_IFREG:
		if st.Nlink != 1 {
			return blocked("tree_unsupported", "A dependency file has multiple hardlinks. Their ownership and scope need separate review.")
		}
		return nil
	default:
		return blocked("tree_unsupported", "The dependency tree contains a special object rather than an ordinary file or directory.")
	}
}

func (w *treeReader) classify(parent, role, name, path string, st unix.Stat_t) (string, error) {
	kind := st.Mode & unix.S_IFMT
	switch role {
	case "boundary", "scope":
		if w.packages[path] && kind == unix.S_IFDIR {
			return "package", nil
		}
		if role == "boundary" && name == ".bin" && kind == unix.S_IFDIR {
			return "bin", nil
		}
		if w.scopes[path] && kind == unix.S_IFDIR {
			return "scope", nil
		}
		if parent == "." && name == ".package-lock.json" && kind == unix.S_IFREG {
			return "content", nil
		}
		return "", blocked("tree_layout_unknown", "A dependency boundary contains an object outside the supported lock-listed package layout.")
	case "bin":
		if kind != unix.S_IFLNK {
			return "", blocked("tree_link_unsupported", "An executable directory contains an object other than a supported internal symlink.")
		}
		return "bin", nil
	default:
		if kind == unix.S_IFLNK {
			return "", blocked("tree_link_unsupported", "A dependency symlink occurs outside a supported executable directory.")
		}
		if role == "package" && name == "node_modules" {
			if kind != unix.S_IFDIR {
				return "", blocked("tree_layout_unknown", "A package's dependency boundary is not an ordinary directory.")
			}
			return "boundary", nil
		}
		return "content", nil
	}
}

func (w *treeReader) validateLinks() error {
	for path, record := range w.records {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		if record.stamp.Mode&unix.S_IFMT != unix.S_IFLNK {
			continue
		}
		if record.link == "" || filepath.IsAbs(record.link) || strings.ContainsRune(record.link, 0) {
			return blocked("tree_link_unsupported", "An executable link uses an unsupported absolute or empty target.")
		}
		// Check the raw component sequence before normalization. A missing or
		// non-directory component followed by '..' is not a usable link even
		// when lexical cleaning would produce an existing regular endpoint.
		target := filepath.Dir(path)
		for _, part := range strings.Split(record.link, "/") {
			parent, exists := w.records[target]
			if !exists || parent.stamp.Mode&unix.S_IFMT != unix.S_IFDIR {
				return blocked("tree_link_unsupported", "An executable link crosses a missing or unsupported directory component.")
			}
			switch part {
			case "", ".":
			case "..":
				if target == "." {
					return blocked("tree_link_unsupported", "An executable link leaves the selected dependency tree.")
				}
				target = filepath.Dir(target)
			default:
				target = filepath.Join(target, part)
				entry, exists := w.records[target]
				if !exists || entry.stamp.Mode&unix.S_IFMT == unix.S_IFLNK {
					return blocked("tree_link_unsupported", "An executable link is dangling or crosses another symlink.")
				}
			}
			if len(filepath.Join(w.path, target)) > 4096 {
				return blocked("tree_limit", "An executable link path exceeds the 4096-byte inspection limit.")
			}
		}
		owned := false
		for current := target; current != "."; current = filepath.Dir(current) {
			if w.packages[current] && current != target {
				owned = true
			}
			entry, exists := w.records[current]
			if !exists || (current != target && entry.stamp.Mode&unix.S_IFMT != unix.S_IFDIR) {
				return blocked("tree_link_unsupported", "An executable link is dangling or crosses an unsupported path component.")
			}
		}
		entry, exists := w.records[target]
		if !owned || !exists || entry.stamp.Mode&unix.S_IFMT != unix.S_IFREG || entry.stamp.Mode&0111 == 0 {
			return blocked("tree_link_unsupported", "An executable link does not point to an observed regular executable inside a recognized package.")
		}
	}
	return nil
}

func (w *treeReader) namedStamp(parent int, name string, before unix.Stat_t) error {
	var named unix.Stat_t
	if err := unix.Fstatat(parent, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil || !sameInputStamp(before, named) {
		return blocked("tree_changed_during_check", "A dependency path link changed during inspection.")
	}
	if err := verifyNamedMount(parent, name, w.mount); err != nil {
		return blocked("mount_boundary", "A dependency object mount changed during inspection.")
	}
	return nil
}

func writeTreeRecord(digest hash.Hash, path string, record treeRecord) {
	text := func(value string) {
		_ = binary.Write(digest, binary.BigEndian, uint64(len(value)))
		_, _ = digest.Write([]byte(value))
	}
	text(path)
	m, c := timestamps(&record.stamp)
	for _, value := range []uint64{uint64(record.stamp.Dev), uint64(record.stamp.Ino), uint64(record.stamp.Mode), uint64(record.stamp.Size), uint64(record.stamp.Nlink), uint64(m), uint64(c)} {
		_ = binary.Write(digest, binary.BigEndian, value)
	}
	text(record.link)
}
