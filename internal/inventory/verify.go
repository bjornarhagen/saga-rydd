package inventory

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

type LiveResult struct {
	FindingID string `json:"finding_id"`
	Status    string `json:"status"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message"`
}

type LiveReport struct {
	Status               string       `json:"status"`
	Source               string       `json:"source"`
	CheckedAt            time.Time    `json:"checked_at"`
	CurrentStateVerified bool         `json:"current_state_verified"`
	Executable           bool         `json:"executable"`
	Targets              []LiveResult `json:"targets"`
}

type liveError struct{ code, message string }

func (e liveError) Error() string        { return e.message }
func blocked(code, message string) error { return liveError{code, message} }

// Verify compares only selected metadata. It reads no file contents or directory
// listings and returns no descriptors or authorization usable by an executor.
func (s *Scanner) Verify(ctx context.Context, targets []state.LiveTarget) (LiveReport, error) {
	r := LiveReport{Status: "metadata_matches", Source: "live_metadata", Targets: []LiveResult{}}
	if len(targets) < 1 || len(targets) > state.PreviewTargetLimit {
		return LiveReport{}, errors.New("live checks require 1–20 exact targets")
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return LiveReport{}, err
		}
		item := LiveResult{FindingID: target.Finding.ID, Status: "metadata_matches", Message: "Selected root, ancestor, directory and manifest metadata matched during this check. Dependency contents were not checked."}
		if err := s.verifyTarget(ctx, target, nil); err != nil {
			if ctx.Err() != nil {
				return LiveReport{}, ctx.Err()
			}
			item.Status = "blocked"
			var failure liveError
			if errors.As(err, &failure) {
				item.Code, item.Message = failure.code, failure.message
			} else {
				item.Code, item.Message = "path_unavailable", "A path is missing, inaccessible, a symlink or otherwise unsupported. Leave the selection unchanged and review the folder."
			}
			r.Status = "blocked"
		}
		r.Targets = append(r.Targets, item)
	}
	r.CheckedAt = time.Now().UTC()
	return r, nil
}

type liveLink struct {
	fd            int
	name          string
	stamp         unix.Stat_t
	volume, mount string
}

func (s *Scanner) verifyTarget(ctx context.Context, t state.LiveTarget, beforeRecheck func()) error {
	return s.verifyTargetWithVisit(ctx, t, nil, nil, beforeRecheck)
}

// A visitor may inspect project inputs while the verified path handles remain
// open. Its final check runs before these handles are released.
func (s *Scanner) verifyTargetWithVisit(ctx context.Context, t state.LiveTarget, visit func(int, string) error, recheck func() error, beforeRecheck func()) error {
	root, path := string(t.Root.PathBytes), string(t.Finding.PathBytes)
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || strings.ContainsRune(root, 0) || len(root) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || len(path) > 4096 || !config.Within(path, root) || path == root || filepath.Base(path) != "node_modules" {
		return blocked("scope_invalid", "The saved target is not an exact dependency directory inside its saved root.")
	}
	if !s.roots[root] || s.excluded(root) || s.excluded(path) || s.excluded(string(t.Finding.ManifestPathBytes)) {
		return blocked("scope_excluded", "The selected root or target is no longer allowed by the current scope or exclusions.")
	}
	for _, excluded := range s.excludes {
		if config.Within(excluded, path) {
			return blocked("scope_excluded", "The selected directory contains a currently excluded or protected path.")
		}
	}
	if string(t.Finding.ManifestPathBytes) != filepath.Join(filepath.Dir(path), "package.json") || t.Binding.FindingID != t.Finding.ID {
		return blocked("evidence_unknown", "The saved manifest or target binding is inconsistent. Save a new selection after scanning.")
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	rootParts := len(strings.Split(strings.TrimPrefix(root, "/"), "/"))
	if root == "/" {
		rootParts = 0
	}
	if len(parts) > state.LivePathDepthLimit {
		return blocked("path_limit", "The path exceeds the 256-directory live check limit.")
	}
	expected := map[string]state.Entry{}
	for _, e := range t.Ancestors {
		expected[string(e.Path)] = e
	}
	rel, _ := filepath.Rel(root, path)
	expected[rel] = state.Entry{Path: []byte(rel), Kind: "directory", Device: t.Binding.Target.Device, Inode: t.Binding.Target.Inode, CtimeNS: t.Binding.Target.ChangedNS, MtimeNS: t.Finding.DirectoryModifiedAt.UnixNano()}
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, err := unix.Open("/", openFlags, 0)
	if err != nil {
		return err
	}
	links := []liveLink{{fd: fd, name: "/"}}
	defer func() {
		for _, link := range links {
			_ = unix.Close(link.fd)
		}
	}()
	if err = unix.Fstat(fd, &links[0].stamp); err != nil {
		return err
	}
	links[0].volume, links[0].mount, err = s.filesystem(fd)
	if err != nil {
		return blocked("filesystem_unsupported", "Filesystem or mount identity is unavailable for this path.")
	}
	var volume, mount string
	if rootParts == 0 {
		volume, mount = links[0].volume, links[0].mount
		if err = checkRoot(t.Root, root, volume, links[0].stamp); err != nil {
			return err
		}
		if e, ok := expected["."]; !ok || !matchesEntry(e, links[0].stamp) {
			return blocked("identity_changed", "The root metadata differs or lacks saved evidence.")
		}
	}
	abs := "/"
	for i, part := range parts {
		if err = ctx.Err(); err != nil {
			return err
		}
		abs = filepath.Join(abs, part)
		if s.excluded(abs) {
			return blocked("scope_excluded", "A path component is excluded or protected by the current configuration.")
		}
		next, err := s.openat(links[len(links)-1].fd, part)
		if err != nil {
			return err
		}
		links = append(links, liveLink{fd: next, name: part})
		link := &links[len(links)-1]
		if err = unix.Fstat(next, &link.stamp); err != nil {
			return err
		}
		link.volume, link.mount, err = s.filesystem(next)
		if err != nil {
			return blocked("filesystem_unsupported", "Filesystem or mount identity is unavailable for this path.")
		}
		if i+1 == rootParts {
			volume, mount = link.volume, link.mount
			if err = checkRoot(t.Root, root, volume, link.stamp); err != nil {
				return err
			}
		}
		if i+1 >= rootParts {
			if volume != link.volume || mount != link.mount {
				return blocked("mount_boundary", "The selected path crosses a filesystem or mount boundary below its root.")
			}
			relative := "."
			if i+1 > rootParts {
				relative = filepath.Join(parts[rootParts : i+1]...)
			}
			e, ok := expected[relative]
			if !ok || e.Device == "" || e.Inode == "" || e.CtimeNS <= 0 {
				return blocked("evidence_unknown", "A path component has no usable saved identity. Scan and review a new selection.")
			}
			if !matchesEntry(e, link.stamp) {
				return blocked("identity_changed", "A root, ancestor or dependency directory changed since the saved observation. Scan and review a new selection.")
			}
		}
	}
	parent := links[len(links)-2].fd
	manifest, err := s.manifestStamp(parent, t, mount)
	if err != nil {
		return err
	}
	if visit != nil {
		if err = visit(parent, mount); err != nil {
			return err
		}
	}
	if beforeRecheck != nil {
		beforeRecheck()
	}
	// Retain and recheck every parent link. A detached open directory must not
	// be mistaken for the object currently reachable through the reviewed path.
	for i, link := range links {
		if err = ctx.Err(); err != nil {
			return err
		}
		var current unix.Stat_t
		if err = unix.Fstat(link.fd, &current); err != nil {
			return err
		}
		if objectID(current) != objectID(link.stamp) || (i >= rootParts && !sameStamp(current, link.stamp)) {
			return blocked("path_changed_during_check", "A checked directory changed during validation. Retry after reviewing the folder.")
		}
		if i == 0 {
			continue
		}
		checkFD, err := s.openat(links[i-1].fd, link.name)
		if err != nil {
			return blocked("path_changed_during_check", "A path link changed or became unavailable during validation.")
		}
		var named unix.Stat_t
		statErr := unix.Fstat(checkFD, &named)
		v, m, fsErr := s.filesystem(checkFD)
		_ = unix.Close(checkFD)
		if statErr != nil || fsErr != nil || objectID(named) != objectID(link.stamp) || (i >= rootParts && !sameStamp(named, link.stamp)) || v != link.volume || m != link.mount {
			return blocked("path_changed_during_check", "A path link or mount changed during validation.")
		}
	}
	after, err := s.manifestStamp(parent, t, mount)
	if err != nil {
		return err
	}
	if !sameStamp(manifest, after) {
		return blocked("path_changed_during_check", "The manifest changed during validation.")
	}
	if recheck != nil {
		if err = recheck(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func checkRoot(root state.RootBinding, path, volume string, st unix.Stat_t) error {
	if root.Fingerprint == "" || root.Fingerprint != rootFingerprint(path, volume, st) {
		return blocked("root_changed", "The live root or volume does not match the saved root. Scan and review a new selection.")
	}
	return nil
}

func matchesEntry(e state.Entry, st unix.Stat_t) bool {
	m, c := timestamps(&st)
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && !dataless(st) && fmt.Sprint(st.Dev) == e.Device && fmt.Sprint(st.Ino) == e.Inode && c == e.CtimeNS && m == e.MtimeNS
}

func (s *Scanner) manifestStamp(parent int, t state.LiveTarget, mount string) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(parent, "package.json", &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return st, err
	}
	m, c := timestamps(&st)
	e := t.Binding.Manifest
	if st.Mode&unix.S_IFMT != unix.S_IFREG || dataless(st) || s.protectedIDs[objectID(st)] || e.Device == "" || e.Inode == "" || e.ChangedNS <= 0 || fmt.Sprint(st.Dev) != e.Device || fmt.Sprint(st.Ino) != e.Inode || c != e.ChangedNS || m != t.Finding.ManifestModifiedAt.UnixNano() || e.Device != t.Binding.Target.Device {
		return st, blocked("manifest_changed", "The manifest is changed, unknown, a symlink or not a supported regular file. Scan and review a new selection.")
	}
	if err := verifyManifestMount(parent, mount); err != nil {
		return st, blocked("mount_boundary", "The manifest's mount identity differs or is unavailable.")
	}
	return st, nil
}
