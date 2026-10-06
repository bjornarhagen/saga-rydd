package inventory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const (
	FileSampleContract            = "file_samples_v1"
	FileSampleTargetLimit         = 20
	FileSampleWindowBytes   int64 = 32 * 1024
	FileSampleFileByteLimit int64 = 3 * FileSampleWindowBytes
	FileSampleCallByteLimit int64 = FileSampleTargetLimit * FileSampleFileByteLimit
	fileSampleEvidenceLimit       = 1024 * 1024
	fileSampleTimeout             = 5 * time.Second
)

// SavedFileTarget supplies one exact ordinary file and its ordered directory
// observations, from the root (".") through the file's parent. A store can
// capture these records with PrepareFileSampleSelection. This sampler does not
// independently verify their database provenance, inventory token, saved root
// revision or generation against a store.
type SavedFileTarget = state.FileSampleTarget

type FileSampleRange struct {
	Offset int64 `json:"offset"`
	Length int64 `json:"length"`
}

type FileSampleResult struct {
	Target         SavedFileTarget   `json:"target"`
	Status         string            `json:"status"`
	Code           string            `json:"code,omitempty"`
	Message        string            `json:"message"`
	Ranges         []FileSampleRange `json:"ranges"`
	RequestedBytes int64             `json:"requested_bytes"`
	ReadBytes      int64             `json:"read_bytes"`
	SHA256         string            `json:"sha256,omitempty"`
	LiveLinkCount  uint64            `json:"live_link_count"`
	// RepeatedSavedIdentity uses caller-supplied device/inode records only. It
	// does not establish continuous live identity or independent physical copies.
	RepeatedSavedIdentity bool `json:"repeated_saved_identity"`
}

type FileSampleReport struct {
	Status                    string             `json:"status"`
	Source                    string             `json:"source"`
	EvidenceSource            string             `json:"evidence_source"`
	Contract                  string             `json:"contract"`
	CheckedAt                 time.Time          `json:"checked_at"`
	ProvenanceVerified        bool               `json:"provenance_verified"`
	ContentVerified           bool               `json:"content_verified"`
	CurrentStateVerified      bool               `json:"current_state_verified"`
	DuplicatesVerified        bool               `json:"duplicates_verified"`
	Executable                bool               `json:"executable"`
	EstimatedReclaimableBytes *int64             `json:"estimated_reclaimable_bytes"`
	RequestedBytes            int64              `json:"requested_bytes"`
	ReadBytes                 int64              `json:"read_bytes"`
	Targets                   []FileSampleResult `json:"targets"`
}

type fileSampleBudget struct{ requested, read int64 }

// Hooks are private deterministic race seams; production calls supply none.
type fileSampleHooks struct {
	afterOpen        func()
	afterRead        func(int)
	beforeFinalCheck func()
}

// ObserveFileSamples explicitly reads at most three merged 32 KiB windows per
// selected file. Equal digests describe equal samples, never exact duplicates
// or permission to remove a file. No digest is retained on a failed check or
// cancellation. The fixed five-second deadline is cooperative: an individual
// filesystem operation can outlast it. No database or directory enumeration is
// involved, and no source bytes are returned. Callers must not mutate targets
// or the scanner's scope while this call runs. Targets are observed sequentially,
// with each file and its parent chain closed before opening the next target.
// At most 258 handles are retained, plus one temporary directory recheck handle.
// Metadata rechecks do not establish one atomic content snapshot across writers
// or targets, nor continuous inode identity since the saved observations.
// Cancellation returns no report; future persistent read-budget accounting must
// account for attempted reads separately, including canceled requests.
func (s *Scanner) ObserveFileSamples(ctx context.Context, targets []SavedFileTarget) (FileSampleReport, error) {
	return s.observeFileSamples(ctx, targets, fileSampleHooks{})
}

func (s *Scanner) observeFileSamples(ctx context.Context, targets []SavedFileTarget, hooks fileSampleHooks) (FileSampleReport, error) {
	if err := boundedSampleRequest(targets); err != nil {
		return FileSampleReport{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, fileSampleTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return FileSampleReport{}, err
	}
	if s.closed.Load() {
		return FileSampleReport{}, errors.New("scanner closed")
	}
	r := FileSampleReport{Status: "samples_observed", Source: "live_file_samples", EvidenceSource: "caller_supplied_saved_records", Contract: FileSampleContract, Targets: []FileSampleResult{}}
	var budget fileSampleBudget
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return FileSampleReport{}, err
		}
		item := FileSampleResult{Target: target, Status: "samples_observed", Message: "Selected sample ranges were read and the held path metadata was rechecked. Equal samples do not prove equal file contents.", Ranges: []FileSampleRange{}}
		// The display path may have lost invalid UTF-8 in a JSON round trip.
		// Only PathBytes identifies the selected filesystem name.
		item.Target.File.Path = string(target.File.PathBytes)
		before := budget
		err := validateSampleTarget(target)
		if err == nil {
			err = conflictingSampleIdentity(target, targets)
		}
		if err == nil {
			item.RepeatedSavedIdentity = repeatedSampleIdentity(target, targets)
			err = s.sampleFile(ctx, target, &budget, &item, hooks)
		}
		if ctx.Err() != nil {
			return FileSampleReport{}, ctx.Err()
		}
		item.RequestedBytes, item.ReadBytes = budget.requested-before.requested, budget.read-before.read
		if err != nil {
			item.Status, item.SHA256, r.Status = "blocked", "", "blocked"
			var failure liveError
			if errors.As(err, &failure) {
				item.Code, item.Message = failure.code, failure.message
			} else {
				item.Code, item.Message = "path_unavailable", "The selected path is missing, inaccessible, a symlink or otherwise unsupported. No sample digest is available."
			}
		}
		r.Targets = append(r.Targets, item)
	}
	if err := ctx.Err(); err != nil {
		return FileSampleReport{}, err
	}
	if s.closed.Load() {
		return FileSampleReport{}, errors.New("scanner closed")
	}
	r.RequestedBytes, r.ReadBytes, r.CheckedAt = budget.requested, budget.read, time.Now().UTC()
	return r, nil
}

// Bound caller-controlled evidence before constructing maps or opening paths.
func boundedSampleRequest(targets []SavedFileTarget) error {
	if len(targets) < 1 || len(targets) > FileSampleTargetLimit {
		return errors.New("file samples require 1–20 exact targets")
	}
	bytes := 0
	for _, t := range targets {
		if len(t.Ancestors) > state.LivePathDepthLimit+1 {
			return errors.New("file sample ancestor evidence exceeds the 256-directory limit")
		}
		add := func(length int) bool {
			if length > 4096 || length > fileSampleEvidenceLimit-bytes {
				return false
			}
			bytes += length
			return true
		}
		// JSON can replace each invalid UTF-8 byte in the display string with
		// a three-byte replacement character. Byte paths remain authoritative.
		if len(t.File.Path) > 3*4096 || len(t.File.Path) > fileSampleEvidenceLimit-bytes {
			return errors.New("file sample evidence exceeds its bounded size")
		}
		bytes += len(t.File.Path)
		lengths := []int{len(t.InventoryID), len(t.Root.PathBytes), len(t.Root.Fingerprint), len(t.File.PathBytes), len(t.File.Device), len(t.File.Inode), len(t.File.ParentPass), len(t.File.SkipReason)}
		for _, e := range t.Ancestors {
			lengths = append(lengths, len(e.Path), len(e.Kind), len(e.Device), len(e.Inode), len(e.SkipReason))
		}
		for _, length := range lengths {
			if !add(length) {
				return errors.New("file sample evidence exceeds its bounded size")
			}
		}
	}
	seenIDs, seenPaths := map[string]bool{}, map[string]bool{}
	ancestors := map[string]state.Entry{}
	for _, t := range targets {
		id := fmt.Sprintf("%s:%d", t.InventoryID, t.File.ID)
		path := string(t.File.PathBytes)
		if seenIDs[id] || seenPaths[path] {
			return errors.New("file samples require distinct exact file IDs and paths")
		}
		seenIDs[id], seenPaths[path] = true, true
		if t.InventoryID != targets[0].InventoryID {
			return errors.New("file samples require one caller-supplied inventory token")
		}
		for _, previous := range targets {
			if previous.Root.ID == t.Root.ID && (previous.Root.Revision != t.Root.Revision || previous.Root.Fingerprint != t.Root.Fingerprint || string(previous.Root.PathBytes) != string(t.Root.PathBytes)) {
				return errors.New("file samples require consistent saved root bindings")
			}
			if string(previous.Root.PathBytes) == string(t.Root.PathBytes) && previous.Root.ID != t.Root.ID {
				return errors.New("file samples require one saved root binding per root path")
			}
		}
		for _, entry := range t.Ancestors {
			key := fmt.Sprintf("%d:%s", t.Root.ID, entry.Path)
			if previous, exists := ancestors[key]; exists && (previous.Kind != entry.Kind || previous.Device != entry.Device || previous.Inode != entry.Inode || previous.Size != entry.Size || previous.Allocated != entry.Allocated || previous.MtimeNS != entry.MtimeNS || previous.CtimeNS != entry.CtimeNS || previous.SkipReason != entry.SkipReason) {
				return errors.New("file samples require consistent saved ancestor observations")
			}
			ancestors[key] = entry
		}
	}
	return nil
}

func sampleIdentityNumber(value string, nonzero bool) bool {
	if len(value) == 0 || len(value) > 20 {
		return false
	}
	n, err := strconv.ParseUint(value, 10, 64)
	return err == nil && (!nonzero || n != 0) && strconv.FormatUint(n, 10) == value
}

func sampleHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			return false
		}
	}
	return true
}

func validateSampleTarget(t SavedFileTarget) error {
	root, path := string(t.Root.PathBytes), string(t.File.PathBytes)
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || strings.ContainsRune(root, 0) || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || path == root || !config.Within(path, root) {
		return blocked("scope_invalid", "The saved file is not an exact ordinary path inside its saved root.")
	}
	if !sampleHex(t.InventoryID) || t.Root.ID <= 0 || t.Root.Revision <= 0 || !strings.HasPrefix(t.Root.Fingerprint, "v1:") || !sampleHex(strings.TrimPrefix(t.Root.Fingerprint, "v1:")) || t.File.ID <= 0 || t.File.RootID != t.Root.ID || t.File.Size < 0 || t.File.Allocated < 0 || t.File.ObservedAt.IsZero() || t.File.ModifiedAt.IsZero() || t.File.ChangedNS <= 0 || t.File.Generation <= 0 || !sampleIdentityNumber(t.File.Device, false) || !sampleIdentityNumber(t.File.Inode, true) || t.File.SkipReason != "" || t.File.ParentPass != "observed_in_completed_parent_pass" {
		return blocked("evidence_unknown", "The saved inventory, root or ordinary-file evidence is missing or inconsistent.")
	}
	parent := filepath.Dir(path)
	for _, part := range strings.Split(parent, string(filepath.Separator)) {
		if part == "node_modules" {
			return blocked("generated_dependency", "Ordinary file sampling excludes generated node_modules trees, including a root inside one.")
		}
	}
	parts := samplePathParts(parent)
	if len(parts) > state.LivePathDepthLimit {
		return blocked("path_limit", "The selected path exceeds the 256-directory sample limit.")
	}
	rootParts := len(samplePathParts(root))
	if len(t.Ancestors) != len(parts)-rootParts+1 {
		return blocked("evidence_unknown", "Exact saved observations are required for every directory from the root through the file's parent.")
	}
	for i, e := range t.Ancestors {
		relative := "."
		if i > 0 {
			relative = filepath.Join(parts[rootParts : rootParts+i]...)
		}
		if string(e.Path) != relative || e.Kind != "directory" || e.SkipReason != "" || !sampleIdentityNumber(e.Device, false) || !sampleIdentityNumber(e.Inode, true) || e.CtimeNS <= 0 || e.MtimeNS == 0 {
			return blocked("evidence_unknown", "The ordered saved ancestor evidence is missing, ambiguous or unsupported.")
		}
	}
	return nil
}

func samplePathParts(path string) []string {
	if path == "/" {
		return nil
	}
	return strings.Split(strings.TrimPrefix(path, "/"), "/")
}

func repeatedSampleIdentity(t SavedFileTarget, targets []SavedFileTarget) bool {
	for _, other := range targets {
		if other.File.ID != t.File.ID && other.File.Device == t.File.Device && other.File.Inode == t.File.Inode {
			return true
		}
	}
	return false
}

func conflictingSampleIdentity(t SavedFileTarget, targets []SavedFileTarget) error {
	for _, other := range targets {
		if other.File.Device == t.File.Device && other.File.Inode == t.File.Inode && (other.File.Size != t.File.Size || other.File.Allocated != t.File.Allocated || other.File.ChangedNS != t.File.ChangedNS || !other.File.ModifiedAt.Equal(t.File.ModifiedAt)) {
			return blocked("identity_conflict", "Selected saved paths disagree about the same filesystem object. No sample digest is available.")
		}
	}
	return nil
}

// Windows cover the start, centre and end. Merge overlap (and adjacency), so
// small files are never read twice. Arithmetic remains safe through MaxInt64.
func fileSampleRanges(size int64) []FileSampleRange {
	if size <= 0 {
		return []FileSampleRange{}
	}
	width := min(size, FileSampleWindowBytes)
	windows := []FileSampleRange{{0, width}, {(size - width) / 2, width}, {size - width, width}}
	ranges := make([]FileSampleRange, 0, 3)
	for _, window := range windows {
		if len(ranges) > 0 {
			last := &ranges[len(ranges)-1]
			if window.Offset <= last.Offset+last.Length {
				last.Length = max(last.Offset+last.Length, window.Offset+window.Length) - last.Offset
				continue
			}
		}
		ranges = append(ranges, window)
	}
	return ranges
}

func (s *Scanner) supportedSampleFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Size >= 0 && !dataless(st) && !s.protectedIDs[objectID(st)]
}

func matchesSampleFile(t SavedFileTarget, st unix.Stat_t) bool {
	m, c := timestamps(&st)
	return fmt.Sprint(st.Dev) == t.File.Device && fmt.Sprint(st.Ino) == t.File.Inode && st.Size == t.File.Size && m == t.File.ModifiedAt.UnixNano() && c == t.File.ChangedNS
}

func (s *Scanner) sampleFile(ctx context.Context, t SavedFileTarget, budget *fileSampleBudget, item *FileSampleResult, hooks fileSampleHooks) error {
	root, path := string(t.Root.PathBytes), string(t.File.PathBytes)
	if !s.roots[root] || s.excluded(root) || s.excluded(path) {
		return blocked("scope_excluded", "The selected root or file is outside the current scope or exclusions.")
	}
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
		return blocked("filesystem_unsupported", "Filesystem or mount identity is unavailable for the selected path.")
	}
	parts, rootParts := samplePathParts(filepath.Dir(path)), len(samplePathParts(root))
	var volume, mount string
	checkDirectory := func(index int, link liveLink) error {
		if index == rootParts {
			volume, mount = link.volume, link.mount
			if err := checkRoot(t.Root, root, volume, link.stamp); err != nil {
				return err
			}
		}
		if index >= rootParts {
			if link.volume != volume || link.mount != mount {
				return blocked("mount_boundary", "The selected path crosses a filesystem or mount boundary below its root.")
			}
			if !matchesEntry(t.Ancestors[index-rootParts], link.stamp) {
				return blocked("identity_changed", "The root or a parent directory differs from its saved observation.")
			}
		}
		return nil
	}
	if rootParts == 0 {
		if err = checkDirectory(0, links[0]); err != nil {
			return err
		}
	}
	abs := "/"
	for i, part := range parts {
		if err = ctx.Err(); err != nil {
			return err
		}
		abs = filepath.Join(abs, part)
		if s.excluded(abs) {
			return blocked("scope_excluded", "A selected path component is excluded or protected.")
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
			return blocked("filesystem_unsupported", "Filesystem or mount identity is unavailable for a selected directory.")
		}
		if err = checkDirectory(i+1, *link); err != nil {
			return err
		}
	}
	parent, name := links[len(links)-1].fd, filepath.Base(path)
	var before unix.Stat_t
	if err = unix.Fstatat(parent, name, &before, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !s.supportedSampleFile(before) || !matchesSampleFile(t, before) {
		return blocked("file_changed", "The selected file is changed, unknown, protected, a placeholder or not an ordinary regular file.")
	}
	if err = verifyNamedMount(parent, name, mount); err != nil {
		return blocked("mount_boundary", "The selected file crosses a mount boundary or lacks mount evidence.")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	fileFD, err := unix.Openat(parent, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fileFD)
	var opened unix.Stat_t
	if err = unix.Fstat(fileFD, &opened); err != nil {
		return err
	}
	if !s.supportedSampleFile(opened) || !sameInputStamp(before, opened) {
		return blocked("file_changed_during_check", "The selected file changed while opening. No sample digest is available.")
	}
	v, m, fsErr := s.filesystem(fileFD)
	if fsErr != nil || v != volume || m != mount {
		return blocked("mount_boundary", "The opened file differs from the root filesystem or mount.")
	}
	item.LiveLinkCount = uint64(opened.Nlink)
	if hooks.afterOpen != nil {
		hooks.afterOpen()
	}
	ranges := fileSampleRanges(opened.Size)
	item.Ranges = ranges
	h := sha256.New()
	_, _ = h.Write([]byte("saga-rydd:" + FileSampleContract + "\x00"))
	var frame [8]byte
	writeNumber := func(value int64) { binary.BigEndian.PutUint64(frame[:], uint64(value)); _, _ = h.Write(frame[:]) }
	writeNumber(opened.Size)
	writeNumber(int64(len(ranges)))
	var buffer [32 * 1024]byte
	for _, sample := range ranges {
		writeNumber(sample.Offset)
		writeNumber(sample.Length)
		for consumed := int64(0); consumed < sample.Length; {
			if err = ctx.Err(); err != nil {
				return err
			}
			if s.closed.Load() {
				return blocked("scanner_closed", "The scanner closed before sample observation completed.")
			}
			want := min(int64(len(buffer)), sample.Length-consumed)
			if want > FileSampleCallByteLimit-budget.requested {
				return blocked("sample_limit", "The fixed per-call sample byte limit was reached.")
			}
			// Charge every requested read, including a short or failed read. There
			// is no extra byte probe and no content read during final validation.
			budget.requested += want
			n, readErr := unix.Pread(fileFD, buffer[:int(want)], sample.Offset+consumed)
			if n > 0 {
				budget.read += int64(n)
				_, _ = h.Write(buffer[:n])
				consumed += int64(n)
			}
			if hooks.afterRead != nil {
				hooks.afterRead(max(n, 0))
			}
			if readErr != nil || int64(n) != want {
				return blocked("sample_incomplete", "A selected range could not be read completely. No sample digest is available.")
			}
		}
	}
	if hooks.beforeFinalCheck != nil {
		hooks.beforeFinalCheck()
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return blocked("scanner_closed", "The scanner closed before sample observation completed.")
	}
	// Retain and recheck every parent. Open handles to detached directories
	// alone are not evidence that the selected named path remains the same.
	for i, link := range links {
		if err = ctx.Err(); err != nil {
			return err
		}
		var current unix.Stat_t
		if err = unix.Fstat(link.fd, &current); err != nil {
			return err
		}
		v, m, fsErr := s.filesystem(link.fd)
		if fsErr != nil || v != link.volume || m != link.mount || objectID(current) != objectID(link.stamp) || (i >= rootParts && !sameInputStamp(current, link.stamp)) {
			return blocked("path_changed_during_check", "A held directory or its mount changed during sample observation.")
		}
		if i == 0 {
			continue
		}
		checkFD, err := s.openat(links[i-1].fd, link.name)
		if err != nil {
			return blocked("path_changed_during_check", "A named directory link changed or became unavailable during sample observation.")
		}
		var named unix.Stat_t
		statErr := unix.Fstat(checkFD, &named)
		v, m, fsErr = s.filesystem(checkFD)
		_ = unix.Close(checkFD)
		if statErr != nil || fsErr != nil || objectID(named) != objectID(link.stamp) || (i >= rootParts && !sameInputStamp(named, link.stamp)) || v != link.volume || m != link.mount {
			return blocked("path_changed_during_check", "A named directory link or mount changed during sample observation.")
		}
	}
	var held, named unix.Stat_t
	if err = unix.Fstat(fileFD, &held); err != nil {
		return err
	}
	if err = unix.Fstatat(parent, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return blocked("file_changed_during_check", "The selected file became unavailable during sample observation.")
	}
	if !s.supportedSampleFile(held) || !s.supportedSampleFile(named) || !sameInputStamp(before, held) || !sameInputStamp(before, named) {
		return blocked("file_changed_during_check", "The file or its named link changed during sample observation.")
	}
	v, m, fsErr = s.filesystem(fileFD)
	if fsErr != nil || v != volume || m != mount {
		return blocked("mount_boundary", "The held file's filesystem or mount changed during sample observation.")
	}
	if err = verifyNamedMount(parent, name, mount); err != nil {
		return blocked("mount_boundary", "The named file's mount changed during sample observation.")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if s.closed.Load() {
		return blocked("scanner_closed", "The scanner closed before sample observation completed.")
	}
	item.SHA256 = hex.EncodeToString(h.Sum(nil))
	return nil
}
