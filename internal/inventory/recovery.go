package inventory

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var ErrRecoveryRequest = errors.New("recovery metadata needs two distinct bounded absolute paths and known recorded directory identities")

// RecoveryIdentity is recorded evidence. Generation is a scanner generation,
// not a platform inode generation; it is never compared with live metadata.
type RecoveryIdentity struct {
	Device     string `json:"device"`
	Inode      string `json:"inode"`
	ChangedNS  int64  `json:"ctime_ns"`
	Generation int64  `json:"generation"`
}

type RecoveryRequest struct {
	SourcePathBytes      []byte           `json:"source_path_bytes"`
	DestinationPathBytes []byte           `json:"destination_path_bytes"`
	SourceParent         RecoveryIdentity `json:"source_parent"`
	SourceObject         RecoveryIdentity `json:"source_object"`
	DestinationParent    RecoveryIdentity `json:"destination_parent"`
}

// RecoveryObservedIdentity describes this request's metadata observation only.
// No inode generation or continuity with a historical mount is established.
type RecoveryObservedIdentity struct {
	Device    string `json:"device"`
	Inode     string `json:"inode"`
	ChangedNS int64  `json:"ctime_ns"`
	Kind      string `json:"kind"`
}

type RecoveryLocation struct {
	PathBytes          []byte                    `json:"path_bytes"`
	Status             string                    `json:"status"`
	Code               string                    `json:"code,omitempty"`
	Message            string                    `json:"message"`
	IdentityRelation   string                    `json:"identity_relation"`
	ParentIdentity     *RecoveryObservedIdentity `json:"parent_identity,omitempty"`
	ObjectIdentity     *RecoveryObservedIdentity `json:"object_identity,omitempty"`
	ParentCtimeChanged bool                      `json:"parent_ctime_changed"`
	ObjectCtimeChanged bool                      `json:"object_ctime_changed"`
	DeviceInodeMatch   bool                      `json:"device_inode_match"`
	CtimeMatch         bool                      `json:"ctime_match"`
}

type RecoveryReport struct {
	Status                  string           `json:"status"`
	Source                  string           `json:"source"`
	CheckedAt               time.Time        `json:"checked_at"`
	CurrentStateVerified    bool             `json:"current_state_verified"`
	Executable              bool             `json:"executable"`
	HistoricalMountVerified bool             `json:"historical_mount_verified"`
	ScopeVerified           bool             `json:"scope_verified"`
	SourceLocation          RecoveryLocation `json:"source_location"`
	DestinationLocation     RecoveryLocation `json:"destination_location"`
}

type recoveryChain struct {
	links []liveLink
	name  string
	path  string
	ref   RecoveryIdentity
}

func (chain *recoveryChain) close() {
	for _, link := range chain.links {
		_ = unix.Close(link.fd)
	}
	chain.links = nil
}

// ObserveRecovery reads metadata for two exact recorded locations. It opens no
// ordinary file contents, lists no directory and changes no file or record.
// Even two agreeing probes leave the operation outcome and retry permission
// unknown: these are finite observations, not an atomic namespace snapshot.
func ObserveRecovery(ctx context.Context, request RecoveryRequest) (RecoveryReport, error) {
	return observeRecovery(ctx, request, nil, nil)
}

func observeRecovery(ctx context.Context, request RecoveryRequest, beforeSecondProbe, beforeFinalCheck func()) (RecoveryReport, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	if !validRecoveryPath(request.SourcePathBytes) || !validRecoveryPath(request.DestinationPathBytes) || string(request.SourcePathBytes) == string(request.DestinationPathBytes) || !validRecoveryIdentity(request.SourceParent) || !validRecoveryIdentity(request.DestinationParent) || !validRecoveryIdentity(request.SourceObject) || request.SourceParent.Device != request.SourceObject.Device || request.DestinationParent.Device != request.SourceObject.Device {
		return RecoveryReport{}, ErrRecoveryRequest
	}
	report := RecoveryReport{Status: "outcome_unknown", Source: "live_recovery_metadata"}
	// Built-in system/provider protections apply without loading configuration
	// or inventory. No selected roots, directory stream or file bodies are read.
	scanner, err := New(nil, nil, nil)
	if err != nil {
		return RecoveryReport{}, errors.New("built-in recovery path protections are unavailable")
	}
	defer scanner.Close()
	paths := [][]byte{request.SourcePathBytes, request.DestinationPathBytes}
	parents := []RecoveryIdentity{request.SourceParent, request.DestinationParent}
	locations := []*RecoveryLocation{&report.SourceLocation, &report.DestinationLocation}
	chains := make([]*recoveryChain, 2)
	probes := make([]*unix.Stat_t, 2)
	for i, path := range paths {
		*locations[i] = RecoveryLocation{PathBytes: append([]byte(nil), path...), Status: "blocked", IdentityRelation: "unknown"}
		chain, err := scanner.openRecoveryChain(ctx, string(path), parents[i])
		if err != nil {
			if ctx.Err() != nil {
				return RecoveryReport{}, ctx.Err()
			}
			setRecoveryFailure(locations[i], err)
			continue
		}
		chains[i] = chain
		defer chain.close()
		probes[i], err = scanner.probeRecoveryChild(ctx, chain)
		if err != nil {
			if ctx.Err() != nil {
				return RecoveryReport{}, ctx.Err()
			}
			setRecoveryFailure(locations[i], err)
			chain.close()
			chains[i] = nil
		}
	}
	if beforeSecondProbe != nil {
		beforeSecondProbe()
	}
	for i, chain := range chains {
		if chain == nil {
			continue
		}
		after, err := scanner.probeRecoveryChild(ctx, chain)
		if err == nil && !sameRecoveryProbe(probes[i], after) {
			err = blocked("path_changed_during_check", "The exact child changed between metadata probes. Its location remains unknown.")
		}
		if err != nil {
			if ctx.Err() != nil {
				return RecoveryReport{}, ctx.Err()
			}
			setRecoveryFailure(locations[i], err)
			chain.close()
			chains[i] = nil
		}
	}
	if beforeFinalCheck != nil {
		beforeFinalCheck()
	}
	for i, chain := range chains {
		if chain == nil {
			continue
		}
		if err := scanner.recheckRecoveryChain(ctx, chain); err != nil {
			if ctx.Err() != nil {
				return RecoveryReport{}, ctx.Err()
			}
			setRecoveryFailure(locations[i], err)
			continue
		}
		final, err := scanner.probeRecoveryChild(ctx, chain)
		if err != nil || !sameRecoveryProbe(probes[i], final) {
			if ctx.Err() != nil {
				return RecoveryReport{}, ctx.Err()
			}
			setRecoveryFailure(locations[i], blocked("path_changed_during_check", "The exact child's final named metadata changed or became unavailable. Its location remains unknown."))
			continue
		}
		parent := chain.links[len(chain.links)-1].stamp
		locations[i].ParentIdentity = recoveryObserved(parent)
		_, parentCtime := timestamps(&parent)
		locations[i].ParentCtimeChanged = parentCtime != chain.ref.ChangedNS
		if probes[i] == nil {
			locations[i].Status = "observed_absent"
			locations[i].Message = "The final child was absent during both probes under its observed recorded parent. The operation outcome remains unknown."
			continue
		}
		object := *probes[i]
		locations[i].Status = "observed_present"
		locations[i].ObjectIdentity = recoveryObserved(object)
		locations[i].DeviceInodeMatch = fmt.Sprint(object.Dev) == request.SourceObject.Device && fmt.Sprint(object.Ino) == request.SourceObject.Inode
		_, ctime := timestamps(&object)
		locations[i].CtimeMatch = ctime == request.SourceObject.ChangedNS
		locations[i].ObjectCtimeChanged = !locations[i].CtimeMatch
		locations[i].IdentityRelation = "different_identity"
		locations[i].Message = "A different recorded device/inode was observed at this location. The operation outcome remains unknown."
		if locations[i].DeviceInodeMatch {
			locations[i].IdentityRelation = "potential_identity"
			locations[i].Message = "Recorded device/inode agree but ctime differs. This is a potential identity only; it does not prove a move."
			if locations[i].CtimeMatch {
				locations[i].IdentityRelation = "metadata_matches_reference"
				locations[i].Message = "Recorded device/inode/ctime agree during these probes. Contents, historical mount continuity and the operation outcome remain unknown."
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	report.CheckedAt = time.Now().UTC()
	return report, nil
}

func validRecoveryPath(path []byte) bool {
	p := string(path)
	return len(p) > 1 && len(p) <= 4096 && filepath.IsAbs(p) && filepath.Clean(p) == p && !strings.ContainsRune(p, 0) && strings.Count(p, "/") <= 256
}

func validRecoveryIdentity(identity RecoveryIdentity) bool {
	for i, number := range []string{identity.Device, identity.Inode} {
		if len(number) < 1 || len(number) > 20 {
			return false
		}
		value, err := strconv.ParseUint(number, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != number || (i == 1 && value == 0) {
			return false
		}
	}
	return identity.ChangedNS > 0 && identity.Generation >= 0
}

func (s *Scanner) openRecoveryChain(ctx context.Context, path string, reference RecoveryIdentity) (_ *recoveryChain, resultErr error) {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	chain := &recoveryChain{name: parts[len(parts)-1], path: path, ref: reference}
	if s.excluded(path) {
		return nil, blocked("scope_excluded", "The exact location is protected by a built-in path rule. Leave it unchanged.")
	}
	defer func() {
		if resultErr != nil {
			chain.close()
		}
	}()
	fd, err := unix.Open("/", openFlags, 0)
	if err != nil {
		return nil, blocked("path_unavailable", "A required ancestor cannot be read. Absence at the final location is unknown.")
	}
	chain.links = append(chain.links, liveLink{fd: fd, name: "/"})
	abs := "/"
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		link := &chain.links[len(chain.links)-1]
		if err := unix.Fstat(link.fd, &link.stamp); err != nil || link.stamp.Mode&unix.S_IFMT != unix.S_IFDIR || dataless(link.stamp) {
			return nil, blocked("ancestor_unsupported", "A required ancestor is unavailable or is not a supported directory. The final location is unknown.")
		}
		if s.protectedIDs[objectID(link.stamp)] {
			return nil, blocked("scope_excluded", "A required ancestor has a protected built-in identity. The final location is unknown.")
		}
		link.volume, link.mount, err = s.filesystem(link.fd)
		if err != nil {
			return nil, blocked("filesystem_unsupported", "Current filesystem or mount identity is unavailable. Historical continuity remains unknown.")
		}
		if i == len(parts)-1 {
			if fmt.Sprint(link.stamp.Dev) != reference.Device || fmt.Sprint(link.stamp.Ino) != reference.Inode {
				return nil, blocked("parent_identity_changed", "The exact parent device/inode differs from the recorded parent. The child location is unknown.")
			}
			return chain, nil
		}
		abs = filepath.Join(abs, parts[i])
		if s.excluded(abs) {
			return nil, blocked("scope_excluded", "A required ancestor is protected by a built-in path rule. The final location is unknown.")
		}
		var named unix.Stat_t
		if err := unix.Fstatat(link.fd, parts[i], &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return nil, blocked("path_unavailable", "A required ancestor is missing or inaccessible. This does not establish final-child absence.")
		}
		if named.Mode&unix.S_IFMT != unix.S_IFDIR || dataless(named) {
			return nil, blocked("ancestor_unsupported", "A required ancestor is a symlink or unsupported object. The final location is unknown.")
		}
		if s.protectedIDs[objectID(named)] {
			return nil, blocked("scope_excluded", "A required ancestor has a protected built-in identity. The final location is unknown.")
		}
		next, err := s.openat(link.fd, parts[i])
		if err != nil {
			return nil, blocked("path_unavailable", "A required ancestor changed or became inaccessible while opening. The final location is unknown.")
		}
		var held unix.Stat_t
		if err := unix.Fstat(next, &held); err != nil || !sameRecoveryStamp(named, held) {
			_ = unix.Close(next)
			return nil, blocked("path_changed_during_check", "A required ancestor changed while opening. The final location is unknown.")
		}
		chain.links = append(chain.links, liveLink{fd: next, name: parts[i]})
	}
}

func (s *Scanner) probeRecoveryChild(ctx context.Context, chain *recoveryChain) (*unix.Stat_t, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.excluded(chain.path) {
		return nil, blocked("scope_excluded", "The exact location is protected by a built-in path rule. Leave it unchanged.")
	}
	parent := chain.links[len(chain.links)-1]
	var st unix.Stat_t
	if err := unix.Fstatat(parent.fd, chain.name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil, nil
		}
		return nil, blocked("path_unavailable", "The exact child is inaccessible. Its presence or absence is unknown.")
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || dataless(st) {
		return nil, blocked("object_unsupported", "The exact child is a symlink, file, special object or unsupported directory. Leave it unchanged; the directory location is unknown.")
	}
	if s.protectedIDs[objectID(st)] {
		return nil, blocked("scope_excluded", "The exact child has a protected built-in identity. Leave it unchanged; its recovery location is unknown.")
	}
	if err := verifyNamedMount(parent.fd, chain.name, parent.mount); err != nil {
		return nil, blocked("mount_boundary", "The exact child's current mount differs or became unavailable. Historical mount continuity is unknown.")
	}
	fd, err := s.openat(parent.fd, chain.name)
	if err != nil {
		return nil, blocked("path_changed_during_check", "The exact directory changed or became unavailable while opening.")
	}
	defer unix.Close(fd)
	var held unix.Stat_t
	if err := unix.Fstat(fd, &held); err != nil || !sameRecoveryStamp(st, held) {
		return nil, blocked("path_changed_during_check", "The exact directory changed while opening.")
	}
	volume, mount, err := s.filesystem(fd)
	if err != nil || volume != parent.volume || mount != parent.mount || st.Dev != parent.stamp.Dev {
		return nil, blocked("mount_boundary", "The exact directory crosses a current filesystem or mount boundary. Historical continuity is unknown.")
	}
	return &st, nil
}

func (s *Scanner) recheckRecoveryChain(ctx context.Context, chain *recoveryChain) error {
	for i, link := range chain.links {
		if err := ctx.Err(); err != nil {
			return err
		}
		var held unix.Stat_t
		if err := unix.Fstat(link.fd, &held); err != nil || !sameRecoveryAncestorStamp(link.stamp, held) || (i == len(chain.links)-1 && !sameRecoveryStamp(link.stamp, held)) {
			return blocked("path_changed_during_check", "A held ancestor changed during observation. The final location remains unknown.")
		}
		volume, mount, err := s.filesystem(link.fd)
		if err != nil || volume != link.volume || mount != link.mount {
			return blocked("path_changed_during_check", "An ancestor mount changed during observation. The final location remains unknown.")
		}
		if i == 0 {
			continue
		}
		fd, err := s.openat(chain.links[i-1].fd, link.name)
		if err != nil {
			return blocked("path_changed_during_check", "An ancestor's named link changed or became unavailable during observation.")
		}
		var named unix.Stat_t
		statErr := unix.Fstat(fd, &named)
		volume, mount, mountErr := s.filesystem(fd)
		_ = unix.Close(fd)
		if statErr != nil || mountErr != nil || !sameRecoveryAncestorStamp(named, link.stamp) || (i == len(chain.links)-1 && !sameRecoveryStamp(named, link.stamp)) || volume != link.volume || mount != link.mount {
			return blocked("path_changed_during_check", "An ancestor's named identity or mount changed during observation.")
		}
	}
	return nil
}

func sameRecoveryAncestorStamp(a, b unix.Stat_t) bool {
	// Outer ancestors have no saved scope boundary. Their held/named identity,
	// permissions and live mount must agree, but unrelated siblings may change
	// their timestamps. The exact recorded parent gets the full stamp check.
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid && !dataless(a) && !dataless(b)
}

func sameRecoveryStamp(a, b unix.Stat_t) bool {
	return sameStamp(a, b) && a.Mode == b.Mode && a.Size == b.Size && a.Nlink == b.Nlink && a.Uid == b.Uid && a.Gid == b.Gid && !dataless(a) && !dataless(b)
}

func sameRecoveryProbe(a, b *unix.Stat_t) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return sameRecoveryStamp(*a, *b)
}

func recoveryObserved(st unix.Stat_t) *RecoveryObservedIdentity {
	_, ctime := timestamps(&st)
	return &RecoveryObservedIdentity{Device: fmt.Sprint(st.Dev), Inode: fmt.Sprint(st.Ino), ChangedNS: ctime, Kind: "directory"}
}

func setRecoveryFailure(location *RecoveryLocation, err error) {
	location.Status, location.IdentityRelation = "blocked", "unknown"
	var failure liveError
	if errors.As(err, &failure) {
		location.Code, location.Message = failure.code, failure.message
	} else {
		location.Code, location.Message = "path_unavailable", "A required path is unavailable. The exact location remains unknown."
	}
	if location.Code == "path_unavailable" {
		location.Status = "unavailable"
	}
}
