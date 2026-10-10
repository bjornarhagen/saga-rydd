//go:build darwin || linux

package doctor

import (
	"context"
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func platformSupported() bool { return true }

type heldDirectory struct {
	fd     int
	parent int
	name   string
	stamp  unix.Stat_t
}
type heldFile struct {
	fd    int
	index int
	stamp unix.Stat_t
}
type unixReader struct {
	ctx      context.Context
	report   *Report
	dirs     []heldDirectory
	files    []heldFile
	attempts int
	reads    int
	closeFD  func(int) error
}

func (r *unixReader) charge() error {
	if err := r.ctx.Err(); err != nil {
		return err
	}
	if r.attempts >= MaxMetadataAttempts {
		return ErrBounds
	}
	r.attempts++
	return nil
}

func (r *unixReader) statAt(fd int, name string) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := r.charge(); err != nil {
		return st, err
	}
	err := unix.Fstatat(fd, name, &st, unix.AT_SYMLINK_NOFOLLOW)
	return st, err
}

func (r *unixReader) statFD(fd int) (unix.Stat_t, error) {
	var st unix.Stat_t
	if err := r.charge(); err != nil {
		return st, err
	}
	err := unix.Fstat(fd, &st)
	return st, err
}

func (r *unixReader) close() error {
	var result error
	for i := len(r.files) - 1; i >= 0; i-- {
		result = errors.Join(result, r.closeFD(r.files[i].fd))
	}
	for i := len(r.dirs) - 1; i >= 0; i-- {
		result = errors.Join(result, r.closeFD(r.dirs[i].fd))
	}
	return result
}

func sameDirectory(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode && a.Uid == b.Uid && a.Gid == b.Gid
}

func sameFile(a, b unix.Stat_t) bool {
	return sameDirectory(a, b) && a.Nlink == b.Nlink && a.Size == b.Size && a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func privateDirectory(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFDIR && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid())
}
func privateRegular(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&0077 == 0 && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 && st.Size >= 0
}

func fileMetadata(st unix.Stat_t, directory bool) *Metadata {
	kind := "other"
	switch st.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		kind = "regular_file"
	case unix.S_IFDIR:
		kind = "directory"
	case unix.S_IFLNK:
		kind = "symlink"
	case unix.S_IFIFO:
		kind = "fifo"
	case unix.S_IFSOCK:
		kind = "socket"
	}
	suitable := privateRegular(st)
	m := &Metadata{Kind: kind, Suitable: &suitable}
	// The regular-file single-link predicate is not applicable to a directory.
	if directory {
		m.Suitable = nil
	} else if st.Size >= 0 {
		value := st.Size
		m.LogicalBytes = &value
	}
	return m
}

func (r *unixReader) walk(path string) error {
	if err := ValidateDirectory(path); err != nil {
		return err
	}
	if err := r.charge(); err != nil {
		return err
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	r.dirs = append(r.dirs, heldDirectory{fd: fd, parent: -1})
	st, err := r.statFD(fd)
	if err != nil {
		return err
	}
	r.dirs[0].stamp = st
	for _, name := range directoryComponents(path) {
		if err = r.charge(); err != nil {
			return err
		}
		child, openErr := unix.Openat(fd, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if openErr != nil {
			return openErr
		}
		r.dirs = append(r.dirs, heldDirectory{fd: child, parent: fd, name: name})
		held, e := r.statFD(child)
		if e != nil {
			return e
		}
		named, e := r.statAt(fd, name)
		if e != nil {
			return e
		}
		if held.Mode&unix.S_IFMT != unix.S_IFDIR || !sameDirectory(held, named) {
			return ErrChanged
		}
		r.dirs[len(r.dirs)-1].stamp = held
		fd = child
	}
	return nil
}

func (r *unixReader) recheckDirectories() error {
	for _, d := range r.dirs {
		held, err := r.statFD(d.fd)
		if err != nil {
			return err
		}
		if !sameDirectory(d.stamp, held) {
			return ErrChanged
		}
		if d.parent >= 0 {
			named, err := r.statAt(d.parent, d.name)
			if err != nil {
				return err
			}
			if !sameDirectory(held, named) {
				return ErrChanged
			}
		}
	}
	return nil
}

func (r *unixReader) openFile(base int, name string, index int, before unix.Stat_t) (int, error) {
	if err := r.charge(); err != nil {
		return -1, err
	}
	fd, err := unix.Openat(base, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return -1, err
	}
	r.files = append(r.files, heldFile{fd: fd, index: index, stamp: before})
	held, err := r.statFD(fd)
	if err != nil {
		return -1, err
	}
	if !privateRegular(held) || !sameFile(before, held) {
		return -1, ErrChanged
	}
	named, err := r.statAt(base, name)
	if err != nil {
		return -1, err
	}
	if !sameFile(held, named) {
		return -1, ErrChanged
	}
	return fd, nil
}

func (r *unixReader) read(fd int, size int64, single bool) ([]byte, error) {
	if size < 0 || size > MaxConfigBytes {
		return nil, ErrBounds
	}
	body := make([]byte, int(size))
	at := 0
	for at < len(body) {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		if r.reads >= MaxReadAttempts {
			return nil, ErrBounds
		}
		next := min(32<<10, len(body)-at)
		if single {
			next = len(body) - at
		}
		if *r.report.RequestedBodyBytes+int64(next) > MaxConfigBytes+HeaderBytes {
			return nil, ErrBounds
		}
		r.reads++
		*r.report.RequestedBodyBytes += int64(next)
		n, err := unix.Pread(fd, body[at:at+next], int64(at))
		if n < 0 || n > next {
			return nil, errors.Join(ErrIncomplete, err)
		}
		*r.report.ReadBodyBytes += int64(n)
		at += n
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, ErrChanged
		}
		if single && at != len(body) {
			return nil, ErrChanged
		}
	}
	return body, nil
}

func readExisting(ctx context.Context, req Request, validate ConfigValidator, report *Report) error {
	return readWithClose(ctx, req, validate, report, unix.Close)
}

// closeFD is private generated-fixture injection only; production uses unix.Close.
func readWithClose(ctx context.Context, req Request, validate ConfigValidator, report *Report, closeFD func(int) error) (result error) {
	r := &unixReader{ctx: ctx, report: report, closeFD: closeFD}
	defer func() { result = errors.Join(result, r.close()) }()
	if err := r.walk(req.DataDirectory); err != nil {
		status, reason, attention := "unavailable", "directory_unavailable", false
		if errors.Is(err, os.ErrNotExist) {
			status, reason, attention = "absent", "directory_absent", true
		}
		if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
			status, reason, attention = "unsafe", "directory_shape_refused", true
		}
		checkStatus(&report.Checks[0], status, reason, "Selected directory was not admitted; no child bodies were read.", attention)
		if attention {
			return nil
		}
		return err
	}
	base := r.dirs[len(r.dirs)-1]
	directory := &report.Checks[0]
	directory.Metadata = fileMetadata(base.stamp, true)
	if !privateDirectory(base.stamp) {
		checkStatus(directory, "unsafe", "directory_private_owner_refused", "Use a dedicated private directory; doctor does not change permissions or ownership.", true)
		return nil
	}
	checkStatus(directory, "present", "private_directory_observed", "This selected directory was admitted; ancestors were held without reading their directory contents.", false)
	var before [6]unix.Stat_t
	var exists [6]bool
	for i, name := range childNames {
		c := &report.Checks[i+1]
		st, err := r.statAt(base.fd, name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				checkStatus(c, "absent", "selected_name_absent", "Absence is an observation, not permission to initialize or a statement about worker activity.", i < 2)
			} else {
				checkStatus(c, "unavailable", "metadata_unavailable", "Selected metadata was unavailable; contents were not read.", false)
				result = errors.Join(result, err)
			}
			continue
		}
		before[i], exists[i] = st, true
		c.Metadata = fileMetadata(st, false)
		if !privateRegular(st) {
			checkStatus(c, "unsafe", "private_regular_shape_refused", "Selected name is not a private, owned, single-link regular file. No repair or body read was performed.", true)
			continue
		}
		checkStatus(c, "present", "private_regular_metadata_observed", "Only metadata was observed. File presence does not prove live health or lock availability.", false)
	}
	// Capture all six names before either body read or configuration validation.
	for i, name := range childNames[:2] {
		st := before[i]
		if !exists[i] || !privateRegular(st) {
			continue
		}
		c := &report.Checks[i+1]
		if i == 0 && st.Size > MaxConfigBytes {
			checkStatus(c, "unsupported_profile", "configuration_byte_bound", "Configuration exceeds this bounded doctor profile; general configuration validity was not evaluated.", true)
			report.attentionError = errors.Join(report.attentionError, ErrConfigUnsupported)
			continue
		}
		fd, err := r.openFile(base.fd, name, i, st)
		if err != nil {
			checkStatus(c, "changed", "body_admission_refused", "Held and named body admission failed; no current object identity is established.", false)
			result = errors.Join(result, err)
			continue
		}
		length := st.Size
		if i == 1 {
			length = min(length, int64(HeaderBytes))
		}
		body, err := r.read(fd, length, i == 1)
		if err != nil {
			checkStatus(c, "unavailable", "body_read_unavailable", "Bounded selected body read did not finish.", false)
			result = errors.Join(result, err)
			continue
		}
		if i == 0 {
			cfg, validationErr := validate(body)
			switch {
			case validationErr == nil:
				c.Configuration = &cfg
				checkStatus(c, "valid_config", "configuration_validated", "Current syntax/default/version/path/budget validation passed within the doctor subset. Roots were not inspected.", false)
			case errors.Is(validationErr, ErrConfigUnsupported):
				report.attentionError = errors.Join(report.attentionError, validationErr)
				checkStatus(c, "unsupported_profile", "configuration_subset_bound", "Configuration is outside this doctor subset; this does not declare it globally invalid.", true)
			case errors.Is(validationErr, ErrConfigEnvironment):
				checkStatus(c, "validation_unavailable", "configuration_environment_unavailable", "Required path expansion could not be validated; no HOME-derived files were selected.", false)
				result = errors.Join(result, validationErr)
			case errors.Is(validationErr, ErrConfigInvalid):
				report.attentionError = errors.Join(report.attentionError, validationErr)
				checkStatus(c, "invalid_config", "configuration_invalid", "Configuration did not pass current validation. Raw configuration and underlying error text are withheld.", true)
			default:
				checkStatus(c, "unavailable", "configuration_validation_unavailable", "Configuration validation did not finish.", false)
				result = errors.Join(result, validationErr)
			}
		} else {
			h, declaration := headerObservation(body, req.Catalog)
			c.Header = h
			checkStatus(c, declaration, declaration, "These are main-file header bytes only. WAL/journal state, current effective schema, database readability and whole migration ledger integrity remain unknown.", declaration != "declared_reader_range")
		}
	}
	for i, name := range childNames {
		c := &report.Checks[i+1]
		after, err := r.statAt(base.fd, name)
		unchanged := (!exists[i] && errors.Is(err, os.ErrNotExist)) || (exists[i] && err == nil && sameFile(before[i], after))
		if !unchanged {
			checkStatus(c, "changed", "selected_metadata_changed", "The paired selected-name metadata did not match; earlier observations are historical only.", false)
			result = errors.Join(result, ErrChanged, err)
		} else if c.Metadata != nil {
			c.Metadata.IdentityRechecked = true
		}
	}
	for _, f := range r.files {
		after, err := r.statFD(f.fd)
		if err != nil || !sameFile(f.stamp, after) {
			c := &report.Checks[f.index+1]
			if c.Metadata != nil {
				c.Metadata.IdentityRechecked = false
			}
			checkStatus(c, "changed", "held_body_changed", "Held body metadata changed; no current identity is established.", false)
			result = errors.Join(result, ErrChanged, err)
		}
	}
	if err := r.recheckDirectories(); err != nil {
		checkStatus(directory, "changed", "directory_identity_changed", "Selected directory/ancestor metadata changed; earlier observations are historical only.", false)
		for i := range report.Checks {
			if report.Checks[i].Metadata != nil {
				report.Checks[i].Metadata.IdentityRechecked = false
			}
		}
		result = errors.Join(result, err)
	} else {
		directory.Metadata.IdentityRechecked = true
	}
	for i := 3; i <= 5; i++ {
		c := &report.Checks[i]
		if c.Status == "present" && c.Metadata != nil && c.Metadata.LogicalBytes != nil && *c.Metadata.LogicalBytes > 0 {
			c.Qualification = "Nonempty sidecar metadata was observed; the main header may be outdated. No sidecar body, hot-journal test, checkpoint or recovery was performed."
		}
	}
	return result
}
