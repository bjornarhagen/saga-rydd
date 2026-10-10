//go:build darwin || linux

package doctor

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func doctorFixture(t *testing.T) string {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(base, "config.toml"), []byte("generated config"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(base, "state.sqlite3"), testHeader(16), 0600); err != nil {
		t.Fatal(err)
	}
	return base
}

func fixtureValidator(body []byte) (Configuration, error) {
	if !bytes.Equal(body, []byte("generated config")) {
		return Configuration{}, ErrConfigInvalid
	}
	return Configuration{Version: 1, Roots: 1}, nil
}

func fixtureObserve(base string, validate ConfigValidator) (Report, error) {
	return Observe(context.Background(), Request{base, testCatalog()}, validate)
}

func TestDoctorExistingOnlyScopes(t *testing.T) {
	base := doctorFixture(t)
	// These generated objects are outside the six-name profile. The command
	// must not enumerate/open them or require any corresponding service.
	if err := os.Mkdir(filepath.Join(base, "manual-inventories"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(base, "plans.sqlite3"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "state.sqlite3-wal"), []byte("metadata only"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	r, err := fixtureObserve(base, fixtureValidator)
	if err != nil || r.Status != "checks_finished" {
		t.Fatalf("%+v / %v", r, err)
	}
	if len(r.Checks) != 7 || *r.ReadBodyBytes != int64(len("generated config")+HeaderBytes) || *r.RequestedBodyBytes != *r.ReadBodyBytes {
		t.Fatalf("counts: %+v", r)
	}
	if r.Checks[3].Status != "present" || r.Checks[3].Metadata.LogicalBytes == nil || *r.Checks[3].Metadata.LogicalBytes != 13 {
		t.Fatalf("sidecar: %+v", r.Checks[3])
	}
	if r.CurrentStateSchema != nil || r.Effects != (Effects{}) || r.LockAvailability != "unknown" {
		t.Fatal("health/effects upgraded")
	}
	if r.Checks[0].Metadata == nil || r.Checks[0].Metadata.Suitable != nil {
		t.Fatal("regular-file single-link suitability must be null for directories")
	}
	after, err := os.ReadDir(base)
	if err != nil {
		t.Fatal(err)
	}
	names := func(es []os.DirEntry) []string {
		a := []string{}
		for _, e := range es {
			a = append(a, e.Name())
		}
		return a
	}
	if !reflect.DeepEqual(names(before), names(after)) {
		t.Fatal("instance entries changed")
	}
	for _, c := range r.Checks {
		if c.Metadata != nil && !c.Metadata.IdentityRechecked {
			t.Fatalf("unrechecked %s", c.ID)
		}
	}
}

func TestDoctorMetadataRefusals(t *testing.T) {
	t.Run("gid_only_change", func(t *testing.T) {
		before := unix.Stat_t{Dev: 1, Ino: 2, Mode: unix.S_IFREG | 0600, Uid: uint32(os.Geteuid()), Gid: 1, Nlink: 1, Size: 7}
		after := before
		after.Gid++
		if sameDirectory(before, after) || sameFile(before, after) {
			t.Fatal("GID-only metadata change was accepted")
		}
	})
	cases := []struct {
		name    string
		prepare func(string) error
	}{
		{"symlink", func(base string) error {
			p := filepath.Join(base, "config.toml")
			if err := os.Remove(p); err != nil {
				return err
			}
			return os.Symlink("state.sqlite3", p)
		}},
		{"hardlink", func(base string) error {
			return os.Link(filepath.Join(base, "config.toml"), filepath.Join(base, "alias"))
		}},
		{"shared", func(base string) error { return os.Chmod(filepath.Join(base, "config.toml"), 0644) }},
		{"fifo", func(base string) error {
			p := filepath.Join(base, "config.toml")
			if err := os.Remove(p); err != nil {
				return err
			}
			return unix.Mkfifo(p, 0600)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := doctorFixture(t)
			if err := tc.prepare(base); err != nil {
				t.Fatal(err)
			}
			called := false
			r, err := fixtureObserve(base, func([]byte) (Configuration, error) { called = true; return Configuration{}, nil })
			if called || !errors.Is(err, ErrAttention) || r.Checks[1].Status != "unsafe" || *r.ReadBodyBytes != HeaderBytes {
				t.Fatalf("%+v / %v called=%v", r, err, called)
			}
		})
	}
	t.Run("private_directory", func(t *testing.T) {
		base := doctorFixture(t)
		if err := os.Chmod(base, 0755); err != nil {
			t.Fatal(err)
		}
		r, err := fixtureObserve(base, fixtureValidator)
		if !errors.Is(err, ErrAttention) || r.Checks[0].Status != "unsafe" || *r.ReadBodyBytes != 0 {
			t.Fatal(r, err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		base := doctorFixture(t)
		missing := filepath.Join(base, "absent")
		r, err := fixtureObserve(missing, fixtureValidator)
		if !errors.Is(err, ErrAttention) || r.Checks[0].Status != "absent" || *r.ReadBodyBytes != 0 {
			t.Fatal(r, err)
		}
		if _, err := os.Lstat(missing); !os.IsNotExist(err) {
			t.Fatal("created selected directory", err)
		}
	})
	t.Run("oversized_config", func(t *testing.T) {
		base := doctorFixture(t)
		f, err := os.OpenFile(filepath.Join(base, "config.toml"), os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		err = f.Truncate(MaxConfigBytes + 1)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		r, err := fixtureObserve(base, fixtureValidator)
		if !errors.Is(err, ErrAttention) || r.Checks[1].Status != "unsupported_profile" || *r.ReadBodyBytes != HeaderBytes {
			t.Fatal(r, err)
		}
	})
}

func TestDoctorChangedObjects(t *testing.T) {
	for _, mode := range []string{"file_replaced", "sidecar_appeared", "directory_replaced"} {
		t.Run(mode, func(t *testing.T) {
			base := doctorFixture(t)
			r, err := fixtureObserve(base, func(body []byte) (Configuration, error) {
				switch mode {
				case "file_replaced":
					if err := os.Rename(filepath.Join(base, "config.toml"), filepath.Join(base, "old-config")); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(base, "config.toml"), body, 0600); err != nil {
						t.Fatal(err)
					}
				case "sidecar_appeared":
					if err := os.WriteFile(filepath.Join(base, "state.sqlite3-wal"), []byte("new"), 0600); err != nil {
						t.Fatal(err)
					}
				case "directory_replaced":
					if err := os.Rename(base, base+"-old"); err != nil {
						t.Fatal(err)
					}
					if err := os.Mkdir(base, 0700); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := os.Remove(base); err != nil {
							t.Error(err)
						}
						if err := os.Rename(base+"-old", base); err != nil {
							t.Error(err)
						}
					})
				}
				return fixtureValidator(body)
			})
			if !errors.Is(err, ErrChanged) || r.Status != "incomplete" {
				t.Fatalf("%+v / %v", r, err)
			}
			for _, g := range r.Guidance {
				if g.ID == "explicit_state_initialization_or_migration" || g.ID == "inspect_saved_and_worker_status" {
					t.Fatal("changed guidance", g)
				}
			}
		})
	}
}

func TestDoctorCheckedCloseAndBounds(t *testing.T) {
	base := doctorFixture(t)
	r := newReport(base)
	count := 0
	closeFailure := errors.New("generated close witness")
	err := readWithClose(context.Background(), Request{base, testCatalog()}, fixtureValidator, &r, func(fd int) error { count++; return errors.Join(unix.Close(fd), closeFailure) })
	if !errors.Is(err, closeFailure) || count != len(directoryComponents(base))+3 {
		t.Fatalf("closes=%d err=%v", count, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err = Observe(ctx, Request{base, testCatalog()}, fixtureValidator)
	if !errors.Is(err, context.Canceled) || r.Status != "incomplete" || *r.ReadBodyBytes != 0 {
		t.Fatal(r, err)
	}
	reader := unixReader{ctx: context.Background(), report: &r, attempts: MaxMetadataAttempts}
	if !errors.Is(reader.charge(), ErrBounds) {
		t.Fatal("metadata bound")
	}
	reader.reads = MaxReadAttempts
	if _, err := reader.read(-1, 1, false); !errors.Is(err, ErrBounds) {
		t.Fatal("read bound", err)
	}
	if _, err := reader.read(-1, MaxConfigBytes+1, false); !errors.Is(err, ErrBounds) {
		t.Fatal("allocation bound", err)
	}
	t.Run("internal_invalid_cause", func(t *testing.T) {
		cause := errors.New("generated private validation cause")
		report, err := fixtureObserve(base, func([]byte) (Configuration, error) { return Configuration{}, errors.Join(ErrConfigInvalid, cause) })
		if !errors.Is(err, cause) || !errors.Is(err, ErrAttention) || report.Status != "attention_required" {
			t.Fatal(report, err)
		}
	})
	t.Run("maximum_retained_descriptors", func(t *testing.T) {
		deep := doctorFixture(t)
		for len(directoryComponents(deep)) < MaxPathComponents {
			deep = filepath.Join(deep, "d")
			if err := os.Mkdir(deep, 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(deep, "config.toml"), []byte("generated config"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(deep, "state.sqlite3"), testHeader(16), 0600); err != nil {
			t.Fatal(err)
		}
		report := newReport(deep)
		closeCount := 0
		if err := readWithClose(context.Background(), Request{deep, testCatalog()}, fixtureValidator, &report, func(fd int) error { closeCount++; return unix.Close(fd) }); err != nil {
			t.Fatal(err)
		}
		if closeCount != 67 {
			t.Fatalf("retained descriptors %d", closeCount)
		}
		called := false
		_, err := observe(context.Background(), Request{filepath.Join(deep, "d"), testCatalog()}, fixtureValidator, func(context.Context, Request, ConfigValidator, *Report) error { called = true; return nil })
		if called || !errors.Is(err, ErrArguments) {
			t.Fatal("65-directory base admitted", called, err)
		}
	})
}
