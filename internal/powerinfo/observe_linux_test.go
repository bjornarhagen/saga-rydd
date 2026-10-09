package powerinfo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func linuxFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, name := range []string{"class/power_supply", "devices/battery"} {
		if err := os.MkdirAll(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	for name, value := range battery("Discharging") {
		if err := os.WriteFile(filepath.Join(root, "devices/battery", name), value, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("../../devices/battery", filepath.Join(root, "class/power_supply/battery")); err != nil {
		t.Fatal(err)
	}
	return root
}

func fixtureLinuxObserve(root string) (Observation, error) {
	return observe(context.Background(), "linux", func(ctx context.Context) (source, error) { return openLinuxSource(ctx, root, false) }, time.Now)
}

func TestPowerObservationLinuxHeldResolverAndConcurrentEnumeration(t *testing.T) {
	root := linuxFixture(t)
	const count = 12
	var wg sync.WaitGroup
	results := make(chan Observation, count)
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); o, err := fixtureLinuxObserve(root); results <- o; errs <- err }()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for o := range results {
		assertQualified(t, o)
		if o.SystemBatteryDischargingObserved == nil || !o.CoverageComplete || o.ProvidersProcessed != 1 || o.SupplyEntriesObserved == nil || *o.SupplyEntriesObserved != 1 || o.AttributeAttempts != 4 {
			t.Fatalf("fresh enumeration/resolver failed: %+v", o)
		}
	}
	// Ordinary files are only a generated resolver fixture. They must not
	// satisfy production's required sysfs filesystem profile.
	o, err := observe(context.Background(), "linux", func(ctx context.Context) (source, error) { return openLinuxSource(ctx, root, true) }, time.Now)
	if err != nil || o.Reason != "sysfs_filesystem_unconfirmed" || o.SystemBatteryDischargingObserved != nil || o.AttributeAttempts != 0 {
		t.Fatalf("non-sysfs accepted: %+v %v", o, err)
	}
}

func TestPowerObservationLinuxTraversalAndAttributeKinds(t *testing.T) {
	for _, kind := range []string{"escape_provider", "absolute_provider", "attribute_symlink", "attribute_fifo", "attribute_directory"} {
		t.Run(kind, func(t *testing.T) {
			root := linuxFixture(t)
			link := filepath.Join(root, "class/power_supply/battery")
			status := filepath.Join(root, "devices/battery/status")
			switch kind {
			case "escape_provider", "absolute_provider":
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				outside := t.TempDir()
				target := outside
				if kind == "escape_provider" {
					var err error
					target, err = filepath.Rel(filepath.Dir(link), outside)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			case "attribute_symlink":
				if err := os.Rename(status, status+"-other"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("status-other", status); err != nil {
					t.Fatal(err)
				}
			case "attribute_fifo":
				if err := os.Remove(status); err != nil {
					t.Fatal(err)
				}
				if err := unix.Mkfifo(status, 0600); err != nil {
					t.Fatal(err)
				}
			case "attribute_directory":
				if err := os.Remove(status); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(status, 0700); err != nil {
					t.Fatal(err)
				}
			}
			o, err := fixtureLinuxObserve(root)
			if err != nil {
				t.Fatal(err)
			}
			if o.SystemBatteryDischargingObserved != nil || o.CoverageComplete || o.AmbiguousProviders != 1 {
				t.Fatalf("unsafe fixture yielded witness: %+v", o)
			}
		})
	}
	for _, err := range []error{unix.ENOSYS, unix.EINVAL, unix.EAGAIN, unix.ELOOP, unix.EXDEV} {
		if _, ok := resolutionError(err).(sourceError); !ok {
			t.Fatalf("resolution error not bounded: %v", err)
		}
	}
	if resolveScope != unix.RESOLVE_BENEATH|unix.RESOLVE_NO_MAGICLINKS|unix.RESOLVE_NO_XDEV {
		t.Fatal("resolver scope flags weakened")
	}
}

func TestPowerObservationLinuxNamedScopeReplacementAndDescriptorCleanup(t *testing.T) {
	root := linuxFixture(t)
	source, err := openLinuxSource(context.Background(), root, false)
	if err != nil {
		t.Fatal(err)
	}
	s := source.(*linuxSource)
	rootFD, classFD := s.root, int(s.class.Fd())
	p, err := s.provider(context.Background(), "battery")
	if err != nil {
		t.Fatal(err)
	}
	providerFD := p.(*linuxSupply).fd
	if err := p.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(providerFD), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("provider FD retained: %v", err)
	}
	if err := os.Rename(root, root+"-parked"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "class/power_supply"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.check(context.Background()); err == nil {
		t.Fatal("named root replacement accepted")
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	for _, fd := range []int{rootFD, classFD} {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatalf("FD %d retained: %v", fd, err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal("idempotent close:", err)
	}
	// Distinct fresh observations must not accumulate descriptors. This
	// reads only this fixture process's FD directory, never host power.
	before, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		o, err := fixtureLinuxObserve(root + "-parked")
		if err != nil || o.SystemBatteryDischargingObserved == nil {
			t.Fatalf("repeated fixture %d: %+v %v", i, o, err)
		}
	}
	after, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("descriptor count changed: %d -> %d", len(before), len(after))
	}
}

func TestPowerObservationLinuxShortReadsAndFiniteByteSentinel(t *testing.T) {
	for _, n := range []int{64, 65, 100} {
		calls, offset := 0, 0
		input := []byte(strings.Repeat("x", n))
		b, err := readAttribute(context.Background(), func(out []byte) (int, error) {
			calls++
			if offset == len(input) {
				return 0, nil
			}
			out[0] = input[offset]
			offset++
			return 1, nil
		})
		if len(b) != min(n, 65) || calls > 65 || (err != nil) != (n > 64) {
			t.Fatalf("short read n=%d len=%d calls=%d err=%v", n, len(b), calls, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	b, err := readAttribute(ctx, func(out []byte) (int, error) { calls++; out[0] = 'x'; cancel(); return 1, nil })
	if !errors.Is(err, context.Canceled) || calls != 1 || string(b) != "x" {
		t.Fatalf("canceled read: %q calls=%d err=%v", b, calls, err)
	}
	// Unknown numeric kernel enum values are not admitted as named states.
	for _, n := range []int{-1, 5, 999} {
		v := battery("Discharging")
		v["status"] = []byte(strconv.Itoa(n) + "\n")
		o, err := fixtureObserve(context.Background(), &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": v}})
		if err != nil || o.SystemBatteryDischargingObserved != nil {
			t.Fatalf("numeric enum admitted: %+v %v", o, err)
		}
	}
}
