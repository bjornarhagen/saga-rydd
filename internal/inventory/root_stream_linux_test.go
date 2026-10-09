package inventory

import (
	"context"
	"testing"

	"golang.org/x/sys/unix"
)

// Called by the existing isolated native mount gate, never ordinary tests.
func testRootStreamsBindMount(t *testing.T) {
	for _, during := range []bool{false, true} {
		t.Run(map[bool]string{false: "between_turns", true: "during_selected_turn"}[during], func(t *testing.T) {
			s, jobs := rootStreamFixture(t, []int{301, 301})
			var generations [2]int64
			for i := range jobs {
				b := next(t, s, jobs[i])
				jobs[i].Cursor = b.Cursor
				jobs[i].RootIdentity = b.Identity
				generations[i] = b.Generation
			}
			path := string(jobs[0].RootPath)
			mounted := false
			mount := func() {
				if err := unix.Mount(path, path, "", unix.MS_BIND, ""); err != nil {
					t.Fatal(err)
				}
				mounted = true
			}
			defer func() {
				// Close retained streams before unmounting. Fixture cleanup runs
				// after this defer, while a live directory FD keeps the mount busy.
				s.Close()
				if mounted {
					if err := unix.Unmount(path, 0); err != nil {
						t.Error(err)
					}
				}
			}()
			if !during {
				mount()
				b := next(t, s, jobs[0])
				if b.Generation == generations[0] || len(b.Entries) != 128 {
					t.Fatal("resumed an old mount stream", b)
				}
			} else {
				reads := 0
				b, err := s.NextPermitted(context.Background(), jobs[0], func(_ context.Context, kind APICallKind) error {
					if kind == APIDirectoryRead {
						reads++
					}
					if reads > 0 && !mounted && kind == APIPathResolution {
						mount()
					}
					return nil
				})
				if err != nil || !mounted || b.Fault == "" || b.Generation != 0 || len(b.Entries) != 0 || s.rootStreams[path] != nil {
					t.Fatal("mount replacement published tentative observations", b, err, mounted)
				}
			}
			other := next(t, s, jobs[1])
			if other.Generation != generations[1] || len(other.Entries) != 128 {
				t.Fatal("mount failure reset the other root", other)
			}
		})
	}
}
