//go:build darwin

package worker

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// Public SDK constants in sys/resource.h; x/sys v0.47.0 supplies the libc
// wrappers but does not export these selectors. No private QoS API or cgo.
const (
	darwinPriorityThread     = 3
	darwinPriorityBackground = 0x1000
)

func darwinThreadBackground() (int, error) {
	value, err := unix.Getpriority(darwinPriorityThread, 0)
	if err != nil || (value != 0 && value != 1) {
		return 0, errors.New("thread background observation unavailable")
	}
	return value, nil
}

func requestThreadPriority(ctx context.Context) ThreadPriorityObservation {
	r := newThreadPriorityObservation("darwin")
	background := requestPrioritySetting(ctx, "darwin_thread_background", darwinPriorityBackground, darwinThreadBackground,
		func(value int) error { return unix.Setpriority(darwinPriorityThread, 0, value) },
		func(value int) bool { return value == 1 })
	r.Background = &background
	r.CheckedAt = time.Now().UTC()
	return r
}
