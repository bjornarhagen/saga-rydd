//go:build linux

package worker

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// Public Linux UAPI values: include/uapi/linux/ioprio.h. Only the current
// calling thread (who=0) is selected; no process-group or user operation occurs.
const (
	linuxIOWhoThread  = 1
	linuxIOClassShift = 13
	linuxIOIdleClass  = 3
	linuxIOIdleValue  = linuxIOIdleClass << linuxIOClassShift
)

func linuxNice(raw int, err error) (int, error) {
	if err != nil || raw < 1 || raw > 40 {
		return 0, errors.New("thread nice observation unavailable")
	}
	// x/sys exposes the kernel result, unlike libc's nice-value conversion.
	return 20 - raw, nil
}

func linuxThreadNice() (int, error) {
	return linuxNice(unix.Getpriority(unix.PRIO_PROCESS, 0))
}

func linuxThreadIO() (int, error) {
	raw, _, errno := unix.Syscall(unix.SYS_IOPRIO_GET, linuxIOWhoThread, 0, 0)
	if errno != 0 || raw >= 1<<16 || raw>>linuxIOClassShift > linuxIOIdleClass {
		return 0, errors.New("thread I/O priority observation unavailable")
	}
	return int(raw), nil
}

func requestThreadPriority(ctx context.Context) ThreadPriorityObservation {
	r := newThreadPriorityObservation("linux")
	cpu := requestPrioritySetting(ctx, "nice_at_least_10", 10, linuxThreadNice,
		func(value int) error { return unix.Setpriority(unix.PRIO_PROCESS, 0, value) },
		func(value int) bool { return value >= 10 })
	r.CPU = &cpu
	io := requestPrioritySetting(ctx, "ioprio_idle", linuxIOIdleValue, linuxThreadIO,
		func(value int) error {
			_, _, errno := unix.Syscall(unix.SYS_IOPRIO_SET, linuxIOWhoThread, 0, uintptr(value))
			if errno != 0 {
				return errors.New("thread I/O priority request unavailable")
			}
			return nil
		}, func(value int) bool { return value>>linuxIOClassShift == linuxIOIdleClass })
	r.IO = &io
	r.CheckedAt = time.Now().UTC()
	return r
}
