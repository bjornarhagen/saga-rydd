package worker

import (
	"errors"
	"math"
	"time"

	"golang.org/x/sys/unix"
)

// processCPUTime observes cumulative user and system CPU time for this process.
// It excludes child processes and measures no physical I/O or energy use.
func processCPUTime() (time.Duration, error) {
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		return 0, errors.New("process CPU observation unavailable")
	}
	return cpuTimevalSum(int64(usage.Utime.Sec), int64(usage.Utime.Usec), int64(usage.Stime.Sec), int64(usage.Stime.Usec))
}

func cpuTimevalSum(userSeconds, userMicros, systemSeconds, systemMicros int64) (time.Duration, error) {
	convert := func(seconds, micros int64) (int64, error) {
		if seconds < 0 || micros < 0 || micros >= 1_000_000 || seconds > math.MaxInt64/int64(time.Second) {
			return 0, errors.New("invalid process CPU observation")
		}
		nanos := seconds * int64(time.Second)
		fraction := micros * int64(time.Microsecond)
		if nanos > math.MaxInt64-fraction {
			return 0, errors.New("process CPU observation exceeds duration range")
		}
		return nanos + fraction, nil
	}
	user, err := convert(userSeconds, userMicros)
	if err != nil {
		return 0, err
	}
	system, err := convert(systemSeconds, systemMicros)
	if err != nil {
		return 0, err
	}
	if user > math.MaxInt64-system {
		return 0, errors.New("process CPU observation exceeds duration range")
	}
	return time.Duration(user + system), nil
}
