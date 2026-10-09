package worker

import (
	"crypto/sha256"
	"math"
	"testing"
	"time"
)

func TestProcessCPUTimevalValidation(t *testing.T) {
	for _, test := range []struct {
		name                  string
		userSec, userUsec     int64
		systemSec, systemUsec int64
		want                  time.Duration
		invalid               bool
	}{
		{name: "zero"},
		{name: "user and system", userSec: 2, userUsec: 123, systemSec: 1, systemUsec: 456, want: 3*time.Second + 579*time.Microsecond},
		{name: "negative user seconds", userSec: -1, invalid: true},
		{name: "negative system seconds", systemSec: -1, invalid: true},
		{name: "negative user fraction", userUsec: -1, invalid: true},
		{name: "negative system fraction", systemUsec: -1, invalid: true},
		{name: "invalid user fraction", userUsec: 1_000_000, invalid: true},
		{name: "invalid system fraction", systemUsec: 1_000_000, invalid: true},
		{name: "seconds overflow", userSec: math.MaxInt64, invalid: true},
		{name: "fraction overflow", userSec: math.MaxInt64 / int64(time.Second), userUsec: 999_999, invalid: true},
		{name: "combined overflow", userSec: math.MaxInt64 / int64(time.Second), systemSec: 1, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := cpuTimevalSum(test.userSec, test.userUsec, test.systemSec, test.systemUsec)
			if (err != nil) != test.invalid || (!test.invalid && got != test.want) || (test.invalid && got != 0) {
				t.Fatal("invalid or exact CPU duration differs", got, err)
			}
		})
	}
}

func TestProcessCPUTimeNativeObservation(t *testing.T) {
	before, err := processCPUTime()
	if err != nil || before < 0 {
		t.Fatal(before, err)
	}
	var block [4096]byte
	var digest [32]byte
	for i := 0; i < 4096; i++ {
		block[i%len(block)] = byte(i)
		digest = sha256.Sum256(block[:])
	}
	after, err := processCPUTime()
	if err != nil || after <= before || digest == ([32]byte{}) {
		t.Fatal("generated CPU work did not advance cumulative observation", before, after, err)
	}
}
