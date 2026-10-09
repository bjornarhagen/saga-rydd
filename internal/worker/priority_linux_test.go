//go:build linux

package worker

import (
	"errors"
	"testing"
)

func TestPriorityLinuxRawNiceConversion(t *testing.T) {
	for raw := 1; raw <= 40; raw++ {
		value, err := linuxNice(raw, nil)
		if err != nil || value != 20-raw {
			t.Fatal(raw, value, err)
		}
	}
	for _, raw := range []int{-1, 0, 41} {
		if _, err := linuxNice(raw, nil); err == nil {
			t.Fatal("invalid raw priority accepted", raw)
		}
	}
	if _, err := linuxNice(20, errors.New("fixture")); err == nil {
		t.Fatal("failed observation became nice zero")
	}
}
