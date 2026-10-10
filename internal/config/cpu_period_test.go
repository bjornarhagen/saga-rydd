package config

import (
	"bytes"
	"github.com/pelletier/go-toml/v2"
	"testing"
)

func TestCPUPeriodConfigDefaultsBoundsAndTrackingDependency(t *testing.T) {
	c := Default()
	c.Roots = []string{"/generated"}
	if c.Scan.CPUChargeSecondsPerHour != 0 || c.Scan.CPUChargeSecondsPerDay != 0 || c.Scan.CPUSessionCharges {
		t.Fatal(c.Scan)
	}
	for _, v := range []struct {
		hour, day      int
		tracking, want bool
	}{
		{0, 0, false, true}, {0, 0, true, true}, {1, 0, true, true}, {0, 1, true, true}, {3600, 86400, true, true},
		{-1, 0, true, false}, {3601, 0, true, false}, {0, -1, true, false}, {0, 86401, true, false}, {1, 0, false, false}, {0, 1, false, false},
	} {
		cfg := c
		cfg.Scan.CPUSessionCharges = v.tracking
		cfg.Scan.CPUChargeSecondsPerHour = v.hour
		cfg.Scan.CPUChargeSecondsPerDay = v.day
		data, err := toml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(data, "/generated-home")
		if (err == nil) != v.want {
			t.Fatal(v, err)
		}
		if v.want && (got.Scan.CPUSessionCharges != v.tracking || got.Scan.CPUChargeSecondsPerHour != v.hour || got.Scan.CPUChargeSecondsPerDay != v.day) {
			t.Fatal(v, got)
		}
	}
	for _, bad := range []string{"cpu_charge_seconds_per_hour = 1.5", "cpu_charge_seconds_per_day = true", "cpu_charge_seconds_per_hour = \"1\"", "cpu_charge_seconds_per_day = 9223372036854775808"} {
		data := []byte("version = 1\nroots = [\"/generated\"]\n[scan]\ncpu_session_charges = true\n" + bad + "\n")
		if _, err := Decode(data, "/generated-home"); err == nil {
			t.Fatal(bad)
		}
	}
	omitted, err := Decode([]byte("version=1\nroots=[\"/generated\"]\n"), "/generated-home")
	if err != nil || omitted.Scan.CPUChargeSecondsPerHour != 0 || omitted.Scan.CPUChargeSecondsPerDay != 0 {
		t.Fatal(omitted, err)
	}
	data, err := toml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("cpu_charge_seconds_per_hour = 0")) || !bytes.Contains(data, []byte("cpu_charge_seconds_per_day = 0")) {
		t.Fatal(string(data))
	}
}
