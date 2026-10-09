package config

import (
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestCPUSessionChargesIsExplicitDefaultOffAndRoundTrips(t *testing.T) {
	if Default().Scan.CPUSessionCharges {
		t.Fatal("session charges must be opt-in")
	}
	for _, enabled := range []bool{false, true} {
		cfg := Default()
		cfg.Roots = []string{"/generated-source"}
		cfg.Scan.CPUSessionCharges = enabled
		cfg.Scan.ReadBytesPerDay = 123456
		data, err := toml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(data, "/generated-home")
		if err != nil || got.Scan.CPUSessionCharges != enabled || got.Scan.ReadBytesPerDay != cfg.Scan.ReadBytesPerDay {
			t.Fatal(got, err)
		}
	}
	if _, err := Decode([]byte("version = 1\nroots = ['/generated-source']\n[scan]\ncpu_session_charges = 'true'\n"), "/generated-home"); err == nil {
		t.Fatal("string opt-in accepted")
	}
}
