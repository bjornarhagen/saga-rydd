package config

import "testing"

func TestCompactBackgroundConfigDefaultAndAdaptiveConflict(t *testing.T) {
	if Default().Scan.CompactInventory {
		t.Fatal("compact storage enabled by default")
	}
	base := "version=1\nroots=['/generated/offline']\n[scan]\n"
	for _, suffix := range []string{"", "compact_inventory=false", "compact_inventory=true"} {
		c, err := Decode([]byte(base+suffix), "/generated/home")
		if err != nil || c.Scan.CompactInventory != (suffix == "compact_inventory=true") || c.Scan.MetadataPerSecond != 100 {
			t.Fatal(c, err)
		}
	}
	for _, suffix := range []string{"compact_inventory=true\nadaptive_revisits=true", "compact_inventory=1", "compact_inventory='true'", "compact_inventory=true\ncompact_inventory=false"} {
		if _, err := Decode([]byte(base+suffix), "/generated/home"); err == nil {
			t.Fatal("invalid/conflicting profile admitted", suffix)
		}
	}
}
