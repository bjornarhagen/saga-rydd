package config

import (
	"strings"
	"testing"
)

func TestAdaptiveRevisitConfigDefaultAndStrictOptIn(t *testing.T) {
	if Default().Scan.AdaptiveRevisits {
		t.Fatal("adaptive scheduling enabled by default")
	}
	legacy := []byte("version = 1\nroots = ['/generated/root']\n")
	c, err := Decode(legacy, "/generated/home")
	if err != nil || c.Scan.AdaptiveRevisits {
		t.Fatal(c, err)
	}
	for _, enabled := range []string{"true", "false"} {
		body := append(append([]byte(nil), legacy...), []byte("[scan]\nadaptive_revisits = "+enabled+"\n")...)
		c, err = Decode(body, "/generated/home")
		if err != nil || c.Scan.AdaptiveRevisits != (enabled == "true") {
			t.Fatal(c, err)
		}
	}
	for _, invalid := range []string{"1", "'true'", "true\nadaptive_revisits = false"} {
		if _, err = Decode([]byte(string(legacy)+"[scan]\nadaptive_revisits = "+invalid+"\n"), "/generated/home"); err == nil {
			t.Fatal("malformed opt-in accepted", invalid)
		}
	}
	if _, err = Decode([]byte(strings.Replace(string(legacy), "version = 1", "version = 2", 1)), "/generated/home"); err == nil {
		t.Fatal("unknown config version accepted")
	}
}
