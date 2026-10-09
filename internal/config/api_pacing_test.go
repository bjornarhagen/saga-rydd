package config

import (
	"strconv"
	"testing"
)

func TestAPIPacingConfigDefaultBoundsAndStrictTypes(t *testing.T) {
	if Default().Scan.APIAttemptsPerSecond != 0 {
		t.Fatal("API pacing enabled by default")
	}
	base := "version=1\nroots=['/generated/offline']\n[scan]\n"
	legacy, err := Decode([]byte(base), "/generated/home")
	if err != nil || legacy.Scan.APIAttemptsPerSecond != 0 || legacy.Scan.MetadataPerSecond != 100 {
		t.Fatal("legacy entry pace/default changed", legacy, err)
	}
	for _, rate := range []int{-1, 0, 1, 1000, 100000, 100001} {
		c, err := Decode([]byte(base+"api_attempts_per_second="+strconv.Itoa(rate)+"\nmetadata_per_second=17\n"), "/generated/home")
		valid := rate >= 0 && rate <= 100000
		if (err == nil) != valid || valid && (c.Scan.APIAttemptsPerSecond != rate || c.Scan.MetadataPerSecond != 17) {
			t.Fatal(rate, c, err)
		}
	}
	for _, value := range []string{"true", "'100'", "1.5", "1\napi_attempts_per_second=2", "9223372036854775808"} {
		if _, err := Decode([]byte(base+"api_attempts_per_second="+value+"\n"), "/generated/home"); err == nil {
			t.Fatal("ambiguous API pacing accepted", value)
		}
	}
}
