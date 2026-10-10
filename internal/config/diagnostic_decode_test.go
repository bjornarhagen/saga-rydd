package config

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/pelletier/go-toml/v2"
)

func TestDecodeSyntaxPreservesDecodeContract(t *testing.T) {
	// This is the pre-seam Decode behavior, retained as an independent oracle
	// for defaults, partial syntax failures, path mutation and validation.
	before := func(data []byte, home string) (Config, error) {
		c := Default()
		if len(data) > maxConfigBytes {
			return c, errors.New("configuration exceeds 1 MiB")
		}
		if err := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(&c); err != nil {
			return c, fmt.Errorf("decode config: %w", err)
		}
		return c, c.Validate(home)
	}
	for _, body := range [][]byte{
		[]byte("version = 1\nroots = [\"~/generated\"]\n"),
		[]byte("version = 9\nroots = [\"/generated\"]\n"),
		[]byte("version = 1\nroots = [\"/generated\",\"relative\"]\n"),
		[]byte("version = 1\nunknown = true\n"),
		[]byte("version = 1\nroots = [\"/generated\"]\n[scan]\nwork_seconds = 0\n"),
		bytes.Repeat([]byte(" "), maxConfigBytes+1),
	} {
		want, we := before(body, "/generated-home")
		got, ge := Decode(body, "/generated-home")
		if !reflect.DeepEqual(want, got) || (we == nil) != (ge == nil) || (we != nil && we.Error() != ge.Error()) {
			t.Fatalf("seam changed Decode: %+v/%v != %+v/%v", want, we, got, ge)
		}
	}
	c, err := DecodeSyntax([]byte("version = 1\nroots = [\"~/generated\"]\n"))
	if err != nil || !reflect.DeepEqual(c.Roots, []string{"~/generated"}) || c.Scan != Default().Scan {
		t.Fatal(c, err)
	}
}
