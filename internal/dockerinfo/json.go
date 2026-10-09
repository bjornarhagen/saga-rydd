package dockerinfo

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"unicode/utf8"
)

// Validate even discarded metadata. JSON's usual lossy replacement of invalid
// UTF-8 or lone UTF-16 surrogates is not accepted in this bounded profile.
func strictJSON(b []byte) error {
	if len(b) == 0 || !utf8.Valid(b) {
		return ErrProtocol
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '"' {
			continue
		}
		start := i
		i++
		for ; i < len(b) && b[i] != '"'; i++ {
			if b[i] != '\\' {
				continue
			}
			i++
			if i >= len(b) {
				return ErrProtocol
			}
			if b[i] != 'u' {
				continue
			}
			if i+4 >= len(b) {
				return ErrProtocol
			}
			n, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
			if err != nil {
				return ErrProtocol
			}
			i += 4
			if n >= 0xdc00 && n <= 0xdfff {
				return ErrProtocol
			}
			if n >= 0xd800 && n <= 0xdbff {
				if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
					return ErrProtocol
				}
				low, err := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
				if err != nil || low < 0xdc00 || low > 0xdfff {
					return ErrProtocol
				}
				i += 6
			}
		}
		if i >= len(b) || i-start > 16386 {
			return ErrBounds
		}
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	nodes := 0
	var value func(int) error
	value = func(depth int) error {
		if depth > 32 || nodes >= 32768 {
			return ErrBounds
		}
		nodes++
		t, err := d.Token()
		if err != nil {
			return ErrProtocol
		}
		if s, ok := t.(string); ok {
			if len(s) > 4096 {
				return ErrBounds
			}
			if !safeText(s, 4096) {
				return ErrProtocol
			}
			return nil
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return ErrProtocol
				}
				name, ok := key.(string)
				if !ok || !safeText(name, 4096) || seen[name] {
					return ErrProtocol
				}
				seen[name] = true
				nodes++
				if nodes >= 32768 {
					return ErrBounds
				}
				if err := value(depth + 1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return ErrProtocol
			}
		case '[':
			for d.More() {
				if err := value(depth + 1); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return ErrProtocol
			}
		default:
			return ErrProtocol
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return ErrProtocol
	}
	return nil
}
