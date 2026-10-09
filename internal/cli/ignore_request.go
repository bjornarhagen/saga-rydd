package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

const ignoreRequestByteLimit = 1 << 20

type ignoreRequestEnvelope struct {
	Version int                    `json:"api_version"`
	OK      bool                   `json:"ok"`
	Command string                 `json:"command"`
	Request plans.DismissalRequest `json:"dismissal_request"`
}

func invalidIgnoreRequest() error {
	return usageError{errors.New("--from requires one complete standard API version 1 successful ignore --preview JSON request with exact saved evidence")}
}

func readIgnoreRequest(ctx context.Context, path string) (plans.DismissalRequest, error) {
	return readIgnoreRequestWithHook(ctx, path, nil)
}

// The private hook exercises changes between bounded reads of generated
// request files. Ordinary CLI callers never supply it.
func readIgnoreRequestWithHook(ctx context.Context, path string, afterChunk func(int)) (plans.DismissalRequest, error) {
	if err := ctx.Err(); err != nil {
		return plans.DismissalRequest{}, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return plans.DismissalRequest{}, ignoreMissing(fmt.Errorf("open named dismissal request: %w", err), "the named preview request file is unavailable")
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return plans.DismissalRequest{}, err
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || before.Size < 0 || before.Size > ignoreRequestByteLimit {
		return plans.DismissalRequest{}, usageError{errors.New("--from requires a regular non-symlink request file of at most 1 MiB")}
	}
	var body bytes.Buffer
	chunk := make([]byte, 32<<10)
	for int64(body.Len()) < before.Size {
		if err := ctx.Err(); err != nil {
			return plans.DismissalRequest{}, err
		}
		next := min(len(chunk), int(before.Size)-body.Len())
		n, readErr := file.Read(chunk[:next])
		body.Write(chunk[:n])
		if n > 0 && afterChunk != nil {
			afterChunk(body.Len())
		}
		if readErr != nil {
			if readErr == io.EOF {
				if int64(body.Len()) == before.Size {
					break
				}
				return plans.DismissalRequest{}, usageError{errors.New("named dismissal request was truncated while being read")}
			}
			return plans.DismissalRequest{}, readErr
		}
		if n == 0 {
			return plans.DismissalRequest{}, io.ErrNoProgress
		}
	}
	var after, named unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return plans.DismissalRequest{}, err
	}
	if err := unix.Lstat(path, &named); err != nil {
		return plans.DismissalRequest{}, err
	}
	if named.Mode&unix.S_IFMT != unix.S_IFREG || int64(body.Len()) != after.Size || !sameHashConfigStamp(before, after) || !sameHashConfigStamp(after, named) {
		return plans.DismissalRequest{}, usageError{errors.New("named dismissal request changed while being read; use one stable saved request")}
	}
	if err := file.Close(); err != nil {
		return plans.DismissalRequest{}, err
	}
	return decodeIgnoreRequest(ctx, body.Bytes())
}

func decodeIgnoreRequest(ctx context.Context, body []byte) (plans.DismissalRequest, error) {
	if len(body) > ignoreRequestByteLimit {
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := checkHashJSONKeys(ctx, decoder, 0); err != nil {
		if ctx.Err() != nil {
			return plans.DismissalRequest{}, ctx.Err()
		}
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	if err := requireIgnoreJSONFields(body, reflect.TypeFor[ignoreRequestEnvelope]()); err != nil {
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope ignoreRequestEnvelope
	if err := decoder.Decode(&envelope); err != nil || envelope.Version != APIVersion || !envelope.OK || envelope.Command != "ignore" {
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	if err := plans.ValidateDismissalRequest(envelope.Request); err != nil {
		return plans.DismissalRequest{}, invalidIgnoreRequest()
	}
	if err := ctx.Err(); err != nil {
		return plans.DismissalRequest{}, err
	}
	return envelope.Request, nil
}

// Canonical evidence deliberately clears slice-valued notes. Permit their
// standard null encoding, while requiring exact field spellings and every
// non-optional field. The core validates the complete canonical request next.
func requireIgnoreJSONFields(raw json.RawMessage, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil
		}
		return requireIgnoreJSONFields(raw, typ.Elem())
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if typ.Kind() == reflect.Slice {
			return nil
		}
		return errors.New("null required dismissal field")
	}
	if typ == reflect.TypeFor[time.Time]() || typ == reflect.TypeFor[[]byte]() {
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil {
			return err
		}
		allowed := make(map[string]bool, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.PkgPath != "" {
				continue
			}
			tag := strings.Split(field.Tag.Get("json"), ",")
			name := tag[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			allowed[name] = true
			value, exists := object[name]
			optional := false
			for _, option := range tag[1:] {
				optional = optional || option == "omitempty"
			}
			if !exists {
				if optional {
					continue
				}
				return errors.New("missing required dismissal field")
			}
			if err := requireIgnoreJSONFields(value, field.Type); err != nil {
				return err
			}
		}
		for key := range object {
			if !allowed[key] {
				return errors.New("unknown dismissal field")
			}
		}
	case reflect.Slice:
		var array []json.RawMessage
		if err := json.Unmarshal(raw, &array); err != nil {
			return err
		}
		for _, value := range array {
			if err := requireIgnoreJSONFields(value, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
