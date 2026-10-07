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
	"time"

	"golang.org/x/sys/unix"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const hashReportByteLimit = 1 << 20

type hashReportEnvelope struct {
	Version int              `json:"api_version"`
	OK      bool             `json:"ok"`
	Command string           `json:"command"`
	Report  state.FileReport `json:"report"`
}

func invalidHashReport() error {
	return usageError{errors.New("--from requires one complete standard API version 1 successful report --same-size JSON page with unchanged metadata qualifications")}
}

// Only the explicitly named report file is opened. Nonblocking, no-follow
// opening rejects final symlinks and avoids waiting on a FIFO. Regular-file
// reads remain cooperative: a filesystem call itself cannot be preempted.
func readHashReport(ctx context.Context, path string) (state.SameSizeReport, error) {
	if err := ctx.Err(); err != nil {
		return state.SameSizeReport{}, err
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return state.SameSizeReport{}, missingHashError{fmt.Errorf("named same-size report is unavailable; save a report --same-size --json page first: %w", err)}
		}
		return state.SameSizeReport{}, fmt.Errorf("open named same-size report: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return state.SameSizeReport{}, err
	}
	if !before.Mode().IsRegular() || before.Size() > hashReportByteLimit {
		return state.SameSizeReport{}, usageError{errors.New("--from requires a regular non-symlink report file of at most 1 MiB")}
	}
	var body bytes.Buffer
	chunk := make([]byte, 32<<10)
	for int64(body.Len()) < before.Size() {
		if err := ctx.Err(); err != nil {
			return state.SameSizeReport{}, err
		}
		request := len(chunk)
		if remaining := int(before.Size()) - body.Len(); request > remaining {
			request = remaining
		}
		n, readErr := file.Read(chunk[:request])
		body.Write(chunk[:n])
		if readErr == io.EOF {
			if int64(body.Len()) != before.Size() {
				return state.SameSizeReport{}, usageError{errors.New("named same-size report was truncated while being read")}
			}
			break
		}
		if readErr != nil {
			return state.SameSizeReport{}, readErr
		}
		if n == 0 {
			return state.SameSizeReport{}, io.ErrNoProgress
		}
	}
	after, err := file.Stat()
	if err != nil {
		return state.SameSizeReport{}, err
	}
	named, err := os.Lstat(path)
	if err != nil {
		return state.SameSizeReport{}, err
	}
	if !named.Mode().IsRegular() || !os.SameFile(before, after) || !os.SameFile(after, named) || before.Size() != after.Size() || after.Size() != named.Size() || int64(body.Len()) != after.Size() || !before.ModTime().Equal(after.ModTime()) || !after.ModTime().Equal(named.ModTime()) {
		return state.SameSizeReport{}, usageError{errors.New("named same-size report changed while being read; use one stable saved page")}
	}
	if err := ctx.Err(); err != nil {
		return state.SameSizeReport{}, err
	}
	return decodeHashReport(ctx, body.Bytes())
}

func decodeHashReport(ctx context.Context, body []byte) (state.SameSizeReport, error) {
	if len(body) > hashReportByteLimit {
		return state.SameSizeReport{}, invalidHashReport()
	}
	// The standard decoder accepts duplicate and case-insensitive field names.
	// Reject those ambiguities before decoding any selected evidence.
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := checkHashJSONKeys(ctx, decoder, 0); err != nil {
		if ctx.Err() != nil {
			return state.SameSizeReport{}, ctx.Err()
		}
		return state.SameSizeReport{}, invalidHashReport()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return state.SameSizeReport{}, invalidHashReport()
	}
	if err := requireHashJSONFields(body, reflect.TypeFor[hashReportEnvelope]()); err != nil {
		return state.SameSizeReport{}, invalidHashReport()
	}
	decoder = json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var envelope hashReportEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return state.SameSizeReport{}, invalidHashReport()
	}
	outer := envelope.Report
	if envelope.Version != APIVersion || !envelope.OK || envelope.Command != "report" || outer.SameSize == nil || outer.Candidates != nil || outer.Directory != nil || outer.Source != "saved_inventory" || outer.CurrentStateVerified || len(outer.Files) != 0 || len(outer.Roots) != 0 || outer.RootsTruncated || outer.NextCursor != "" || outer.Limit != 0 {
		return state.SameSizeReport{}, invalidHashReport()
	}
	// Even null alternative report modes are not part of standard same-size
	// output. Their presence must not silently become a different mode.
	var fields struct {
		Report map[string]json.RawMessage `json:"report"`
	}
	_ = json.Unmarshal(body, &fields)
	if _, exists := fields.Report["candidates"]; exists {
		return state.SameSizeReport{}, invalidHashReport()
	}
	if _, exists := fields.Report["directory"]; exists {
		return state.SameSizeReport{}, invalidHashReport()
	}
	page := *outer.SameSize
	if page.Source != "saved_inventory" || !validHashSelectionID(page.InventoryID) || page.CurrentStateVerified || page.ContentVerified || page.EstimatedReclaimableBytes != nil || page.MinimumBytes < 1 || page.Limit < 1 || page.Limit > 200 || page.EntriesExamined < 0 || page.EntriesExamined > page.Limit || !page.GeneratedAt.Equal(outer.GeneratedAt) || page.GeneratedAt.IsZero() || (page.PageCoverage != "saved_entries_exhausted" && page.PageCoverage != "more_saved_entries") || (page.PageCoverage == "more_saved_entries") != (page.NextCursor != "") || len(page.NextCursor) > 512 {
		return state.SameSizeReport{}, invalidHashReport()
	}
	seen := map[int64]bool{}
	rows := 0
	for _, band := range page.Bands {
		if band.LogicalBytes < page.MinimumBytes || len(band.Files) == 0 || band.KnownObjects < 0 || band.RepeatedSavedObjects < 0 || band.UnknownIdentities < 0 || band.ConflictingIdentities < 0 {
			return state.SameSizeReport{}, invalidHashReport()
		}
		for _, file := range band.Files {
			if file.ID <= 0 || file.RootID <= 0 || seen[file.ID] || file.Size != band.LogicalBytes || len(file.PathBytes) == 0 || len(file.PathBytes) > 4096 {
				return state.SameSizeReport{}, invalidHashReport()
			}
			seen[file.ID] = true
			rows++
		}
	}
	if rows > page.EntriesExamined {
		return state.SameSizeReport{}, invalidHashReport()
	}
	if err := ctx.Err(); err != nil {
		return state.SameSizeReport{}, err
	}
	return page, nil
}

func selectHashReportFiles(page state.SameSizeReport, ids []int64) ([]state.SameSizeFile, error) {
	rows := make(map[int64]state.SameSizeFile)
	for _, band := range page.Bands {
		for _, file := range band.Files {
			rows[file.ID] = file
		}
	}
	selected := make([]state.SameSizeFile, 0, len(ids))
	for _, id := range ids {
		file, exists := rows[id]
		if !exists {
			return nil, usageError{fmt.Errorf("saved file ID %d is absent from the named report page", id)}
		}
		selected = append(selected, file)
	}
	return selected, nil
}

func checkHashJSONKeys(ctx context.Context, decoder *json.Decoder, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 16 {
		return errors.New("report JSON nesting exceeds its schema")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || keys[name] {
				return errors.New("duplicate or invalid report field")
			}
			keys[name] = true
			if err := checkHashJSONKeys(ctx, decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := checkHashJSONKeys(ctx, decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected report delimiter")
	}
	_, err = decoder.Token()
	return err
}

// Require exactly the emitted field spellings and all non-optional fields.
// Only pointer fields can be null; in this report the savings estimate is
// deliberately null. Byte slices and time values have scalar JSON codecs.
func requireHashJSONFields(raw json.RawMessage, typ reflect.Type) error {
	if typ.Kind() == reflect.Pointer {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil
		}
		return requireHashJSONFields(raw, typ.Elem())
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("null required report field")
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
		allowed := map[string]reflect.StructField{}
		var collect func(reflect.Type)
		collect = func(t reflect.Type) {
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if field.PkgPath != "" {
					continue
				}
				tag := field.Tag.Get("json")
				if field.Anonymous && tag == "" {
					collect(field.Type)
					continue
				}
				name := tag
				for j, c := range name {
					if c == ',' {
						name = name[:j]
						break
					}
				}
				if name == "" {
					name = field.Name
				}
				if name != "-" {
					allowed[name] = field
				}
			}
		}
		collect(typ)
		for key := range object {
			if _, ok := allowed[key]; !ok {
				return errors.New("unknown report field")
			}
		}
		for key, field := range allowed {
			value, exists := object[key]
			optional := false
			for _, option := range bytes.Split([]byte(field.Tag.Get("json")), []byte(","))[1:] {
				if string(option) == "omitempty" {
					optional = true
				}
			}
			if !exists {
				if optional {
					continue
				}
				return errors.New("missing required report field")
			}
			if err := requireHashJSONFields(value, field.Type); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var array []json.RawMessage
		if err := json.Unmarshal(raw, &array); err != nil {
			return err
		}
		for _, item := range array {
			if err := requireHashJSONFields(item, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
