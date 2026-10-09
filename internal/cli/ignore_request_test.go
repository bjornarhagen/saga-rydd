package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

func TestIgnoreRequestStrictEnvelopeAndEvidence(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "project")
	request, valid := ignoreCLIPreview(t, f)
	decoded, err := decodeIgnoreRequest(context.Background(), valid)
	if err != nil || !reflect.DeepEqual(request, decoded) {
		t.Fatal("the standard preview including canonical null slices was rejected", err)
	}
	mutate := func(change func(map[string]any)) []byte {
		var envelope map[string]any
		decoder := json.NewDecoder(bytes.NewReader(valid))
		decoder.UseNumber()
		if err := decoder.Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		change(envelope)
		body, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	req := func(e map[string]any) map[string]any { return e["dismissal_request"].(map[string]any) }
	selection := func(e map[string]any) map[string]any { return req(e)["selection"].(map[string]any) }
	cases := map[string][]byte{
		"unknown_envelope":       mutate(func(e map[string]any) { e["extra"] = true }),
		"unknown_request":        mutate(func(e map[string]any) { req(e)["extra"] = true }),
		"case_alias":             mutate(func(e map[string]any) { e["API_VERSION"] = e["api_version"]; delete(e, "api_version") }),
		"case_alias_nested":      mutate(func(e map[string]any) { req(e)["Current_State_Verified"] = false }),
		"missing_version":        mutate(func(e map[string]any) { delete(e, "api_version") }),
		"missing_nullable_field": mutate(func(e map[string]any) { delete(req(e), "estimated_reclaimable_bytes") }),
		"wrong_api_version":      mutate(func(e map[string]any) { e["api_version"] = 2 }),
		"failure_envelope":       mutate(func(e map[string]any) { e["ok"] = false }),
		"wrong_command":          mutate(func(e map[string]any) { e["command"] = "plan" }),
		"wrong_contract":         mutate(func(e map[string]any) { req(e)["contract"] = plans.DismissalContract }),
		"authority_true":         mutate(func(e map[string]any) { req(e)["executable"] = true }),
		"authority_null":         mutate(func(e map[string]any) { req(e)["current_state_verified"] = nil }),
		"null_selection":         mutate(func(e map[string]any) { req(e)["selection"] = nil }),
		"null_roots":             mutate(func(e map[string]any) { selection(e)["roots"] = nil }),
		"changed_manual_root":    mutate(func(e map[string]any) { req(e)["manual_root_bytes"] = []byte(f.root + ".different") }),
		"changed_inventory":      mutate(func(e map[string]any) { selection(e)["inventory_id"] = strings.Repeat("a", 64) }),
		"changed_observation": mutate(func(e map[string]any) {
			evidence := selection(e)["evidence"].(map[string]any)
			evidence["minimum_age_days"] = 30
		}),
		"changed_binding": mutate(func(e map[string]any) {
			target := selection(e)["targets"].([]any)[0].(map[string]any)["target"].(map[string]any)
			target["inode"] = "different"
		}),
		"duplicate_envelope": bytes.Replace(valid, []byte(`"api_version":1`), []byte(`"api_version":1,"api_version":1`), 1),
		"escaped_duplicate":  bytes.Replace(valid, []byte(`"api_version":1`), []byte(`"api_version":1,"api_\u0076ersion":1`), 1),
		"duplicate_request":  bytes.Replace(valid, []byte(`"current_state_verified":false`), []byte(`"current_state_verified":false,"current_state_verified":false`), 1),
		"truncated":          valid[:len(valid)/2],
		"second_envelope":    append(append([]byte(nil), valid...), valid...),
		"wrong_json_kind":    []byte(`[]`),
		"too_deep":           []byte(strings.Repeat("[", 20) + "0" + strings.Repeat("]", 20)),
		"too_large":          bytes.Repeat([]byte(" "), ignoreRequestByteLimit+1),
	}
	before := hashCLIBytes(t, f.root, f.inventory)
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if bytes.Equal(body, valid) {
				t.Fatal("invalid request fixture did not change the preview")
			}
			var usage usageError
			if _, err := decodeIgnoreRequest(context.Background(), body); !errors.As(err, &usage) {
				t.Fatal("strict request decoder accepted invalid evidence", err)
			}
			if err := os.WriteFile(f.requestFile, body, 0600); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", f.requestFile, "--json")
			ignoreCLIError(t, code, raw, stderr, "invalid_arguments", 2)
		})
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.inventory)) {
		t.Fatal("invalid requests changed the source or inventory")
	}
	if _, err := os.Lstat(filepath.Join(f.base, "plans")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused requests initialized plan storage", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := decodeIgnoreRequest(ctx, valid); !errors.Is(err, context.Canceled) {
		t.Fatal("request decoding ignored cancellation", err)
	}
}

func TestIgnoreRequestNamedRegularFileOnly(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "project")
	request, valid := ignoreCLIPreview(t, f)
	if got, err := readIgnoreRequest(context.Background(), f.requestFile); err != nil || !reflect.DeepEqual(got, request) {
		t.Fatal("stable regular preview file was rejected", got, err)
	}
	fifo := filepath.Join(filepath.Dir(f.requestFile), "preview.fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(filepath.Dir(f.requestFile), "preview.symlink")
	if err := os.Symlink(f.requestFile, symlink); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(filepath.Dir(f.requestFile), "empty.json")
	if err := os.WriteFile(empty, nil, 0600); err != nil {
		t.Fatal(err)
	}
	oversize := filepath.Join(filepath.Dir(f.requestFile), "oversize.json")
	if err := os.WriteFile(oversize, append(valid, bytes.Repeat([]byte(" "), ignoreRequestByteLimit)...), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path, code string
		exit             int
	}{
		{"fifo", fifo, "invalid_arguments", 2},
		{"directory", filepath.Dir(f.requestFile), "invalid_arguments", 2},
		{"symlink", symlink, "command_failed", 1},
		{"empty", empty, "invalid_arguments", 2},
		{"oversize", oversize, "invalid_arguments", 2},
		{"missing", filepath.Join(filepath.Dir(f.requestFile), "missing.json"), "not_found", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, raw, stderr := f.run(context.Background(), "ignore", "--save", "--from", tc.path, "--json")
			ignoreCLIError(t, code, raw, stderr, tc.code, tc.exit)
		})
	}
	if _, err := os.Lstat(filepath.Join(f.base, "plans")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refused named file initialized plan storage", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readIgnoreRequest(ctx, symlink); !errors.Is(err, context.Canceled) {
		t.Fatal("early cancellation opened the named request", err)
	}
}

func TestIgnoreRequestSameInodeEditWithRestoredMtimeRefuses(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "project")
	request, valid := ignoreCLIPreview(t, f)
	// Legal trailing whitespace makes this a multi-chunk request without
	// changing the exact request or exceeding the bounded file limit.
	large := append(append([]byte(nil), valid...), bytes.Repeat([]byte(" "), 64<<10)...)
	if err := os.WriteFile(f.requestFile, large, 0600); err != nil {
		t.Fatal(err)
	}
	modified := time.Unix(946684800, 0)
	if err := os.Chtimes(f.requestFile, modified, modified); err != nil {
		t.Fatal(err)
	}
	var before unix.Stat_t
	if err := unix.Lstat(f.requestFile, &before); err != nil {
		t.Fatal(err)
	}
	changed := false
	got, err := readIgnoreRequestWithHook(context.Background(), f.requestFile, func(offset int) {
		if changed {
			return
		}
		changed = true
		if offset != 32<<10 || len(valid) >= offset {
			t.Fatal("fixture did not reach the bounded read seam in its whitespace", offset, len(valid))
		}
		writer, err := os.OpenFile(f.requestFile, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := writer.WriteAt([]byte("\n"), int64(offset+1))
		closeErr := writer.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal(writeErr, closeErr)
		}
		if err := os.Chtimes(f.requestFile, modified, modified); err != nil {
			t.Fatal(err)
		}
	})
	var usage usageError
	if !changed || !errors.As(err, &usage) || !strings.Contains(err.Error(), "changed while being read") || got.ID != "" {
		t.Fatal("same-size valid request edit with restored mtime escaped stability checks", got, err)
	}
	var after unix.Stat_t
	if err := unix.Lstat(f.requestFile, &after); err != nil {
		t.Fatal(err)
	}
	bm, bc := hashConfigTimes(before)
	am, ac := hashConfigTimes(after)
	if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || bm != am || bc == ac {
		t.Fatal("fixture did not preserve identity, size and mtime while changing ctime", before, after)
	}
	// The final bytes still contain the same valid request. Only instability
	// during the first read caused the refusal; a later stable read is allowed.
	if got, err := readIgnoreRequest(context.Background(), f.requestFile); err != nil || !reflect.DeepEqual(got, request) {
		t.Fatal("stable changed whitespace invalidated the exact request", got, err)
	}
	if _, err := os.Lstat(filepath.Join(f.base, "plans")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unstable request initialized dismissal storage", err)
	}
}

func TestIgnorePublicationErrorRetainsCandidateIDsWithoutClaimingCommit(t *testing.T) {
	f := newIgnoreCLIFixture(t, false, "project")
	request, _ := ignoreCLIPreview(t, f)
	saved, err := plans.SaveDismissal(context.Background(), f.base, request)
	if err != nil {
		t.Fatal(err)
	}
	undone, err := plans.UndoDismissal(context.Background(), f.base, saved.ID)
	if err != nil || undone.Undo == nil {
		t.Fatal("generated undo fixture failed", undone, err)
	}
	paths, err := config.ResolvePaths(f.base)
	if err != nil {
		t.Fatal(err)
	}
	// Actual commit uncertainty is exercised by plans' transaction/process-loss
	// tests. This seam verifies both CLI publication call sites retain the IDs
	// and error identity required by the normal failure-envelope renderer.
	for _, candidate := range []ignoreSavedResult{{Mode: "save", Saved: saved}, {Mode: "undo", Saved: undone}} {
		for _, cause := range []error{plans.ErrDismissalPublication, errors.Join(plans.ErrDismissalPublication, context.Canceled), context.Canceled} {
			result, replyErr := ignorePublicationResult(candidate.Mode, candidate.Saved, cause, paths)
			if !reflect.DeepEqual(result, candidate) || !errors.Is(replyErr, cause) || !strings.Contains(replyErr.Error(), commandPrefix(paths)+" ignore --show "+saved.ID) || !strings.Contains(replyErr.Error(), "candidate "+saved.ID) || strings.Contains(replyErr.Error(), "has saved status") || strings.Contains(replyErr.Error(), "was saved") {
				t.Fatal("publication failure lost candidate evidence or claimed a commit", result, replyErr)
			}
			if candidate.Saved.Undo != nil && !strings.Contains(replyErr.Error(), candidate.Saved.Undo.ID) {
				t.Fatal("undo uncertainty omitted the exact undo ID", replyErr)
			}
			var out, stderr bytes.Buffer
			code := operationFailure(&out, &stderr, "ignore", replyErr)
			want := "canceled"
			if errors.Is(cause, plans.ErrDismissalPublication) {
				want = "dismissal_outcome_unknown"
			}
			ignoreCLIError(t, code, out.String(), stderr.String(), want, 1)
			var envelope struct {
				Error struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.Error.Message != replyErr.Error() {
				t.Fatal("failure envelope hid recovery guidance", out.String(), err)
			}
		}
	}
	if _, err := ignorePublicationResult("save", plans.SavedDismissal{}, plans.ErrDismissalCapacity, paths); !errors.Is(err, plans.ErrDismissalCapacity) || strings.Contains(err.Error(), "candidate") {
		t.Fatal("a refusal without publication invented a candidate", err)
	}
}
