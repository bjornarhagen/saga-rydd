package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
)

func TestVersionMetadataPreservesScalarAndNeedsNoState(t *testing.T) {
	for _, machine := range []bool{false, true} {
		var out, stderr bytes.Buffer
		args := []string{"--version", "--data-dir", filepath.Join(t.TempDir(), "missing")}
		if machine {
			args = append(args, "--json")
		}
		if code := Run(context.Background(), args, &out, &stderr); code != 0 {
			t.Fatal(code, stderr.String())
		}
		if machine {
			var reply struct {
				OK       bool                 `json:"ok"`
				Version  string               `json:"version"`
				Metadata buildmetadata.Record `json:"build_metadata"`
			}
			if err := json.Unmarshal(out.Bytes(), &reply); err != nil || !reply.OK || reply.Version != buildmetadata.Current().Version || reply.Version != reply.Metadata.Version || reply.Metadata.Contract != buildmetadata.Contract || reply.Metadata.GOOS == "unknown" || reply.Metadata.GOARCH == "unknown" {
				t.Fatal(reply, err)
			}
		} else if out.String() != "rydd "+buildmetadata.Current().Version+" (experimental inventory)\n" {
			t.Fatal(out.String())
		}
	}
}

func TestVersionCancellationAndOutputFailureProduceOneReply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, stderr bytes.Buffer
	if code := Run(ctx, []string{"--json", "--version"}, &out, &stderr); code == 0 || strings.Count(out.String(), "\n") != 1 || !strings.Contains(out.String(), "canceled") {
		t.Fatal(code, out.String(), stderr.String())
	}
	for _, machine := range []bool{false, true} {
		args := []string{"--version"}
		if machine {
			args = append(args, "--json")
		}
		for _, fullCount := range []bool{false, true} {
			var stderr bytes.Buffer
			writer := &versionFailWriter{full: fullCount}
			if code := Run(context.Background(), args, writer, &stderr); code == 0 || writer.calls != 1 {
				t.Fatal(code, writer.calls, stderr.String())
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		writer := &versionCancelWriter{cancel: cancel}
		var stderr bytes.Buffer
		if code := Run(ctx, args, writer, &stderr); code == 0 || writer.calls != 1 || !strings.Contains(stderr.String(), "canceled during output") {
			t.Fatal(code, writer.calls, stderr.String())
		}
	}
}

type versionFailWriter struct {
	full  bool
	calls int
}

func (w *versionFailWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.full {
		return len(p), errors.New("generated reply failure")
	}
	return len(p) - 1, io.ErrShortWrite
}

type versionCancelWriter struct {
	cancel context.CancelFunc
	calls  int
}

func (w *versionCancelWriter) Write(p []byte) (int, error) { w.calls++; w.cancel(); return len(p), nil }
