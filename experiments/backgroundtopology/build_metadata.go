package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
)

const versionReplyLimit = 16 << 10

func (r *runner) declaredMetadata(ctx context.Context) (buildmetadata.Record, error) {
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	child, err := start(call, r.o.Binary, []string{"--version", "--json"}, r.env, versionReplyLimit)
	if err != nil {
		return buildmetadata.Record{}, err
	}
	err = child.join(call)
	r.seq++
	body, _ := child.out.value()
	diagnostic, _ := child.diagnostic.value()
	writeErr := errors.Join(writeReceipt(filepath.Join(r.base, fmt.Sprintf("command-%03d.stdout", r.seq)), body), writeReceipt(filepath.Join(r.base, fmt.Sprintf("command-%03d.stderr", r.seq)), diagnostic))
	if !child.usage.Available {
		r.observers.Available = false
	}
	r.observers.UserNS += child.usage.UserNS
	r.observers.SystemNS += child.usage.SystemNS
	r.observers.ElapsedNS += child.usage.ElapsedNS
	if child.usage.RSS > r.observers.RSS {
		r.observers.RSS = child.usage.RSS
	}
	if err != nil || writeErr != nil {
		return buildmetadata.Record{}, errors.Join(err, writeErr)
	}
	return parseDeclaredMetadata(body)
}
func parseDeclaredMetadata(body []byte) (buildmetadata.Record, error) {
	var envelope struct {
		APIVersion int                  `json:"api_version"`
		OK         bool                 `json:"ok"`
		Command    string               `json:"command"`
		Version    string               `json:"version"`
		Record     buildmetadata.Record `json:"build_metadata"`
	}
	if len(body) == 0 || len(body) > versionReplyLimit || uniqueMetadataJSON(body) != nil {
		return envelope.Record, errProfile
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil {
		return buildmetadata.Record{}, errProfile
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return buildmetadata.Record{}, errProfile
	}
	r := envelope.Record
	if envelope.APIVersion != 1 || !envelope.OK || envelope.Command != "version" || envelope.Version != r.Version || r.Contract != buildmetadata.Contract || !buildmetadata.ValidVersion(r.Version) {
		return buildmetadata.Record{}, errProfile
	}
	if r.VersionSource != "candidate_recipe" && r.VersionSource != "linked_value" && r.VersionSource != "unknown" {
		return buildmetadata.Record{}, errProfile
	}
	for _, value := range []string{r.GoVersion, r.GOOS, r.GOARCH} {
		if len(value) > 32 || !buildmetadata.ValidVersion(value) {
			return buildmetadata.Record{}, errProfile
		}
	}
	switch r.SourceStatus {
	case "unknown":
		if r.Revision != nil {
			return buildmetadata.Record{}, errProfile
		}
	case "clean", "dirty":
		if r.Revision == nil || !declaredRevision(*r.Revision) {
			return buildmetadata.Record{}, errProfile
		}
	default:
		return buildmetadata.Record{}, errProfile
	}
	return r, nil
}
func declaredRevision(value string) bool {
	if len(value) != 40 && len(value) != 64 {
		return false
	}
	for _, c := range []byte(value) {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func uniqueMetadataJSON(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 4 {
			return errProfile
		}
		token, err := decoder.Token()
		if err != nil {
			return errProfile
		}
		delimiter, object := token.(json.Delim)
		if !object {
			return nil
		}
		if delimiter != '{' {
			return errProfile
		}
		keys := map[string]bool{}
		for decoder.More() {
			token, err = decoder.Token()
			if err != nil {
				return errProfile
			}
			key, ok := token.(string)
			if !ok || len(key) > 64 || keys[key] || len(keys) >= 16 {
				return errProfile
			}
			keys[key] = true
			if err = walk(depth + 1); err != nil {
				return err
			}
		}
		token, err = decoder.Token()
		if err != nil || token != json.Delim('}') {
			return errProfile
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errProfile
	}
	return nil
}
