package storeexport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
)

type manifest struct {
	Version                           int       `json:"version"`
	Contract                          string    `json:"contract"`
	ExportID                          string    `json:"export_id"`
	CreatedAt                         time.Time `json:"created_at"`
	SourcePathBytes                   []byte    `json:"source_path_bytes"`
	DataFile                          string    `json:"data_file"`
	DataBytes                         int64     `json:"data_bytes"`
	DataSHA256                        string    `json:"data_sha256"`
	Profile                           profile   `json:"profile"`
	SourceApplicationWriteRequested   bool      `json:"source_application_write_requested"`
	SourceStorageEffectsAuthenticated bool      `json:"source_storage_effects_authenticated"`
	SourceBytesEqual                  *bool     `json:"source_bytes_equal"`
	FullInstanceBackup                bool      `json:"full_instance_backup"`
	CrossStoreAtomicity               bool      `json:"cross_store_atomicity"`
	RecordSemanticsChecked            bool      `json:"record_semantics_checked"`
	CurrentPermissionEvaluated        bool      `json:"current_permission_evaluated"`
	CleanupAuthority                  bool      `json:"cleanup_authority"`
	RestoreAvailable                  bool      `json:"restore_available"`
}

func digestValid(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && strings.ToLower(s) == s
}

func manifestID(m manifest) (string, error) {
	m.ExportID = ""
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if len(b) > maxManifestBytes-1 {
		return "", ErrBounds
	}
	sum := sha256.Sum256(b)
	return "plans-export-v1-" + hex.EncodeToString(sum[:]), nil
}

func encodeManifest(m manifest) ([]byte, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	if len(b) >= maxManifestBytes {
		return nil, ErrBounds
	}
	return append(b, '\n'), nil
}

func validateManifest(m manifest) error {
	app, _, ok := plans.ExportProfile(m.Profile.SchemaVersion)
	if !ok || m.Version != 1 || m.Contract != Contract || m.DataFile != "plans.sqlite3" || m.DataBytes <= 0 || m.DataBytes > maxDatabaseBytes || m.CreatedAt.IsZero() || m.Profile.ApplicationID != app || cleanPath(string(m.SourcePathBytes)) != nil || filepath.Base(string(m.SourcePathBytes)) != "plans.sqlite3" || filepath.Base(filepath.Dir(string(m.SourcePathBytes))) != "plans" || !digestValid(m.DataSHA256) {
		return ErrProfile
	}
	if m.Profile.PageSize < 512 || m.Profile.PageSize > 65536 || m.Profile.PageSize&(m.Profile.PageSize-1) != 0 || m.DataBytes%m.Profile.PageSize != 0 {
		return ErrProfile
	}
	if (m.Profile.SchemaVersion >= 2 && !digestValid(m.Profile.StoreToken)) || (m.Profile.SchemaVersion < 2 && m.Profile.StoreToken != "") || (m.Profile.SchemaVersion >= 5 && !digestValid(m.Profile.DismissalToken)) || (m.Profile.SchemaVersion < 5 && m.Profile.DismissalToken != "") {
		return ErrProfile
	}
	if m.SourceApplicationWriteRequested || m.SourceStorageEffectsAuthenticated || m.SourceBytesEqual != nil || m.FullInstanceBackup || m.CrossStoreAtomicity || m.RecordSemanticsChecked || m.CurrentPermissionEvaluated || m.CleanupAuthority || m.RestoreAvailable {
		return ErrProfile
	}
	id, err := manifestID(m)
	if err != nil {
		return err
	}
	if m.ExportID != id {
		return ErrProfile
	}
	return nil
}

// tokenBounds rejects duplicate keys and excessive nesting before typed decode.
// Its input is already held, identity-checked and <=64KiB.
func tokenBounds(body []byte) error {
	if len(body) > maxManifestBytes {
		return ErrBounds
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	tokens := 0
	var read func(int) error
	read = func(depth int) error {
		if depth > maxManifestDepth {
			return ErrBounds
		}
		t, err := d.Token()
		if err != nil {
			return err
		}
		tokens++
		if tokens > maxManifestTokens {
			return ErrBounds
		}
		open, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch open {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				tokens++
				if tokens > maxManifestTokens {
					return ErrBounds
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return ErrProfile
				}
				seen[key] = true
				if err = read(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for d.More() {
				if err = read(depth + 1); err != nil {
					return err
				}
			}
		default:
			return ErrProfile
		}
		end, err := d.Token()
		if err != nil {
			return err
		}
		tokens++
		if tokens > maxManifestTokens {
			return ErrBounds
		}
		if (open == '{' && end != json.Delim('}')) || (open == '[' && end != json.Delim(']')) {
			return ErrProfile
		}
		return nil
	}
	if err := read(1); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		if err == nil {
			err = ErrProfile
		}
		return err
	}
	return nil
}

func parseManifest(body []byte) (manifest, error) {
	var m manifest
	if err := tokenBounds(body); err != nil {
		return m, err
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return m, errors.Join(err, ErrProfile)
	}
	canonical, err := encodeManifest(m)
	if err != nil {
		return m, err
	}
	if !bytes.Equal(canonical, body) {
		return m, ErrProfile
	}
	return m, nil
}
