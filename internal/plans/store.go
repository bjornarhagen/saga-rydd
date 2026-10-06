// Package plans preserves review records separately from rebuildable inventory.
// Review consent is stored separately; no record can execute cleanup.
package plans

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const MaxRecordBytes = 1 << 20
const filename = "plans.sqlite3"
const applicationID = 0x5259504c // RYPL; deliberately distinct from inventory.

var ErrID = errors.New("use the full saved plan ID returned by plan --save")
var ErrCorrupt = errors.New("saved plan is invalid or its content digest does not match; no action is authorized")

type Record struct {
	Version                   int                     `json:"version"`
	CreatedAt                 time.Time               `json:"created_at"`
	Status                    string                  `json:"status"`
	Action                    string                  `json:"proposed_future_action"`
	Activity                  string                  `json:"project_activity"`
	Executable                bool                    `json:"executable"`
	ApprovalAvailable         bool                    `json:"approval_available"`
	QuarantineReclaimsSpace   bool                    `json:"quarantine_reclaims_space"`
	EstimatedReclaimableBytes *int64                  `json:"estimated_reclaimable_bytes"`
	Selection                 state.SelectionSnapshot `json:"selection"`
}

type Saved struct {
	ID          string               `json:"id"`
	Record      Record               `json:"record"`
	Review      *Review              `json:"review,omitempty"`
	Observation *CapturedObservation `json:"observation,omitempty"`
}

func ValidID(id string) bool {
	return strings.HasPrefix(id, "plan-v1-") && validDigest(strings.TrimPrefix(id, "plan-v1-"))
}

func validDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}

func Save(ctx context.Context, base string, selection state.SelectionSnapshot) (Saved, error) {
	if err := ctx.Err(); err != nil {
		return Saved{}, err
	}
	r := Record{Version: 1, CreatedAt: time.Now().UTC(), Status: "unapproved", Action: "same_filesystem_quarantine", Activity: "unconfirmed", Selection: selection}
	if err := validate(r); err != nil {
		return Saved{}, err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return Saved{}, err
	}
	if len(payload) > MaxRecordBytes {
		return Saved{}, errors.New("selected evidence exceeds the 1 MiB saved-plan limit; select fewer targets")
	}
	id := fmt.Sprintf("plan-v1-%x", sha256.Sum256(payload))
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	// One FULL-synchronous SQLite commit publishes the entire immutable record.
	if _, err = db.ExecContext(ctx, "INSERT INTO plans(id,payload) VALUES(?,?)", id, payload); err != nil {
		return Saved{}, err
	}
	return Saved{ID: id, Record: r}, nil
}

func Load(ctx context.Context, base, id string) (Saved, error) {
	if !ValidID(id) {
		return Saved{}, ErrID
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		return Saved{}, err
	}
	defer closeDB()
	return load(ctx, db, id)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func load(ctx context.Context, db queryer, id string) (Saved, error) {
	var payload []byte
	// The SQL bound also protects readers from an oversized externally altered row.
	err := db.QueryRowContext(ctx, "SELECT substr(payload,1,?) FROM plans WHERE id=?", MaxRecordBytes+1, id).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return Saved{}, fmt.Errorf("saved plan not found: %w", os.ErrNotExist)
	}
	if err != nil {
		return Saved{}, err
	}
	if len(payload) > MaxRecordBytes || id != fmt.Sprintf("plan-v1-%x", sha256.Sum256(payload)) {
		return Saved{}, ErrCorrupt
	}
	var r Record
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&r); err != nil || validate(r) != nil {
		return Saved{}, ErrCorrupt
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		return Saved{}, ErrCorrupt
	}
	return Saved{ID: id, Record: r}, nil
}

func validate(r Record) error {
	s := r.Selection
	f := s.Evidence.Findings
	if r.Version != 1 || r.CreatedAt.IsZero() || r.Status != "unapproved" || r.Action != "same_filesystem_quarantine" || r.Activity != "unconfirmed" {
		return ErrCorrupt
	}
	if r.Executable || r.ApprovalAvailable || r.QuarantineReclaimsSpace || r.EstimatedReclaimableBytes != nil {
		return ErrCorrupt
	}
	if !validDigest(s.InventoryID) || len(f) < 1 || len(f) > state.PreviewTargetLimit || len(s.Targets) != len(f) || len(s.Roots) < 1 || len(s.Roots) > len(f) {
		return ErrCorrupt
	}
	if s.Evidence.CurrentStateVerified || s.Evidence.MinimumAgeDays < 1 || s.Evidence.MinimumAgeDays > state.MaxFindingAgeDays || s.Evidence.Source != "saved_inventory" || s.Evidence.PageCoverage != "selected_entries_only" {
		return ErrCorrupt
	}
	roots := map[int64]bool{}
	for _, root := range s.Roots {
		if roots[root.ID] || root.ID <= 0 || !filepath.IsAbs(string(root.PathBytes)) {
			return ErrCorrupt
		}
		roots[root.ID] = true
	}
	seen := map[string]bool{}
	for i, finding := range f {
		if seen[finding.ID] || !roots[finding.RootID] || finding.EntryID <= 0 || finding.ID != fmt.Sprintf("node-modules-v1:%d:%d", finding.RootID, finding.EntryID) || s.Targets[i].FindingID != finding.ID || len(finding.Actions) != 0 || finding.Measurement.CurrentStateVerified || !filepath.IsAbs(string(finding.PathBytes)) {
			return ErrCorrupt
		}
		if s.Targets[i].Target.Device != finding.Device || s.Targets[i].Target.Inode != finding.Inode {
			return ErrCorrupt
		}
		seen[finding.ID] = true
	}
	return nil
}

func privateFile(path string) error {
	if err := localfs.CheckPrivateFile(path); err != nil {
		return err
	}
	var st unix.Stat_t
	if err := unix.Lstat(path, &st); err != nil {
		return err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Nlink != 1 {
		return errors.New("plan database files must be owned by this user with one link")
	}
	return nil
}

func open(ctx context.Context, base string, write bool) (*sql.DB, func(), error) {
	return openWithMigration(ctx, base, write, true)
}

// Observation capture defers additive migration until its publication transaction.
func openWithMigration(ctx context.Context, base string, write, migrate bool) (*sql.DB, func(), error) {
	if !filepath.IsAbs(base) {
		return nil, nil, errors.New("plan storage requires an absolute data directory")
	}
	dir := filepath.Join(base, "plans")
	for _, path := range []string{base, dir} {
		if write {
			if err := localfs.EnsurePrivateDir(path); err != nil {
				return nil, nil, err
			}
		}
		if err := localfs.CheckOwnedDir(path); err != nil {
			return nil, nil, err
		}
	}
	var lock *localfs.Lock
	var err error
	if write {
		lock, err = localfs.AcquireLock(dir)
		if err != nil {
			return nil, nil, err
		}
	}
	var db *sql.DB
	closeDB := func() {
		if db != nil {
			_ = db.Close()
		}
		if lock != nil {
			_ = lock.Close()
		}
	}
	fail := func(err error) (*sql.DB, func(), error) { closeDB(); return nil, nil, err }
	path := filepath.Join(dir, filename)
	if write {
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e == nil {
			e = f.Close()
		}
		if e != nil && !errors.Is(e, os.ErrExist) {
			return fail(e)
		}
	}
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		if err = privateFile(path + suffix); err != nil && !(suffix != "" && errors.Is(err, os.ErrNotExist)) {
			return fail(err)
		}
	}
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{"mode": {"ro"}, "_pragma": {"busy_timeout(1000)", "cache_size(-1024)", "mmap_size(0)"}}
	if write {
		query.Set("mode", "rw")
		query.Add("_pragma", "synchronous(FULL)")
		query.Add("_pragma", "wal_autocheckpoint(256)")
	} else {
		query.Add("_pragma", "query_only(1)")
	}
	u.RawQuery = query.Encode()
	db, err = sql.Open("sqlite", u.String())
	if err != nil {
		return fail(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var app, version int
	if err = db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&app); err != nil {
		return fail(err)
	}
	if err = db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fail(err)
	}
	if write && app == 0 && version == 0 {
		var tables int
		if err = db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").Scan(&tables); err != nil {
			return fail(err)
		}
		if tables != 0 {
			return fail(errors.New("refusing unidentified plan database"))
		}
		var journal string
		if err = db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&journal); err != nil {
			return fail(err)
		}
		if journal != "wal" {
			return fail(errors.New("plan storage requires a local filesystem with WAL support"))
		}
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			return fail(e)
		}
		defer tx.Rollback()
		_, e = tx.ExecContext(ctx, `CREATE TABLE plans (id TEXT PRIMARY KEY, payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=1048576));
CREATE TRIGGER plans_no_update BEFORE UPDATE ON plans BEGIN SELECT RAISE(ABORT,'plans are immutable'); END;
CREATE TRIGGER plans_no_delete BEFORE DELETE ON plans BEGIN SELECT RAISE(ABORT,'plans are immutable'); END;
PRAGMA application_id=0x5259504c; PRAGMA user_version=1;`)
		if e != nil {
			_ = tx.Rollback()
			return fail(e)
		}
		if e = tx.Commit(); e != nil {
			return fail(e)
		}
		version = 1
	} else if app != applicationID || (version != 1 && version != 2 && version != 3) {
		return fail(errors.New("unsupported or unidentified plan database; left intact"))
	}
	if write && migrate && version < 3 {
		if err = migrateObservationStore(ctx, db, version); err != nil {
			return fail(err)
		}
	}
	return db, closeDB, nil
}
