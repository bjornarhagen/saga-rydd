package storeexport

import (
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"modernc.org/sqlite"
)

// phaseError adds only a fixed caller-supplied diagnostic phase. It preserves
// nil and errors.Is/As. The helper adds no SQL, path or body text; underlying
// error text is preserved, not sanitized.
func phaseError(phase string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", phase, err)
}

type engineEnv struct {
	guard *guard
	admit func() error
}

func (e engineEnv) fence() error {
	if err := e.guard.check(); err != nil {
		return err
	}
	if e.admit != nil {
		if err := e.admit(); err != nil {
			return err
		}
	}
	return e.guard.check()
}

func engineURI(path string, destination bool) string {
	u := url.URL{Scheme: "file", Path: path}
	q := url.Values{"mode": {"ro"}, "cache": {"private"}, "_pragma": {"busy_timeout(1000)", "cache_size(-4096)", "mmap_size(0)", "trusted_schema(0)"}}
	if destination {
		q.Set("mode", "rw")
		q.Add("_pragma", "journal_mode(DELETE)")
		q.Add("_pragma", "synchronous(FULL)")
	} else {
		q.Add("_pragma", "query_only(1)")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func openEngine(path string, e engineEnv) (*sql.DB, *sql.Conn, error) {
	if err := e.fence(); err != nil {
		return nil, nil, err
	}
	db, err := sql.Open("sqlite", engineURI(path, false))
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(e.guard.ctx)
	if err != nil {
		return nil, nil, errors.Join(err, db.Close())
	}
	if err = e.fence(); err != nil {
		_, closeErr := consumeConnection(conn, db, func(any) error { return nil })
		return nil, nil, errors.Join(err, closeErr)
	}
	return db, conn, nil
}

// query copies only a fixed projection and checks every owned Rows close.
// These row limits do not cap SQLite's internal execution or schema parsing.
func query(conn *sql.Conn, query string, columns, maximum int, e engineEnv) (values [][]any, err error) {
	if err = e.fence(); err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(e.guard.ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { joined(&err, rows.Close()); joined(&err, rows.Err()); joined(&err, e.fence()) }()
	if err = e.fence(); err != nil {
		return nil, err
	}
	for i := 0; i <= maximum; i++ {
		if err = e.fence(); err != nil {
			return nil, err
		}
		more := rows.Next()
		if err = e.fence(); err != nil {
			return nil, err
		}
		if !more {
			return values, rows.Err()
		}
		if i == maximum {
			return nil, phaseError("query row cap", ErrBounds)
		}
		row := make([]any, columns)
		ptrs := make([]any, columns)
		for j := range row {
			ptrs[j] = &row[j]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for j, v := range row {
			if b, ok := v.([]byte); ok {
				if len(b) > 4097 {
					return nil, phaseError("query byte-column cap", ErrBounds)
				}
				row[j] = append([]byte(nil), b...)
			}
			if s, ok := v.(string); ok && len(s) > 4097 {
				return nil, phaseError("query string-column cap", ErrBounds)
			}
		}
		values = append(values, row)
	}
	return nil, phaseError("query iteration cap", ErrBounds)
}

func scalar(conn *sql.Conn, q string, e engineEnv) (int64, error) {
	rows, err := query(conn, q, 1, 1, e)
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 {
		return 0, ErrProfile
	}
	v, ok := rows[0][0].(int64)
	if !ok {
		return 0, ErrProfile
	}
	return v, nil
}

func textValue(v any, max int) (string, error) {
	var s string
	switch v := v.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return "", ErrProfile
	}
	if len(s) > max {
		return "", phaseError("profile text cap", ErrBounds)
	}
	return s, nil
}

func identityToken(conn *sql.Conn, q string, e engineEnv) (string, error) {
	rows, err := query(conn, q, 1, 1, e)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", ErrProfile
	}
	s, err := textValue(rows[0][0], 64)
	if err != nil {
		return "", err
	}
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 32 || strings.ToLower(s) != s {
		return "", ErrProfile
	}
	return s, nil
}

func readProfile(conn *sql.Conn, e engineEnv) (p profile, err error) {
	if p.ApplicationID, err = scalar(conn, "PRAGMA application_id", e); err != nil {
		return
	}
	if p.SchemaVersion, err = scalar(conn, "PRAGMA user_version", e); err != nil {
		return
	}
	app, known, ok := plans.ExportProfile(p.SchemaVersion)
	if !ok || p.ApplicationID != app {
		return p, ErrProfile
	}
	if p.PageSize, err = scalar(conn, "PRAGMA page_size", e); err != nil {
		return
	}
	if p.PageSize < 512 || p.PageSize > 65536 || p.PageSize&(p.PageSize-1) != 0 {
		return p, ErrProfile
	}
	pages, err := scalar(conn, "PRAGMA page_count", e)
	if err != nil {
		return p, err
	}
	if pages < 1 || pages > maxDatabaseBytes/p.PageSize {
		return p, phaseError("profile page-count cap", ErrBounds)
	}
	for _, check := range []struct {
		query    string
		expected int64
	}{{"PRAGMA query_only", 1}, {"PRAGMA mmap_size", 0}, {"PRAGMA trusted_schema", 0}, {"PRAGMA busy_timeout", 1000}, {"PRAGMA cache_size", -4096}} {
		actual, e2 := scalar(conn, check.query, e)
		if e2 != nil {
			return p, e2
		}
		if actual != check.expected {
			return p, ErrProfile
		}
	}
	rows, err := query(conn, "SELECT substr(CAST(name AS BLOB),1,129),substr(CAST(type AS BLOB),1,129),substr(CAST(tbl_name AS BLOB),1,129),CASE WHEN sql IS NULL THEN NULL ELSE substr(CAST(sql AS BLOB),1,4097) END FROM sqlite_schema ORDER BY name LIMIT 65", 4, maxSchemaRows, e)
	if err != nil {
		return p, err
	}
	if len(rows) != len(known) {
		return p, ErrProfile
	}
	for i, row := range rows {
		name, er := textValue(row[0], 128)
		if er != nil {
			return p, er
		}
		typeName, er := textValue(row[1], 128)
		if er != nil {
			return p, er
		}
		table, er := textValue(row[2], 128)
		if er != nil {
			return p, er
		}
		want := known[i]
		if name != want.Name || typeName != want.Type || table != want.Table {
			return p, ErrProfile
		}
		if want.SQL == "" {
			if row[3] != nil {
				return p, ErrProfile
			}
			continue
		}
		ddl, er := textValue(row[3], 4096)
		if er != nil {
			return p, er
		}
		if strings.TrimSpace(ddl) != strings.TrimSpace(want.SQL) {
			return p, ErrProfile
		}
	}
	if p.SchemaVersion >= 2 {
		p.StoreToken, err = identityToken(conn, "SELECT substr(CAST(token AS BLOB),1,65) FROM plan_store_identity WHERE singleton=1 LIMIT 2", e)
		if err != nil {
			return
		}
	}
	if p.SchemaVersion >= 5 {
		p.DismissalToken, err = identityToken(conn, "SELECT substr(CAST(token AS BLOB),1,65) FROM dismissal_store_identity WHERE singleton=1 LIMIT 2", e)
	}
	return
}

func checkedClose(conn driver.Conn) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("engine Close panicked; closure remains unknown")
		}
	}()
	return conn.Close()
}

// consumeConnection closes the borrowed physical connection inside Raw, then
// purposefully discards the sql wrapper. This is pinned to Go1.27.1's Raw release
// and modernc v1.59.0's successful-Close idempotence. Raw is never retried.
func consumeConnection(conn *sql.Conn, db *sql.DB, action func(any) error) (closed bool, err error) {
	entered, attempted := false, false
	var operationErr, sourceCloseErr error
	rawErr := conn.Raw(func(raw any) (discard error) {
		entered = true
		discard = driver.ErrBadConn
		source, ok := raw.(driver.Conn)
		if !ok {
			operationErr = ErrProfile
			return
		}
		defer func() {
			if recover() != nil {
				operationErr = errors.Join(operationErr, errors.New("engine callback panicked; operation refused"))
			}
			attempted = true
			sourceCloseErr = checkedClose(source)
		}()
		operationErr = action(raw)
		return
	})
	connCloseErr := conn.Close()
	poolCloseErr := db.Close()
	closed = entered && attempted && sourceCloseErr == nil && rawErr == driver.ErrBadConn && connCloseErr == sql.ErrConnDone && poolCloseErr == nil
	err = errors.Join(operationErr, sourceCloseErr, poolCloseErr)
	if !entered || !attempted || rawErr != driver.ErrBadConn {
		err = errors.Join(err, rawErr, ErrProfile)
	}
	if connCloseErr != sql.ErrConnDone {
		err = errors.Join(err, connCloseErr, ErrProfile)
	}
	return
}

type backupHandle interface {
	Step(int32) (bool, error)
	PageCount() int
	Commit() (driver.Conn, error)
}

// Match the pinned driver API at compile time, independently of generated fakes.
var _ backupHandle = (*sqlite.Backup)(nil)

type backupStarter func(any, string, engineEnv) (backupHandle, error)

func startBackup(raw any, uri string, e engineEnv) (backupHandle, error) {
	source, ok := raw.(interface {
		NewBackup(string) (*sqlite.Backup, error)
		IsReadOnly(string) (bool, error)
	})
	if !ok {
		return nil, ErrProfile
	}
	if err := e.fence(); err != nil {
		return nil, err
	}
	ro, err := source.IsReadOnly("main")
	if err != nil {
		return nil, err
	}
	if !ro {
		return nil, ErrProfile
	}
	if err = e.fence(); err != nil {
		return nil, err
	}
	b, err := source.NewBackup(uri)
	return b, errors.Join(err, e.fence())
}

func finalizeBackup(b backupHandle) (destination driver.Conn, err error) {
	defer func() {
		if recover() != nil {
			destination = nil
			err = errors.New("backup finalization panicked; destination closure remains unknown")
		}
	}()
	return b.Commit()
}

func checkedRowsClose(rows driver.Rows) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("destination Rows.Close panicked; operation refused")
		}
	}()
	return rows.Close()
}

// normalizeDestination uses only the completed backup's returned destination,
// while still inside the same source Raw callback. SQLite may write its own
// journal/sidecars here; entered calls are not hard time or write-budget caps.
// A successful DELETE reply is not enough: closed candidate header and exact
// member checks remain separate requirements before verification/publication.
func normalizeDestination(destination driver.Conn, e engineEnv) (err error) {
	var rows driver.Rows
	defer func() {
		if recover() != nil {
			joined(&err, errors.New("destination normalization panicked; operation refused"))
		}
		if rows != nil {
			joined(&err, checkedRowsClose(rows))
		}
		joined(&err, e.fence())
	}()
	if err = e.fence(); err != nil {
		return
	}
	queryer, ok := destination.(driver.QueryerContext)
	if !ok {
		return ErrProfile
	}
	rows, err = queryer.QueryContext(e.guard.ctx, "PRAGMA main.journal_mode=DELETE", nil)
	if err != nil {
		return
	}
	if rows == nil {
		return ErrProfile
	}
	if err = e.fence(); err != nil {
		return
	}
	columns := rows.Columns()
	if err = e.fence(); err != nil {
		return
	}
	if len(columns) != 1 || columns[0] != "journal_mode" {
		return ErrProfile
	}
	var values [1]driver.Value
	if err = e.fence(); err != nil {
		return
	}
	if err = rows.Next(values[:]); err != nil {
		if err == io.EOF {
			err = ErrProfile
		}
		return
	}
	if err = e.fence(); err != nil {
		return
	}
	mode, ok := values[0].(string)
	if !ok || mode != "delete" {
		return ErrProfile
	}
	values[0] = nil
	if err = e.fence(); err != nil {
		return
	}
	nextErr := rows.Next(values[:])
	if err = e.fence(); err != nil {
		return
	}
	if nextErr != io.EOF {
		if nextErr == nil {
			return ErrProfile
		}
		return nextErr
	}
	return nil
}

// finishDestination always attempts its checked physical close, including
// failed/partial backups, normalization refusals, late guards and panics.
// Only the caller's observed DONE/successful finalization/no-prior-error path
// enables normalization. Closure knowledge does not imply operation success.
func finishDestination(destination driver.Conn, normalize bool, e engineEnv) (closed bool, err error) {
	defer func() {
		if recover() != nil {
			joined(&err, errors.New("destination completion panicked; operation refused"))
		}
		closeErr := checkedClose(destination)
		closed = closeErr == nil
		joined(&err, closeErr)
		joined(&err, e.fence())
	}()
	if normalize {
		err = phaseError("destination rollback journal normalization", normalizeDestination(destination, e))
	}
	return
}

func transfer(raw any, destination string, p profile, e engineEnv, start backupStarter) (destinationClosed bool, err error) {
	if err = e.fence(); err != nil {
		return
	}
	b, err := start(raw, engineURI(destination, true), e)
	if b == nil {
		if err == nil {
			err = ErrProfile
		}
		return
	} // Init may hide a destination Close error.
	finalizationAttempted, doneObserved := false, false
	defer func() {
		if recover() != nil {
			err = errors.Join(err, errors.New("backup method panicked; operation refused"))
		}
		if !finalizationAttempted {
			finalizationAttempted = true
			dest, finishErr := finalizeBackup(b)
			joined(&err, finishErr)
			if dest == nil {
				joined(&err, ErrProfile)
			} else {
				closed, destErr := finishDestination(dest, doneObserved && err == nil, e)
				joined(&err, destErr)
				destinationClosed = finishErr == nil && closed
			}
		}
		joined(&err, e.fence())
	}()
	if err != nil {
		return
	}
	if err = e.fence(); err != nil {
		return
	}
	pages := int32(bufferBytes / p.PageSize)
	if pages < 1 || pages > 128 {
		return false, phaseError("backup step-page cap", ErrBounds)
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err = e.fence(); err != nil {
			return
		}
		more, stepErr := b.Step(pages)
		if stepErr != nil {
			return false, stepErr
		}
		if err = e.fence(); err != nil {
			return
		}
		// Go int widens losslessly to int64 on both supported integer widths.
		// Compare to the divided byte cap before any size multiplication.
		count := int64(b.PageCount())
		if err = e.fence(); err != nil {
			return
		}
		if count < 1 || count > maxDatabaseBytes/p.PageSize {
			return false, phaseError("backup page-count cap", ErrBounds)
		}
		if !more {
			doneObserved = true
			return destinationClosed, nil
		} // Deferred finalization/normalization/Close completes before return.
	}
	return false, phaseError("backup attempt cap", ErrBounds)
}

func integrity(conn *sql.Conn, e engineEnv) error {
	rows, err := query(conn, "SELECT substr(CAST(integrity_check AS BLOB),1,129) FROM pragma_integrity_check LIMIT 2", 1, 1, e)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return ErrProfile
	}
	text, err := textValue(rows[0][0], 128)
	if err != nil {
		return err
	}
	if text != "ok" {
		return fmt.Errorf("structural SQLite integrity check failed: %w", ErrProfile)
	}
	rows, err = query(conn, "SELECT 1 FROM pragma_foreign_key_check LIMIT 1", 1, 0, e)
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return ErrProfile
	}
	return nil
}

// No driver, Backup or destination connection is returned by these helpers.
// All use, finalization and checked physical-close attempts stay synchronous.
