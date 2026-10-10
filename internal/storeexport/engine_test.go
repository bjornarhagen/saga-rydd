package storeexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

type fakeConnection struct {
	closes, queries                     int
	firstCloseErr, queryErr             error
	panicClose, panicQuery, missingRows bool
	rows                                *fakeJournalRows
	events                              *[]string
	afterQuery                          func()
}

func (c *fakeConnection) Prepare(string) (driver.Stmt, error) { return nil, ErrProfile }
func (c *fakeConnection) Begin() (driver.Tx, error)           { return nil, ErrProfile }
func (c *fakeConnection) Close() error {
	c.closes++
	if c.events != nil {
		*c.events = append(*c.events, "destination-close")
	}
	if c.panicClose {
		panic("generated Close failure")
	}
	if c.closes == 1 {
		return c.firstCloseErr
	}
	return nil
}
func (c *fakeConnection) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.queries++
	if c.events != nil {
		*c.events = append(*c.events, "normalize-query")
	}
	if c.afterQuery != nil {
		c.afterQuery()
	}
	if c.panicQuery {
		panic("generated query panic")
	}
	if q != "PRAGMA main.journal_mode=DELETE" || args != nil || ctx == nil {
		return nil, ErrProfile
	}
	if c.missingRows {
		return nil, c.queryErr
	}
	if c.rows == nil {
		c.rows = &fakeJournalRows{columns: []string{"journal_mode"}, records: [][]driver.Value{{"delete"}}}
	}
	c.rows.events = c.events
	return c.rows, c.queryErr
}

type fakeJournalRows struct {
	columns               []string
	records               [][]driver.Value
	nextCalls, closes     int
	nextErr, closeErr     error
	panicNext, panicClose bool
	events                *[]string
	afterNext, afterClose func()
}
type noQueryDestination struct{ driver.Conn }

func (r *fakeJournalRows) Columns() []string { return r.columns }
func (r *fakeJournalRows) Next(values []driver.Value) error {
	r.nextCalls++
	if r.events != nil {
		*r.events = append(*r.events, "normalize-next")
	}
	if r.afterNext != nil {
		r.afterNext()
	}
	if r.panicNext {
		panic("generated Next panic")
	}
	if r.nextErr != nil {
		return r.nextErr
	}
	if r.nextCalls > len(r.records) {
		return io.EOF
	}
	copy(values, r.records[r.nextCalls-1])
	return nil
}
func (r *fakeJournalRows) Close() error {
	r.closes++
	if r.events != nil {
		*r.events = append(*r.events, "rows-close")
	}
	if r.afterClose != nil {
		r.afterClose()
	}
	if r.panicClose {
		panic("generated Rows.Close panic")
	}
	return r.closeErr
}

type fakeConnector struct {
	c     *fakeConnection
	opens *int
}

func (c fakeConnector) Connect(context.Context) (driver.Conn, error) { (*c.opens)++; return c.c, nil }
func (c fakeConnector) Driver() driver.Driver                        { return fakeDriver{c} }

type fakeDriver struct{ connector fakeConnector }

func (d fakeDriver) Open(string) (driver.Conn, error) {
	return d.connector.Connect(context.Background())
}

func fakeSQLConn(t *testing.T, c *fakeConnection) (*sql.DB, *sql.Conn, *int) {
	t.Helper()
	opens := 0
	db := sql.OpenDB(fakeConnector{c, &opens})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return db, conn, &opens
}

func TestRawCheckedPhysicalCloseAndPoolDiscardAreSeparate(t *testing.T) {
	for _, kind := range []string{"success", "operation", "real-ErrBadConn", "close", "panic"} {
		t.Run(kind, func(t *testing.T) {
			physical := &fakeConnection{}
			cause := errors.New("generated exact cause")
			if kind == "close" {
				physical.firstCloseErr = cause
			}
			db, conn, opens := fakeSQLConn(t, physical)
			callbacks := 0
			closed, err := consumeConnection(conn, db, func(any) error {
				callbacks++
				if kind == "operation" {
					return cause
				}
				if kind == "real-ErrBadConn" {
					return driver.ErrBadConn
				}
				if kind == "panic" {
					panic("generated callback failure")
				}
				return nil
			})
			if callbacks != 1 || *opens != 1 || physical.closes != 2 {
				t.Fatalf("callbacks=%d opens=%d physical closes=%d", callbacks, *opens, physical.closes)
			}
			if db.Stats().OpenConnections != 0 {
				t.Fatal("discard did not empty private pool")
			}
			if _, e := conn.ExecContext(context.Background(), "unused"); e != sql.ErrConnDone {
				t.Fatal(e)
			}
			if kind == "success" {
				if err != nil || !closed {
					t.Fatalf("closed=%v err=%v", closed, err)
				}
			} else if err == nil {
				t.Fatal("failure normalized as success")
			}
			if kind == "close" && (closed || !errors.Is(err, cause)) {
				t.Fatal("hidden first-close failure")
			}
			if kind == "operation" && !errors.Is(err, cause) {
				t.Fatal("operation cause lost")
			}
			if kind == "real-ErrBadConn" && !errors.Is(err, driver.ErrBadConn) {
				t.Fatal("actual operation sentinel lost")
			}
		})
	}
}

type fakeBackup struct {
	steps, commits     int
	requested          []int32
	more               bool
	stepErr, commitErr error
	panicStep          bool
	count              int
	destination        *fakeConnection
	afterStep          func()
	events             *[]string
	afterCommit        func()
}

func (b *fakeBackup) Step(n int32) (bool, error) {
	b.steps++
	if b.events != nil {
		*b.events = append(*b.events, "step")
	}
	if len(b.requested) < 3 {
		b.requested = append(b.requested, n)
	}
	if b.afterStep != nil {
		b.afterStep()
	}
	if b.panicStep {
		panic("generated Step failure")
	}
	return b.more, b.stepErr
}
func (b *fakeBackup) PageCount() int { return b.count }
func (b *fakeBackup) Commit() (driver.Conn, error) {
	b.commits++
	if b.events != nil {
		*b.events = append(*b.events, "commit")
	}
	if b.afterCommit != nil {
		b.afterCommit()
	}
	if b.commitErr != nil {
		return nil, b.commitErr
	}
	return b.destination, nil
}

func TestBackupDONEAndFiniteAbandonmentFinalizeExactlyOnce(t *testing.T) {
	for _, kind := range []string{"done", "OK-until-cap", "step-error", "finish-error", "close-error", "panic", "late-DONE", "growth", "zero-pages", "negative-pages", "page-cap", "page-cap-plus-one", "native-int-max", "native-int-min"} {
		t.Run(kind, func(t *testing.T) {
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			elapsed := time.Duration(0)
			g := guardWithClocks(context.Background(), func() time.Time { return now }, func() time.Duration { return elapsed })
			e := engineEnv{guard: g}
			b := &fakeBackup{count: 1, destination: &fakeConnection{}}
			switch kind {
			case "OK-until-cap":
				b.more = true
			case "step-error":
				b.stepErr = errors.New("Step failed")
			case "finish-error":
				b.commitErr = errors.New("Commit failed")
			case "close-error":
				b.destination.firstCloseErr = errors.New("Close failed")
			case "panic":
				b.panicStep = true
			case "late-DONE":
				b.afterStep = func() { elapsed += wholeLimit }
			case "growth":
				b.count = 1 << 22
			case "zero-pages":
				b.count = 0
			case "negative-pages":
				b.count = -1
			case "page-cap":
				b.count = int(maxDatabaseBytes / 512)
			case "page-cap-plus-one":
				b.count = int(maxDatabaseBytes/512) + 1
			case "native-int-max":
				b.count = int(^uint(0) >> 1)
			case "native-int-min":
				b.count = -int(^uint(0)>>1) - 1
			}
			starts := 0
			closed, err := transfer(nil, "/generated/candidate", profile{PageSize: 512}, e, func(any, string, engineEnv) (backupHandle, error) { starts++; return b, nil })
			if starts != 1 || b.commits != 1 {
				t.Fatalf("starts=%d finalizations=%d", starts, b.commits)
			}
			if kind == "done" || kind == "page-cap" {
				if !closed || err != nil {
					t.Fatalf("closed=%v err=%v", closed, err)
				}
			} else if err == nil {
				t.Fatal("incomplete or invalid backup accepted")
			}
			wantQueries := 0
			if kind == "done" || kind == "page-cap" || kind == "close-error" {
				wantQueries = 1
			}
			if b.destination.queries != wantQueries {
				t.Fatal("failed/partial backup normalized or DONE normalization missed", kind, b.destination.queries, wantQueries)
			}
			switch kind {
			case "zero-pages", "negative-pages", "page-cap-plus-one", "native-int-max", "native-int-min":
				if !errors.Is(err, ErrBounds) || b.steps != 1 || !closed {
					t.Fatalf("invalid native-int page count escaped bounded one-step finalization: closed=%v steps=%d err=%v", closed, b.steps, err)
				}
			}
			if kind == "OK-until-cap" && b.steps != maxAttempts {
				t.Fatal(b.steps)
			}
			for _, n := range b.requested {
				if n != 128 {
					t.Fatalf("positive512-page request=%d", n)
				}
			}
			if kind == "finish-error" {
				if b.destination.closes != 0 || closed {
					t.Fatal("unreturned destination claimed closed")
				}
			} else if b.destination.closes != 1 {
				t.Fatal("destination close not exact once")
			}
		})
	}
	// These generated cases retain the existing substantive parent selection.
	// The exact destination query runs only after valid DONE and successful Commit;
	// each returned Rows and destination physical Close is still checked on refusal.
	t.Run("completed-destination-missing-query-interface", func(t *testing.T) {
		dest := &fakeConnection{}
		closed, err := finishDestination(noQueryDestination{dest}, true, engineEnv{guard: newGuard(context.Background())})
		if !closed || !errors.Is(err, ErrProfile) || dest.queries != 0 || dest.closes != 1 {
			t.Fatal("missing pinned API hidden or checked close missed", closed, err, dest)
		}
	})
	for _, kind := range []string{"exact", "query-error-with-rows", "query-error-no-rows", "missing-rows", "missing-column", "extra-column", "wrong-column", "no-row", "wrong-mode", "wrong-type", "extra-row", "next-error", "rows-close-error", "rows-close-panic", "query-panic", "next-panic", "late-query", "late-next", "late-rows-close", "late-commit", "canceled-query", "destination-close-error", "destination-close-panic"} {
		t.Run("completed-destination-"+kind, func(t *testing.T) {
			elapsed := time.Duration(0)
			wall := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g := guardWithClocks(ctx, func() time.Time { return wall }, func() time.Duration { return elapsed })
			events := []string{}
			cause := errors.New("generated destination refusal")
			rows := &fakeJournalRows{columns: []string{"journal_mode"}, records: [][]driver.Value{{"delete"}}}
			dest := &fakeConnection{rows: rows, events: &events}
			b := &fakeBackup{count: 1, destination: dest, events: &events}
			switch kind {
			case "query-error-with-rows":
				dest.queryErr = cause
			case "query-error-no-rows":
				dest.queryErr = cause
				dest.missingRows = true
			case "missing-rows":
				dest.missingRows = true
			case "missing-column":
				rows.columns = nil
			case "extra-column":
				rows.columns = []string{"journal_mode", "unexpected"}
			case "wrong-column":
				rows.columns = []string{"unexpected"}
			case "no-row":
				rows.records = nil
			case "wrong-mode":
				rows.records = [][]driver.Value{{"wal"}}
			case "wrong-type":
				rows.records = [][]driver.Value{{int64(1)}}
			case "extra-row":
				rows.records = [][]driver.Value{{"delete"}, {"delete"}}
			case "next-error":
				rows.nextErr = cause
			case "rows-close-error":
				rows.closeErr = cause
			case "rows-close-panic":
				rows.panicClose = true
			case "query-panic":
				dest.panicQuery = true
			case "next-panic":
				rows.panicNext = true
			case "late-query":
				dest.afterQuery = func() { elapsed = wholeLimit }
			case "late-next":
				rows.afterNext = func() { elapsed = wholeLimit }
			case "late-rows-close":
				rows.afterClose = func() { elapsed = wholeLimit }
			case "late-commit":
				b.afterCommit = func() { elapsed = wholeLimit }
			case "canceled-query":
				dest.afterQuery = cancel
			case "destination-close-error":
				dest.firstCloseErr = cause
			case "destination-close-panic":
				dest.panicClose = true
			}
			closed, err := transfer(nil, "/generated/candidate", profile{PageSize: 4096}, engineEnv{guard: g}, func(any, string, engineEnv) (backupHandle, error) { return b, nil })
			if b.steps != 1 || b.commits != 1 || dest.closes != 1 {
				t.Fatal("DONE/finalization/physical-close conservation", b.steps, b.commits, dest.closes, err)
			}
			if rows.nextCalls > 2 {
				t.Fatal("unbounded normalization row probe", rows.nextCalls)
			}
			if kind == "exact" {
				if err != nil || !closed || rows.closes != 1 || !reflect.DeepEqual(events, []string{"step", "commit", "normalize-query", "normalize-next", "normalize-next", "rows-close", "destination-close"}) {
					t.Fatal("checked DONE normalization ordering", events, closed, err)
				}
			} else if err == nil {
				t.Fatal("destination refusal normalized as success", kind)
			}
			wantQueries := 1
			if kind == "late-commit" {
				wantQueries = 0
			}
			if dest.queries != wantQueries {
				t.Fatal("late Commit query admitted", dest.queries, wantQueries)
			}
			wantRowsClose := 1
			if kind == "query-error-no-rows" || kind == "missing-rows" || kind == "query-panic" || kind == "late-commit" {
				wantRowsClose = 0
			}
			if rows.closes != wantRowsClose {
				t.Fatal("returned Rows close lost or repeated", rows.closes, wantRowsClose, err)
			}
			if kind == "destination-close-error" || kind == "destination-close-panic" {
				if closed {
					t.Fatal("unknown physical close accepted")
				}
			} else if !closed {
				t.Fatal("successful physical close knowledge lost", kind, err)
			}
			if kind == "query-error-with-rows" || kind == "query-error-no-rows" || kind == "next-error" || kind == "rows-close-error" || kind == "destination-close-error" {
				if !errors.Is(err, cause) {
					t.Fatal("exact refusal cause lost", err)
				}
			}
			if kind == "late-query" || kind == "late-next" || kind == "late-rows-close" || kind == "late-commit" {
				if !errors.Is(err, ErrClock) {
					t.Fatal("late normalization completion accepted", err)
				}
			}
			if kind == "canceled-query" && !errors.Is(err, context.Canceled) {
				t.Fatal("normalization cancellation cause lost", err)
			}
		})
	}
}

type diagnosticCause struct{}

func (*diagnosticCause) Error() string { return "generated typed cause" }

func TestBackupInitWithHandleAndLateGuardStillFinalizes(t *testing.T) {
	t.Run("diagnostic phases preserve exact causes", func(t *testing.T) {
		if phaseError("generated phase", nil) != nil {
			t.Fatal("nil changed to failure")
		}
		for _, cause := range []error{ErrBounds, ErrChanged, context.Canceled, context.DeadlineExceeded} {
			err := phaseError("generated phase", cause)
			if !errors.Is(err, cause) || err.Error() != "generated phase: "+cause.Error() {
				t.Fatal("phase lost exact cause", err)
			}
		}
		joinedCause := errors.Join(ErrBounds, context.Canceled)
		err := phaseError("outer", phaseError("inner", joinedCause))
		if !errors.Is(err, ErrBounds) || !errors.Is(err, context.Canceled) {
			t.Fatal("nested phase lost joined causes", err)
		}
		var typed *diagnosticCause
		if !errors.As(phaseError("generated phase", &diagnosticCause{}), &typed) {
			t.Fatal("typed diagnostic cause lost")
		}
	})
	b := &fakeBackup{count: 1, destination: &fakeConnection{}}
	e := engineEnv{guard: newGuard(context.Background())}
	cause := errors.New("post-init refusal")
	closed, err := transfer(nil, "/generated/candidate", profile{PageSize: 65536}, e, func(any, string, engineEnv) (backupHandle, error) { return b, cause })
	if !closed || !errors.Is(err, cause) || b.commits != 1 || b.steps != 0 || b.destination.closes != 1 {
		t.Fatalf("closed=%v err=%v state=%+v", closed, err, b)
	}
	closed, err = transfer(nil, "/generated/candidate", profile{PageSize: 4096}, e, func(any, string, engineEnv) (backupHandle, error) { return nil, cause })
	if closed || !errors.Is(err, cause) {
		t.Fatal("unknown initialization cleanup accepted")
	}
}
