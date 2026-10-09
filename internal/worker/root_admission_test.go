package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/powerinfo"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type rootAdmissionWorkerEvidence struct {
	Version                            int
	Roots, Jobs                        []string
	CPU                                string
	DispatchRows, MetadataReservations int64
}

func rootAdmissionWorkerSaved(t *testing.T, dir string) rootAdmissionWorkerEvidence {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(dir, state.Filename))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var saved rootAdmissionWorkerEvidence
	if err = db.QueryRow("PRAGMA user_version").Scan(&saved.Version); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM scan_dispatch").Scan(&saved.DispatchRows); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT count(*) FROM scan_metadata_reservations").Scan(&saved.MetadataReservations); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT id,hex(path),enabled FROM roots ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, enabled int64
		var path string
		if err = rows.Scan(&id, &path, &enabled); err != nil {
			t.Fatal(err)
		}
		saved.Roots = append(saved.Roots, fmt.Sprintf("%d:%s:%d", id, path, enabled))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = db.Query("SELECT id,root_id,kind,hex(path),status,hex(cursor),due_at_ns,coalesce(lease_token,''),coalesce(last_error,'') FROM jobs ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, rootID, due int64
		var kind, path, status, cursor, lease, lastError string
		if err = rows.Scan(&id, &rootID, &kind, &path, &status, &cursor, &due, &lease, &lastError); err != nil {
			t.Fatal(err)
		}
		saved.Jobs = append(saved.Jobs, fmt.Sprintf("%d:%d:%s:%s:%s:%s:%d:%s:%s", id, rootID, kind, path, status, cursor, due, lease, lastError))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	cpu, err := r.CPUCharges(context.Background())
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(cpu)
	if err != nil {
		t.Fatal(err)
	}
	saved.CPU = string(b)
	return saved
}

func rootAdmissionWorkerFixture(t *testing.T, schema15, legacy bool) (string, config.Config) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	c := config.Default()
	roots := make([]string, state.RootAdmissionLimit)
	for i := range roots {
		roots[i] = filepath.Join(filepath.Dir(dir), fmt.Sprintf("offline-root-%03d", i))
	}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), roots); err != nil {
		t.Fatal(err)
	}
	if err = w.SyncRoots(context.Background(), roots[:1]); err != nil {
		t.Fatal(err)
	}
	if err = w.EnqueueJob(context.Background(), 1, state.ScanKind, []byte("."), time.Now().Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = w.SetPaused(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if schema15 {
		if _, err = w.ActivateCPUCharges(context.Background(), time.Now().Round(0).UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if legacy {
		// Deliberately construct a valid historical over-limit store. Updated
		// writers may reactivate these rows, but cannot create them via SyncRoots.
		db, e := sql.Open("sqlite", filepath.Join(dir, state.Filename))
		if e != nil {
			t.Fatal(e)
		}
		_, e = db.Exec("INSERT INTO roots(path,enabled) VALUES(?,0)", []byte(filepath.Join(filepath.Dir(dir), "legacy-root")))
		closeErr := db.Close()
		if e != nil || closeErr != nil {
			t.Fatal(e, closeErr)
		}
	}
	c.Roots = roots[:1]
	return dir, c
}

type rootAdmissionStartupCounts struct{ ready, scanner, power, cpu, session, state, priority atomic.Int32 }

func rootAdmissionWorkerOptions(counts *rootAdmissionStartupCounts, experimental bool) Options {
	return Options{
		ExperimentalScan: experimental,
		Ready:            func(Snapshot) { counts.ready.Add(1) },
		scannerNew: func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error) {
			counts.scanner.Add(1)
			return nil, errors.New("scanner must not start")
		},
		powerCoordinator: newPowerCoordinator(func(context.Context) (powerinfo.Observation, error) {
			counts.power.Add(1)
			return powerinfo.Observation{}, errors.New("power must not start")
		}, nil, nil),
		cpuObserve:        func() (time.Duration, error) { counts.cpu.Add(1); return 0, nil },
		cpuSessionObserve: func() (time.Duration, error) { counts.session.Add(1); return 0, nil },
		inventoryStateObserve: func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error) {
			counts.state.Add(1)
			return state.InventoryStateBudget{}, errors.New("state sample must not start")
		},
		priorityRequest: func(context.Context) ThreadPriorityObservation {
			counts.priority.Add(1)
			return newThreadPriorityObservation("fixture")
		},
	}
}

func (c *rootAdmissionStartupCounts) assertNoWork(t *testing.T) {
	t.Helper()
	if c.scanner.Load() != 0 || c.power.Load() != 0 || c.cpu.Load() != 0 || c.session.Load() != 0 || c.state.Load() != 0 || c.priority.Load() != 0 {
		t.Fatalf("startup performed work: scanner=%d power=%d cpu=%d session=%d state=%d priority=%d", c.scanner.Load(), c.power.Load(), c.cpu.Load(), c.session.Load(), c.state.Load(), c.priority.Load())
	}
}

func TestWorkerRootAdmissionRefusesBeforeSourceAndAccountingStartup(t *testing.T) {
	for _, schema15 := range []bool{false, true} {
		for _, experimental := range []bool{false, true} {
			t.Run(fmt.Sprintf("schema15=%t/experimental=%t", schema15, experimental), func(t *testing.T) {
				dir, c := rootAdmissionWorkerFixture(t, schema15, false)
				before := rootAdmissionWorkerSaved(t, dir)
				expectedVersion := 14
				if schema15 {
					expectedVersion = 15
				}
				if before.Version != expectedVersion {
					t.Fatal("wrong initial schema", before.Version)
				}
				c.Roots = []string{filepath.Join(filepath.Dir(dir), "new-root")}
				c.Scan.CPUSessionCharges = true // Admission must precede optional activation.
				var counts rootAdmissionStartupCounts
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				err := Run(ctx, dir, c, rootAdmissionWorkerOptions(&counts, experimental))
				if !errors.Is(err, state.ErrRootAdmissionCapacity) || ctx.Err() != nil {
					t.Fatal("bounded root refusal lost", err, ctx.Err())
				}
				if counts.ready.Load() != 0 {
					t.Fatal("refused worker became ready")
				}
				counts.assertNoWork(t)
				if after := rootAdmissionWorkerSaved(t, dir); !reflect.DeepEqual(before, after) {
					t.Fatal("refusal changed enabled roots, job provenance, CPU history or schema", before, after)
				}
			})
		}
	}
}

func TestWorkerRootAdmissionLegacyExistingStartupPreservesSchemaAndHistory(t *testing.T) {
	for _, schema15 := range []bool{false, true} {
		for _, experimental := range []bool{false, true} {
			t.Run(fmt.Sprintf("schema15=%t/experimental=%t", schema15, experimental), func(t *testing.T) {
				dir, c := rootAdmissionWorkerFixture(t, schema15, true)
				before := rootAdmissionWorkerSaved(t, dir)
				expectedVersion := 14
				if schema15 {
					expectedVersion = 15
				}
				if before.Version != expectedVersion {
					t.Fatal("wrong initial schema", before.Version)
				}
				if len(before.Roots) != 129 {
					t.Fatal("legacy setup did not exceed admission bound")
				}
				var counts rootAdmissionStartupCounts
				options := rootAdmissionWorkerOptions(&counts, experimental)
				ready := make(chan Snapshot, 1)
				options.Ready = func(s Snapshot) { counts.ready.Add(1); ready <- s }
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				t.Cleanup(func() {
					cancel()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Error("owned worker failed to join")
					}
				})
				go func() { defer close(done); done <- Run(ctx, dir, c, options) }()
				select {
				case s := <-ready:
					if !s.Paused || s.ActiveJob != 0 {
						t.Fatal("legacy startup dispatched work", s)
					}
				case err := <-done:
					t.Fatal("valid legacy startup refused", err)
				case <-time.After(3 * time.Second):
					t.Fatal("legacy startup timed out")
				}
				counts.assertNoWork(t)
				stop, cancelStop := context.WithTimeout(context.Background(), 2*time.Second)
				_, err := Send(stop, dir, "stop")
				cancelStop()
				if err != nil {
					t.Fatal(err)
				}
				select {
				case err = <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("legacy stop timed out")
				}
				if after := rootAdmissionWorkerSaved(t, dir); !reflect.DeepEqual(before, after) {
					t.Fatal("valid legacy startup rewrote root/job/CPU/schema history", before, after)
				}
			})
		}
	}
}
