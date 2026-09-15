package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func fixture(t *testing.T) (string, config.Config) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "private")
	c := config.Default()
	c.Roots = []string{"/synthetic"}
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.SyncRoots(context.Background(), c.Roots); err != nil {
		t.Fatal(err)
	}
	w.Close()
	return dir, c
}

func start(t *testing.T, dir string, c config.Config, options Options) (Snapshot, <-chan error) {
	t.Helper()
	ready := make(chan Snapshot, 1)
	options.Ready = func(s Snapshot) { ready <- s }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- Run(ctx, dir, c, options) }()
	select {
	case s := <-ready:
		return s, done
	case err := <-done:
		t.Fatal("worker did not start", err)
	case <-time.After(5 * time.Second):
		t.Fatal("start timed out")
	}
	return Snapshot{}, nil
}

func control(t *testing.T, dir, command string) Snapshot {
	t.Helper()
	s, err := Send(context.Background(), dir, command)
	if err != nil {
		t.Fatal(command, err)
	}
	return s
}

func waitExit(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func waitUntil(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition timed out")
}

func TestLifecyclePersistentPauseAndWriterExclusion(t *testing.T) {
	dir, c := fixture(t)
	initial, done := start(t, dir, c, Options{})
	if initial.Paused || initial.Handlers != 0 {
		t.Fatal(initial)
	}
	if w, err := state.OpenWriter(context.Background(), dir); !errors.Is(err, localfs.ErrLocked) {
		if w != nil {
			w.Close()
		}
		t.Fatal("writer was not excluded", err)
	}
	if s := control(t, dir, "pause"); !s.Paused {
		t.Fatal(s)
	}
	if s := control(t, dir, "pause"); !s.Paused {
		t.Fatal("pause not idempotent")
	}
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if paused, err := r.Paused(context.Background()); err != nil || !paused {
		t.Fatal(paused, err)
	}
	r.Close()
	if s := control(t, dir, "stop"); !s.Stopping {
		t.Fatal(s)
	}
	waitExit(t, done)
	if _, err := Send(context.Background(), dir, "status"); !errors.Is(err, ErrNotRunning) {
		t.Fatal(err)
	}
	restarted, done := start(t, dir, c, Options{})
	if !restarted.Paused || restarted.Instance == initial.Instance {
		t.Fatal(restarted)
	}
	if s := control(t, dir, "resume"); s.Paused {
		t.Fatal(s)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestChunkCheckpointPauseAndResume(t *testing.T) {
	dir, c := fixture(t)
	w, err := state.OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueJob(context.Background(), 1, "fixture", nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	w.Close()
	var calls atomic.Int32
	starts := make(chan time.Time, 3)
	entered := make(chan struct{}, 1)
	h := func(ctx context.Context, j state.Job) (Result, error) {
		starts <- time.Now()
		n := calls.Add(1)
		if n == 1 {
			return Result{Cursor: []byte("checkpoint")}, nil
		}
		if string(j.Cursor) != "checkpoint" {
			return Result{}, errors.New("missing checkpoint")
		}
		if n == 2 {
			entered <- struct{}{}
			<-ctx.Done()
			return Result{}, ctx.Err()
		}
		return Result{Done: true}, nil
	}
	_, done := start(t, dir, c, Options{Handlers: map[string]Handler{"fixture": h}, Interval: 50 * time.Millisecond, WorkDuration: time.Second})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second chunk not dispatched")
	}
	first, second := <-starts, <-starts
	if second.Sub(first) < 40*time.Millisecond {
		t.Fatal("chunks bypassed interval pacing", second.Sub(first))
	}
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	if calls.Load() != 2 {
		t.Fatal("dispatched while paused")
	}
	control(t, dir, "resume")
	waitUntil(t, func() bool { return calls.Load() == 3 && control(t, dir, "status").ActiveJob == 0 })
	control(t, dir, "stop")
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil || summary.PendingJobs != 0 || summary.RunningJobs != 0 {
		t.Fatal(summary, err)
	}
}

func TestFailedChunkPreservesCursorAndBacksOff(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "error", true: "panic"}[panics], func(t *testing.T) {
			dir, c := fixture(t)
			ctx := context.Background()
			w, err := state.OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.EnqueueJob(ctx, 1, "fixture", nil, time.Now()); err != nil {
				t.Fatal(err)
			}
			w.Close()
			entered := make(chan time.Time, 1)
			h := func(context.Context, state.Job) (Result, error) {
				entered <- time.Now()
				if panics {
					panic("synthetic failure")
				}
				return Result{Done: true, Cursor: []byte("uncommitted")}, errors.New("synthetic failure")
			}
			_, done := start(t, dir, c, Options{Handlers: map[string]Handler{"fixture": h}, Interval: time.Millisecond})
			var started time.Time
			select {
			case started = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("handler not called")
			}
			waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
			control(t, dir, "stop")
			waitExit(t, done)
			w, err = state.OpenWriter(ctx, dir)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			due, err := w.NextJobDue(ctx, []string{"fixture"})
			if err != nil || due.Before(started.Add(2*time.Second)) {
				t.Fatal("missing retry backoff", due, err)
			}
			j, err := w.ClaimJob(ctx, []string{"fixture"}, due, time.Minute)
			if err != nil || j == nil || len(j.Cursor) != 0 || j.Attempts != 2 {
				t.Fatal("failed result committed or attempts lost", j, err)
			}
		})
	}
}

func TestControlValidationAndSocketProtection(t *testing.T) {
	dir, c := fixture(t)
	path, err := Endpoint(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := localfs.EnsurePrivateDir(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := Run(context.Background(), dir, c, Options{}); err == nil {
		t.Fatal("overwrote regular file at endpoint")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "keep" {
		t.Fatal("endpoint file modified")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Leave an actual stale socket to exercise cleanup after acquiring the lock.
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	os.Chmod(path, 0600)
	stale.Close()
	_, done := start(t, dir, c, Options{})
	for _, request := range []string{
		`{"version":999,"command":"pause"}`,
		`{"version":1,"command":"pause","unexpected":true}`,
		`{"version":1,"command":"unknown"}`,
		`{"version":1,"command":"pause"} {}`,
		strings.Repeat("x", 4097),
	} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte(request + "\n")); err != nil {
			t.Fatal(err)
		}
		var response Response
		err = json.NewDecoder(conn).Decode(&response)
		conn.Close()
		if err != nil || response.OK {
			t.Fatal(response, err)
		}
	}
	if s := control(t, dir, "status"); s.Paused {
		t.Fatal("invalid request changed pause")
	}
	if err := os.Chmod(path, 0666); err != nil {
		t.Fatal(err)
	}
	if _, err := Send(context.Background(), dir, "pause"); err == nil {
		t.Fatal("insecure endpoint accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}

func TestDailyDispatchCapAcrossWorkerRestart(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MaxScanChunksPerDay = 1
	for i := 0; i < 140; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%03d", i)), []byte("fixture"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	options := Options{ExperimentalScan: true, Interval: 10 * time.Millisecond}
	_, done := start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_chunk_limit" })
	control(t, dir, "pause")
	control(t, dir, "resume")
	control(t, dir, "stop")
	waitExit(t, done)
	_, done = start(t, dir, c, options)
	waitUntil(t, func() bool { return control(t, dir, "status").WaitReason == "daily_chunk_limit" })
	snapshot := control(t, dir, "status")
	if snapshot.InventoryMetrics == nil || snapshot.InventoryMetrics.StatCalls == 0 {
		t.Fatal("missing live scanner counters", snapshot)
	}
	if snapshot.Dispatch == nil || snapshot.Dispatch.Used != 1 || snapshot.ActiveJob != 0 {
		t.Fatal(snapshot)
	}
	control(t, dir, "stop")
	waitExit(t, done)
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	summary, err := r.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.Entries != 129 {
		t.Fatalf("daily cap allowed extra batch: %+v", summary)
	}
}

func TestPacedInventoryControlsAndCompletion(t *testing.T) {
	dir, c := fixture(t)
	root := t.TempDir()
	c.Roots = []string{root}
	c.Scan.MetadataPerSecond = 5
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("file-%d", i)), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, done := start(t, dir, c, Options{ExperimentalScan: true, Interval: 350 * time.Millisecond, WorkDuration: 300 * time.Millisecond})
	waitUntil(t, func() bool { m := control(t, dir, "status").InventoryMetrics; return m != nil && m.Throttled })
	control(t, dir, "pause")
	waitUntil(t, func() bool { return control(t, dir, "status").ActiveJob == 0 })
	control(t, dir, "resume")
	r, err := state.OpenReader(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	waitUntil(t, func() bool {
		summary, err := r.Summary(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return summary.Entries == 6 && summary.CompleteDirectories == 1 && summary.PendingJobs == 0 && summary.RunningJobs == 0
	})
	m := control(t, dir, "status").InventoryMetrics
	if m == nil || m.EntryRatePerSecond != 5 || m.EntryInspections < 5 || m.ThrottleWaitNS == 0 {
		t.Fatal(m)
	}
	control(t, dir, "stop")
	waitExit(t, done)
}
