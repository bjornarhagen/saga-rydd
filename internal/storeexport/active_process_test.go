//go:build darwin || linux

package storeexport

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"golang.org/x/sys/unix"
)

const partialExportFixtureByteLimit int64 = 512 << 10

type partialExportWitness struct {
	Phase            string `json:"phase"`
	Candidate        string `json:"candidate"`
	Destination      string `json:"destination"`
	RequestedPages   int32  `json:"requested_pages"`
	PageCount        int    `json:"page_count"`
	StepCalls        int    `json:"step_calls"`
	StepReturnedMore bool   `json:"step_returned_more"`
	FinalizeCalls    int    `json:"finalize_calls"`
	StageDevice      uint64 `json:"stage_device"`
	StageInode       uint64 `json:"stage_inode"`
	FileDevice       uint64 `json:"file_device"`
	FileInode        uint64 `json:"file_inode"`
}

// This test-only wrapper calls the real pinned SQLite backup. It pauses after
// Step returns more=true, while the active backup and source Raw callback remain
// owned. It does not witness interruption inside an executing SQLite Step.
type partialExportBackup struct {
	backupHandle
	parent, candidate, destination string
	steps, finalizations           int
}

func (b *partialExportBackup) Step(pages int32) (bool, error) {
	b.steps++
	more, err := b.backupHandle.Step(pages)
	if err != nil {
		return more, err
	}
	if b.steps != 1 || !more || b.finalizations != 0 || pages != bufferBytes/4096 {
		return more, errors.New("generated backup did not reach the first real partial step")
	}
	count := b.backupHandle.PageCount()
	if count <= int(pages) || int64(count) > partialExportFixtureByteLimit/4096 {
		return more, errors.New("generated backup page count is outside its finite fixture bound")
	}
	held, err := openDir(b.parent)
	if err != nil {
		return more, err
	}
	stage, err := held.child(filepath.Base(b.candidate))
	if err != nil {
		return more, errors.Join(err, held.close())
	}
	file, err := stage.stat("plans.sqlite3")
	if err == nil && (!privateDir(stage.stamp) || stage.stamp.Mode&0777 != 0700 || !privateFile(file) || file.Mode&0777 != 0600 || file.Size > partialExportFixtureByteLimit) {
		err = ErrProfile
	}
	witness := partialExportWitness{
		Phase: "real-partial-backup-after-step", Candidate: b.candidate, Destination: b.destination,
		RequestedPages: pages, PageCount: count, StepCalls: b.steps,
		StepReturnedMore: more, FinalizeCalls: b.finalizations,
		StageDevice: uint64(stage.stamp.Dev), StageInode: uint64(stage.stamp.Ino),
		FileDevice: uint64(file.Dev), FileInode: uint64(file.Ino),
	}
	err = errors.Join(err, stage.close(), held.close())
	if err != nil {
		return more, err
	}
	if err = json.NewEncoder(os.Stdout).Encode(witness); err != nil {
		return more, err
	}
	// Use the existing child-fixture timer. Nothing finalizes or closes the real
	// backup before the exact parent SIGKILL. The parent owns and reaps the PID.
	<-time.After(6 * time.Second)
	return more, errors.New("parent did not kill the active generated export child")
}

func (b *partialExportBackup) Commit() (driver.Conn, error) {
	b.finalizations++
	return b.backupHandle.Commit()
}

func TestPlansExportPartialBackupChild(t *testing.T) {
	if os.Getenv("RYDD_STORE_EXPORT_CHILD") != "real-partial-backup-after-step" {
		t.Skip("child helper is executed only by the exact generated active-export fixture")
	}
	base, parent, destination := os.Getenv("RYDD_STORE_EXPORT_BASE"), os.Getenv("RYDD_STORE_EXPORT_PARENT"), os.Getenv("RYDD_STORE_EXPORT_DESTINATION")
	if cleanPath(base) != nil || cleanPath(parent) != nil || cleanPath(destination) != nil || filepath.Dir(destination) != parent {
		t.Fatal("invalid generated child scope")
	}
	start := func(raw any, uri string, e engineEnv) (backupHandle, error) {
		parsed, err := url.Parse(uri)
		if err != nil || parsed.Scheme != "file" || parsed.Host != "" || filepath.Base(parsed.Path) != "plans.sqlite3" || filepath.Dir(filepath.Dir(parsed.Path)) != parent {
			return nil, errors.New("wrong generated backup destination")
		}
		actual, err := startBackup(raw, uri, e)
		if actual == nil || err != nil {
			return actual, err
		}
		return &partialExportBackup{backupHandle: actual, parent: parent, candidate: filepath.Dir(parsed.Path), destination: destination}, nil
	}
	result, err := exportPlanStore(context.Background(), base, destination, start)
	t.Fatal("active fixture returned before the parent killed its exact child", result, err)
}

func TestPlansExportSIGKILLAfterGenuinePartialBackupStep(t *testing.T) {
	f := fixtureStore(t, 6, 4096)
	setup, finishSetup := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishSetup()
	// Eighteen total plan rows fit the existing 32-row history oracle. Every
	// payload fits its existing 4 KiB bound; no source-semantic claim is made.
	payload := bytes.Repeat([]byte("generated partial-backup payload;"), 128)[:4096]
	for i := 0; i < 16; i++ {
		if _, err := f.db.ExecContext(setup, "INSERT INTO plans(id,payload) VALUES(?,?)", fmt.Sprintf("active-partial-%02d", i), payload); err != nil {
			t.Fatal(err)
		}
	}
	var pages int64
	if err := f.db.QueryRowContext(setup, "PRAGMA page_count").Scan(&pages); err != nil || pages <= bufferBytes/4096 || pages > partialExportFixtureByteLimit/4096 {
		t.Fatal("generated source page count outside partial-backup bound", pages, err)
	}
	beforeHistory := historyRows(t, f.db, f.version)
	var beforeLock unix.Stat_t
	if err := unix.Lstat(filepath.Join(f.base, "plans", "writer.lock"), &beforeLock); err != nil || !privateFile(beforeLock) {
		t.Fatal("generated source writer lock unavailable", err)
	}
	parent := privateTemp(t)
	destination := filepath.Join(parent, "published")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPlansExportPartialBackupChild$", "-test.timeout=7s")
	cmd.WaitDelay = time.Second
	env := make([]string, 0, len(os.Environ())+4)
	for _, item := range os.Environ() {
		if !strings.HasPrefix(item, "RYDD_STORE_EXPORT_CHILD=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_BASE=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_PARENT=") && !strings.HasPrefix(item, "RYDD_STORE_EXPORT_DESTINATION=") {
			env = append(env, item)
		}
	}
	cmd.Env = append(env, "RYDD_STORE_EXPORT_CHILD=real-partial-backup-after-step", "RYDD_STORE_EXPORT_BASE="+f.base, "RYDD_STORE_EXPORT_PARENT="+parent, "RYDD_STORE_EXPORT_DESTINATION="+destination)
	ready := make(chan struct{}, 1)
	stdout := &boundedProcessOutput{limit: 64 << 10, cancel: cancel, ready: ready}
	stderr := &boundedProcessOutput{limit: 64 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := false
	defer func() {
		if !reaped {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatalf("real partial-backup witness not reached: %v stdout=%q stderr=%q", ctx.Err(), stdout.snapshot(), stderr.snapshot())
	}
	body := stdout.snapshot()
	if len(body) > 4096 {
		t.Fatal("partial-backup witness metadata exceeded cap")
	}
	var witness partialExportWitness
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&witness); err != nil {
		t.Fatal(err)
	}
	if err = decoder.Decode(new(any)); err != io.EOF {
		t.Fatal("extra partial-backup witness output", err)
	}
	if witness.Phase != "real-partial-backup-after-step" || witness.Destination != destination || cleanPath(witness.Candidate) != nil || filepath.Dir(witness.Candidate) != parent || witness.Candidate == destination || witness.RequestedPages != bufferBytes/4096 || witness.PageCount != int(pages) || witness.StepCalls != 1 || !witness.StepReturnedMore || witness.FinalizeCalls != 0 || witness.StageInode == 0 || witness.FileInode == 0 {
		t.Fatal("wrong real partial-backup witness", witness)
	}
	locked, lockErr := localfs.AcquireExistingLock(ctx, filepath.Join(f.base, "plans"))
	if locked != nil {
		_ = locked.Close()
	}
	if !errors.Is(lockErr, localfs.ErrLocked) {
		t.Fatal("active export did not retain the source writer lock", lockErr)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := cmd.Wait()
	reaped = true
	var exit *exec.ExitError
	if !errors.As(waitErr, &exit) {
		t.Fatal("partial-backup child was not signaled", waitErr)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("expected exact owned SIGKILL after the real partial step", waitErr)
	}
	if len(stderr.snapshot()) != 0 || !bytes.Equal(stdout.snapshot(), body) || ctx.Err() != nil {
		t.Fatal("unexpected child output or expired fixture bound", string(stderr.snapshot()), ctx.Err())
	}
	if _, err = os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatal("interrupted active export published the destination", err)
	}
	inspectRetainedPartialStage(ctx, t, parent, witness)
	if afterHistory := historyRows(t, f.db, f.version); !reflect.DeepEqual(beforeHistory, afterHistory) {
		t.Fatal("committed source history changed after the real partial-export interruption")
	}
	released, err := localfs.AcquireExistingLock(ctx, filepath.Join(f.base, "plans"))
	if err != nil {
		t.Fatal("source writer lock not released after exact child reap", err)
	}
	if err = errors.Join(released.Check(ctx), released.Close()); err != nil {
		t.Fatal(err)
	}
	var afterLock unix.Stat_t
	if err = unix.Lstat(filepath.Join(f.base, "plans", "writer.lock"), &afterLock); err != nil || !sameFile(beforeLock, afterLock) {
		t.Fatal("stable source writer-lock object changed", err)
	}
	t.Log("genuine non-DONE backup step observed; active child reaped after SIGKILL; partial stage retained and source history/lock preserved; executing-Step interruption remains unverified")
}

func inspectRetainedPartialStage(ctx context.Context, t *testing.T, parent string, witness partialExportWitness) {
	t.Helper()
	g := newGuard(ctx)
	held, err := openDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := held.child(filepath.Base(witness.Candidate))
	if err != nil {
		_ = held.close()
		t.Fatal(err)
	}
	defer func() {
		if err := errors.Join(stage.close(), held.close()); err != nil {
			t.Error(err)
		}
	}()
	if !privateDir(stage.stamp) || stage.stamp.Mode&0777 != 0700 || uint64(stage.stamp.Dev) != witness.StageDevice || uint64(stage.stamp.Ino) != witness.StageInode {
		t.Fatal("retained partial stage no longer matches the generated witness")
	}
	names, err := stage.names(g, 2)
	if err != nil || len(names) < 1 {
		t.Fatal("retained partial stage has unexpected members", names, err)
	}
	sawDatabase := false
	for _, name := range names {
		if name != "plans.sqlite3" && name != "plans.sqlite3-journal" {
			t.Fatal("partial export contains an unexpected or publication member", name)
		}
		file, err := stage.stat(name)
		if err != nil || !privateFile(file) || file.Mode&0777 != 0600 || file.Size > partialExportFixtureByteLimit {
			t.Fatal("retained partial member is not bounded private regular storage", name, err)
		}
		if name == "plans.sqlite3" {
			sawDatabase = true
			if uint64(file.Dev) != witness.FileDevice || uint64(file.Ino) != witness.FileInode {
				t.Fatal("retained partial database object changed")
			}
		}
	}
	if !sawDatabase {
		t.Fatal("interrupted active export lost its exact partial database")
	}
	// Copied pages can remain in SQLite's cache until DONE. Zero on-disk database
	// length is therefore valid partial retention, never successful publication.
}
