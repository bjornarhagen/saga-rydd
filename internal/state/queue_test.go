package state

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

func queueStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := privateDir(t)
	s, err := OpenWriter(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SyncRoots(context.Background(), []string{"/fixture/a", "/fixture/b"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, dir
}

func TestQueueOrderingFilteringAndCursor(t *testing.T) {
	ctx := context.Background()
	s, _ := queueStore(t)
	now := time.Unix(1700000000, 0)
	if err := s.EnqueueJob(ctx, 1, "scan", []byte("a"), now); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 1, "unknown", []byte("x"), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueJob(ctx, 2, "scan", []byte("b"), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncRoots(ctx, []string{"/fixture/a"}); err != nil {
		t.Fatal(err)
	}
	due, err := s.NextJobDue(ctx, []string{"scan"})
	if err != nil || !due.Equal(now) {
		t.Fatal(due, err)
	}
	if j, err := s.ClaimJob(ctx, []string{"scan"}, now.Add(-time.Second), time.Minute); j != nil || err != nil {
		t.Fatal("claimed early", j, err)
	}
	j, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Minute)
	if err != nil || j == nil || string(j.Path) != "a" || j.Attempts != 1 {
		t.Fatal(j, err)
	}
	if err := s.EnqueueJob(ctx, 1, "scan", []byte("a"), now); err != nil {
		t.Fatal(err)
	}
	if next, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Minute); next != nil || err != nil {
		t.Fatal("duplicate claim", next, err)
	}
	wrong := *j
	wrong.Token = "wrong"
	if err := s.FinishJob(ctx, wrong, true, nil, now, ""); !errors.Is(err, ErrStaleLease) {
		t.Fatal(err)
	}
	cursor := []byte{0xff, 0x00, 0x01}
	if err := s.FinishJob(ctx, *j, false, cursor, now.Add(time.Hour), ""); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimJob(ctx, []string{"scan"}, now.Add(time.Hour), time.Minute)
	if err != nil || next == nil || !bytes.Equal(next.Cursor, cursor) || next.Attempts != 1 {
		t.Fatal(next, err)
	}
	if err := s.FinishJob(ctx, *next, true, nil, now, ""); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs").Scan(&count); err != nil || count != 2 {
		t.Fatal("unhandled/disabled jobs lost", count, err)
	}
}

func TestRecoveryFencesOldAttemptAndKeepsProgress(t *testing.T) {
	ctx := context.Background()
	s, dir := queueStore(t)
	now := time.Now()
	if err := s.EnqueueJob(ctx, 1, "scan", nil, now); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Hour)
	if err != nil || j == nil {
		t.Fatal(j, err)
	}
	if err := s.FinishJob(ctx, *j, false, []byte("saved"), now, ""); err != nil {
		t.Fatal(err)
	}
	old, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Hour)
	if err != nil || old == nil {
		t.Fatal(old, err)
	}
	if other, err := OpenWriter(ctx, dir); !errors.Is(err, localfs.ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	count, err := s.RecoverJobs(ctx, now)
	if err != nil || count != 1 {
		t.Fatal(count, err)
	}
	fresh, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Hour)
	if err != nil || fresh == nil || fresh.Token == old.Token || string(fresh.Cursor) != "saved" {
		t.Fatal(fresh, err)
	}
	if err := s.FinishJob(ctx, *old, true, nil, now, ""); !errors.Is(err, ErrStaleLease) {
		t.Fatal("old attempt completed new lease", err)
	}
	if err := s.FinishJob(ctx, *fresh, true, nil, now, ""); err != nil {
		t.Fatal(err)
	}
}

func TestQueueBoundsAndDurablePause(t *testing.T) {
	ctx := context.Background()
	s, dir := queueStore(t)
	now := time.Now()
	for _, path := range []string{"../escape", "/absolute", "a\x00b"} {
		if err := s.EnqueueJob(ctx, 1, "scan", []byte(path), now); err == nil {
			t.Fatal("invalid path accepted", path)
		}
	}
	if err := s.EnqueueJob(ctx, 1, "bad'kind", nil, now); err == nil {
		t.Fatal("invalid kind accepted")
	}
	if err := s.EnqueueJob(ctx, 1, "scan", nil, now); err != nil {
		t.Fatal(err)
	}
	j, err := s.ClaimJob(ctx, []string{"scan"}, now, time.Minute)
	if err != nil || j == nil {
		t.Fatal(err)
	}
	if err := s.FinishJob(ctx, *j, false, make([]byte, MaxCursorBytes+1), now, ""); err == nil {
		t.Fatal("oversize cursor accepted")
	}
	if err := s.SetPaused(ctx, true); err != nil {
		t.Fatal(err)
	}
	s.Close()
	r, err := OpenReader(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if paused, err := r.Paused(ctx); err != nil || !paused {
		t.Fatal(paused, err)
	}
	if err := r.SetPaused(ctx, false); err == nil {
		t.Fatal("read-only pause write")
	}
	if _, err := r.RecoverJobs(ctx, now); err == nil {
		t.Fatal("read-only recovery")
	}
}

func TestUpgradeFromV1PreservesInventoryAndJobs(t *testing.T) {
	ctx := context.Background()
	dir := privateDir(t)
	path := filepath.Join(dir, Filename)
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	old, err := connect(ctx, path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.Exec(migration1); err != nil {
		t.Fatal(err)
	}
	if _, err := old.db.Exec(`INSERT INTO schema_migrations VALUES(1,'inventory-foundation',0);
 PRAGMA application_id=0x52594444; PRAGMA user_version=1;
 INSERT INTO roots(id,path) VALUES(42,X'2f66697874757265');
 INSERT INTO jobs(root_id,kind,path,status,due_at_ns,cursor) VALUES(42,'scan',X'2e','running',0,X'7361766564');
 INSERT INTO settings VALUES('preserve',X'01');`); err != nil {
		t.Fatal(err)
	}
	old.Close()
	if r, err := OpenReader(ctx, dir); err == nil {
		r.Close()
		t.Fatal("old schema read without migration")
	}
	w, err := OpenWriter(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if n, err := w.RecoverJobs(ctx, time.Now()); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	j, err := w.ClaimJob(ctx, []string{"scan"}, time.Now(), time.Minute)
	if err != nil || j == nil || j.RootID != 42 || string(j.Cursor) != "saved" {
		t.Fatal(j, err)
	}
	var n int
	if err := w.db.QueryRow("SELECT count(*) FROM settings WHERE key='preserve'").Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if paused, err := w.Paused(ctx); err != nil || paused {
		t.Fatal(paused, err)
	}
}
