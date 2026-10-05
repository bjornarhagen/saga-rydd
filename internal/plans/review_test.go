package plans

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func reviewFixture(t *testing.T) (string, Saved) {
	t.Helper()
	base := filepath.Join(t.TempDir(), "state")
	saved, err := Save(context.Background(), base, selection())
	if err != nil {
		t.Fatal(err)
	}
	saved, err = Load(context.Background(), base, saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	return base, saved
}

func TestReviewLifecycleAndRetries(t *testing.T) {
	ctx := context.Background()
	base, saved := reviewFixture(t)
	if _, err := Approve(ctx, base, saved.ID, nil, Confirmations{}); !errors.Is(err, ErrConfirmation) {
		t.Fatal(err)
	}
	if _, err := Approve(ctx, base, saved.ID, nil, Confirmations{true, true}); !errors.Is(err, ErrReviewEvidence) {
		t.Fatal(err)
	}
	if _, err := Revoke(ctx, base, saved.ID); err == nil {
		t.Fatal("revoked a plan with no review")
	}
	now := time.Now().UTC()
	// Exercise private persistence after the public API's separate evidence gate.
	approved, err := approve(ctx, base, saved.ID, now)
	if err != nil || approved.Review == nil || approved.Review.Status != "review_approved" || approved.Review.Executable || approved.Review.CurrentStateVerified || !reflect.DeepEqual(approved.Record, saved.Record) {
		t.Fatal(approved, err)
	}
	retry, err := approve(ctx, base, saved.ID, now.Add(time.Minute))
	if err != nil || !reflect.DeepEqual(retry.Review, approved.Review) {
		t.Fatal("retry changed approval", retry, err)
	}
	shown, err := Show(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(shown.Review, approved.Review) {
		t.Fatal(shown, err)
	}
	revoked, err := Revoke(ctx, base, saved.ID)
	if err != nil || revoked.Review.Status != "revoked" || revoked.Review.Revocation == nil {
		t.Fatal(revoked, err)
	}
	retry, err = Revoke(ctx, base, saved.ID)
	if err != nil || !reflect.DeepEqual(retry.Review, revoked.Review) {
		t.Fatal(retry, err)
	}
	if _, err = approve(ctx, base, saved.ID, now.Add(time.Minute)); !errors.Is(err, ErrReviewTerminal) {
		t.Fatal("revoked approval renewed", err)
	}
	loaded, err := Load(ctx, base, saved.ID)
	if err != nil || loaded.Review != nil || !reflect.DeepEqual(loaded.Record, shown.Record) {
		t.Fatal("original plan changed", loaded, err)
	}
	db, closeDB, err := open(ctx, base, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	for _, table := range []string{"review_approvals", "review_revocations", "plan_store_identity"} {
		for _, q := range []string{"DELETE FROM " + table, "UPDATE " + table + " SET " + map[string]string{"review_approvals": "id=id", "review_revocations": "id=id", "plan_store_identity": "token=token"}[table]} {
			if _, err = db.Exec(q); err == nil {
				t.Fatal("mutable review history", q)
			}
		}
	}
}

func TestReviewExpiryAndClock(t *testing.T) {
	ctx := context.Background()
	base, saved := reviewFixture(t)
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	if _, err := approve(ctx, base, saved.ID, now); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := open(ctx, base, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at     time.Time
		status string
	}{{now.Add(-time.Nanosecond), "not_yet_valid"}, {now, "review_approved"}, {now.Add(ReviewLifetime - time.Nanosecond), "review_approved"}, {now.Add(ReviewLifetime), "expired"}} {
		r, err := readReview(ctx, db, saved, tc.at)
		if err != nil || r.Status != tc.status {
			t.Fatal(tc, r, err)
		}
	}
	closeDB()
	for _, at := range []time.Time{now.Add(-time.Second), now.Add(ReviewLifetime)} {
		if _, err := approve(ctx, base, saved.ID, at); !errors.Is(err, ErrReviewTerminal) {
			t.Fatal(err)
		}
	}
	if r, err := Revoke(ctx, base, saved.ID); err != nil || r.Review.Status != "revoked" {
		t.Fatal(r, err)
	}
}

func TestReviewBindingsAndCorruption(t *testing.T) {
	for _, kind := range []string{"payload", "plan", "store", "inventory", "contract", "confirmation", "expiry", "version", "revocation", "orphan"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			base, saved := reviewFixture(t)
			r, err := approve(ctx, base, saved.ID, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "revocation" || kind == "orphan" {
				if _, err = Revoke(ctx, base, saved.ID); err != nil {
					t.Fatal(err)
				}
			}
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			if kind == "revocation" {
				_, err = db.Exec("DROP TRIGGER review_revocations_no_update; UPDATE review_revocations SET payload=X'00'")
			} else if kind == "orphan" {
				_, err = db.Exec("DROP TRIGGER review_approvals_no_delete; DELETE FROM review_approvals")
			} else {
				a := r.Review.Approval
				switch kind {
				case "plan":
					a.PlanID = "plan-v1-wrong"
				case "store":
					a.StoreID = "wrong-store"
				case "inventory":
					a.InventoryID = "wrong-inventory"
				case "contract":
					a.Contract = "permanent_purge"
				case "confirmation":
					a.Confirmations.ProjectReview = false
				case "expiry":
					a.ExpiresAt = a.ExpiresAt.Add(time.Hour)
				case "version":
					a.Version = 99
				}
				payload, e := json.Marshal(a)
				if e != nil {
					t.Fatal(e)
				}
				id := digest("approval-v1-", payload)
				if kind == "payload" {
					payload = []byte{0}
				}
				if _, err = db.Exec("DROP TRIGGER review_approvals_no_update"); err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec("UPDATE review_approvals SET id=?,payload=?", id, payload)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = Show(ctx, base, saved.ID); !errors.Is(err, ErrReviewCorrupt) {
				t.Fatal(err)
			}
		})
	}
}

func TestReviewMigrationAndRollback(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "migrate", true: "rollback"}[conflict], func(t *testing.T) {
			ctx := context.Background()
			base, saved := reviewFixture(t)
			db, closeDB, err := open(ctx, base, true)
			if err != nil {
				t.Fatal(err)
			}
			// Reproduce schema 1 from an existing plan, preserving its exact bytes.
			_, err = db.Exec("DROP TABLE review_revocations; DROP TABLE review_approvals; DROP TABLE plan_store_identity; PRAGMA user_version=1")
			if err != nil {
				t.Fatal(err)
			}
			if conflict {
				if _, err = db.Exec("CREATE TABLE review_approvals(conflict TEXT)"); err != nil {
					t.Fatal(err)
				}
			}
			closeDB()
			shown, err := Show(ctx, base, saved.ID)
			if err != nil || shown.Review != nil {
				t.Fatal(shown, err)
			}
			_, err = approve(ctx, base, saved.ID, time.Now().UTC())
			if conflict {
				if err == nil {
					t.Fatal("migration conflict ignored")
				}
				db, closeDB, err = open(ctx, base, false)
				if err != nil {
					t.Fatal(err)
				}
				defer closeDB()
				var version, identities int
				if err = db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
					t.Fatal(err)
				}
				if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='plan_store_identity'").Scan(&identities); err != nil || version != 1 || identities != 0 {
					t.Fatal(version, identities, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			loaded, err := Load(ctx, base, saved.ID)
			if err != nil || !reflect.DeepEqual(loaded.Record, shown.Record) {
				t.Fatal(loaded, err)
			}
		})
	}
}

func TestReviewCrashHelper(t *testing.T) {
	base, id, mode := os.Getenv("RYDD_REVIEW_CRASH_BASE"), os.Getenv("RYDD_REVIEW_CRASH_ID"), os.Getenv("RYDD_REVIEW_CRASH_MODE")
	if base == "" {
		return
	}
	ctx := context.Background()
	switch mode {
	case "approval":
		if _, err := approve(ctx, base, id, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
	case "revocation":
		if _, err := Revoke(ctx, base, id); err != nil {
			t.Fatal(err)
		}
	case "uncommitted":
		db, closeDB, err := open(ctx, base, true)
		if err != nil {
			t.Fatal(err)
		}
		defer closeDB()
		saved, err := load(ctx, db, id)
		if err != nil {
			t.Fatal(err)
		}
		r, err := readReview(ctx, db, saved, time.Now().UTC())
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(Revocation{Version: 1, PlanID: id, StoreID: r.Approval.StoreID, ApprovalID: r.ID, CreatedAt: time.Now().UTC()})
		if _, err = tx.Exec("INSERT INTO review_revocations VALUES(?,?,?)", id, digest("revocation-v1-", payload), payload); err != nil {
			t.Fatal(err)
		}
		// Force uncommitted WAL spills beyond the page cache.
		for i := 0; i < 128; i++ {
			if _, err = tx.Exec("INSERT INTO review_approvals VALUES(?,?,?)", fmt.Sprint(i), fmt.Sprint(i), []byte(strings.Repeat("x", maxReviewBytes))); err != nil {
				t.Fatal(err)
			}
		}
	}
	fmt.Println("READY")
	time.Sleep(time.Minute)
}

func TestReviewCrashRecovery(t *testing.T) {
	for _, mode := range []string{"approval", "revocation", "uncommitted"} {
		t.Run(mode, func(t *testing.T) {
			base, saved := reviewFixture(t)
			ctx := context.Background()
			if mode != "approval" {
				if _, err := approve(ctx, base, saved.ID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestReviewCrashHelper$")
			cmd.Env = append(os.Environ(), "RYDD_REVIEW_CRASH_BASE="+base, "RYDD_REVIEW_CRASH_ID="+saved.ID, "RYDD_REVIEW_CRASH_MODE="+mode)
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			ready := make(chan bool, 1)
			go func() { scan := bufio.NewScanner(stdout); ready <- scan.Scan() && scan.Text() == "READY" }()
			select {
			case ok := <-ready:
				if !ok {
					t.Fatal("helper not ready")
				}
			case <-time.After(20 * time.Second):
				t.Fatal("helper timed out")
			}
			if err = cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("expected killed helper")
			}
			r, err := Show(ctx, base, saved.ID)
			want := "review_approved"
			if mode == "revocation" {
				want = "revoked"
			}
			if err != nil || r.Review == nil || r.Review.Status != want {
				t.Fatal(r, err)
			}
			if mode == "approval" {
				retry, err := approve(ctx, base, saved.ID, time.Now().UTC())
				if err != nil || retry.Review.ID != r.Review.ID {
					t.Fatal(retry, err)
				}
			}
		})
	}
}
