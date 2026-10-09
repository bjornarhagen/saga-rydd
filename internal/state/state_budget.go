//go:build darwin || linux

package state

import (
	"context"
	"errors"
	"math"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const (
	InventoryStateBudgetContract       = "inventory_state_budget_v1"
	MinInventoryStateBytes       int64 = 1 << 20
	MaxInventoryStateBytes       int64 = 1 << 40
	inventoryStateSampleTimeout        = 5 * time.Second
)

var (
	ErrInventoryStateBudgetInput = errors.New("inventory state threshold requires a context and 1 MiB–1 TiB limit")
	ErrInventoryStateBudgetClock = errors.New("inventory state observation clock moved backwards")
)

// InventoryStateBudget is one sequential observation of two named private
// files. Available means both logical lengths were observed consistently;
// it does not verify a SQLite connection, namespace or physical allocation.
// A caller must perform a new sample at its source-admission boundary.
type InventoryStateBudget struct {
	Contract                   string    `json:"contract"`
	Scope                      string    `json:"scope"`
	Available                  bool      `json:"available"`
	Status                     string    `json:"status"`
	Reason                     string    `json:"reason,omitempty"`
	Limit                      int64     `json:"limit"`
	DatabaseBytes              *int64    `json:"database_bytes"`
	WALBytes                   *int64    `json:"wal_bytes"`
	TotalBytes                 *int64    `json:"total_bytes"`
	SampleStartedAt            time.Time `json:"sample_started_at"`
	SampleFinishedAt           time.Time `json:"sample_finished_at"`
	SequentialObservations     bool      `json:"sequential_observations"`
	HardLimitEnforced          bool      `json:"hard_limit_enforced"`
	PhysicalAllocationVerified bool      `json:"physical_allocation_verified"`
	NamespaceAuthenticated     bool      `json:"namespace_authenticated"`
	OtherStoresIncluded        bool      `json:"other_stores_included"`
}

type inventoryStateBudgetHooks struct {
	lstat               func(string, *unix.Stat_t) error
	wallNow, elapsedNow func() time.Time
	afterStat           func(int)
}

type inventoryStateStamp struct {
	present bool
	stat    unix.Stat_t
}

type inventoryStateClock struct {
	ctx                         context.Context
	wallNow, elapsedNow         func() time.Time
	startedWall, startedElapsed time.Time
	highWall, highElapsed       time.Time
	refusal                     error
}

func (c *inventoryStateClock) check() (time.Time, error) {
	if err := c.ctx.Err(); err != nil {
		return time.Time{}, err
	}
	if c.refusal != nil {
		return time.Time{}, c.refusal
	}
	wall, elapsed := c.wallNow().UTC(), c.elapsedNow()
	if wall.Before(c.highWall) || elapsed.Before(c.highElapsed) {
		c.refusal = ErrInventoryStateBudgetClock
	} else if !wall.Before(c.startedWall.Add(inventoryStateSampleTimeout)) || !elapsed.Before(c.startedElapsed.Add(inventoryStateSampleTimeout)) {
		c.refusal = context.DeadlineExceeded
	}
	if c.refusal != nil {
		return time.Time{}, c.refusal
	}
	c.highWall, c.highElapsed = wall, elapsed
	if err := c.ctx.Err(); err != nil {
		return time.Time{}, err
	}
	return wall, nil
}

func validInventoryStateFile(st unix.Stat_t) bool {
	return st.Mode&unix.S_IFMT == unix.S_IFREG && st.Mode&0077 == 0 && st.Mode&(unix.S_ISUID|unix.S_ISGID|unix.S_ISVTX) == 0 && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1 && st.Size >= 0
}

func sameInventoryStateStamp(a, b inventoryStateStamp) bool {
	if a.present != b.present {
		return false
	}
	if !a.present {
		return true
	}
	x, y := a.stat, b.stat
	return x.Dev == y.Dev && x.Ino == y.Ino && x.Mode == y.Mode && x.Uid == y.Uid && x.Gid == y.Gid && x.Nlink == y.Nlink && x.Size == y.Size && x.Mtim == y.Mtim && x.Ctim == y.Ctim
}

// InventoryStateBudget examines only the existing database name and its WAL
// name. It performs at most four no-follow metadata calls, with no file opens,
// SQL, directory enumeration, mutation, recovery or source-body reads. Missing
// WAL is known zero. Any other missing/unsafe/changed observation is unavailable.
// Cooperative guards cannot interrupt an already-entered filesystem call.
func (s *Store) InventoryStateBudget(ctx context.Context, limit int64) (InventoryStateBudget, error) {
	return s.inventoryStateBudget(ctx, limit, inventoryStateBudgetHooks{})
}

func (s *Store) inventoryStateBudget(parent context.Context, limit int64, hooks inventoryStateBudgetHooks) (InventoryStateBudget, error) {
	if parent == nil || limit < MinInventoryStateBytes || limit > MaxInventoryStateBytes {
		return InventoryStateBudget{}, ErrInventoryStateBudgetInput
	}
	if err := parent.Err(); err != nil {
		return InventoryStateBudget{}, err
	}
	ctx, cancel := context.WithTimeout(parent, inventoryStateSampleTimeout)
	defer cancel()
	wallNow, elapsedNow, lstat := hooks.wallNow, hooks.elapsedNow, hooks.lstat
	if wallNow == nil {
		wallNow = time.Now
	}
	if elapsedNow == nil {
		elapsedNow = time.Now
	}
	if lstat == nil {
		lstat = unix.Lstat
	}
	startedWall, startedElapsed := wallNow().UTC(), elapsedNow()
	clock := inventoryStateClock{ctx: ctx, wallNow: wallNow, elapsedNow: elapsedNow, startedWall: startedWall, startedElapsed: startedElapsed, highWall: startedWall, highElapsed: startedElapsed}
	report := InventoryStateBudget{Contract: InventoryStateBudgetContract, Scope: "configured_inventory_database_and_wal", Status: "unavailable", Reason: "inventory_state_unavailable", Limit: limit, SampleStartedAt: startedWall, SequentialObservations: true}
	finish := func() (InventoryStateBudget, error) {
		stamp, err := clock.check()
		if err != nil {
			return InventoryStateBudget{}, err
		}
		report.SampleFinishedAt = stamp
		if err := ctx.Err(); err != nil {
			return InventoryStateBudget{}, err
		}
		return report, nil
	}
	if s == nil || s.path == "" {
		return finish()
	}
	paths := [2]string{s.path, s.path + "-wal"}
	var before [2]inventoryStateStamp
	for round := 0; round < 2; round++ {
		for i, path := range paths {
			if _, err := clock.check(); err != nil {
				return InventoryStateBudget{}, err
			}
			var st unix.Stat_t
			err := lstat(path, &st)
			if hooks.afterStat != nil {
				hooks.afterStat(round*2 + i)
			}
			current := inventoryStateStamp{present: err == nil, stat: st}
			if err != nil && !(i == 1 && errors.Is(err, unix.ENOENT)) {
				return finish()
			}
			if current.present && !validInventoryStateFile(st) {
				return finish()
			}
			if round == 0 {
				before[i] = current
			} else if !sameInventoryStateStamp(before[i], current) {
				report.Reason = "inventory_state_changed"
				return finish()
			}
		}
	}
	database, wal := before[0].stat.Size, before[1].stat.Size
	if !before[1].present {
		wal = 0
	}
	report.DatabaseBytes, report.WALBytes = &database, &wal
	if database > math.MaxInt64-wal {
		report.Reason = "inventory_state_overflow"
		return finish()
	}
	total := database + wal
	report.Available, report.Status, report.Reason, report.TotalBytes = true, "below_limit", "", &total
	if total >= limit {
		report.Status, report.Reason = "limit_reached", "inventory_state_limit"
	}
	return finish()
}
