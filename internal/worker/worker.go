// Package worker runs one cooperative, read-only job chunk at a time and commits
// bounded results through its single owning event loop.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type Snapshot struct {
	PID              int                   `json:"pid"`
	Instance         string                `json:"instance"`
	StartedAt        time.Time             `json:"started_at"`
	Paused           bool                  `json:"paused"`
	Stopping         bool                  `json:"stopping"`
	ActiveJob        int64                 `json:"active_job,omitempty"`
	RecoveredJobs    int64                 `json:"recovered_jobs"`
	Handlers         int                   `json:"handlers"`
	WaitReason       string                `json:"wait_reason"`
	InventoryMetrics *inventory.Metrics    `json:"inventory_metrics,omitempty"`
	Dispatch         *state.DispatchBudget `json:"dispatch,omitempty"`
}

type Result struct {
	Done   bool
	Cursor []byte
	NextAt time.Time
	Scan   *state.ScanBatch
}

// Handlers must honor cancellation and have no irreversible effects. They return
// bounded progress to the owning event loop rather than holding a DB connection.
type Handler func(context.Context, state.Job) (Result, error)
type Options struct {
	Handlers         map[string]Handler
	Ready            func(Snapshot)
	ExperimentalScan bool
	PrivatePaths     []string
	// Overrides support small deterministic lifecycle fixtures, not public flags.
	Interval, WorkDuration time.Duration
}
type outcome struct {
	result    Result
	err       error
	startedAt time.Time
}

func Run(ctx context.Context, dir string, cfg config.Config, options Options) error {
	interval := time.Duration(cfg.Scan.IntervalSeconds) * time.Second
	work := time.Duration(cfg.Scan.WorkSeconds) * time.Second
	if options.Interval > 0 {
		interval = options.Interval
	}
	if options.WorkDuration > 0 {
		work = options.WorkDuration
	}
	if interval <= 0 || work <= 0 || work > 24*time.Hour {
		return errors.New("invalid worker cadence")
	}
	handlers := make(map[string]Handler, len(options.Handlers))
	kinds := make([]string, 0, len(options.Handlers))
	for kind, h := range options.Handlers {
		if h == nil {
			return errors.New("nil job handler")
		}
		handlers[kind] = h
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.SyncRoots(ctx, cfg.Roots); err != nil {
		return err
	}
	var scannerMetrics func() inventory.Metrics
	if options.ExperimentalScan {
		endpoint, err := Endpoint(dir)
		if err != nil {
			return err
		}
		scanner, err := inventory.New(cfg.Roots, cfg.Excludes, append(append([]string{}, options.PrivatePaths...), dir, filepath.Dir(endpoint)), inventory.WithEntryRate(cfg.Scan.MetadataPerSecond))
		if err != nil {
			return err
		}
		defer scanner.Close()
		scannerMetrics = scanner.Metrics
		if _, exists := handlers[state.ScanKind]; exists {
			return errors.New("inventory handler already registered")
		}
		handlers[state.ScanKind] = func(ctx context.Context, j state.Job) (Result, error) {
			batch, err := scanner.Next(ctx, j)
			if err != nil {
				return Result{}, err
			}
			return Result{Scan: &batch}, nil
		}
		kinds = append(kinds, state.ScanKind)
		if err := w.SeedInventory(ctx); err != nil {
			return err
		}
	}
	recovered, err := w.RecoverJobs(ctx, time.Now())
	if err != nil {
		return err
	}
	paused, err := w.Paused(ctx)
	if err != nil {
		return err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	live := Snapshot{PID: os.Getpid(), Instance: hex.EncodeToString(id[:]), StartedAt: time.Now().UTC(), Paused: paused, RecoveredJobs: recovered, Handlers: len(handlers)}
	refreshMetrics := func() {
		if scannerMetrics != nil {
			metrics := scannerMetrics()
			live.InventoryMetrics = &metrics
		}
	}
	refreshMetrics()
	calls := make(chan controlCall, 8)
	srv, err := listen(dir, calls)
	if err != nil {
		return err
	}
	defer srv.Close()
	if options.Ready != nil {
		options.Ready(live)
	}
	var timer *time.Timer
	var tick <-chan time.Time
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var shutdown *time.Timer
	var shutdownTick <-chan time.Time
	defer func() {
		if shutdown != nil {
			shutdown.Stop()
		}
	}()
	var active *state.Job
	var cancelTask context.CancelFunc
	defer func() {
		if cancelTask != nil {
			cancelTask()
		}
	}()
	done := make(chan outcome, 1)
	ctxDone := ctx.Done()
	reschedule := true
	nextAllowed := time.Time{}
	stop := func() {
		live.Stopping = true
		live.WaitReason = "stopping"
		ctxDone = nil
		if timer != nil {
			timer.Stop()
			tick = nil
		}
		if cancelTask != nil {
			cancelTask()
		}
		if active != nil && shutdown == nil {
			shutdown = time.NewTimer(5 * time.Second)
			shutdownTick = shutdown.C
		}
	}
	for {
		if live.Stopping && active == nil {
			return nil
		}
		if reschedule {
			if timer != nil {
				timer.Stop()
			}
			tick = nil
			reschedule = false
			if live.Paused {
				live.WaitReason = "paused"
			}
			if !live.Paused && !live.Stopping && active == nil {
				due, err := w.NextJobDue(ctx, kinds)
				if err != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return err
				}
				if !due.IsZero() {
					if options.ExperimentalScan {
						budget, err := w.DispatchBudget(ctx, time.Now(), cfg.Scan.MaxScanChunksPerDay)
						if err != nil {
							if ctx.Err() != nil {
								stop()
								continue
							}
							return err
						}
						live.Dispatch = &budget
						if budget.NextAllowed.After(due) {
							due = budget.NextAllowed
						}
						if live.WaitReason != "wal_backpressure" {
							live.WaitReason = budget.Reason
						}
					}
					if due.Before(nextAllowed) {
						due = nextAllowed
					}
					timer = time.NewTimer(max(time.Until(due), 0))
					tick = timer.C
				} else {
					live.WaitReason = "idle"
				}
			}
		}
		select {
		case <-ctxDone:
			stop()
		case err := <-srv.errors:
			return fmt.Errorf("control listener: %w", err)
		case <-shutdownTick:
			return errors.New("job handler did not stop within 5 seconds; its saved lease will be recovered on restart")
		case call := <-calls:
			if call.ctx.Err() != nil {
				continue
			}
			response := Response{Version: protocolVersion, OK: true}
			switch call.request.Command {
			case "status":
			case "pause", "resume":
				if live.Stopping {
					response.OK = false
					response.Error = "worker is stopping"
					break
				}
				value := call.request.Command == "pause"
				if err := w.SetPaused(call.ctx, value); err != nil {
					response.OK = false
					response.Error = err.Error()
					break
				}
				live.Paused = value
				reschedule = true
				if value && cancelTask != nil {
					cancelTask()
				}
			case "stop":
				stop()
			default:
				response.OK = false
				response.Error = "unknown control command"
			}
			refreshMetrics()
			response.Status = live
			call.reply <- response
		case <-tick:
			tick = nil
			if options.ExperimentalScan {
				blocked, err := w.WALBlocked(ctx, state.WALBackpressureBytes)
				if err != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return err
				}
				if blocked {
					live.WaitReason = "wal_backpressure"
					nextAllowed = time.Now().Add(time.Minute)
					reschedule = true
					continue
				}
				budget, err := w.DispatchBudget(ctx, time.Now(), cfg.Scan.MaxScanChunksPerDay)
				if err != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return err
				}
				live.Dispatch = &budget
				if budget.Reason != "" {
					reschedule = true
					continue
				}
			}
			now := time.Now()
			job, err := w.ClaimJob(ctx, kinds, now, work+10*time.Second)
			if err != nil {
				if ctx.Err() != nil {
					stop()
					continue
				}
				return err
			}
			if job == nil {
				reschedule = true
				continue
			}
			if options.ExperimentalScan && job.Kind == state.ScanKind {
				budget, err := w.ReserveScanChunk(ctx, time.Now(), interval, cfg.Scan.MaxScanChunksPerDay)
				if errors.Is(err, state.ErrDispatchDeferred) {
					if err := w.FinishJob(ctx, *job, false, job.Cursor, budget.NextAllowed, ""); err != nil {
						return err
					}
					reschedule = true
					continue
				}
				if err != nil {
					return fmt.Errorf("reserve scan dispatch: %w", err)
				}
				live.Dispatch = &budget
			}
			active = job
			live.WaitReason = "running"
			live.ActiveJob = job.ID
			cancelTask = startChunk(ctx, work, *job, handlers[job.Kind], done)
		case result := <-done:
			cancelTask()
			cancelTask = nil
			// Claiming can take time on a busy disk. Pace from actual handler
			// execution so that database latency never shortens the interval.
			nextAllowed = result.startedAt.Add(interval)
			due := result.result.NextAt
			if due.IsZero() {
				due = time.Now()
			}
			lastError := ""
			if result.err != nil {
				result.result = Result{Cursor: active.Cursor}
				lastError = result.err.Error()
				if errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded) {
					lastError = ""
					due = time.Now()
					if active.Kind == state.ScanKind {
						due = time.Unix(0, 1)
					}
				} else {
					due = time.Now().Add(min(time.Second*time.Duration(1<<min(active.Attempts, 12)), time.Hour))
				}
			}
			finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			var err error
			if result.result.Scan != nil {
				err = w.CommitScan(finishCtx, *active, *result.result.Scan)
			} else {
				err = w.FinishJob(finishCtx, *active, result.result.Done, result.result.Cursor, due, lastError)
			}
			cancel()
			if err != nil {
				return fmt.Errorf("save job progress: %w", err)
			}
			active = nil
			live.ActiveJob = 0
			reschedule = true
		}
	}
}

func startChunk(ctx context.Context, duration time.Duration, job state.Job, handler Handler, done chan<- outcome) context.CancelFunc {
	taskCtx, cancel := context.WithTimeout(ctx, duration)
	go func() {
		defer cancel()
		result := outcome{startedAt: time.Now()}
		defer func() {
			if recover() != nil {
				result.result = Result{}
				result.err = errors.New("job handler panicked")
			}
			done <- result
		}()
		result.result, result.err = handler(taskCtx, job)
	}()
	return cancel
}
