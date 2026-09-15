// Package worker runs one cooperative, read-only job chunk at a time. Filesystem
// handlers are intentionally absent until the scanner milestone.
package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type Snapshot struct {
	PID           int       `json:"pid"`
	Instance      string    `json:"instance"`
	StartedAt     time.Time `json:"started_at"`
	Paused        bool      `json:"paused"`
	Stopping      bool      `json:"stopping"`
	ActiveJob     int64     `json:"active_job,omitempty"`
	RecoveredJobs int64     `json:"recovered_jobs"`
	Handlers      int       `json:"handlers"`
}

type Result struct {
	Done   bool
	Cursor []byte
	NextAt time.Time
}

// Handlers must honor cancellation and have no irreversible effects. They return
// bounded progress to the owning event loop rather than holding a DB connection.
type Handler func(context.Context, state.Job) (Result, error)
type Options struct {
	Handlers map[string]Handler
	Ready    func(Snapshot)
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
					if due.Before(nextAllowed) {
						due = nextAllowed
					}
					timer = time.NewTimer(max(time.Until(due), 0))
					tick = timer.C
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
			response.Status = live
			call.reply <- response
		case <-tick:
			tick = nil
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
			active = job
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
				} else {
					due = time.Now().Add(min(time.Second*time.Duration(1<<min(active.Attempts, 12)), time.Hour))
				}
			}
			finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := w.FinishJob(finishCtx, *active, result.result.Done, result.result.Cursor, due, lastError)
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
