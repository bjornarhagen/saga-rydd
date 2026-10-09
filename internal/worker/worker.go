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
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

type Snapshot struct {
	PID              int                        `json:"pid"`
	Instance         string                     `json:"instance"`
	StartedAt        time.Time                  `json:"started_at"`
	Paused           bool                       `json:"paused"`
	Stopping         bool                       `json:"stopping"`
	ActiveJob        int64                      `json:"active_job,omitempty"`
	RecoveredJobs    int64                      `json:"recovered_jobs"`
	Handlers         int                        `json:"handlers"`
	WaitReason       string                     `json:"wait_reason"`
	InventoryMetrics *inventory.Metrics         `json:"inventory_metrics,omitempty"`
	Dispatch         *state.DispatchBudget      `json:"dispatch,omitempty"`
	CPU              *CPUObservation            `json:"cpu,omitempty"`
	Metadata         *state.MetadataBudget      `json:"metadata,omitempty"`
	Priority         *ThreadPriorityObservation `json:"thread_priority,omitempty"`
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
	// StartupCheck runs under the state writer lock before roots, scanner or
	// recovery use the captured settings. It can refuse a stale configuration.
	StartupCheck func(context.Context) error
	// Overrides support small deterministic lifecycle fixtures, not public flags.
	Interval, WorkDuration time.Duration
	// cpuObserve is private so production always uses native process accounting.
	cpuObserve func() (time.Duration, error)
	// Scheduling requests are private so production uses the native source-thread adapter.
	priorityRequest func(context.Context) ThreadPriorityObservation
	// Production revisits root listings every 24 hours; only fixtures shorten it.
	revisitInterval time.Duration
	// Source hooks are private, for bounded permit/failure lifecycle fixtures.
	scannerNew  func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error)
	scannerNext func(context.Context, *inventory.Scanner, state.Job, inventory.APIPermit) (state.ScanBatch, error)
}
type outcome struct {
	result    Result
	err       error
	startedAt time.Time
	panicked  bool
}

func Run(ctx context.Context, dir string, cfg config.Config, options Options) error {
	requestPriority := options.priorityRequest
	if requestPriority == nil {
		requestPriority = requestThreadPriority
	}
	var priorityObservation atomic.Pointer[ThreadPriorityObservation]
	observeCPU := options.cpuObserve
	if observeCPU == nil {
		observeCPU = processCPUTime
	}
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
	revisitInterval := state.InventoryRevisitInterval
	if options.revisitInterval != 0 {
		revisitInterval = options.revisitInterval
	}
	if revisitInterval <= 0 || revisitInterval > 30*24*time.Hour {
		return errors.New("invalid root-listing revisit interval")
	}
	if options.ExperimentalScan && (cfg.Scan.MetadataAttemptsPerDay < 1 || cfg.Scan.MetadataAttemptsPerDay > state.MetadataDailyLimit) {
		return state.ErrMetadataInvalid
	}
	if options.ExperimentalScan {
		if err := state.ValidateFairInventoryPaths(cfg.Roots); err != nil {
			return err
		}
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
	genericKinds := append([]string(nil), kinds...)
	w, err := state.OpenWriter(ctx, dir)
	if err != nil {
		return err
	}
	defer w.Close()
	if options.StartupCheck != nil {
		if err := options.StartupCheck(ctx); err != nil {
			return err
		}
	}
	if err := w.SyncRoots(ctx, cfg.Roots); err != nil {
		return err
	}
	var source scannerHolder
	defer source.close()
	var privatePaths []string
	var startupAllowance int64
	var revisitCursor int64
	var revisitMore bool
	var fairRoots state.FairInventoryRoots
	scannerNew := options.scannerNew
	if scannerNew == nil {
		scannerNew = inventory.NewPermitted
	}
	scannerNext := options.scannerNext
	if scannerNext == nil {
		scannerNext = func(ctx context.Context, scanner *inventory.Scanner, job state.Job, permit inventory.APIPermit) (state.ScanBatch, error) {
			return scanner.NextPermitted(ctx, job, permit)
		}
	}
	if options.ExperimentalScan {
		endpoint, err := Endpoint(dir)
		if err != nil {
			return err
		}
		privatePaths = append(append([]string{}, options.PrivatePaths...), dir, filepath.Dir(endpoint))
		startupAllowance, err = inventory.StartupAPIAttemptAllowance(cfg.Roots, cfg.Excludes, privatePaths)
		if err != nil {
			return err
		}
		if _, exists := handlers[state.ScanKind]; exists {
			return errors.New("inventory handler already registered")
		}
		// The source handler is bound to reserved windows at dispatch time.
		handlers[state.ScanKind] = nil
		kinds = append(kinds, state.ScanKind)
		compact, modeErr := w.ConfigureCompact(ctx, nil)
		if modeErr != nil {
			return modeErr
		}
		if compact {
			return errors.New("compact inventories currently require the manual scan command; background compact scanning is not enabled")
		}
		page, err := w.SeedInventoryRevisitPage(ctx, 0, time.Now(), revisitInterval)
		if err != nil {
			return err
		}
		revisitCursor, revisitMore = page.Cursor, page.More
		fairRoots, err = w.ResolveFairInventoryRoots(ctx, cfg.Roots)
		if err != nil {
			return err
		}
	}
	// Writer-owned restart recovery retains full unknown charges. Status and
	// ordinary store opening never perform this mutation, including idle mode.
	if _, err := w.RecoverMetadataReservations(ctx, time.Now()); err != nil {
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
	cpu := newCPUBudget()
	refreshMetrics := func() {
		live.Priority = priorityObservation.Load()
		observation := cpu.observation
		live.CPU = &observation
		if scanner := source.value.Load(); scanner != nil {
			metrics := scanner.Metrics()
			live.InventoryMetrics = &metrics
		}
	}
	refreshMetadata := func(queryCtx context.Context, now time.Time) error {
		if !options.ExperimentalScan {
			return nil
		}
		budget, err := w.MetadataBudget(queryCtx, now, cfg.Scan.MetadataAttemptsPerDay)
		if err == nil {
			live.Metadata = &budget
		}
		return err
	}
	if err := refreshMetadata(ctx, time.Now()); err != nil {
		return err
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
	walNextAllowed := time.Time{}
	var activeCPUWindow cpuWindow
	var activeMetadata []*metadataWindow
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
				if revisitMore {
					// Only a bounded raw-root page runs per event-loop turn.
					// Controls remain available between pages; source permits
					// are still required before any actual scanner dispatch.
					due := time.Now()
					live.WaitReason = "inventory_revisit_setup"
					if cpu.nextAllowed.After(due) {
						due, live.WaitReason = cpu.nextAllowed, "cpu_backoff"
					}
					if walNextAllowed.After(due) {
						due, live.WaitReason = walNextAllowed, "wal_backpressure"
					}
					timer = time.NewTimer(max(time.Until(due), 0))
					tick = timer.C
				} else {
					now := time.Now()
					var due time.Time
					var plan fairInventoryPlan
					var err error
					if options.ExperimentalScan {
						if err = refreshMetadata(ctx, now); err != nil {
							if ctx.Err() != nil {
								stop()
								continue
							}
							return err
						}
						neededStartup := int64(0)
						if source.value.Load() == nil {
							neededStartup = startupAllowance
						}
						planCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
						plan, err = nextFairInventoryPlan(planCtx, w, fairRoots, genericKinds, now, *live.Metadata, neededStartup)
						cancel()
						due = plan.due
						if err == nil {
							releaseDormantRootStreams(&source, plan.schedule)
						}
					} else {
						due, err = w.NextJobDue(ctx, kinds)
					}
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return err
					}
					if !due.IsZero() {
						live.WaitReason = plan.waitReason
						if due.After(now) {
							if live.WaitReason == "" {
								live.WaitReason = "job_retry"
							}
							if options.ExperimentalScan && !plan.generic && plan.waitReason == "job_retry" {
								revisit, err := w.InventoryRevisitPending(ctx, due, revisitInterval)
								if err != nil {
									if ctx.Err() != nil {
										stop()
										continue
									}
									return err
								}
								if revisit {
									live.WaitReason = "inventory_revisit"
								}
							}
						} else {
							due = now
						}
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
							if budget.Reason != "" && !budget.NextAllowed.Before(due) {
								due = budget.NextAllowed
								live.WaitReason = budget.Reason
							}

						}
						if due.Before(nextAllowed) {
							due = nextAllowed
							live.WaitReason = "cadence"
						}
						if cpu.nextAllowed.After(due) {
							due = cpu.nextAllowed
							live.WaitReason = "cpu_backoff"
						}
						if walNextAllowed.After(due) {
							due = walNextAllowed
							live.WaitReason = "wal_backpressure"
						}
						timer = time.NewTimer(max(time.Until(due), 0))
						tick = timer.C
					} else {
						live.WaitReason = "idle"
					}
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
			if err := refreshMetadata(call.ctx, time.Now()); err != nil {
				response.OK, response.Error = false, err.Error()
			}
			response.Status = live
			call.reply <- response
		case <-tick:
			tick = nil
			window := beginCPUWindow(observeCPU)
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
					walNextAllowed = time.Now().Add(time.Minute)
					reschedule = true
					continue
				}
				if revisitMore {
					pageCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					page, err := w.SeedInventoryRevisitPage(pageCtx, revisitCursor, time.Now(), revisitInterval)
					cancel()
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("schedule root-listing revisits: %w", err)
					}
					revisitCursor, revisitMore = page.Cursor, page.More
					after, cpuErr := observeCPU()
					cpu.finish(window, time.Now(), after, cpuErr)
					refreshMetrics()
					reschedule = true
					continue
				}
			}
			now := time.Now()
			var job *state.Job
			var err error
			if options.ExperimentalScan {
				if err = refreshMetadata(ctx, now); err != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return err
				}
				neededStartup := int64(0)
				if source.value.Load() == nil {
					neededStartup = startupAllowance
				}
				planCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				plan, planErr := nextFairInventoryPlan(planCtx, w, fairRoots, genericKinds, now, *live.Metadata, neededStartup)
				cancel()
				if planErr != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return planErr
				}
				releaseDormantRootStreams(&source, plan.schedule)
				if plan.due.IsZero() || plan.due.After(now) {
					reschedule = true
					continue
				}
				if plan.generic {
					budget, budgetErr := w.DispatchBudget(ctx, time.Now(), cfg.Scan.MaxScanChunksPerDay)
					if budgetErr != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return budgetErr
					}
					live.Dispatch = &budget
					if budget.Reason != "" {
						reschedule = true
						continue
					}
					job, err = w.ClaimJob(ctx, genericKinds, now, work+10*time.Second)
				} else {
					// One committed dispatch receipt funds one fair root turn.
					reservedAt := time.Now()
					turnCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					budget, reserveErr := w.ReserveScanChunk(turnCtx, reservedAt, interval, cfg.Scan.MaxScanChunksPerDay)
					live.Dispatch = &budget
					if errors.Is(reserveErr, state.ErrDispatchDeferred) {
						cancel()
						reschedule = true
						continue
					}
					if reserveErr != nil {
						cancel()
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("reserve inventory root turn: %w", reserveErr)
					}
					turn, claimErr := w.ClaimFairInventoryTurn(turnCtx, fairRoots, time.Now(), work+10*time.Second, plan.allowSource, reservedAt)
					if claimErr != nil {
						cancel()
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("claim inventory root turn: %w", claimErr)
					}
					if turn == nil {
						cancel()
						reschedule = true
						continue
					}
					nextAllowed = reservedAt.Add(interval)
					if turn.Kind == state.FairInventoryMaintenance {
						step, retireErr := w.RetireInventoryForRoot(turnCtx, turn.RootID)
						if retireErr == nil && step.Eligible && !step.Remaining {
							_, retireErr = w.ScheduleInventoryRevisit(turnCtx, turn.RootID, time.Now(), revisitInterval)
						}
						cancel()
						if retireErr != nil {
							if ctx.Err() != nil {
								stop()
								continue
							}
							return fmt.Errorf("retire saved inventory: %w", retireErr)
						}
						if step.Eligible && step.Remaining && !step.Worked {
							return state.ErrInventoryRetirementCorrupt
						}
						after, cpuErr := observeCPU()
						cpu.finish(window, time.Now(), after, cpuErr)
						refreshMetrics()
						reschedule = true
						continue
					}
					job = turn.Job
					cancel()
				}
			} else {
				job, err = w.ClaimJob(ctx, kinds, now, work+10*time.Second)
			}
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
			handler := handlers[job.Kind]
			activeMetadata = nil
			if options.ExperimentalScan && job.Kind == state.ScanKind {

				var startupWindow *metadataWindow
				var reserveErr error
				if source.value.Load() == nil {
					reservation, err := w.ReserveMetadata(ctx, time.Now(), state.MetadataStartup, nil, startupAllowance, cfg.Scan.MetadataAttemptsPerDay)
					reserveErr = err
					if err == nil {
						startupWindow = newMetadataWindow(reservation, work)
						activeMetadata = append(activeMetadata, startupWindow)
					}
				}
				var reservation state.MetadataReservation
				if reserveErr == nil {
					reservation, reserveErr = w.ReserveMetadata(ctx, time.Now(), state.MetadataNext, job, inventory.MaxAPIAttemptAllowance, cfg.Scan.MetadataAttemptsPerDay)
				}
				if reserveErr != nil {
					// A rollback or cancellation can race with readiness. No
					// source call has run: settle any charge as known zero, then
					// retain the old cursor and keep the control loop available.
					settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					settleErr := settleMetadata(settleCtx, w, activeMetadata, false, time.Now())
					if settleErr == nil {
						settleErr = w.FinishJob(settleCtx, *job, false, job.Cursor, time.Unix(0, 1), "")
					}
					cancel()
					activeMetadata = nil
					if settleErr != nil {
						return errors.Join(reserveErr, settleErr)
					}
					if ctx.Err() != nil {
						stop()
						continue
					}
					if errors.Is(reserveErr, state.ErrMetadataDeferred) {
						reschedule = true
						continue
					}
					return fmt.Errorf("reserve scanner metadata: %w", reserveErr)
				}
				nextWindow := newMetadataWindow(reservation, work)
				if startupWindow != nil {
					startupWindow.highWater = reservation.ClockHighWater
				}
				activeMetadata = append(activeMetadata, nextWindow)
				handler = func(ctx context.Context, j state.Job) (Result, error) {
					scanner := source.value.Load()
					if scanner == nil {
						created, err := scannerNew(ctx, cfg.Roots, cfg.Excludes, privatePaths, startupWindow.permit, inventory.WithEntryRate(cfg.Scan.MetadataPerSecond), inventory.WithRootStreams(state.MaxFairInventoryRoots))
						if err != nil {
							return Result{}, err
						}
						source.publish(created)
						scanner = created
						// Construction may have observed wall time newer than
						// both reservations. Fence Next before its first call.
						nextWindow.inheritHighWater(startupWindow)
					}
					batch, err := scannerNext(ctx, scanner, j, nextWindow.permit)
					if err != nil {
						return Result{}, err
					}
					return Result{Scan: &batch}, nil
				}
			}
			active = job
			activeCPUWindow = window
			live.WaitReason = "running"
			live.ActiveJob = job.ID
			var priorityRequest func(context.Context) ThreadPriorityObservation
			if options.ExperimentalScan && job.Kind == state.ScanKind {
				priorityRequest = func(ctx context.Context) ThreadPriorityObservation {
					observation := requestPriority(ctx)
					priorityObservation.Store(&observation)
					return observation
				}
			}
			cancelTask = startChunkWithPriority(ctx, work, *job, handler, done, priorityRequest)
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
				if metadataInterruption(result.err) {
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
			if err := settleMetadata(finishCtx, w, activeMetadata, result.panicked, time.Now()); err != nil {
				cancel()
				return fmt.Errorf("settle scanner metadata: %w", err)
			}
			activeMetadata = nil
			var err error
			if result.result.Scan != nil {
				err = w.CommitScan(finishCtx, *active, *result.result.Scan)
			} else {
				err = w.FinishJob(finishCtx, *active, result.result.Done, result.result.Cursor, due, lastError)
			}
			if err != nil {
				cancel()
				return fmt.Errorf("save job progress: %w", err)
			}
			if options.ExperimentalScan && active.Kind == state.ScanKind {
				if _, err := w.ScheduleInventoryRevisit(finishCtx, active.RootID, time.Now(), revisitInterval); err != nil {
					cancel()
					return fmt.Errorf("schedule completed root-listing work: %w", err)
				}
			}
			if err := refreshMetadata(finishCtx, time.Now()); err != nil {
				cancel()
				return err
			}
			cancel()
			// Include claiming, reservation, handler and owning-loop commit work.
			// Errors/cancellation still consumed process CPU in this window.
			after, cpuErr := observeCPU()
			cpu.finish(activeCPUWindow, time.Now(), after, cpuErr)
			refreshMetrics()
			active = nil
			live.ActiveJob = 0
			reschedule = true
		}
	}
}

func startChunk(ctx context.Context, duration time.Duration, job state.Job, handler Handler, done chan<- outcome) context.CancelFunc {
	return startChunkWithPriority(ctx, duration, job, handler, done, nil)
}

func startChunkWithPriority(ctx context.Context, duration time.Duration, job state.Job, handler Handler, done chan<- outcome, priorityRequest func(context.Context) ThreadPriorityObservation) context.CancelFunc {
	taskCtx, cancel := context.WithTimeout(ctx, duration)
	go func() {
		defer cancel()
		result := outcome{startedAt: time.Now()}
		defer func() {
			if recover() != nil {
				result.result = Result{}
				result.err = errors.New("job handler panicked")
				result.panicked = true
			}
			done <- result
		}()
		if priorityRequest != nil {
			runtime.LockOSThread()
			// A Linux nice reduction may be irreversible without privilege.
			// Never unlock a changed thread into Go's pool; goroutine exit
			// terminates the locked thread, including cancellation or panic.
			_ = priorityRequest(taskCtx)
		}
		if err := taskCtx.Err(); err != nil {
			result.err = err
			return
		}
		result.result, result.err = handler(taskCtx, job)
	}()
	return cancel
}
