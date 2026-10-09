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
	PID              int                           `json:"pid"`
	Instance         string                        `json:"instance"`
	StartedAt        time.Time                     `json:"started_at"`
	Paused           bool                          `json:"paused"`
	Stopping         bool                          `json:"stopping"`
	ActiveJob        int64                         `json:"active_job,omitempty"`
	RecoveredJobs    int64                         `json:"recovered_jobs"`
	Handlers         int                           `json:"handlers"`
	WaitReason       string                        `json:"wait_reason"`
	InventoryMetrics *inventory.Metrics            `json:"inventory_metrics,omitempty"`
	Dispatch         *state.DispatchBudget         `json:"dispatch,omitempty"`
	CPU              *CPUObservation               `json:"cpu,omitempty"`
	CPUFeedback      *state.CPUFeedbackState       `json:"cpu_feedback,omitempty"`
	Power            *PowerPolicySnapshot          `json:"power,omitempty"`
	InventoryState   *InventoryStatePolicySnapshot `json:"inventory_state,omitempty"`
	AdaptiveRevisits *AdaptiveRevisitSnapshot      `json:"adaptive_revisits,omitempty"`
	InventoryMode    *InventoryModeSnapshot        `json:"inventory_mode,omitempty"`
	APIPacing        *APIPacingSnapshot            `json:"api_pacing,omitempty"`
	Metadata         *state.MetadataBudget         `json:"metadata,omitempty"`
	Priority         *ThreadPriorityObservation    `json:"thread_priority,omitempty"`
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
	// Optional session observations/hooks are private; production uses SELF.
	cpuSessionObserve func() (time.Duration, error)
	cpuSessionHooks   *cpuSessionHooks
	// Private fixture coordinator; production retains one process-wide slot.
	powerCoordinator *powerCoordinator
	// Metadata-only admission fixture seam; production samples the fixed DB/WAL.
	inventoryStateObserve func(context.Context, *state.Store, int64) (state.InventoryStateBudget, error)
	// Clock and accounting hooks are private. Production uses the native clocks
	// and exact writer APIs; fixtures cannot change the recovery-delay policy.
	wallNow, elapsedNow func() time.Time
	cpuBegin            func(context.Context, *state.Store, *state.FairInventoryTurn, time.Time, state.CPUWindowStart) (state.CPUWindowMarker, error)
	cpuSettle           func(context.Context, *state.Store, state.CPUWindowMarker, time.Time, state.CPUWindowMeasurement) (state.CPUFeedbackState, error)
	cpuRecover          func(context.Context, *state.Store, time.Time) (state.CPUFeedbackState, error)
	// Scheduling requests are private so production uses the native source-thread adapter.
	priorityRequest func(context.Context) ThreadPriorityObservation
	// Production revisits root listings every 24 hours; only fixtures shorten it.
	revisitInterval time.Duration
	// Source hooks are private, for bounded permit/failure lifecycle fixtures.
	scannerNew       func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error)
	scannerNext      func(context.Context, *inventory.Scanner, state.Job, inventory.APIPermit) (state.ScanBatch, error)
	scannerPacedNew  func(context.Context, []string, []string, []string, inventory.APIPermit, ...inventory.Option) (*inventory.Scanner, error)
	scannerPacedNext func(context.Context, *inventory.Scanner, state.Job, inventory.APIPermit, inventory.APIEntryCapacity) (state.ScanBatch, error)
}
type outcome struct {
	result    Result
	err       error
	startedAt time.Time
	panicked  bool
}

func Run(ctx context.Context, dir string, cfg config.Config, options Options) error {
	wallClock, elapsedNow := options.wallNow, options.elapsedNow
	if wallClock == nil {
		wallClock = time.Now
	}
	if elapsedNow == nil {
		elapsedNow = time.Now
	}
	wallNow := func() time.Time { return wallClock().UTC() }
	requestPriority := options.priorityRequest
	if requestPriority == nil {
		requestPriority = requestThreadPriority
	}
	var priorityObservation atomic.Pointer[ThreadPriorityObservation]
	power := options.powerCoordinator
	if power == nil {
		power = processPowerCoordinator
	}
	powerLifetime := newSourcePowerLifetime(ctx, power)
	defer powerLifetime.cancelCurrent()
	powerState := sourcePowerState{enabled: options.ExperimentalScan && cfg.Scan.PauseOnBattery}
	inventoryState := sourceInventoryState{enabled: options.ExperimentalScan, limit: cfg.Scan.MaxStateBytes}
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
	if options.ExperimentalScan && (cfg.Scan.MaxStateBytes < state.MinInventoryStateBytes || cfg.Scan.MaxStateBytes > state.MaxInventoryStateBytes) {
		return state.ErrInventoryStateBudgetInput
	}
	if options.ExperimentalScan {
		if cfg.Scan.CompactInventory && cfg.Scan.AdaptiveRevisits {
			return fmt.Errorf("compact inventory uses fixed daily revisits: %w", state.ErrBackgroundInventoryInput)
		}
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
	var adaptive adaptiveRevisitPolicy
	var apiPacer *sourceAPIPacer
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
		if cfg.Scan.APIAttemptsPerSecond != 0 {
			bounds, err := inventory.APIPacingWorkBounds(cfg.Roots, cfg.Excludes, privatePaths)
			if err != nil {
				return err
			}
			apiPacer, err = newSourceAPIPacer(cfg.Scan.APIAttemptsPerSecond, work, cfg.Scan.MetadataPerSecond, bounds, wallNow, elapsedNow)
			if err != nil {
				return err
			}
		}
		if _, exists := handlers[state.ScanKind]; exists {
			return errors.New("inventory handler already registered")
		}
		// The source handler is bound to reserved windows at dispatch time.
		handlers[state.ScanKind] = nil
		kinds = append(kinds, state.ScanKind)
		fairRoots, err = w.ResolveFairInventoryRoots(ctx, cfg.Roots)
		if err != nil {
			return err
		}
	}
	// Writer-owned restart recovery retains full unknown charges. Status and
	// ordinary store opening never perform this mutation, including idle mode.
	if _, err := w.RecoverMetadataReservations(ctx, wallNow()); err != nil {
		return err
	}
	recovered, err := w.RecoverJobs(ctx, wallNow())
	if err != nil {
		return err
	}
	if options.ExperimentalScan {
		background, err := w.ConfigureBackgroundInventoryMode(ctx, fairRoots, cfg.Scan.CompactInventory, wallNow())
		if err != nil {
			return fmt.Errorf("configure background inventory mode: %w", err)
		}
		adaptive.background = background
		home, _ := os.UserHomeDir()
		digest, err := adaptiveRevisitDigest(cfg.Roots, cfg.Excludes, privatePaths, home)
		if err != nil {
			return err
		}
		if err = adaptive.configure(ctx, w, fairRoots, cfg.Scan.AdaptiveRevisits, digest, wallNow()); err != nil {
			return fmt.Errorf("configure adaptive inventory revisits: %w", err)
		}
		if !adaptive.enabled {
			page, err := w.SeedBackgroundInventoryRevisitPage(ctx, adaptive.background, 0, wallNow(), revisitInterval)
			if err != nil {
				return err
			}
			revisitCursor, revisitMore = page.Cursor, page.More
		}
	}
	paused, err := w.Paused(ctx)
	if err != nil {
		return err
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	live := Snapshot{PID: os.Getpid(), Instance: hex.EncodeToString(id[:]), StartedAt: wallNow().UTC(), Paused: paused, RecoveredJobs: recovered, Handlers: len(handlers)}
	var session *cpuSessionPolicy
	if options.ExperimentalScan && cfg.Scan.CPUSessionCharges {
		sessionObserve := options.cpuSessionObserve
		if sessionObserve == nil {
			sessionObserve = observeCPU
		}
		session = &cpuSessionPolicy{store: w, instance: live.Instance, observe: sessionObserve, wall: wallNow, elapsed: elapsedNow, hooks: options.cpuSessionHooks}
		live.WaitReason = "cpu_session_startup"
	}
	cpu := newCPUBudget()
	var savedCPU cpuFeedbackGate
	setCPUFeedbackAt := func(feedback state.CPUFeedbackState, wall, elapsed time.Time) {
		live.CPUFeedback = &feedback
		savedCPU.update(feedback, wall, elapsed)
	}
	setCPUFeedback := func(feedback state.CPUFeedbackState) {
		setCPUFeedbackAt(feedback, wallNow(), elapsedNow())
	}
	refreshCPUFeedback := func(queryCtx context.Context) error {
		if !options.ExperimentalScan {
			return nil
		}
		feedback, err := w.CPUFeedback(queryCtx)
		if err == nil {
			setCPUFeedback(feedback)
		}
		return err
	}
	recoverCPUFeedback := func(queryCtx context.Context) error {
		if !options.ExperimentalScan {
			return nil
		}
		// Anchor a newly recovered delay before publication. A forward wall
		// adjustment during the write cannot shorten its live elapsed wait.
		wall, elapsed := wallNow(), elapsedNow()
		var feedback state.CPUFeedbackState
		var err error
		if options.cpuRecover == nil {
			feedback, err = w.RecoverCPUWindow(queryCtx, wall)
		} else {
			feedback, err = options.cpuRecover(queryCtx, w, wall)
		}
		if err == nil || errors.Is(err, state.ErrCPUFeedbackClockRollback) {
			setCPUFeedbackAt(feedback, wall, elapsed)
			return nil // Keep controls available during a saved clock wait.
		}
		return err
	}
	if options.ExperimentalScan {
		feedbackCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := recoverCPUFeedback(feedbackCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("recover worker CPU feedback: %w", err)
		}
	}
	refreshMetrics := func() {
		if options.ExperimentalScan {
			live.Power = powerState.snapshot()
			live.InventoryState = inventoryState.snapshot()
			live.AdaptiveRevisits = adaptive.snapshot()
			live.InventoryMode = &InventoryModeSnapshot{Compact: adaptive.background.Compact()}
			live.APIPacing = apiPacer.snapshot()
		}
		live.Priority = priorityObservation.Load()
		observation := cpu.observation
		live.CPU = &observation
		if scanner := source.value.Load(); scanner != nil {
			metrics := scanner.Metrics()
			live.InventoryMetrics = &metrics
		}
	}
	finishCPU := func(window cpuWindow, marker *state.CPUWindowMarker) error {
		after, observationErr := observeCPU()
		completedWall, completedElapsed := wallNow(), elapsedNow()
		cpu.finishWithClocks(window, completedWall, completedElapsed, after, observationErr)
		refreshMetrics()
		if marker == nil {
			return nil
		}
		settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var feedback state.CPUFeedbackState
		var err error
		if options.cpuSettle == nil {
			feedback, err = w.SettleCPUWindow(settleCtx, *marker, completedWall, cpuMeasurement(cpu.observation))
		} else {
			feedback, err = options.cpuSettle(settleCtx, w, *marker, completedWall, cpuMeasurement(cpu.observation))
		}
		if err != nil {
			return fmt.Errorf("settle worker CPU window %s (saved source progress is retained; further dispatch stopped): %w", marker.Token(), err)
		}
		// The saved and live delays for this same completed window share
		// their original anchors; publication latency adds no new wait.
		setCPUFeedbackAt(feedback, completedWall, completedElapsed)
		return nil
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
	observeInventoryState := func(sampleCtx context.Context, phase string) bool {
		allowed := inventoryState.observe(sampleCtx, phase, wallNow, elapsedNow, func(sampleCtx context.Context) (state.InventoryStateBudget, error) {
			if options.inventoryStateObserve == nil {
				return w.InventoryStateBudget(sampleCtx, cfg.Scan.MaxStateBytes)
			}
			return options.inventoryStateObserve(sampleCtx, w, cfg.Scan.MaxStateBytes)
		})
		refreshMetrics()
		return allowed
	}
	if err := refreshMetadata(ctx, wallNow()); err != nil {
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
	var activeCPUMarker *state.CPUWindowMarker
	var activeMetadata []*metadataWindow
	stop := func() {
		powerLifetime.cancelCurrent()
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
			if session != nil {
				return errors.Join(ctx.Err(), session.finish())
			}
			return nil
		}
		if reschedule {
			if timer != nil {
				timer.Stop()
			}
			tick = nil
			reschedule = false
			if session != nil && active == nil && !live.Stopping && (session.stage < 3 || session.refused != nil) {
				wait := dispatchWait{}
				session.wait(&wait)
				live.WaitReason = wait.reason
				if session.refused == nil {
					timer = time.NewTimer(wait.duration)
					tick = timer.C
				}
			}
			if live.Paused {
				live.WaitReason = "paused"
			}
			if !live.Paused && !live.Stopping && active == nil && (session == nil || session.stage == 3 && session.refused == nil) {
				if err := refreshCPUFeedback(ctx); err != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return err
				}
				if feedback := live.CPUFeedback; feedback != nil && feedback.Status == "pending" && feedback.ClockHighWater != nil && !wallNow().Before(*feedback.ClockHighWater) {
					recoveryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					err := recoverCPUFeedback(recoveryCtx)
					cancel()
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("recover worker CPU feedback: %w", err)
					}
				}
				if feedback := live.CPUFeedback; feedback != nil && feedback.Status == "pending" {
					// Accounting recovery has its own clock wait. A future job,
					// cadence or exhausted quota cannot postpone that transition.
					wait := dispatchWait{reason: "cpu_accounting_pending"}
					if feedback.ClockHighWater != nil {
						wait.wall(*feedback.ClockHighWater, wallNow(), "cpu_clock_rollback")
					}
					live.WaitReason = wait.reason
					timer = time.NewTimer(wait.duration)
					tick = timer.C
				} else {
					now, elapsed := wallNow(), elapsedNow()
					var due time.Time
					var plan fairInventoryPlan
					var err error
					if revisitMore || adaptive.startupPending() {
						due, live.WaitReason = now, "inventory_revisit_setup"
					} else if options.ExperimentalScan {
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
						apiReady := apiPacer.capacity(source.value.Load() == nil)
						plan, err = nextFairInventoryPlan(planCtx, w, fairRoots, genericKinds, now, *live.Metadata, neededStartup, apiReady && powerState.remaining(now, elapsed) == 0 && inventoryState.remaining(now, elapsed) == 0, apiPacer.blockedReason(apiReady))
						cancel()
						due, live.WaitReason = plan.due, plan.waitReason
						if err == nil {
							releaseDormantRootStreams(&source, plan.schedule)
						}
					} else {
						due, err = w.NextJobDue(ctx, kinds)
						live.WaitReason = ""
					}
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return err
					}
					if !due.IsZero() {
						if due.After(now) {
							if live.WaitReason == "" {
								live.WaitReason = "job_retry"
							}
							if options.ExperimentalScan && !plan.generic && plan.waitReason == "job_retry" {
								revisit, err := adaptive.pending(ctx, w, due, revisitInterval)
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
						}
						wait := dispatchWait{reason: live.WaitReason}
						wait.wall(due, now, live.WaitReason)
						if options.ExperimentalScan && !revisitMore && !adaptive.startupPending() && !plan.generic && plan.schedule.Turn == nil && !plan.schedule.NextSourceDue.IsZero() {
							wait.add(powerState.remaining(now, elapsed), "power_source_backoff")
							wait.add(inventoryState.remaining(now, elapsed), "inventory_state_source_backoff")
						}
						if options.ExperimentalScan && !revisitMore && !adaptive.startupPending() {
							budget, err := w.DispatchBudget(ctx, now, cfg.Scan.MaxScanChunksPerDay)
							if err != nil {
								if ctx.Err() != nil {
									stop()
									continue
								}
								return err
							}
							live.Dispatch = &budget
							if remaining := budget.NextAllowed.UTC().Sub(now); budget.Reason != "" && remaining >= wait.duration {
								wait.duration, wait.reason = max(remaining, 0), budget.Reason
							}
						}
						if !revisitMore {
							wait.elapsed(nextAllowed, elapsed, "cadence")
						}
						wait.elapsed(cpu.nextAllowed, elapsed, "cpu_backoff")
						if live.CPUFeedback != nil {
							savedCPU.add(&wait, *live.CPUFeedback, now, elapsed)
						}
						wait.elapsed(walNextAllowed, elapsed, "wal_backpressure")
						if !session.wait(&wait) && session != nil && session.refused != nil {
							live.WaitReason = wait.reason
							continue
						}
						live.WaitReason = wait.reason
						timer = time.NewTimer(wait.duration)
						tick = timer.C
					} else {
						live.WaitReason = plan.waitReason
						if live.WaitReason == "" {
							live.WaitReason = "idle"
						}
					}
				}
			}
		}

		select {
		case <-powerState.wake:
			powerState.wake = nil
			powerState.refreshCompletion()
			refreshMetrics()
			reschedule = true // A completion only wakes full admission replanning.
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
				if value {
					powerLifetime.cancelCurrent()
				} else {
					powerLifetime.resume()
				}
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
			if err := refreshMetadata(call.ctx, wallNow()); err != nil {
				response.OK, response.Error = false, err.Error()
			}
			if err := refreshCPUFeedback(call.ctx); err != nil {
				response.OK, response.Error = false, err.Error()
			}
			response.Status = live
			call.reply <- response
		case <-tick:
			tick = nil
			if session != nil && session.stage < 3 {
				session.startup(ctx)
				reschedule = true
				continue
			}
			if live.CPUFeedback != nil && live.CPUFeedback.Status == "pending" {
				// A rollback timer may reach the saved high-water without any
				// control request. Recover before reserving another root turn.
				reschedule = true
				continue
			}
			// A timer is only a wakeup. Recheck independent clock domains before
			// a reservation, so forward wall changes cannot erase live waits.
			wait := dispatchWait{}
			wait.elapsed(nextAllowed, elapsedNow(), "cadence")
			wait.elapsed(cpu.nextAllowed, elapsedNow(), "cpu_backoff")
			if live.CPUFeedback != nil {
				savedCPU.add(&wait, *live.CPUFeedback, wallNow(), elapsedNow())
			}
			wait.elapsed(walNextAllowed, elapsedNow(), "wal_backpressure")
			if !session.wait(&wait) && session != nil && session.refused != nil {
				reschedule = true
				continue
			}
			if wait.duration > 0 {
				reschedule = true
				continue
			}
			windowWall := wallNow()
			window := beginCPUWindowAt(observeCPU, elapsedNow())
			var marker *state.CPUWindowMarker
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
					walNextAllowed = elapsedNow().Add(time.Minute)
					reschedule = true
					continue
				}
				if revisitMore {
					pageCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					page, err := w.SeedBackgroundInventoryRevisitPage(pageCtx, adaptive.background, revisitCursor, wallNow(), revisitInterval)
					cancel()
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("schedule root-listing revisits: %w", err)
					}
					revisitCursor, revisitMore = page.Cursor, page.More
					if err := finishCPU(window, nil); err != nil {
						return err
					}
					reschedule = true
					continue
				}
				if adaptive.startupPending() {
					pageCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					err := adaptive.initializeNext(pageCtx, w, wallNow())
					cancel()
					if err != nil {
						if ctx.Err() != nil {
							stop()
							continue
						}
						return fmt.Errorf("initialize adaptive inventory revisits: %w", err)
					}
					if err := finishCPU(window, nil); err != nil {
						return err
					}
					reschedule = true
					continue
				}
			}
			now := wallNow()
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
				apiReady := apiPacer.capacity(source.value.Load() == nil)
				plan, planErr := nextFairInventoryPlan(planCtx, w, fairRoots, genericKinds, now, *live.Metadata, neededStartup, apiReady, apiPacer.blockedReason(apiReady))
				cancel()
				if planErr != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return planErr
				}
				// No observation starts on a quota-gated timer or before source
				// metadata readiness. A maintenance-selected turn may still have
				// another eligible due source, which must be screened before claim.
				budget, budgetErr := w.DispatchBudget(ctx, wallNow(), cfg.Scan.MaxScanChunksPerDay)
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
				sourceScreened := plan.allowSource && !plan.schedule.NextSourceDue.IsZero() && !plan.schedule.NextSourceDue.After(now)
				allowSource := sourceScreened && inventoryState.remaining(wallNow(), elapsedNow()) == 0 && powerState.remaining(wallNow(), elapsedNow()) == 0
				if allowSource {
					sampleCtx, sampleCancel := context.WithTimeout(ctx, inventoryStateSampleWindow)
					allowSource = observeInventoryState(sampleCtx, "before_dispatch")
					sampleCancel()
					if ctx.Err() != nil {
						stop()
						continue
					}
				}
				stateScreened := allowSource
				if stateScreened && cfg.Scan.PauseOnBattery {
					decision := power.sourcePolicy(powerLifetime.ctx)
					powerState.update(decision, wallNow())
					if decision.Started {
						powerLifetime.owned = decision.Ticket
					}
					allowSource = !decision.SourceBackoff
					refreshMetrics()
				}
				planCtx, cancel = context.WithTimeout(ctx, 2*time.Second)
				plan, planErr = nextFairInventoryPlan(planCtx, w, fairRoots, genericKinds, wallNow(), *live.Metadata, neededStartup, allowSource, apiPacer.blockedReason(apiReady))
				cancel()
				if planErr != nil {
					if ctx.Err() != nil {
						stop()
						continue
					}
					return planErr
				}
				releaseDormantRootStreams(&source, plan.schedule)
				if !plan.generic && plan.schedule.Turn == nil {
					reschedule = true
					continue
				}
				if plan.due.IsZero() || plan.due.After(wallNow()) {
					reschedule = true
					continue
				}
				// Only a genuinely due, otherwise-admissible selection samples
				// native session CPU. New debt or uncertainty precedes receipts.
				if session != nil {
					if !session.sampleAtBoundary(ctx, false) {
						reschedule = true
						continue
					}
					admission := dispatchWait{}
					session.wait(&admission)
					admission.elapsed(nextAllowed, elapsedNow(), "cadence")
					admission.elapsed(cpu.nextAllowed, elapsedNow(), "cpu_backoff")
					if live.CPUFeedback != nil {
						savedCPU.add(&admission, *live.CPUFeedback, wallNow(), elapsedNow())
					}
					if session.refused != nil || admission.duration > 0 {
						reschedule = true
						continue
					}
					blocked, checkErr := w.WALBlocked(ctx, state.WALBackpressureBytes)
					if checkErr != nil {
						return checkErr
					}
					if blocked {
						walNextAllowed = elapsedNow().Add(time.Minute)
						reschedule = true
						continue
					}
				}
				if plan.generic {
					budget, budgetErr := w.DispatchBudget(ctx, wallNow(), cfg.Scan.MaxScanChunksPerDay)
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
					reservedAt := wallNow()
					reservedElapsed := elapsedNow()
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
					nextAllowed = reservedElapsed.Add(interval)
					// The receipt stays charged if the second sample refuses.
					// No source claim or CPU marker exists at this boundary.
					if stateScreened && plan.allowSource {
						plan.allowSource = observeInventoryState(turnCtx, "after_receipt")
						if ctx.Err() != nil {
							cancel()
							stop()
							continue
						}
					}
					// The receipt remains charged if freshness changed. This
					// recheck cannot launch another callback after reservation.
					if sourceScreened && cfg.Scan.PauseOnBattery && plan.allowSource {
						decision := power.recheckSourcePolicy(powerLifetime.ctx)
						powerState.update(decision, wallNow())
						refreshMetrics()
						if decision.Status == "sample_required" {
							cancel()
							reschedule = true
							continue
						}
						plan.allowSource = !decision.SourceBackoff
					}
					claimWall, claimElapsed := wallNow(), elapsedNow()
					if !inventoryStateReceiptCurrent(turnCtx, reservedAt, reservedElapsed, claimWall, claimElapsed) {
						cancel()
						reschedule = true
						continue
					}
					admission := dispatchWait{}
					if !session.wait(&admission) {
						cancel()
						reschedule = true
						continue // The committed receipt is never refunded.
					}
					turn, claimErr := w.ClaimFairInventoryTurn(turnCtx, fairRoots, claimWall, work+10*time.Second, plan.allowSource, reservedAt)
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
					start := state.CPUWindowStart{Instance: live.Instance, WindowStartedAt: windowWall}
					var begun state.CPUWindowMarker
					var beginErr error
					if options.cpuBegin == nil {
						begun, beginErr = w.BeginCPUWindow(turnCtx, turn, wallNow(), start)
					} else {
						begun, beginErr = options.cpuBegin(turnCtx, w, turn, wallNow(), start)
					}
					if beginErr != nil {
						cancel()
						if begun.Token() != "" || errors.Is(beginErr, state.ErrCPUFeedbackPublication) {
							return fmt.Errorf("begin worker CPU window %s (no handler/source call attempted; inspect saved feedback before restart): %w", begun.Token(), beginErr)
						}
						// A definite denial cannot leave an unstarted source lease
						// running. Keep the exact old cursor and charged rotation.
						if turn.Job != nil {
							releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
							releaseErr := w.FinishJob(releaseCtx, *turn.Job, false, turn.Job.Cursor, time.Unix(0, 1), "")
							releaseCancel()
							if releaseErr != nil {
								return errors.Join(beginErr, releaseErr)
							}
						}
						if err := finishCPU(window, nil); err != nil {
							return err
						}
						if ctx.Err() != nil {
							stop()
							continue
						}
						if errors.Is(beginErr, state.ErrCPUFeedbackDeferred) || errors.Is(beginErr, state.ErrCPUFeedbackClockRollback) {
							reschedule = true
							continue
						}
						return fmt.Errorf("begin worker CPU window: %w", beginErr)
					}
					marker = &begun
					if err := refreshCPUFeedback(turnCtx); err != nil {
						cancel()
						return err // Pending provenance remains; no work has run.
					}
					if turn.Kind == state.FairInventoryMaintenance {
						admission := dispatchWait{}
						if !session.wait(&admission) {
							cancel()
							if cpuErr := finishCPU(window, marker); cpuErr != nil {
								return errors.Join(cpuErr, session.error())
							}
							reschedule = true
							continue // Charged receipt retained; no retirement/finalization.
						}
						step, retireErr := w.RetireBackgroundInventoryForRoot(turnCtx, adaptive.background, turn.RootID)
						if retireErr == nil && step.Eligible && !step.Remaining {
							retireErr = adaptive.finalize(turnCtx, w, turn.RootID, wallNow(), revisitInterval)
						}
						cancel()
						cpuErr := finishCPU(window, marker)
						if retireErr != nil {
							if cpuErr != nil {
								return errors.Join(retireErr, cpuErr)
							}
							if ctx.Err() != nil {
								stop()
								continue
							}
							return fmt.Errorf("retire saved inventory: %w", retireErr)
						}
						if step.Eligible && step.Remaining && !step.Worked {
							return state.ErrInventoryRetirementCorrupt
						}
						if cpuErr != nil {
							return cpuErr
						}
						if session != nil {
							session.sampleAtBoundary(context.Background(), false)
						}
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
			admission := dispatchWait{}
			if !session.wait(&admission) {
				// A late clock/debt refusal cannot leave an unstarted exact
				// lease running or send an unscreened handler to the source.
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
				releaseErr := w.FinishJob(releaseCtx, *job, false, job.Cursor, time.Unix(0, 1), "")
				releaseCancel()
				cpuErr := finishCPU(window, marker)
				if releaseErr != nil || cpuErr != nil {
					return errors.Join(releaseErr, cpuErr, session.error())
				}
				reschedule = true
				continue
			}
			handler := handlers[job.Kind]
			activeMetadata = nil
			if options.ExperimentalScan && job.Kind == state.ScanKind {

				var startupWindow *metadataWindow
				var reserveErr error
				if source.value.Load() == nil {
					reservation, err := w.ReserveMetadata(ctx, wallNow(), state.MetadataStartup, nil, startupAllowance, cfg.Scan.MetadataAttemptsPerDay)
					reserveErr = err
					if err == nil {
						startupWindow = metadataWindowWithClocks(reservation, work, wallNow, elapsedNow)
						activeMetadata = append(activeMetadata, startupWindow)
					}
				}
				var reservation state.MetadataReservation
				if reserveErr == nil {
					reservation, reserveErr = w.ReserveMetadata(ctx, wallNow(), state.MetadataNext, job, inventory.MaxAPIAttemptAllowance, cfg.Scan.MetadataAttemptsPerDay)
				}
				if reserveErr != nil {
					// A rollback or cancellation can race with readiness. No
					// source call has run: settle any charge as known zero, then
					// retain the old cursor and keep the control loop available.
					settleCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					settleErr := settleMetadata(settleCtx, w, activeMetadata, false, wallNow())
					if settleErr == nil {
						settleErr = w.FinishJob(settleCtx, *job, false, job.Cursor, time.Unix(0, 1), "")
					}
					cancel()
					activeMetadata = nil
					cpuErr := finishCPU(window, marker)
					if settleErr != nil {
						return errors.Join(reserveErr, settleErr, cpuErr)
					}
					if cpuErr != nil {
						return errors.Join(reserveErr, cpuErr)
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
				nextWindow := metadataWindowWithClocks(reservation, work, wallNow, elapsedNow)
				if startupWindow != nil {
					startupWindow.highWater = reservation.ClockHighWater
				}
				activeMetadata = append(activeMetadata, nextWindow)
				handler = func(ctx context.Context, j state.Job) (Result, error) {
					scanner := source.value.Load()
					if scanner == nil {
						var created *inventory.Scanner
						var err error
						if apiPacer == nil {
							created, err = scannerNew(ctx, cfg.Roots, cfg.Excludes, privatePaths, startupWindow.permit, inventory.WithEntryRate(cfg.Scan.MetadataPerSecond), inventory.WithRootStreams(state.MaxFairInventoryRoots))
						} else {
							factory := options.scannerPacedNew
							if factory == nil {
								factory = inventory.NewPermittedPaced
							}
							created, err = factory(ctx, cfg.Roots, cfg.Excludes, privatePaths, apiPacer.permit(startupWindow), inventory.WithEntryRate(cfg.Scan.MetadataPerSecond), inventory.WithRootStreams(state.MaxFairInventoryRoots))
						}
						if err != nil {
							return Result{}, err
						}
						source.publish(created)
						scanner = created
						// Construction may have observed wall time newer than
						// both reservations. Fence Next before its first call.
						nextWindow.inheritHighWater(startupWindow)
					}
					var batch state.ScanBatch
					var err error
					if apiPacer == nil {
						batch, err = scannerNext(ctx, scanner, j, nextWindow.permit)
					} else if options.scannerPacedNext != nil {
						batch, err = options.scannerPacedNext(ctx, scanner, j, apiPacer.permit(nextWindow), apiPacer.entryCapacity(nextWindow))
					} else {
						batch, err = scanner.NextPermittedPaced(ctx, j, apiPacer.permit(nextWindow), apiPacer.entryCapacity(nextWindow))
					}
					if err != nil {
						return Result{}, err
					}
					return Result{Scan: &batch}, nil
				}
			}
			admission = dispatchWait{}
			if !session.wait(&admission) {
				releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 2*time.Second)
				settleErr := settleMetadata(releaseCtx, w, activeMetadata, false, wallNow())
				if settleErr == nil {
					settleErr = w.FinishJob(releaseCtx, *job, false, job.Cursor, time.Unix(0, 1), "")
				}
				releaseCancel()
				activeMetadata = nil
				cpuErr := finishCPU(window, marker)
				if settleErr != nil || cpuErr != nil {
					return errors.Join(settleErr, cpuErr, session.error())
				}
				reschedule = true
				continue
			}
			active = job
			activeCPUWindow = window
			activeCPUMarker = marker
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
			cancelTask = startChunkWithPriorityClock(ctx, work, *job, handler, done, priorityRequest, elapsedNow)
		case result := <-done:
			cancelTask()
			cancelTask = nil
			if apiPacer != nil && errors.Is(result.err, inventory.ErrAPIPacingProfile) {
				apiPacer.blockProfile()
			}
			// Claiming can take time on a busy disk. Pace from actual handler
			// execution so that database latency never shortens the interval.
			nextAllowed = result.startedAt.Add(interval)
			due := result.result.NextAt
			if due.IsZero() {
				due = wallNow()
			}
			lastError := ""
			if result.err != nil {
				result.result = Result{Cursor: active.Cursor}
				lastError = result.err.Error()
				if metadataInterruption(result.err) {
					lastError = ""
					due = wallNow()
					if active.Kind == state.ScanKind {
						due = time.Unix(0, 1)
					}
				} else {
					due = wallNow().Add(min(time.Second*time.Duration(1<<min(active.Attempts, 12)), time.Hour))
				}
			}
			finishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := settleMetadata(finishCtx, w, activeMetadata, result.panicked, wallNow()); err != nil {
				cancel()
				return errors.Join(fmt.Errorf("settle scanner metadata: %w", err), finishCPU(activeCPUWindow, activeCPUMarker))
			}
			activeMetadata = nil
			var err error
			if result.result.Scan != nil {
				err = adaptive.commit(finishCtx, w, *active, *result.result.Scan, wallNow())
			} else {
				err = w.FinishJob(finishCtx, *active, result.result.Done, result.result.Cursor, due, lastError)
			}
			if err != nil {
				cancel()
				return errors.Join(fmt.Errorf("save job progress: %w", err), finishCPU(activeCPUWindow, activeCPUMarker))
			}
			if options.ExperimentalScan && active.Kind == state.ScanKind {
				if err := adaptive.finalize(finishCtx, w, active.RootID, wallNow(), revisitInterval); err != nil {
					cancel()
					return errors.Join(fmt.Errorf("schedule completed root-listing work: %w", err), finishCPU(activeCPUWindow, activeCPUMarker))
				}
			}
			if err := refreshMetadata(finishCtx, wallNow()); err != nil {
				cancel()
				return errors.Join(err, finishCPU(activeCPUWindow, activeCPUMarker))
			}
			cancel()
			// Include claiming, reservation, handler and owning-loop commit work.
			// Errors/cancellation still consumed process CPU in this window.
			if err := finishCPU(activeCPUWindow, activeCPUMarker); err != nil {
				return err
			}
			if session != nil {
				session.sampleAtBoundary(context.Background(), false)
			}
			activeCPUMarker = nil
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
	return startChunkWithPriorityClock(ctx, duration, job, handler, done, priorityRequest, time.Now)
}

func startChunkWithPriorityClock(ctx context.Context, duration time.Duration, job state.Job, handler Handler, done chan<- outcome, priorityRequest func(context.Context) ThreadPriorityObservation, elapsedNow func() time.Time) context.CancelFunc {
	taskCtx, cancel := context.WithTimeout(ctx, duration)
	go func() {
		defer cancel()
		result := outcome{startedAt: elapsedNow()}
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
