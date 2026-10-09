package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

type fixtureIOSource struct {
	reads, closes int
	readFn        func(context.Context, <-chan struct{}) (kernelIOCounters, string)
}

func (s *fixtureIOSource) read(ctx context.Context, done <-chan struct{}) (kernelIOCounters, string) {
	s.reads++
	return s.readFn(ctx, done)
}
func (s *fixtureIOSource) close() { s.closes++ }

func fixtureIOTracker(s *fixtureIOSource, done <-chan struct{}) (*kernelIOTracker, *time.Time) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	return newKernelIOTracker(s, done, now.Add(-time.Second), func() time.Time { return now }, "unavailable", "not_sampled"), &now
}

func TestKernelIOSampledDeltaAndImmutableEvidence(t *testing.T) {
	first := kernelIOCounters{1, 2, 3, 4, 5, 6, 7}
	last := kernelIOCounters{11, 12, 13, 14, 15, 16, 37}
	source := &fixtureIOSource{readFn: func(context.Context, <-chan struct{}) (kernelIOCounters, string) { return first, "" }}
	tk, now := fixtureIOTracker(source, make(chan struct{}))
	one := tk.sample(context.Background())
	if one.Status != "observed" || one.Counters == nil || *one.Counters != first {
		t.Fatal(one)
	}
	one.Counters.RChar = math.MaxUint64
	*now = now.Add(time.Second)
	source.readFn = func(context.Context, <-chan struct{}) (kernelIOCounters, string) { return last, "" }
	two := tk.sample(context.Background())
	saved := tk.summary()
	if two.Status != "observed" || saved.ObservedSamples != 2 || saved.RefusedSamples != 0 || saved.FirstCounters == nil || *saved.FirstCounters != first || saved.LastCounters == nil || *saved.LastCounters != last || saved.SampledDelta == nil || *saved.SampledDelta != (kernelIOCounters{10, 10, 10, 10, 10, 10, 30}) || saved.FullLifetime || saved.AtomicSnapshot || saved.Gaps {
		t.Fatal(saved)
	}
	// Cancelled write accounting can exceed write accounting. It is neither
	// subtracted from write_bytes nor interpreted as a negative net write.
	*saved.FirstCounters, *saved.LastCounters, *saved.SampledDelta = kernelIOCounters{}, kernelIOCounters{}, kernelIOCounters{}
	*saved.FirstElapsedNS, *saved.LastElapsedNS = -1, -1
	again := tk.summary()
	if *again.FirstCounters != first || *again.LastCounters != last || again.SampledDelta.CancelledWriteBytes != 30 || *again.FirstElapsedNS != int64(time.Second) || *again.LastElapsedNS != int64(2*time.Second) {
		t.Fatal("returned evidence aliases retained state", again)
	}
	tk.close()
	tk.close()
	if source.closes != 1 {
		t.Fatal("descriptor not closed exactly once", source.closes)
	}
}

func TestKernelIOEveryCounterRegressionIsPermanent(t *testing.T) {
	for _, change := range []func(*kernelIOCounters){func(c *kernelIOCounters) { c.RChar-- }, func(c *kernelIOCounters) { c.WChar-- }, func(c *kernelIOCounters) { c.SysCR-- }, func(c *kernelIOCounters) { c.SysCW-- }, func(c *kernelIOCounters) { c.ReadBytes-- }, func(c *kernelIOCounters) { c.WriteBytes-- }, func(c *kernelIOCounters) { c.CancelledWriteBytes-- }} {
		source := &fixtureIOSource{}
		value := kernelIOCounters{10, 10, 10, 10, 10, 10, 10}
		source.readFn = func(context.Context, <-chan struct{}) (kernelIOCounters, string) { return value, "" }
		tk, now := fixtureIOTracker(source, make(chan struct{}))
		if tk.sample(context.Background()).Status != "observed" {
			t.Fatal("baseline refused")
		}
		change(&value)
		*now = now.Add(time.Second)
		got := tk.sample(context.Background())
		if got.Status != "regressed" || got.Reason != "counter_regressed" || got.Counters != nil {
			t.Fatal(got)
		}
		value = kernelIOCounters{100, 100, 100, 100, 100, 100, 100}
		if retry := tk.sample(context.Background()); retry.Status != "regressed" || source.reads != 2 || source.closes != 1 {
			t.Fatal("regression restarted observation", retry, source)
		}
		saved := tk.summary()
		if saved.Status != "regressed" || saved.SampledDelta != nil || !saved.Gaps || saved.ObservedSamples != 1 || saved.LastCounters.ReadBytes != 10 {
			t.Fatal("regression exposed positive delta", saved)
		}
	}
}

func TestKernelIOReapNotificationPreventsAnyFurtherSourceCalls(t *testing.T) {
	done := make(chan struct{})
	source := &fixtureIOSource{readFn: func(context.Context, <-chan struct{}) (kernelIOCounters, string) {
		return kernelIOCounters{RChar: 123}, ""
	}}
	tk, _ := fixtureIOTracker(source, done)
	if tk.sample(context.Background()).Status != "observed" {
		t.Fatal("baseline refused")
	}
	close(done)
	for i := 0; i < 3; i++ {
		got := tk.sample(context.Background())
		if got.Status != "unavailable" || got.Reason != "worker_reaped" || got.Counters != nil || source.reads != 1 {
			t.Fatal("reaped source reopened/read", got, source.reads)
		}
	}
	// The dated sampled prefix remains evidence, with no claim that it covers
	// the exit gap or the child's full lifetime.
	saved := tk.summary()
	if saved.ObservedSamples != 1 || saved.RefusedSamples != 3 || saved.FullLifetime || saved.LastCounters.RChar != 123 {
		t.Fatal(saved)
	}
	tk.close()
	if source.closes != 1 {
		t.Fatal("retained descriptor leaked")
	}
}

func TestKernelIOReapDuringReadSuppressesPositivePublication(t *testing.T) {
	done := make(chan struct{})
	source := &fixtureIOSource{readFn: func(context.Context, <-chan struct{}) (kernelIOCounters, string) {
		close(done)
		return kernelIOCounters{RChar: 123}, ""
	}}
	tk, _ := fixtureIOTracker(source, done)
	got := tk.sample(context.Background())
	if got.Counters != nil || got.Reason != "worker_reaped" || tk.summary().ObservedSamples != 0 {
		t.Fatal("post-reap positive observation", got)
	}
	if tk.sample(context.Background()).Reason != "worker_reaped" || source.reads != 1 {
		t.Fatal("reaped source retried")
	}
	tk.close()
}

func TestKernelIOCancellationAndUnavailableAreTerminal(t *testing.T) {
	for _, mode := range []string{"before", "during", "deadline", "deadline_during", "failure", "private_failure", "nil"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &fixtureIOSource{readFn: func(ctx context.Context, _ <-chan struct{}) (kernelIOCounters, string) {
				if mode == "during" {
					cancel()
				}
				if mode == "deadline_during" {
					<-ctx.Done()
				}
				if mode == "failure" {
					return kernelIOCounters{}, "attribute_read_limit"
				}
				if mode == "private_failure" {
					return kernelIOCounters{}, "/proc/123/private-error"
				}
				return kernelIOCounters{RChar: 123}, ""
			}}
			tk, _ := fixtureIOTracker(source, make(chan struct{}))
			var sampleCtx context.Context = ctx
			if mode == "before" {
				cancel()
			}
			if mode == "deadline" {
				expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer stop()
				sampleCtx = expired
			}
			if mode == "deadline_during" {
				expired, stop := context.WithTimeout(context.Background(), 2*time.Millisecond)
				defer stop()
				sampleCtx = expired
			}
			if mode == "nil" {
				sampleCtx = nil
			}
			got := tk.sample(sampleCtx)
			if got.Status != "unavailable" || got.Counters != nil || tk.summary().ObservedSamples != 0 || source.closes != 1 {
				t.Fatal(got, tk.summary(), source)
			}
			if mode == "private_failure" && got.Reason != "observation_unavailable" {
				t.Fatal("private reason published", got)
			}
			if mode == "deadline_during" && got.Reason != "deadline_exceeded" {
				t.Fatal("expired observation published", got)
			}
			reads := source.reads
			if retry := tk.sample(context.Background()); retry.Status != "unavailable" || source.reads != reads {
				t.Fatal("failure rebound source", retry, source)
			}
		})
	}
}

func TestKernelIOElapsedRegressionBeforeAndAfterRead(t *testing.T) {
	for _, during := range []bool{false, true} {
		source := &fixtureIOSource{}
		tk, now := fixtureIOTracker(source, make(chan struct{}))
		source.readFn = func(context.Context, <-chan struct{}) (kernelIOCounters, string) { return kernelIOCounters{}, "" }
		if tk.sample(context.Background()).Status != "observed" {
			t.Fatal("baseline refused")
		}
		if during {
			source.readFn = func(context.Context, <-chan struct{}) (kernelIOCounters, string) {
				*now = now.Add(-time.Nanosecond)
				return kernelIOCounters{}, ""
			}
		} else {
			*now = now.Add(-time.Nanosecond)
		}
		got := tk.sample(context.Background())
		if got.Status != "regressed" || got.Reason != "elapsed_regressed" || got.Counters != nil || source.closes != 1 || tk.summary().SampledDelta != nil {
			t.Fatal(got)
		}
	}
}

func TestKernelIOUnsupportedAndConcurrentSnapshotsDoNotProbe(t *testing.T) {
	if runtime.GOOS == "darwin" {
		// A nil child would panic if this leaf attempted numeric PID binding.
		source, status, reason := openKernelIO(context.Background(), nil)
		if source != nil || status != "unsupported" || reason != "platform_unsupported" {
			t.Fatal("Darwin attempted a kernel probe", status, reason)
		}
	}
	tk := newKernelIOTracker(nil, make(chan struct{}), time.Now(), time.Now, "unsupported", "platform_unsupported")
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := tk.sample(context.Background())
			if got.Status != "unsupported" || got.Counters != nil {
				t.Error(got)
			}
			_ = tk.summary()
		}()
	}
	wg.Wait()
	if saved := tk.summary(); saved.RefusedSamples != 16 || saved.ObservedSamples != 0 || saved.FirstCounters != nil || saved.SampledDelta != nil {
		t.Fatal(saved)
	}
	tk.close()
}

func TestKernelIOMaximumSamplesJSONIsFiniteExactAndPrivate(t *testing.T) {
	maximum := kernelIOCounters{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	source := &fixtureIOSource{readFn: func(context.Context, <-chan struct{}) (kernelIOCounters, string) { return maximum, "" }}
	tk, now := fixtureIOTracker(source, make(chan struct{}))
	r := runner{base: "/private/generated/fixture", worker: &child{started: time.Now().Add(-time.Second), kernelIO: tk}}
	s := stablePausedView()
	for i := 0; i < maxSamples; i++ {
		*now = now.Add(time.Second)
		if err := r.sample(context.Background(), s); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.sample(context.Background(), s); err == nil || source.reads != maxSamples {
		t.Fatal("sample ceiling allowed more kernel calls", err, source.reads)
	}
	encoded, err := json.MarshalIndent(r.result, "", "  ")
	if err != nil || len(encoded) > 4*1024*1024 {
		t.Fatal("bounded JSON exceeded 4 MiB fixture ceiling", len(encoded), err)
	}
	for _, forbidden := range []string{"/proc/", "/private/", "pid", "start_ticks", "process_identity", "raw_error"} {
		if bytes.Contains(encoded, []byte(forbidden)) {
			t.Fatal("private binding exposed", forbidden)
		}
	}
	var decoded result
	if err := json.Unmarshal(encoded, &decoded); err != nil || len(decoded.Samples) != maxSamples || decoded.KernelIO.LastCounters.RChar != math.MaxUint64 || decoded.Samples[0].KernelIO.Counters.CancelledWriteBytes != math.MaxUint64 || decoded.PhysicalReadBytes != nil || decoded.SystemWakeups != nil || decoded.KernelIO.FullLifetime || decoded.KernelIO.AtomicSnapshot {
		t.Fatal("JSON lost integer or coverage semantics", err)
	}
	t.Logf("750 populated samples, 7 MaxUint64 counters each: %d pretty-JSON bytes", len(encoded))
	tk.close()
}
