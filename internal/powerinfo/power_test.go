package powerinfo

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fixtureSource struct {
	entries                                         []string
	values                                          map[string]map[string][]byte
	providerErrors                                  map[string]error
	namesErr                                        error
	checkFn                                         func() error
	readFn                                          func(string, string)
	readCtxFn                                       func(context.Context, string, string) error
	attributeErrors                                 map[string]error
	closeFn                                         func()
	providersOpened, providersClosed, closed, reads int
	limit                                           int
}

type fixtureSupply struct {
	source *fixtureSource
	name   string
}

func (s *fixtureSource) names(_ context.Context, limit int) ([]string, error) {
	s.limit = limit
	return append([]string(nil), s.entries...), s.namesErr
}
func (s *fixtureSource) provider(_ context.Context, name string) (supply, error) {
	if err := s.providerErrors[name]; err != nil {
		return nil, err
	}
	s.providersOpened++
	return &fixtureSupply{source: s, name: name}, nil
}
func (s *fixtureSource) check(context.Context) error {
	if s.checkFn != nil {
		return s.checkFn()
	}
	return nil
}
func (s *fixtureSource) close() error {
	s.closed++
	if s.closeFn != nil {
		s.closeFn()
	}
	return nil
}
func (p *fixtureSupply) attribute(ctx context.Context, name string) ([]byte, error) {
	p.source.reads++
	if p.source.readFn != nil {
		p.source.readFn(p.name, name)
	}
	if p.source.readCtxFn != nil {
		if err := p.source.readCtxFn(ctx, p.name, name); err != nil {
			return nil, err
		}
	}
	v, ok := p.source.values[p.name][name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return append([]byte(nil), v...), p.source.attributeErrors[p.name+"/"+name]
}
func (p *fixtureSupply) close() error { p.source.providersClosed++; return nil }

func battery(status string) map[string][]byte {
	return map[string][]byte{"type": []byte("Battery\n"), "scope": []byte("System\n"), "present": []byte("1\n"), "status": []byte(status + "\n")}
}
func fixtureObserve(ctx context.Context, s *fixtureSource) (Observation, error) {
	return observe(ctx, "linux", func(context.Context) (source, error) { return s, nil }, func() time.Time { return time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) })
}
func assertQualified(t *testing.T, o Observation) {
	t.Helper()
	if o.Contract != Contract || o.Profile != LinuxProfile || !o.SequentialObservations || o.NamespaceAuthenticated || o.PhysicalPowerVerified {
		t.Fatalf("incorrect qualification: %+v", o)
	}
	if o.SystemBatteryDischargingObserved != nil && !*o.SystemBatteryDischargingObserved {
		t.Fatal("false power-state value fabricated")
	}
	if o.AttributeAttempts > MaxAttributeAttempts || o.ReturnedAttributeBytes > MaxReturnedAttributeBytes || o.ProvidersProcessed > MaxSupplyEntries {
		t.Fatalf("bounds exceeded: %+v", o)
	}
}

func TestPowerObservationWitnessAndUnknownCoverage(t *testing.T) {
	for _, test := range []struct {
		name              string
		entries           []string
		values            map[string]map[string][]byte
		witness, complete bool
		ambiguous         int
	}{
		{"battery", []string{"battery"}, map[string]map[string][]byte{"battery": battery("Discharging")}, true, true, 0},
		{"AC_coexists", []string{"ac", "battery"}, map[string]map[string][]byte{"battery": battery("Discharging"), "ac": {"type": []byte("USB_PD\n"), "scope": []byte("System\n"), "online": []byte("2\n")}}, true, true, 0},
		{"unknown_does_not_erase_witness", []string{"unknown", "battery"}, map[string]map[string][]byte{"battery": battery("Discharging"), "unknown": {"type": []byte("Battery\n"), "scope": []byte("Unknown\n")}}, true, false, 1},
		{"Device_excluded", []string{"device"}, map[string]map[string][]byte{"device": {"scope": []byte("Device\n"), "status": []byte("Discharging\n")}}, false, true, 0},
		{"charging", []string{"battery"}, map[string]map[string][]byte{"battery": battery("Charging")}, false, true, 0},
		{"empty", nil, nil, false, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &fixtureSource{entries: test.entries, values: test.values}
			o, err := fixtureObserve(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			assertQualified(t, o)
			if (o.SystemBatteryDischargingObserved != nil) != test.witness || o.CoverageComplete != test.complete || o.AmbiguousProviders != test.ambiguous {
				t.Fatalf("wrong classification: %+v", o)
			}
			if o.SupplyEntriesObserved == nil || *o.SupplyEntriesObserved != len(test.entries) || s.closed != 1 || s.providersOpened != s.providersClosed || s.limit != 33 {
				t.Fatalf("scope/cleanup mismatch: %+v %+v", o, s)
			}
		})
	}
}

func TestPowerObservationExactTokensAndRequiredEvidence(t *testing.T) {
	for _, test := range []struct {
		name, field string
		value       []byte
		remove      bool
	}{
		{"missing_present", "present", nil, true}, {"missing_scope", "scope", nil, true}, {"unknown_scope", "scope", []byte("Unknown\n"), false},
		{"numeric_status", "status", []byte("17\n"), false}, {"status_CR", "status", []byte("Discharging\r\n"), false},
		{"status_tab", "status", []byte("Discharging\t"), false}, {"status_padding", "status", []byte(" Discharging\n"), false},
		{"double_LF", "status", []byte("Discharging\n\n"), false}, {"invalid_utf8", "status", []byte{0xff}, false},
		{"absent_battery", "present", []byte("0\n"), false}, {"unknown_presence", "present", []byte("2\n"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := battery("Discharging")
			if test.remove {
				delete(v, test.field)
			} else {
				v[test.field] = test.value
			}
			s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": v}}
			o, err := fixtureObserve(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			assertQualified(t, o)
			if o.SystemBatteryDischargingObserved != nil {
				t.Fatalf("invalid evidence yielded witness: %+v", o)
			}
		})
	}
	// No trailing LF is also a valid exact token; no broad trim is needed.
	v := battery("Discharging")
	v["status"] = []byte("Discharging")
	o, err := fixtureObserve(context.Background(), &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": v}})
	if err != nil || o.SystemBatteryDischargingObserved == nil {
		t.Fatalf("exact no-LF token refused: %+v %v", o, err)
	}
}

func TestPowerObservationProviderAndAttributeBounds(t *testing.T) {
	for _, n := range []int{32, 33, 34} {
		s := &fixtureSource{values: make(map[string]map[string][]byte)}
		for i := 0; i < n; i++ {
			name := strings.Repeat("b", i+1)
			s.entries = append(s.entries, name)
			s.values[name] = battery("Discharging")
		}
		o, err := fixtureObserve(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		assertQualified(t, o)
		if o.ProvidersProcessed != 32 || *o.SupplyEntriesObserved != min(n, 33) || o.CoverageComplete != (n == 32) || s.providersOpened != 32 {
			t.Fatalf("overflow not bounded: %+v", o)
		}
	}
	s := &fixtureSource{values: map[string]map[string][]byte{"b": {"value": []byte(strings.Repeat("a", 65))}}}
	p := &fixtureSupply{source: s, name: "b"}
	o := Observation{}
	for i := 0; i < MaxAttributeAttempts; i++ {
		if _, err := readToken(context.Background(), p, "value", &o); err == nil {
			t.Fatal("65-byte attribute accepted")
		}
	}
	if _, err := readToken(context.Background(), p, "value", &o); err == nil {
		t.Fatal("probe limit not enforced")
	}
	if s.reads != 160 || o.AttributeAttempts != 160 || o.ReturnedAttributeBytes != 10400 {
		t.Fatalf("wrong probe/byte counters: %+v reads=%d", o, s.reads)
	}
	s.values["b"]["value"] = []byte(strings.Repeat("a", 64))
	o = Observation{}
	if value, err := readToken(context.Background(), p, "value", &o); err != nil || len(value) != 64 {
		t.Fatalf("64-byte field refused: %q %v", value, err)
	}
}

func TestPowerObservationFailuresAndFinalScopeVeto(t *testing.T) {
	s := &fixtureSource{entries: []string{"broken", "b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, providerErrors: map[string]error{"broken": errors.New("private provider detail")}}
	o, err := fixtureObserve(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if o.SystemBatteryDischargingObserved == nil || o.CoverageComplete || o.AmbiguousProviders != 1 || strings.Contains(o.Reason, "private") {
		t.Fatalf("provider failure lost witness/qualification: %+v", o)
	}
	s = &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, checkFn: func() error { return os.ErrNotExist }}
	o, err = fixtureObserve(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if o.SystemBatteryDischargingObserved != nil || o.Status != "unknown" || o.Reason != "scope_changed" || o.CoverageComplete {
		t.Fatalf("final scope failure published witness: %+v", o)
	}
	s = &fixtureSource{namesErr: errors.New("private name")}
	o, err = fixtureObserve(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if o.SupplyEntriesObserved != nil || o.SystemBatteryDischargingObserved != nil || o.Reason != "enumeration_unavailable" || s.closed != 1 {
		t.Fatalf("unavailable enumeration became empty success: %+v", o)
	}
}

func TestPowerObservationCancellationAndBlockedProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opened := 0
	o, err := observe(ctx, "linux", func(context.Context) (source, error) { opened++; return nil, nil }, time.Now)
	if !errors.Is(err, context.Canceled) || opened != 0 || !reflect.DeepEqual(o, Observation{}) {
		t.Fatalf("early cancellation: %+v %v opened=%d", o, err, opened)
	}
	for _, stage := range []string{"attribute", "final_check", "close"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}}
			if stage == "attribute" {
				s.readFn = func(_, name string) {
					if name == "status" {
						cancel()
					}
				}
			}
			if stage == "final_check" {
				s.checkFn = func() error { cancel(); return nil }
			}
			if stage == "close" {
				s.closeFn = cancel
			}
			o, err := fixtureObserve(ctx, s)
			if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(o, Observation{}) || s.closed != 1 || s.providersOpened != s.providersClosed {
				t.Fatalf("late cancellation published evidence/leaked: %+v %v %+v", o, err, s)
			}
		})
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, readFn: func(_, name string) {
		if name == "status" {
			close(entered)
			<-release
		}
	}}
	var got Observation
	var gotErr error
	go func() { defer close(done); got, gotErr = fixtureObserve(ctx, s) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("provider not entered")
	}
	<-ctx.Done()
	select {
	case <-done:
		t.Fatal("blocked callback incorrectly claimed hard cancellation")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("released provider did not finish")
	}
	if !errors.Is(gotErr, context.DeadlineExceeded) || !reflect.DeepEqual(got, Observation{}) || s.closed != 1 || s.providersOpened != s.providersClosed {
		t.Fatalf("blocked cancellation: %+v %v %+v", got, gotErr, s)
	}
}

func TestPowerObservationIndependentWallDeadlineAndRollback(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta time.Duration
		want  error
	}{{"wall_forward_exact", ObservationTimeout, context.DeadlineExceeded}, {"wall_forward_gap", time.Hour, context.DeadlineExceeded}, {"wall_rollback", -time.Second, ErrWallClockRollback}} {
		t.Run(test.name, func(t *testing.T) {
			wall := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, readFn: func(_, name string) {
				if name == "status" {
					wall = wall.Add(test.delta)
				}
			}}
			o, err := observe(context.Background(), "linux", func(context.Context) (source, error) { return s, nil }, func() time.Time { return wall })
			if !errors.Is(err, test.want) || !reflect.DeepEqual(o, Observation{}) || s.closed != 1 || s.providersOpened != s.providersClosed {
				t.Fatalf("wall seam published witness: %+v %v %+v", o, err, s)
			}
		})
	}
	if _, err := Observe(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context: %v", err)
	}
}

func TestPowerObservationFirstWallRefusalSurvivesRestoredClock(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta time.Duration
		want  error
	}{{"rollback", -time.Second, ErrWallClockRollback}, {"deadline", ObservationTimeout, context.DeadlineExceeded}} {
		t.Run(test.name, func(t *testing.T) {
			start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			wall := start
			s := &fixtureSource{entries: []string{"first", "later"}, values: map[string]map[string][]byte{"first": battery("Discharging"), "later": battery("Discharging")}}
			s.readCtxFn = func(ctx context.Context, provider, field string) error {
				if provider == "first" && field == "type" {
					wall = start.Add(test.delta)
					err := canceled(ctx)
					wall = start
					return err
				}
				return nil
			}
			o, err := observe(context.Background(), "linux", func(context.Context) (source, error) { return s, nil }, func() time.Time { return wall })
			if !errors.Is(err, test.want) || !reflect.DeepEqual(o, Observation{}) || s.providersOpened != 1 || s.providersClosed != 1 || s.reads != 1 || s.closed != 1 {
				t.Fatalf("restored clock cleared refusal: %+v %v %+v", o, err, s)
			}
		})
	}
}

func TestPowerObservationFailedAttributesRetainAttemptAndPartialByteCounts(t *testing.T) {
	s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, attributeErrors: map[string]error{"b/status": errors.New("private provider failure")}}
	s.values["b"]["status"] = []byte("Dis")
	o, err := fixtureObserve(context.Background(), s)
	if err != nil || o.SystemBatteryDischargingObserved != nil || o.CoverageComplete || o.AttributeAttempts != 4 || o.ReturnedAttributeBytes != 20 || o.AmbiguousProviders != 1 || strings.Contains(o.Reason, "private") {
		t.Fatalf("failed attribute accounting: %+v %v", o, err)
	}
}

func TestPowerObservationFinalStampParticipatesInWallGuard(t *testing.T) {
	for _, mode := range []string{"supported", "unsupported", "open_error"} {
		for _, delta := range []time.Duration{ObservationTimeout + time.Second, 3 * time.Second} {
			t.Run(mode+"/"+delta.String(), func(t *testing.T) {
				start := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
				finishing, calls := false, 0
				now := func() time.Time {
					if !finishing {
						return start
					}
					calls++
					if calls == 2 {
						return start.Add(delta)
					}
					if calls > 2 {
						return start.Add(time.Second)
					}
					return start
				}
				s := &fixtureSource{entries: []string{"b"}, values: map[string]map[string][]byte{"b": battery("Discharging")}, closeFn: func() { finishing = true }}
				platform := "linux"
				open := func(context.Context) (source, error) { return s, nil }
				if mode != "supported" {
					if mode == "unsupported" {
						platform = "darwin"
					}
					open = func(context.Context) (source, error) { finishing = true; return nil, sourceError{mode} }
				}
				o, err := observe(context.Background(), platform, open, now)
				want := ErrWallClockRollback
				if delta >= ObservationTimeout {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) || !reflect.DeepEqual(o, Observation{}) {
					t.Fatalf("invalid final stamp escaped guard: %+v %v calls=%d", o, err, calls)
				}
				if mode == "supported" && (s.closed != 1 || s.providersOpened != s.providersClosed) {
					t.Fatalf("final stamp refusal leaked scopes: %+v", s)
				}
			})
		}
	}
}
