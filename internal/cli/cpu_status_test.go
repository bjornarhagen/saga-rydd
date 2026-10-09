package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestWorkerCPUStatusQualifications(t *testing.T) {
	zero := int64(0)
	elapsed := int64(time.Second)
	for _, test := range []struct {
		name string
		cpu  *worker.CPUObservation
		want []string
		omit []string
	}{
		{name: "older worker", omit: []string{"CPU"}},
		{name: "not recorded", cpu: &worker.CPUObservation{Status: "not_recorded", TargetPercent: 1}, want: []string{"not_recorded", "not saved quotas", "do not enforce an hourly CPU limit"}, omit: []string{"Process CPU in completed work window", "Added CPU dispatch wait"}},
		{name: "unknown", cpu: &worker.CPUObservation{Status: "unknown", TargetPercent: 1, UnknownObservations: 2}, want: []string{"unknown windows this worker: 2", "CPU time is unknown", "No new CPU wait was calculated"}, omit: []string{"Process CPU in completed work window", "Added CPU dispatch wait"}},
		{name: "measured zero", cpu: &worker.CPUObservation{Status: "observed", TargetPercent: 1, WindowCPUNS: &zero, WindowElapsedNS: &elapsed}, want: []string{"Process CPU in completed work window: 0s", "elapsed window: 1s", "Added CPU dispatch wait: 0s", "1% of one core", "exclude children"}, omit: []string{"CPU time is unknown"}},
		{name: "capped", cpu: &worker.CPUObservation{Status: "observed", TargetPercent: 1, WindowCPUNS: &elapsed, WindowElapsedNS: &elapsed, BackoffNS: int64(time.Hour), BackoffCapped: true}, want: []string{"Added CPU dispatch wait: 1h0m0s", "wait capped: true", "do not enforce an hourly CPU limit"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var out bytes.Buffer
			printWorkerCPU(&out, test.cpu)
			for _, want := range test.want {
				if !strings.Contains(out.String(), want) {
					t.Fatal("missing CPU qualification", want, out.String())
				}
			}
			for _, omit := range test.omit {
				if strings.Contains(out.String(), omit) {
					t.Fatal("unsupported CPU observation claimed", omit, out.String())
				}
			}
		})
	}
	data, err := json.Marshal(capabilities())
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if !report.Features["process_cpu_accounting"] || !report.Features["cooperative_cpu_backoff"] || report.Features["cpu_limit"] || report.Features["power_controls"] {
		t.Fatal("CPU pacing was upgraded to unsupported resource enforcement", report.Features)
	}
}
