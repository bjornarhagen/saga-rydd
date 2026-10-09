package cli

import (
	"bytes"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func TestWorkerPriorityStatusKeepsRequestsSeparateFromObservations(t *testing.T) {
	accepted, refused, zero := true, false, 0
	r := worker.ThreadPriorityObservation{Platform: "linux", CPU: &worker.PrioritySetting{Attempted: true, Accepted: &accepted}, IO: &worker.PrioritySetting{Attempted: true, Accepted: &refused, Observed: &zero}}
	var out bytes.Buffer
	printWorkerPriority(&out, nil)
	if out.Len() != 0 {
		t.Fatal("missing older-worker field invented an observation", out.String())
	}
	printWorkerPriority(&out, &r)
	lines := strings.Split(out.String(), "\n")
	for _, line := range lines {
		if strings.Contains(line, "CPU nice") && (!strings.Contains(line, "Accepted") || !strings.Contains(line, "Unknown")) {
			t.Fatal("accepted request invented a readback", line)
		}
		if strings.Contains(line, "I/O priority") && (!strings.Contains(line, "Refused") || !strings.HasSuffix(line, "0")) {
			t.Fatal("zero readback lost its distinct refused request", line)
		}
	}
	for _, want := range []string{"sequential observations", "one experimental", "inherit settings", "process-wide settings\nare unverified", "does not prove effective scheduling", "power savings", "resource budgets still apply"} {
		if !strings.Contains(out.String(), want) {
			t.Fatal("priority report lost scope qualification", want, out.String())
		}
	}
}
