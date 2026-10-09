package cli

import (
	"fmt"
	"io"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerAdaptiveRevisits(out io.Writer, policy *worker.AdaptiveRevisitSnapshot) {
	if policy == nil {
		return
	}
	if !policy.PolicyEnabled {
		fmt.Fprintln(out, "Adaptive revisits: DISABLED")
		return
	}
	fmt.Fprintf(out, "Adaptive revisits: ENABLED\nSaved scheduling summary at: %s\nTracked roots: %d; saved passes in progress: %d\nSaved interval profile: daily=%d; weekly=%d\n", policy.CachedAt.UTC().Format(time.RFC3339Nano), policy.TrackedRoots, policy.ActiveEpochs, policy.DailyRoots, policy.WeeklyRoots)
	fmt.Fprintf(out, "Saved passes with observed changes: %d; uncertain evidence: %d\nRoots with two completed passes without an observed change: %d\n", policy.ChangedEpochs, policy.UnknownEpochs, policy.StableRoots)
	printWrapped(out, "This is cached historical metadata scheduling evidence. Status does not inspect source folders. After saved work drains, changed or uncertain passes use a daily interval; two completed passes without an observed change can use seven days.", "")
	printWrapped(out, "Saved intervals do not prove current contents, project inactivity or cleanup safety. Pending work and all existing resource gates remain in force.", "")
}
