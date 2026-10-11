package inventory

import (
	"context"
	"errors"
	"time"
)

// ObserveFileSamplesWithUsage performs the same bounded observation as
// ObserveFileSamples and returns issued-read usage on every ordinary return.
// RequestedBytes charges each real read request, including failed or short reads;
// ReadBytes counts returned source bytes. Elapsed includes validation and cleanup.
// A call-level error publishes no report or digest, but preserves attempted usage.
// The five-second deadline remains cooperative. Usage is a fixed per-call value,
// not a durable reservation, a daily quota ledger or permission to remove data.
func (s *Scanner) ObserveFileSamplesWithUsage(ctx context.Context, targets []SavedFileTarget) (FileSampleReport, FileReadUsage, error) {
	return s.observeFileSamplesWithUsage(ctx, targets, fileSampleHooks{})
}

func (s *Scanner) observeFileSamplesWithUsage(ctx context.Context, targets []SavedFileTarget, hooks fileSampleHooks) (report FileSampleReport, usage FileReadUsage, err error) {
	started := time.Now()
	var budget fileSampleBudget
	defer func() {
		usage.RequestedBytes, usage.ReadBytes = budget.requested, budget.read
		usage.Elapsed = time.Since(started)
	}()
	if err := boundedSampleRequest(targets); err != nil {
		return FileSampleReport{}, usage, err
	}
	ctx, cancel := context.WithTimeout(ctx, fileSampleTimeout)
	defer cancel()
	defer func() {
		if failure := ctx.Err(); failure != nil {
			report, err = FileSampleReport{}, failure
		} else if s.closed.Load() {
			report, err = FileSampleReport{}, errors.New("scanner closed")
		}
	}()
	if err := ctx.Err(); err != nil {
		return FileSampleReport{}, usage, err
	}
	if s.closed.Load() {
		return FileSampleReport{}, usage, errors.New("scanner closed")
	}
	r := FileSampleReport{Status: "samples_observed", Source: "live_file_samples", EvidenceSource: "caller_supplied_saved_records", Contract: FileSampleContract, Targets: []FileSampleResult{}}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return FileSampleReport{}, usage, err
		}
		item := FileSampleResult{Target: target, Status: "samples_observed", Message: "Selected sample ranges were read and the held path metadata was rechecked. Equal samples do not prove equal file contents.", Ranges: []FileSampleRange{}}
		// The display path may have lost invalid UTF-8 in a JSON round trip.
		// Only PathBytes identifies the selected filesystem name.
		item.Target.File.Path = string(target.File.PathBytes)
		before := budget
		err := validateSampleTarget(target)
		if err == nil {
			err = conflictingSampleIdentity(target, targets)
		}
		if err == nil {
			item.RepeatedSavedIdentity = repeatedSampleIdentity(target, targets)
			err = s.sampleFile(ctx, target, &budget, &item, hooks)
		}
		if ctx.Err() != nil {
			return FileSampleReport{}, usage, ctx.Err()
		}
		item.RequestedBytes, item.ReadBytes = budget.requested-before.requested, budget.read-before.read
		if err != nil {
			item.Status, item.SHA256, r.Status = "blocked", "", "blocked"
			var failure liveError
			if errors.As(err, &failure) {
				item.Code, item.Message = failure.code, failure.message
			} else {
				item.Code, item.Message = "path_unavailable", "The selected path is missing, inaccessible, a symlink or otherwise unsupported. No sample digest is available."
			}
		}
		r.Targets = append(r.Targets, item)
	}
	if err := ctx.Err(); err != nil {
		return FileSampleReport{}, usage, err
	}
	if s.closed.Load() {
		return FileSampleReport{}, usage, errors.New("scanner closed")
	}
	r.RequestedBytes, r.ReadBytes, r.CheckedAt = budget.requested, budget.read, time.Now().UTC()
	if hooks.beforeReturn != nil {
		hooks.beforeReturn()
	}
	return r, usage, nil
}
