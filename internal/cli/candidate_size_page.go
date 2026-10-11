package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

// This selector is strict and once-only. Absence or explicit false keeps the
// original candidate report path and its serialized evidence unchanged.
type candidateSizePageMode struct {
	value bool
	set   bool
}

func (v *candidateSizePageMode) String() string   { return strconv.FormatBool(v.value) }
func (v *candidateSizePageMode) IsBoolFlag() bool { return true }
func (v *candidateSizePageMode) Set(value string) error {
	if v.set {
		return errors.New("--page-size-summary cannot be repeated")
	}
	if value != "true" && value != "false" {
		return errors.New("--page-size-summary accepts only true or false")
	}
	v.value, v.set = value == "true", true
	return nil
}

// The interface is local to this reader. It permits generated close-failure
// fixtures without a production hook or a second inventory read.
type candidateSizePageReader interface {
	NodeModulesSizePage(context.Context, string, int) (state.SelectionSnapshot, *state.CandidateSizePage, error)
	Close() error
}

func readCandidateSizePage(ctx context.Context, s candidateSizePageReader, paths config.Paths, directory, cursor string, minimumAge int, includeDismissed bool) (reportResult, error) {
	page, receipt, readErr := s.NodeModulesSizePage(ctx, cursor, minimumAge)
	closeErr := s.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		if errors.Is(err, state.ErrReportCursor) {
			err = usageError{err}
		}
		return reportResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return reportResult{}, err
	}
	var err error
	if directory != "" {
		page, err = filterDismissalPage(ctx, paths.StateDir, directory, page, includeDismissed)
		if err != nil {
			return reportResult{}, err
		}
	}
	ids := make([]string, len(page.Evidence.Findings))
	for i, finding := range page.Evidence.Findings {
		ids[i] = finding.ID
	}
	if receipt == nil {
		return reportResult{}, errors.New("candidate page size receipt is missing")
	}
	summary, err := receipt.SummarizeVisible(ids)
	if err != nil {
		return reportResult{}, err
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return reportResult{}, err
	}
	if len(encoded) > 4096 {
		return reportResult{}, errors.New("candidate page size summary exceeds 4096 bytes")
	}
	if err := ctx.Err(); err != nil {
		return reportResult{}, err
	}
	command := candidateReportCommand(paths, directory, minimumAge) + " --page-size-summary"
	if includeDismissed {
		command += " --include-dismissed"
	}
	c := page.Evidence
	return reportResult{FileReport: state.FileReport{Candidates: &c, GeneratedAt: c.GeneratedAt, Source: c.Source, Files: []state.ReportFile{}, Roots: []state.ReportRoot{}, Notes: c.Notes}, CandidateSizeSummary: &summary, candidateCommand: command}, nil
}

func printCandidateSizeSummary(out io.Writer, summary state.CandidatePageSizeSummary) {
	printWrapped(out, "Historical sizes on this page", "")
	printField(out, "Visible candidate folders", humanCount(summary.DisplayedFolders))
	printField(out, "Historical logical bytes by file path", candidateSizeAmount(summary.LogicalPathBytes, summary.LogicalUnavailableReason))
	printField(out, "Historical measured file paths", candidateSizeCount(summary.MeasuredFilePaths, summary.FilePathsUnavailableReason))
	printField(out, "Historical within-folder allocated bytes", candidateSizeAmount(summary.WithinFolderAllocatedBytes, summary.AllocatedUnavailableReason))
	if summary.Status == "historical_measured_portion" {
		printWrapped(out, "These totals describe saved measured portions. Partial, stale or truncated evidence does not bound whole folders or their current contents.", "")
	}
	printField(out, "Partial folders", humanCount(summary.PartialFolders))
	printField(out, "Stale folders", humanCount(summary.StaleFolders))
	printField(out, "Unknown folders", humanCount(summary.UnknownFolders))
	printField(out, "Truncated folders", humanCount(summary.TruncatedFolders))
	printField(out, "Overlapping folder pairs", humanCount(summary.OverlapPairs))
	printField(out, "Unknown directory identities", humanCount(summary.UnknownDirectoryIdentities))
	printField(out, "Known directory identity alias pairs", humanCount(summary.KnownDirectoryAliasPairs))
	printWrapped(out, "File paths are the logical denominator; distinct hardlink names can count more than once. Cross-folder object identities and private physical extents have not been verified. Space you could free is unknown; approval and cleanup are unavailable.", "")
	printWrapped(out, "This page uses one saved inventory snapshot. Dismissals use a separate saved snapshot. Do not add pages or categories; a later page is a new observation.", "")
	fmt.Fprintln(out)
}

func candidateSizeAmount(value *int64, reason string) string {
	if value == nil {
		return candidateSizeUnavailable(reason)
	}
	return fmt.Sprintf("%s (%d bytes)", humanBytes(*value), *value)
}

func candidateSizeCount(value *int64, reason string) string {
	if value == nil {
		return candidateSizeUnavailable(reason)
	}
	return humanCount(*value)
}

func candidateSizeUnavailable(reason string) string {
	switch reason {
	case "overlap_detected":
		return "unknown (folder paths overlap)"
	case "measurement_unavailable":
		return "unknown (a saved size measurement is not available)"
	case "aggregate_overflow":
		return "unknown (the total exceeds the supported range)"
	case "single_folder_incomplete_or_stale":
		return "unknown (saved folder evidence is incomplete or stale)"
	case "single_folder_identity_unavailable":
		return "unknown (saved folder or file identities are not available)"
	case "cross_folder_identity_evidence_unavailable":
		return "unknown (file identities across folders are not available)"
	default:
		return "unknown"
	}
}
