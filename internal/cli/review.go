package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/plans"
	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

const reviewInputLimit = 4096

// review never holds an inventory reader or transaction while waiting for input.
// Numbers address only the frozen records displayed on the current page.
func review(ctx context.Context, args []string, paths config.Paths, in io.Reader, out io.Writer) error {
	return reviewWithPlanSaver(ctx, args, paths, in, out, plans.Save)
}

func reviewWithPlanSaver(ctx context.Context, args []string, paths config.Paths, in io.Reader, out io.Writer, savePlan planSaver) error {
	f := flag.NewFlagSet("review", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	var path string
	f.StringVar(&path, "d", "", "exact manual scan root")
	f.StringVar(&path, "directory", "", "exact manual scan root")
	age := f.Int("min-age-days", state.FindingAgeDays, "minimum saved modification age")
	var hashMode, saveChoice hashSelectOption
	f.Var(&hashMode, "hashes", "guide an ephemeral keeper/copy preview from existing saved hashes")
	f.Var(&saveChoice, "save-choice", "with --hashes, offer an explicit unapproved historical choice save")
	if err := f.Parse(args); err != nil {
		return usageError{err}
	}
	seen := map[string]bool{}
	f.Visit(func(v *flag.Flag) { seen[v.Name] = true })
	if hashMode.set {
		if !hashMode.value || saveChoice.set && !saveChoice.value || f.NArg() != 0 || seen["d"] || seen["directory"] || seen["min-age-days"] {
			return usageError{errors.New("review --hashes accepts no directory, age or positional selection options; use the global data directory containing saved hashes")}
		}
		return reviewHashes(ctx, paths, in, out, saveChoice.value)
	}
	if saveChoice.set {
		return usageError{errors.New("review --save-choice requires --hashes; it is unavailable for candidate review")}
	}
	if f.NArg() != 0 || seen["d"] && seen["directory"] || *age < 1 || *age > state.MaxFindingAgeDays {
		return usageError{errors.New("review accepts -d PATH and --min-age-days 1–36500; do not combine directory aliases")}
	}
	root, err := directoryPath(path)
	if err != nil {
		return err
	}
	base := manualState(paths, root)
	display := &reviewOutput{writer: out}
	out = display
	input := in
	if file, ok := in.(*os.File); ok {
		input = reviewFileInput{ctx: ctx, file: file}
	}
	lines := bufio.NewReaderSize(input, reviewInputLimit+1)
	cursor := ""
	for {
		opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		page, err := loadDismissalReviewPage(opCtx, paths.StateDir, base, root, cursor, *age)
		cancel()
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return missingScanMessage(root, paths, err)
			}
			return fmt.Errorf("cannot freeze this review page; run review again after checking the saved scan: %w", err)
		}
		command := candidateReportCommand(paths, root, *age)
		if cursor != "" {
			command += " --cursor " + shellQuote(cursor)
		}
		printReviewPage(out, page, *age, command)
		for {
			fmt.Fprint(out, "\nChoose numbers (for example 1,3), next, refresh, or quit: ")
			if display.err != nil {
				return fmt.Errorf("cannot display review; nothing saved: %w", display.err)
			}
			line, err := readReviewLine(ctx, lines)
			if errors.Is(err, io.EOF) || err == nil && line == "quit" {
				return printReviewCancelled(out)
			}
			if err != nil {
				return err
			}
			if line == "next" {
				if page.NextCursor == "" {
					fmt.Fprintln(out, "\nNo more saved entries. Use refresh after an explicit scan.")
					continue
				}
				cursor = page.NextCursor
				break
			}
			if line == "refresh" {
				cursor = ""
				break
			}
			numbers, err := parseReviewNumbers(line)
			if err != nil {
				fmt.Fprintln(out, "\nUse unique numbers from this page, separated by spaces or commas. Ranges and all are not accepted.")
				continue
			}
			selection, err := selectReviewPage(page, numbers)
			if err != nil {
				fmt.Fprintln(out, "\nUse unique numbers from the displayed page. Nothing has been saved.")
				continue
			}
			fmt.Fprintln(out, "\nYOUR SELECTION")
			for i, finding := range selection.Evidence.Findings {
				printFinding(out, i+1, finding)
			}
			printWrapped(out, "This saves historical evidence only. Partial or unknown measurements stay qualified. Project activity, local edits and reinstall safety remain unconfirmed. Unselected folders stay unchanged; no keep policy is saved.", "")
			for {
				fmt.Fprint(out, "\nType save to save this unapproved selection, back, or quit: ")
				if display.err != nil {
					return fmt.Errorf("cannot display selection; nothing saved: %w", display.err)
				}
				line, err = readReviewLine(ctx, lines)
				if errors.Is(err, io.EOF) || err == nil && line == "quit" {
					return printReviewCancelled(out)
				}
				if err != nil {
					return err
				}
				if line == "back" {
					break
				}
				if line != "save" {
					fmt.Fprintln(out, "\nNothing saved. Enter save, back, or quit.")
					continue
				}
				// Recheck the frozen subset, never recapture it using reusable IDs.
				opCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
				check, err := checkReviewSelection(opCtx, base, selection)
				cancel()
				if err != nil {
					return err
				}
				if check.Status == "changed" {
					return errors.New("saved inventory changed during review; nothing saved. Run review again to see a new page")
				}
				if check.Status == "unverifiable" {
					printWrapped(out, "Some saved evidence is incomplete or unknown. Saving it as historical, unapproved evidence cannot establish a current match.", "")
				}
				if display.err != nil {
					return fmt.Errorf("cannot display evidence qualification; nothing saved: %w", display.err)
				}
				opCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
				saved, err := savePlan(opCtx, paths.StateDir, selection)
				cancel()
				if err == nil && ctx.Err() != nil {
					return fmt.Errorf("%s; completion reply was canceled: %w", planReplyMessage(saved, paths), ctx.Err())
				}
				if err != nil {
					result, publicationErr := planPublicationResult(saved, err, paths)
					if candidate, ok := result.(planPublicationCandidate); ok {
						printPlanCandidate(out, candidate)
					}
					return publicationErr
				}
				printResultBanner(out, "SELECTION SAVED - CLEANUP UNAVAILABLE")
				fmt.Fprintf(out, "Saved plan: %s\n", saved.ID)
				printWrapped(out, "No files were moved or deleted. No cleanup consent was recorded. A later inventory change can make this historical selection outdated.", "")
				fmt.Fprintf(out, "\nReopen the selection:\n  %s plan --show %s\n", commandPrefix(paths), saved.ID)
				fmt.Fprintf(out, "\nStart another explicit scan when needed:\n  %s scan -d %s\n", commandPrefix(paths), shellQuote(root))
				fmt.Fprintf(out, "\nThen review saved candidates:\n  %s review -d %s --min-age-days %d\n", commandPrefix(paths), shellQuote(root), *age)
				if display.err != nil {
					return fmt.Errorf("%s; completion output failed: %w", planReplyMessage(saved, paths), display.err)
				}
				if ctx.Err() != nil {
					return fmt.Errorf("%s; completion reply was canceled: %w", planReplyMessage(saved, paths), ctx.Err())
				}
				return nil
			}
		}
	}
}

type reviewOutput struct {
	writer io.Writer
	err    error
}

func (w *reviewOutput) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.writer.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

func checkReviewSelection(ctx context.Context, base string, selection state.SelectionSnapshot) (state.SelectionCheck, error) {
	r, err := state.OpenReader(ctx, base)
	if err != nil {
		return state.SelectionCheck{}, err
	}
	defer r.Close()
	return r.CheckSelection(ctx, selection)
}

func printReviewPage(out io.Writer, page reviewPage, age int, command string) {
	printResultBanner(out, "REVIEW SAVED CANDIDATES - CLEANUP UNAVAILABLE")
	printWrapped(out, fmt.Sprintf("%d candidate(s) on this page. Both saved modification dates must be at least %d days old. Age does not prove inactivity or safe deletion.", len(page.Evidence.Findings), age), "")
	printWrapped(out, "The numbered rows are frozen saved evidence. Current files have not been checked. Sizes are not estimates of space you can free. Do not add overlapping sizes.", "")
	for i, finding := range page.Evidence.Findings {
		printFinding(out, i+1, finding)
	}
	printDismissalNotes(out, page.Evidence)
	if page.NextCursor != "" {
		printWrapped(out, "More saved entries remain. next replaces this page and resets its numbers; selections do not carry forward.", "")
	} else {
		printWrapped(out, "End of saved entries. This does not prove the scan is complete.", "")
	}
	if len(page.Evidence.Findings) == 0 {
		printWrapped(out, "No candidates available for review on this page. An empty page can still have more saved entries.", "")
		printFindingPageSummary(out, page.Evidence)
		fmt.Fprintf(out, "\nView this saved-entry page again:\n  %s\n", command)
		printWrapped(out, "A later scan can change these results.", "")
	}
	printWrapped(out, "Unselected folders stay unchanged. This does not save a keep decision or change future reports.", "")
}

func printReviewCancelled(out io.Writer) error {
	_, err := fmt.Fprintln(out, "\nReview ended. Nothing saved. All folders stay unchanged.")
	return err
}

func parseReviewNumbers(line string) ([]int, error) {
	fields := strings.Fields(strings.ReplaceAll(line, ",", " "))
	if len(fields) == 0 || len(fields) > state.PreviewTargetLimit {
		return nil, errors.New("choose 1–20 numbers")
	}
	numbers := make([]int, len(fields))
	for i, field := range fields {
		n, err := strconv.Atoi(field)
		if err != nil || strconv.Itoa(n) != field || n < 1 || n > state.PreviewTargetLimit {
			return nil, errors.New("use exact row numbers")
		}
		numbers[i] = n
	}
	return numbers, nil
}

func readReviewLine(ctx context.Context, reader *bufio.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > reviewInputLimit {
		return "", errors.New("review input exceeds 4096 bytes; nothing saved")
	}
	if err != nil {
		// A command requires a completed line. EOF never confirms a save.
		return "", err
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	return strings.TrimSpace(string(line)), nil
}

// Polling bounds cancellation latency without changing inherited stdin blocking mode,
// closing the caller's descriptor or leaving a goroutine blocked in Read.
type reviewFileInput struct {
	ctx  context.Context
	file *os.File
}

func (r reviewFileInput) Read(p []byte) (int, error) {
	conn, err := r.file.SyscallConn()
	if err != nil {
		return 0, err
	}
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		var n int
		var readErr error
		ready := false
		err = conn.Control(func(fd uintptr) {
			if fd > 1<<31-1 {
				readErr = os.ErrClosed
				return
			}
			poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			_, readErr = unix.Poll(poll, 100)
			if readErr != nil {
				return
			}
			if readErr = r.ctx.Err(); readErr != nil {
				return
			}
			if poll[0].Revents&unix.POLLNVAL != 0 {
				readErr = os.ErrClosed
				return
			}
			ready = poll[0].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) != 0
			if ready {
				n, readErr = unix.Read(int(fd), p)
			}
		})
		if err != nil {
			return 0, err
		}
		if errors.Is(readErr, unix.EINTR) || errors.Is(readErr, unix.EAGAIN) {
			continue
		}
		if readErr != nil {
			return n, readErr
		}
		if !ready {
			continue
		}
		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}
}
