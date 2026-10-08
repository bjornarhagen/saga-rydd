package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const hashReviewPreviewBanner = "HISTORICAL KEEPER PREVIEW - CURRENT FILES NOT CHECKED"

type hashGuidedReviewFixture struct {
	*hashProposalCLIFixture
	proposal inventory.HashProposal
	consent  inventory.HashReadConsent
	contents []byte
}

func newHashGuidedReviewFixture(t *testing.T, completed int) hashGuidedReviewFixture {
	t.Helper()
	f, p, contents := hashKeeperFourCLIFixture(t)
	c := hashKeeperApprove(t, f, p)
	for i := 1; i <= completed; i++ {
		hashKeeperRun(t, f, c, fmt.Sprint(i))
	}
	return hashGuidedReviewFixture{f, p, c, contents}
}

func runHashGuidedReview(t *testing.T, ctx context.Context, base string, input io.Reader, options ...string) (int, string, string) {
	t.Helper()
	args := append([]string{"--data-dir", base, "review", "--hashes"}, options...)
	var out, stderr bytes.Buffer
	code := runWithInput(ctx, args, input, &out, &stderr)
	return code, out.String(), stderr.String()
}

func hashReviewExactCommand(t *testing.T, base, selection, keeper, copies string) string {
	t.Helper()
	paths, err := config.ResolvePaths(base)
	if err != nil {
		t.Fatal(err)
	}
	return commandPrefix(paths) + " hashes --preview " + selection + " --keeper " + keeper + " " + copies
}

func hashReviewFinalPreview(t *testing.T, output string) string {
	t.Helper()
	index := strings.LastIndex(output, hashReviewPreviewBanner)
	if index < 0 {
		t.Fatal("guided review did not return an explicit historical preview", output)
	}
	return output[index:]
}

func hashReviewNoSavedDecision(t *testing.T, base string) {
	t.Helper()
	for _, name := range []string{"plans", "journal"} {
		if _, err := os.Lstat(filepath.Join(base, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("hash review initialized decision/action storage", name, err)
		}
	}
}

func TestGuidedHashReviewExplicitRolesOfflineAndCopyOrder(t *testing.T) {
	ctx := context.Background()
	f := newHashGuidedReviewFixture(t, 4)
	code, raw, stderr := f.run(ctx, "hash", "--revoke", f.consent.ID, "--json")
	f.consent = hashCLIConsentResult(t, code, raw, stderr, "revoke")
	if err := os.WriteFile(f.base+"/config.toml", []byte("malformed [ TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.root, f.root+".offline"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(f.source, f.source+".offline"); err != nil {
		t.Fatal(err)
	}
	before := hashCLIBytes(t, f.base, f.root+".offline")
	for _, tc := range []struct {
		name, keeper, copies, example string
	}{
		{"first_keeper", "1", "2,3", "2,3"},
		{"nonfirst_keeper_subset", "2", "1", "1,3"},
		{"nonfirst_keeper_copy_order", "2", "3,1", "1,3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Base(string(f.proposal.Targets[0].File.PathBytes))
			input := &guidedReviewLineReader{lines: []string{"1\n", filename + "\n", tc.keeper + "\n", tc.copies + "\n"}}
			code, output, stderr := runHashGuidedReview(t, ctx, f.base, input)
			if code != 0 || stderr != "" || input.index != 4 || !strings.Contains(output, "Choose a group number") || strings.Count(output, "Choose one keeper row number, back, or quit: ") != 2 || !strings.Contains(output, "Choose copy row numbers (for example "+tc.example+"), back, or quit: ") {
				t.Fatal("group or role choice was automatic or retained extra input", code, output, stderr, input.index)
			}
			if !strings.Contains(output, "Choose exactly one available keeper row number from this frozen group") || strings.Count(output, "Possible keeper you selected:") != 1 {
				t.Fatal("filename input selected a keeper instead of requesting a row number", output)
			}
			preview := hashReviewFinalPreview(t, output)
			flat := strings.Join(strings.Fields(preview), " ")
			ordered := strings.ReplaceAll(tc.copies, ",", " ")
			command := hashReviewExactCommand(t, f.base, f.proposal.SelectionID, tc.keeper, ordered)
			for _, want := range []string{command, fmt.Sprintf("%x", sha256.Sum256(f.contents)), "Possible keeper", "Possible copy for review", "No decision was saved", "Approval Unavailable", "Reclaimable space Unknown", "Selected work 4", "Completed observations 4", "Completed without a match 1", "Total reserved " + humanBytes(4*int64(len(f.contents))), "Revocation recorded", "Current read permission Not evaluated"} {
				if !strings.Contains(preview, want) && !strings.Contains(flat, want) {
					t.Fatal("guided preview lost scope, accounting or authority qualification", want, preview)
				}
			}
			if strings.Count(preview, "Possible copy for review") != len(strings.Fields(ordered)) || strings.Contains(preview, fmt.Sprintf("%q", string(f.proposal.Targets[3].File.PathBytes))) {
				t.Fatal("guided review added an unrequested copy", preview)
			}
			if tc.copies == "1" && strings.Contains(preview, fmt.Sprintf("%q", string(f.proposal.Targets[2].File.PathBytes))) {
				t.Fatal("one-copy choice expanded to the third matching file", preview)
			}
			if !strings.Contains(preview, fmt.Sprintf("%q", string(f.proposal.Targets[1].File.PathBytes))) || strings.Contains(output, "nested\nfolder") || strings.ContainsRune(output, '\x1b') || !utf8.ValidString(output) {
				t.Fatal("guided review lost exact quoted Unicode/control paths", output)
			}
		})
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline")) {
		t.Fatal("guided review read-modified hash, source, inventory or invalid config bytes")
	}
	for _, missing := range []string{f.root, f.source} {
		if _, err := os.Lstat(missing); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("guided hash review initialized offline source/inventory", missing, err)
		}
	}
	hashReviewNoSavedDecision(t, f.base)
}

func TestGuidedHashReviewSecondGroupRemapsOnlyExplicitRows(t *testing.T) {
	ctx := context.Background()
	f := newHashProposalCLIFixture(t)
	if err := os.Remove(f.base + "/config.toml"); err != nil {
		t.Fatal(err)
	}
	firstContents := bytes.Repeat([]byte("generated"), 17)
	secondContents := bytes.Repeat([]byte{'z'}, len(firstContents))
	parent := filepath.Dir(string(f.page.Bands[0].Files[0].PathBytes))
	for name, contents := range map[string][]byte{
		"third first-body file": firstContents,
		"second-body one":       secondContents,
		"second-body two":       secondContents,
		"second-body three":     secondContents,
	} {
		if err := os.WriteFile(filepath.Join(parent, name), contents, 0600); err != nil {
			t.Fatal(err)
		}
	}
	p := hashKeeperRescanAndSelect(t, f, []string{"peer", "second-body one", "quote\"雪", "second-body two", "third first-body file", "second-body three"})
	c := hashKeeperApprove(t, f, p)
	for id := 1; id <= 6; id++ {
		hashKeeperRun(t, f, c, fmt.Sprint(id))
	}
	code, raw, stderr := f.run(ctx, "hashes", "--groups", "--json")
	groups := hashGroupsCLIReport(t, code, raw, stderr)
	if len(groups.Groups) != 2 || len(groups.Groups[0].Members) != 3 || len(groups.Groups[1].Members) != 3 {
		t.Fatal("fixture did not produce two distinct three-member groups", raw)
	}
	expectedSHA := map[string]bool{
		fmt.Sprintf("%x", sha256.Sum256(firstContents)):  true,
		fmt.Sprintf("%x", sha256.Sum256(secondContents)): true,
	}
	for _, group := range groups.Groups {
		if !expectedSHA[group.SHA256] || group.LogicalBytes != int64(len(firstContents)) {
			t.Fatal("saved group differs from the independently hashed fixture bodies", raw)
		}
		delete(expectedSHA, group.SHA256)
	}
	if len(expectedSHA) != 0 {
		t.Fatal("two different saved groups did not preserve both hashes", raw)
	}
	before := hashCLIBytes(t, f.base, f.root)
	// Return from the first group's keeper prompt. Ordinals then address the
	// explicitly chosen second group's frozen rows, without inheriting roles.
	input := &guidedReviewLineReader{lines: []string{"1\n", "back\n", "2\n", "2\n", "1\n"}}
	code, output, stderr := runHashGuidedReview(t, ctx, f.base, input)
	preview := hashReviewFinalPreview(t, output)
	chosen := groups.Groups[1]
	keeper, copy := chosen.Members[1], chosen.Members[0]
	if code != 0 || stderr != "" || input.index != 5 || strings.Count(output, "Choose a group number") != 2 || strings.Count(output, "Choose one keeper row number") != 2 || !strings.Contains(preview, hashReviewExactCommand(t, f.base, p.SelectionID, keeper.WorkID, copy.WorkID)) || !strings.Contains(preview, chosen.SHA256) || strings.Count(preview, "Possible copy for review") != 1 {
		t.Fatal("second-group row choices reused first-group ordinals or automatic roles", code, output, stderr, input.index)
	}
	for _, member := range []inventory.SavedHashGroupMember{keeper, copy} {
		if !strings.Contains(preview, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("explicit second-group role lost its exact saved path", preview)
		}
	}
	for _, member := range append(append([]inventory.SavedHashGroupMember(nil), groups.Groups[0].Members...), chosen.Members[2]) {
		if strings.Contains(preview, fmt.Sprintf("%q", string(member.PathBytes))) {
			t.Fatal("preview included a first-group or unselected matching path", preview)
		}
	}
	flat := strings.Join(strings.Fields(preview), " ")
	for _, want := range []string{"Selected work 6", "Completed observations 6", "Total reserved " + humanBytes(6*int64(len(firstContents)))} {
		if !strings.Contains(flat, want) {
			t.Fatal("explicit subset lost whole-selection coverage/accounting", want, preview)
		}
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("multi-group navigation changed saved records or source bytes")
	}
	hashReviewNoSavedDecision(t, f.base)
}

func TestGuidedHashReviewBackRetainsFrozenRowsAndRefreshIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name, keeper string
		lines        []string
		changeAt     int
		revoke       bool
	}{
		{"keeper_back", "2", []string{"1\n", "back\n", "1\n", "3\n", "2\n", "1\n"}, 1, false},
		{"copies_back", "2", []string{"1\n", "2\n", "back\n", "3\n", "2\n", "1\n"}, 2, false},
		{"explicit_refresh", "3", []string{"refresh\n", "1\n", "3\n", "1\n"}, 0, false},
		{"latest_accounting", "2", []string{"1\n", "2\n", "1\n"}, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newHashGuidedReviewFixture(t, 2)
			var afterChange map[string][32]byte
			input := &guidedReviewLineReader{lines: tc.lines, before: func(index int) {
				if index == tc.changeAt {
					hashKeeperRun(t, f.hashProposalCLIFixture, f.consent, "3")
					if tc.revoke {
						code, raw, stderr := f.run(context.Background(), "hash", "--revoke", f.consent.ID, "--json")
						_ = hashCLIConsentResult(t, code, raw, stderr, "revoke")
					}
					afterChange = hashCLIBytes(t, f.base, f.root)
				}
			}}
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, input)
			preview := hashReviewFinalPreview(t, output)
			if code != 0 || stderr != "" || input.index != len(tc.lines) || !strings.Contains(preview, hashReviewExactCommand(t, f.base, f.proposal.SelectionID, tc.keeper, "1")) || !strings.Contains(strings.Join(strings.Fields(preview), " "), "Completed observations 3") || !reflect.DeepEqual(afterChange, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("back silently refreshed rows or explicit refresh reused old rows", code, output, stderr, input.index)
			}
			if strings.HasSuffix(tc.name, "_back") && !strings.Contains(output, "Choose exactly one available keeper row number from this frozen group") {
				t.Fatal("newly completed row entered a frozen member list", output)
			}
			flat := strings.Join(strings.Fields(preview), " ")
			if !strings.Contains(flat, "Without a full observation 1") || !strings.Contains(flat, "Total reserved "+humanBytes(3*int64(len(f.contents)))) || tc.revoke && !strings.Contains(preview, "Revocation recorded") {
				t.Fatal("latest whole-selection accounting or consent was lost", preview)
			}
			if tc.revoke && strings.Contains(preview, fmt.Sprintf("%q", string(f.proposal.Targets[2].File.PathBytes))) {
				t.Fatal("unselected completion added a copy", preview)
			}
			hashReviewNoSavedDecision(t, f.base)
		})
	}
}

func TestGuidedHashReviewEmptyGroupsExitWithoutInput(t *testing.T) {
	f := newHashCLIFixture(t)
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	input := &guidedReviewLineReader{lines: []string{"1\n"}, before: func(int) { t.Fatal("empty groups requested input") }}
	code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, input)
	flat := strings.Join(strings.Fields(output), " ")
	for _, want := range []string{"Completed observations 0", "Without a full observation 2", "No matching completed observations", "does not prove that there are no duplicate files", "No decisions were saved"} {
		if code != 0 || stderr != "" || !strings.Contains(flat, want) {
			t.Fatal("empty groups omitted coverage or made a stronger negative claim", want, code, output, stderr)
		}
	}
	if input.index != 0 || strings.Contains(output, "Choose a group") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) {
		t.Fatal("empty report prompted, dispatched or changed records", output)
	}
	hashReviewNoSavedDecision(t, f.base)
}

func TestGuidedHashReviewAliasOutsideGroupIsUnavailable(t *testing.T) {
	for _, extraCopy := range []bool{false, true} {
		t.Run(fmt.Sprintf("extra_copy_%t", extraCopy), func(t *testing.T) {
			f := newHashProposalCLIFixture(t)
			if err := os.Remove(f.base + "/config.toml"); err != nil {
				t.Fatal(err)
			}
			parent := filepath.Dir(string(f.page.Bands[0].Files[0].PathBytes))
			if err := os.Link(filepath.Join(parent, "peer"), filepath.Join(parent, "unselected alias")); err != nil {
				t.Fatal(err)
			}
			names := []string{"peer", "quote\"雪", "unselected alias"}
			work := []string{"1", "2"}
			lines := []string{"1\n", "quit\n"}
			if extraCopy {
				if err := os.WriteFile(filepath.Join(parent, "independent copy"), bytes.Repeat([]byte("generated"), 17), 0600); err != nil {
					t.Fatal(err)
				}
				names = []string{"peer", "quote\"雪", "independent copy", "unselected alias"}
				work = []string{"1", "2", "3"}
				lines = []string{"1\n", "2\n", "3\n"}
			}
			p := hashKeeperRescanAndSelect(t, f, names)
			c := hashKeeperApprove(t, f, p)
			for _, id := range work {
				hashKeeperRun(t, f, c, id)
			}
			before := hashCLIBytes(t, f.base, f.root)
			input := &guidedReviewLineReader{lines: lines}
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, input)
			flat := strings.Join(strings.Fields(output), " ")
			if code != 0 || stderr != "" || input.index != len(lines) || !strings.Contains(flat, "Unavailable: repeated or conflicting saved identity") || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("known alias was omitted or review changed evidence", code, output, stderr)
			}
			if extraCopy {
				preview := hashReviewFinalPreview(t, output)
				if !strings.Contains(output, "Choose copy row numbers (for example 3), back, or quit: ") || !strings.Contains(preview, hashReviewExactCommand(t, f.base, p.SelectionID, "2", "3")) || strings.Count(preview, "Possible copy for review") != 1 || strings.Contains(preview, fmt.Sprintf("%q", string(p.Targets[0].File.PathBytes))) {
					t.Fatal("one-copy example included the keeper or an unavailable row", output)
				}
			} else if !strings.Contains(flat, "A role preview is unavailable") || !strings.Contains(flat, "No alternative paths were chosen") || strings.Contains(output, "Choose one keeper") || strings.Contains(output, hashReviewPreviewBanner) {
				t.Fatal("known alias was replaced or allowed as an independent role", output)
			}
			hashReviewNoSavedDecision(t, f.base)
		})
	}
}

func TestGuidedHashReviewReplacedStoreCannotRemapFrozenOrdinals(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	var replacement inventory.HashSnapshot
	var afterReplacement map[string][32]byte
	input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n"}, before: func(index int) {
		if index != 2 {
			return
		}
		if err := os.Rename(filepath.Join(f.base, "hashes"), filepath.Join(f.base, "previous-hashes")); err != nil {
			t.Fatal(err)
		}
		source, err := state.OpenReader(context.Background(), f.source)
		if err != nil {
			t.Fatal(err)
		}
		defer source.Close()
		writer, err := inventory.OpenHashWriter(context.Background(), f.base)
		if err != nil {
			t.Fatal(err)
		}
		expected := make([]state.SameSizeFile, len(f.proposal.Targets))
		for i, target := range f.proposal.Targets {
			expected[i] = target.File
		}
		if _, err = writer.CreateManualSelection(context.Background(), source, f.proposal.InventoryID, expected, []byte(f.root)); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		scanner, err := inventory.New([]string{f.root}, nil, nil)
		if err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		defer scanner.Close()
		for i := 0; i < 3; i++ {
			if _, err = writer.RunNext(context.Background(), source, scanner, inventory.FileHashStepByteLimit, 8192); err != nil {
				_ = writer.Close()
				t.Fatal(err)
			}
		}
		replacement, err = writer.Snapshot(context.Background())
		if closeErr := writer.Close(); err != nil || closeErr != nil {
			t.Fatal(err, closeErr)
		}
		afterReplacement = hashCLIBytes(t, f.base, f.root)
	}}
	code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, input)
	if replacement.StoreID == f.proposal.StoreID || len(replacement.Work) != 4 || replacement.Work[0].ID != "1" || replacement.Work[1].ID != "2" || code != 1 || !strings.Contains(stderr, "frozen selection") && !strings.Contains(stderr, "changed during review") || strings.Contains(output, hashReviewPreviewBanner) || !reflect.DeepEqual(afterReplacement, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("new store reused frozen ordinals for a different observation", code, output, stderr, replacement)
	}
	hashReviewNoSavedDecision(t, f.base)
}

type hashReviewFailAtWriter struct {
	match  string
	short  bool
	failed bool
	after  int
}

func (w *hashReviewFailAtWriter) Write(p []byte) (int, error) {
	if w.failed {
		w.after++
		return 0, errors.New("write after display failure")
	}
	if strings.Contains(string(p), w.match) {
		w.failed = true
		if w.short {
			return len(p) / 2, nil
		}
		return 0, errors.New("generated output failure")
	}
	return len(p), nil
}

type hashReviewPipePromptWriter struct {
	bytes.Buffer
	ready chan struct{}
	once  sync.Once
}

func (w *hashReviewPipePromptWriter) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	if strings.Contains(string(p), "Choose copy row numbers") {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}

func TestGuidedHashReviewInputOutputAndPipeControls(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	before := hashCLIBytes(t, f.base, f.root)
	for i, text := range []string{"", "1", "1\n", "1\n2", "1\n2\n", "1\n2\n1", "quit\n", "1\nquit\n", "1\n2\nquit\n"} {
		t.Run(fmt.Sprintf("eof_or_quit_%d", i), func(t *testing.T) {
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader(text))
			if code != 0 || stderr != "" || strings.Contains(output, hashReviewPreviewBanner) || !strings.Contains(output, "No decisions were saved") {
				t.Fatal("EOF, a partial response or quit confirmed a role", code, output, stderr)
			}
		})
	}
	for stage, prefix := range []string{"", "1\n", "1\n2\n"} {
		t.Run(fmt.Sprintf("oversize_%d", stage), func(t *testing.T) {
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader(prefix+strings.Repeat("1", reviewInputLimit+1)+"\n"))
			if code != 1 || stderr == "" || strings.Contains(output, hashReviewPreviewBanner) {
				t.Fatal("oversize response was accepted", code, output, stderr)
			}
		})
		t.Run(fmt.Sprintf("canceled_%d", stage), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n"}, before: func(index int) {
				if index == stage {
					cancel()
				}
			}}
			code, output, stderr := runHashGuidedReview(t, ctx, f.base, input)
			if code != 1 || !strings.Contains(stderr, context.Canceled.Error()) || strings.Contains(output, hashReviewPreviewBanner) || input.index != stage+1 {
				t.Fatal("canceled input continued to a preview", code, output, stderr, input.index)
			}
		})
	}
	for _, tc := range []struct{ name, response, want string }{
		{"group", "2\nquit\n", "Choose one group number from this frozen list"},
		{"keeper", "1\n4\nquit\n", "Choose exactly one available keeper row"},
		{"duplicate_copies", "1\n2\n1,1\nquit\n", "Copy rows must be unique"},
		{"keeper_as_copy", "1\n2\n2\nquit\n", "Copy rows must be unique"},
		{"range", "1\n2\n1-3\nquit\n", "Ranges and all are not accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader(tc.response))
			if code != 0 || stderr != "" || !strings.Contains(output, tc.want) || strings.Contains(output, hashReviewPreviewBanner) {
				t.Fatal("invalid role request chose an alternative", code, output, stderr)
			}
		})
	}
	for stage, prompt := range []string{"Choose a group number", "Choose one keeper row number", "Choose copy row numbers", hashReviewPreviewBanner} {
		for _, short := range []bool{false, true} {
			t.Run(fmt.Sprintf("output_%d_short_%t", stage, short), func(t *testing.T) {
				out := &hashReviewFailAtWriter{match: prompt, short: short}
				input := &guidedReviewLineReader{lines: []string{"1\n", "2\n", "1\n", "quit\n"}, before: func(index int) {
					if index >= stage {
						t.Fatal("failed display consumed further role input", index)
					}
				}}
				var stderr bytes.Buffer
				code := runWithInput(context.Background(), []string{"--data-dir", f.base, "review", "--hashes"}, input, out, &stderr)
				if code != 1 || stderr.Len() == 0 || !out.failed || out.after != 0 || input.index != stage {
					t.Fatal("display failure was not sticky", code, stderr.String(), out, input.index)
				}
			})
		}
	}
	// A native pipe remains open with an unterminated copy choice. Cancellation
	// must return without accepting that choice or closing the caller's FD.
	t.Run("native_pipe_partial_copy", func(t *testing.T) {
		readEnd, writeEnd, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer readEnd.Close()
		defer writeEnd.Close()
		if _, err = writeEnd.WriteString("1\n2\n1"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := &hashReviewPipePromptWriter{ready: make(chan struct{})}
		var stderr bytes.Buffer
		done := make(chan int, 1)
		go func() {
			done <- runWithInput(ctx, []string{"--data-dir", f.base, "review", "--hashes"}, readEnd, out, &stderr)
		}()
		select {
		case <-out.ready:
		case <-time.After(5 * time.Second):
			t.Fatal("pipe review did not reach explicit copy prompt")
		}
		select {
		case code := <-done:
			t.Fatal("unterminated native response was accepted", code)
		case <-time.After(120 * time.Millisecond):
		}
		cancel()
		select {
		case code := <-done:
			if code != 1 || !strings.Contains(stderr.String(), context.Canceled.Error()) || strings.Contains(out.String(), hashReviewPreviewBanner) {
				t.Fatal("native blocked input did not cancel safely", code, out.String(), stderr.String())
			}
		case <-time.After(2 * time.Second):
			t.Fatal("native blocked input did not release on cancellation")
		}
		if _, err := readEnd.Stat(); err != nil {
			t.Fatal("review closed caller input", err)
		}
	})
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
		t.Fatal("read-only input/output paths changed saved records or source bytes")
	}
	hashReviewNoSavedDecision(t, f.base)
}

func TestGuidedHashReviewRawBytePaths(t *testing.T) {
	for _, name := range []string{"raw\x1b\t雪\"", "raw\xff\x1b\t雪"} {
		t.Run(fmt.Sprintf("%x", []byte(name)), func(t *testing.T) {
			f := newHashProposalCLIFixtureWithFilename(t, name)
			if err := os.Remove(f.base + "/config.toml"); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), f.selectArgs()...)
			p := hashCLIProposal(t, code, raw, stderr)
			c := hashKeeperApprove(t, f, p)
			for _, id := range []string{"1", "2"} {
				hashKeeperRun(t, f, c, id)
			}
			before := hashCLIBytes(t, f.base, f.root)
			code, output, stderr := runHashGuidedReview(t, context.Background(), f.base, strings.NewReader("1\n2\n1\n"))
			preview := hashReviewFinalPreview(t, output)
			if code != 0 || stderr != "" || !utf8.ValidString(output) || strings.ContainsRune(output, '\x1b') || strings.Contains(output, "nested\nfolder") || !strings.Contains(preview, hashReviewExactCommand(t, f.base, p.SelectionID, "2", "1")) {
				t.Fatal("raw-path review failed or emitted terminal controls", code, output, stderr)
			}
			found := false
			for _, target := range p.Targets {
				path := string(target.File.PathBytes)
				if filepath.Base(path) == name {
					found = true
				}
				if !strings.Contains(preview, fmt.Sprintf("%q", path)) {
					t.Fatal("preview lost authoritative path bytes", preview)
				}
			}
			if !found || !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root)) {
				t.Fatal("raw-path review changed evidence or fixture omitted requested bytes")
			}
			hashReviewNoSavedDecision(t, f.base)
		})
	}
}

func TestGuidedHashReviewFlagsJSONAndMissingStorage(t *testing.T) {
	base := filepath.Join(t.TempDir(), "must-not-exist")
	input := &guidedReviewLineReader{lines: []string{"1\n"}, before: func(int) { t.Fatal("invalid/unsupported/missing hash review read input") }}
	for _, flags := range [][]string{
		{"--hashes=false"}, {"-hashes=false"}, {"--hashes=bad"},
		{"--hashes", "--hashes"}, {"--hashes", "-hashes"}, {"-hashes=true", "--hashes=true"},
		{"--hashes", "-d", "/generated-missing"}, {"--hashes", "--directory", "/generated-missing"},
		{"--hashes", "--min-age-days", "90"}, {"--hashes", "positional"}, {"--hashes", "--unknown"},
	} {
		var out, stderr bytes.Buffer
		args := append([]string{"--data-dir", base, "review"}, flags...)
		if code := runWithInput(context.Background(), args, input, &out, &stderr); code != 2 || out.Len() != 0 || stderr.Len() == 0 {
			t.Fatal("strict hash mode arguments touched storage or were accepted", flags, code, out.String(), stderr.String())
		}
	}
	for _, tail := range [][]string{{"--json", "review", "--hashes"}, {"review", "--hashes", "--json"}, {"review", "-hashes=true", "--json"}} {
		var out, stderr bytes.Buffer
		args := append([]string{"--data-dir", base}, tail...)
		code := runWithInput(context.Background(), args, input, &out, &stderr)
		var result struct {
			OK      bool   `json:"ok"`
			Command string `json:"command"`
			Error   struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(out.Bytes(), &result); err != nil || code != 2 || result.OK || result.Command != "review" || result.Error.Code != "unsupported_output" || stderr.Len() != 0 {
			t.Fatal("JSON guided review did not refuse before storage/input", tail, code, out.String(), stderr.String(), err)
		}
	}
	for _, flag := range []string{"--hashes", "-hashes", "--hashes=true", "-hashes=true"} {
		var out, stderr bytes.Buffer
		code := runWithInput(context.Background(), []string{"--data-dir", base, "review", flag}, input, &out, &stderr)
		if code != 1 || out.Len() != 0 || !strings.Contains(stderr.String(), "only reads existing observations") {
			t.Fatal("missing hash storage was initialized or valid alias refused", flag, code, out.String(), stderr.String())
		}
	}
	if _, err := os.Lstat(base); !errors.Is(err, os.ErrNotExist) || input.index != 0 {
		t.Fatal("argument or missing-store refusal initialized storage/read input", err, input.index)
	}
}
