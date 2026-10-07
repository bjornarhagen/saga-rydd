package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"golang.org/x/sys/unix"
)

type duplicateGateCloneUnavailable struct {
	api   string
	cause error
}

func (e *duplicateGateCloneUnavailable) Error() string { return e.api + ": " + e.cause.Error() }
func (e *duplicateGateCloneUnavailable) Unwrap() error { return e.cause }

// This fixture requires an actual native clone operation. File identities,
// copy-on-write isolation and full hashes are independent checks; allocated
// bytes do not reveal shared extents or space that cleanup could reclaim.
func TestDuplicateGateCLINativeClonesHistoricalHashesAndFreshRoles(t *testing.T) {
	const size = 64<<10 + 65
	equal := make([]byte, size)
	for i := range equal {
		equal[i] = byte((i*37 + i/11 + 19) % 251)
	}
	near := bytes.Clone(equal)
	near[len(near)/2] ^= 1
	names := []string{"source object", "equal clone", "keeper clone\"雪", "edited clone"}
	expected := [][]byte{equal, equal, equal, near}
	var actual [4]unix.Stat_t
	f, p := duplicateGateFixture(t, names, func(parent string) {
		duplicateGateWrite(t, parent, names[0], equal)
		for _, name := range names[1:] {
			if err := duplicateGateCloneFile(parent, names[0], name); err != nil {
				var unsupported *duplicateGateCloneUnavailable
				if errors.As(err, &unsupported) {
					t.Skipf("native clone API unavailable on generated fixture filesystem (%v); clone acceptance remains unverified", unsupported)
				}
				t.Fatal("native generated clone failed", err)
			}
		}
		// Make the near miss before scan/hash baseline capture. This write
		// also checks that modifying a clone leaves every equal object intact.
		edited, err := os.OpenFile(filepath.Join(parent, names[3]), os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := edited.WriteAt(near[size/2:size/2+1], int64(size/2)); err != nil || n != 1 {
			_ = edited.Close()
			t.Fatal("generated copy-on-write mutation failed", n, err)
		}
		if err = edited.Close(); err != nil {
			t.Fatal(err)
		}
		for i, name := range names {
			path := filepath.Join(parent, name)
			if err := unix.Stat(path, &actual[i]); err != nil {
				t.Fatal(err)
			}
			if actual[i].Mode&unix.S_IFMT != unix.S_IFREG || actual[i].Nlink != 1 || actual[i].Size != size || actual[i].Dev != actual[0].Dev {
				t.Fatal("native clone lost regular-file identity or logical size", i, actual[i])
			}
			for j := 0; j < i; j++ {
				if actual[i].Ino == actual[j].Ino {
					t.Fatal("clone paths share one native file identity", i, j)
				}
			}
			body, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(body, expected[i]) {
				t.Fatal("clone mutation was not private to the edited object", i, err)
			}
		}
	})
	for i, target := range p.Targets {
		if target.File.Size != size || target.File.Allocated != actual[i].Blocks*512 || target.File.Device != fmt.Sprint(actual[i].Dev) || target.File.Inode != fmt.Sprint(actual[i].Ino) {
			t.Fatal("saved clone metadata differs from independent native metadata", i, target.File)
		}
	}
	sourceBytes := hashCLIBytes(t, f.root, f.source)
	original, originalRaw, groups, groupsRaw := duplicateGateOriginalHashes(t, f, p, expected)
	wantSHA := fmt.Sprintf("%x", sha256.Sum256(equal))
	if len(groups.Groups) != 1 || groups.UnmatchedCompletedObservations != 1 {
		t.Fatal("edited clone was grouped as equal historical content", groupsRaw)
	}
	g := groups.Groups[0]
	if g.SHA256 != wantSHA || g.LogicalBytes != size || g.SavedIdentities != 3 || g.RepeatedSavedPaths != 0 || g.ConflictingSavedIdentities != 0 || len(g.Members) != 3 || groups.ContentVerified || groups.CurrentStateVerified || groups.DuplicatesVerified || groups.EstimatedReclaimableBytes != nil {
		t.Fatal("native clone group changed distinct identities or claimed current equality/savings", groupsRaw)
	}
	for i, member := range g.Members {
		if member.WorkID != strconv.Itoa(i+1) || member.FileID != p.Targets[i].File.ID || !bytes.Equal(member.PathBytes, p.Targets[i].File.PathBytes) || member.RepeatedSavedIdentity || member.SavedIdentityConflict {
			t.Fatal("clone group changed exact historical membership", member)
		}
	}
	choice := hashMetadataCLIChoice(t, f, p.SelectionID, "3", "2", "1")
	if choice.Record.Evidence.Keeper.WorkID != "3" || len(choice.Record.Evidence.Copies) != 2 || choice.Record.Evidence.Copies[0].WorkID != "2" || choice.Record.Evidence.Copies[1].WorkID != "1" || choice.Record.Evidence.SelectedWork != 4 || !reflect.DeepEqual(choice.Record.Evidence.Budget, original.Budget) {
		t.Fatal("clone choice changed nonfirst keeper or ordered subset", choice)
	}
	ctx := context.Background()
	code, raw, diagnostic := f.run(ctx, "hash", "--new-job-key", "--json")
	var key struct {
		OK   bool                  `json:"ok"`
		Hash HashFreshJobKeyResult `json:"hash"`
	}
	if err := json.Unmarshal([]byte(raw), &key); err != nil || code != 0 || diagnostic != "" || !key.OK || !inventory.ValidHashFreshJobKey(key.Hash.JobKey) || key.Hash.Saved || key.Hash.ApprovalAvailable || key.Hash.Executable {
		t.Fatal("clone fixture job key changed authority", code, raw, diagnostic, err)
	}
	code, raw, diagnostic = f.run(ctx, "hash", "--save-choice-job", choice.ID, "--job-key", key.Hash.JobKey, "--json")
	job := hashFreshJobCLIReport(t, code, raw, diagnostic, "save").Job
	wantIDs := []string{"3", "2", "1"}
	if len(job.Record.Request.Targets) != len(wantIDs) || !reflect.DeepEqual(job.Record.Request.HistoricalChoice, choice) {
		t.Fatal("clone job widened saved choice", raw)
	}
	for i, target := range job.Record.Request.Targets {
		old, _ := strconv.Atoi(wantIDs[i])
		role := "copy"
		if i == 0 {
			role = "keeper"
		}
		if target.Role != role || target.Observation.WorkID != wantIDs[i] || !reflect.DeepEqual(target.Target, p.Targets[old-1]) {
			t.Fatal("clone job changed full target or caller role order", target)
		}
	}
	capBytes := int64(3 * size)
	code, raw, diagnostic = f.run(ctx, "hash", "--approve-job", job.ID, "--confirm-content-read", "--max-day-bytes", strconv.FormatInt(capBytes, 10), "--max-total-bytes", strconv.FormatInt(capBytes, 10), "--json")
	consent := hashFreshReadCLIReport(t, code, raw, diagnostic, "approve")
	if consent.ID == original.ReadConsent.ID || consent.Approval.JobID != job.ID || consent.Approval.JobKey != job.Record.JobKey || consent.Approval.RequestID != job.Record.Request.RequestID || consent.Approval.DailyReservedByteLimit != capBytes || consent.Approval.LifetimeReservedByteLimit != capBytes {
		t.Fatal("clone fresh approval reused original authority or changed exact caps", raw)
	}
	for i, id := range wantIDs {
		code, raw, diagnostic = f.run(ctx, "hash", "--run-job", consent.ID, "--json")
		r := hashFreshRunCLIReport(t, code, raw, diagnostic).Result
		if r.Ordinal != i+1 || r.HistoricalWorkID != id || r.Role != job.Work[i].Role || r.Status != "hash_observed" || r.Progress.SHA256 != wantSHA || r.DurableOffset != size || !bytes.Equal(r.Progress.PathBytes, job.Record.Request.Targets[i].Target.File.PathBytes) || r.ReservedBytes != size || r.Usage.RequestedBytes != size || r.Usage.ReadBytes != size || r.FreshBudget == nil || r.FreshBudget.TotalReadBytes != int64((i+1)*size) || r.FreshBudget.TotalUnknownReservedBytes != 0 {
			t.Fatal("clone fresh hash changed role, body or independent accounting", raw)
		}
	}
	code, raw, diagnostic = f.run(ctx, "hash", "--show-job", job.ID, "--json")
	shown := hashFreshComparisonCLIReport(t, code, raw, diagnostic, job)
	c := shown.Comparison
	if c.Status != "historical_hashes_match" || c.MatchingCopies != 2 || c.DifferingCopies != 0 || c.IncompleteCopies != 0 || c.BlockedCopies != 0 || c.Keeper.HistoricalWorkID != "3" || c.Keeper.Observation == nil || c.Keeper.Observation.SHA256 != wantSHA || shown.FreshReservedBytes != capBytes || shown.FreshRequestedBytes != capBytes || shown.FreshReadBytes != capBytes {
		t.Fatal("cloned objects did not preserve exact historical fresh comparison", raw)
	}
	for i, pair := range c.Copies {
		if pair.Relation != "historical_hashes_match" || pair.Copy.HistoricalWorkID != wantIDs[i+1] || pair.Copy.Observation == nil || pair.Copy.Observation.Status != "complete" || pair.Copy.Observation.Sequence != 1 || pair.Copy.Observation.CheckedAt.IsZero() || pair.Copy.Observation.SHA256 != wantSHA {
			t.Fatal("clone comparison changed historical copy evidence", pair)
		}
	}
	code, human, diagnostic := f.run(ctx, "hash", "--show-job", job.ID)
	flat := strings.Join(strings.Fields(human), " ")
	for _, want := range []string{"SAVED FRESH KEEPER/COPY COMPARISON - HISTORICAL", "Historical hashes match", "saved observations made at separate times", "Reclaimable space remains unknown."} {
		if code != 0 || diagnostic != "" || !strings.Contains(flat, want) {
			t.Fatal("human clone comparison lost historical/savings qualification", want, code, human, diagnostic)
		}
	}
	for _, report := range []struct {
		args []string
		want string
	}{{[]string{"hashes", "--json"}, originalRaw}, {[]string{"hashes", "--groups", "--json"}, groupsRaw}} {
		code, raw, diagnostic = f.run(ctx, report.args...)
		if code != 0 || diagnostic != "" || raw != report.want {
			t.Fatal("fresh clone reads changed original historical records", code, raw, diagnostic)
		}
	}
	if !reflect.DeepEqual(sourceBytes, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("clone observations changed generated source or saved inventory")
	}
}
