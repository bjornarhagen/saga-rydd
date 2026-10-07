package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"golang.org/x/sys/unix"
)

// This gate exercises bounded continuation on huge logical files. Truncate
// creates real sparse holes; neither this fixture nor an oracle reads the
// sources in full. Only three explicit production hashing calls read bodies.
// It cannot establish completed hashes, duplicates or eventual completion.
func TestDuplicateGateCLILargeSparseContinuationStopsAtExplicitCaps(t *testing.T) {
	const logicalBytes int64 = 1<<30 + 65
	for _, caps := range []struct {
		name          string
		day, lifetime int64
		deferredCode  string
	}{
		{"lifetime", 8 << 20, 3 << 20, "lifetime_byte_limit"},
		{"day", 3 << 20, 8 << 20, "daily_byte_limit"},
	} {
		t.Run(caps.name, func(t *testing.T) {
			names := []string{"first sparse", "second sparse"}
			initial := make([]unix.Stat_t, len(names))
			f, proposal := duplicateGateFixture(t, names, func(parent string) {
				for i, name := range names {
					path := filepath.Join(parent, name)
					file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
					if err != nil {
						t.Fatal(err)
					}
					if err = file.Truncate(logicalBytes); err != nil {
						_ = file.Close()
						t.Fatal(err)
					}
					if err = file.Close(); err != nil {
						t.Fatal(err)
					}
					if err = unix.Stat(path, &initial[i]); err != nil {
						t.Fatal(err)
					}
					if initial[i].Size != logicalBytes {
						t.Fatal("truncate did not create the exact huge logical file", initial[i].Size)
					}
					if initial[i].Blocks*512 >= logicalBytes {
						t.Skip("fixture filesystem did not expose sparse allocation; native huge sparse acceptance remains unverified")
					}
				}
			})
			for i, target := range proposal.Targets {
				if target.File.Size != logicalBytes || target.File.Allocated != initial[i].Blocks*512 || target.File.Allocated >= target.File.Size {
					t.Fatal("production scan lost qualified huge sparse logical/allocated evidence", target.File)
				}
			}
			// Capture only the tiny generated inventory, never either huge source.
			inventoryBytes := hashCLIBytes(t, f.source)
			ctx := context.Background()
			code, raw, diagnostic := f.run(ctx, hashReadApproveArgs(proposal.SelectionID, caps.day, caps.lifetime)...)
			consent := hashCLIConsentResult(t, code, raw, diagnostic, "approve")
			if consent.Approval.DailyReservedByteLimit != caps.day || consent.Approval.LifetimeReservedByteLimit != caps.lifetime {
				t.Fatal("huge sparse fixture changed its explicit finite caps", raw)
			}
			var saved HashReport
			// Each Run invocation opens and closes the production writer. The
			// saved-only report between calls independently reopens its checkpoint.
			for i, expected := range []struct {
				work   string
				offset int64
			}{{"1", 1 << 20}, {"2", 1 << 20}, {"1", 2 << 20}} {
				code, raw, diagnostic = f.run(ctx, "hash", "--run", consent.ID, "--json")
				r := hashCLIStepResult(t, code, raw, diagnostic).Result
				ordinal, err := strconv.Atoi(expected.work)
				if err != nil {
					t.Fatal(err)
				}
				if r.Status != "pending" || r.Progress.Status != "partial" || r.WorkID != expected.work || r.Progress.LogicalBytes != logicalBytes || !bytes.Equal(r.Progress.PathBytes, proposal.Targets[ordinal-1].File.PathBytes) || r.DurableOffset != expected.offset || r.Progress.Offset != expected.offset || r.Progress.SHA256 != "" || r.ReservedBytes != inventory.FileHashStepByteLimit || r.Usage.RequestedBytes != inventory.FileHashStepByteLimit || r.Usage.ReadBytes != inventory.FileHashStepByteLimit {
					t.Fatal("huge sparse call exceeded one step, lost fair continuation or claimed a full hash", raw)
				}
				charged := int64(i+1) * inventory.FileHashStepByteLimit
				if r.Budget == nil || r.Budget.TotalReservedBytes != charged || r.Budget.TotalRequestedBytes != charged || r.Budget.TotalReadBytes != charged || r.Budget.TotalUnknownReservedBytes != 0 {
					t.Fatal("huge sparse call reused/refunded bytes or hid known accounting", raw)
				}
				code, raw, diagnostic = f.run(ctx, "hashes", "--json")
				saved = hashCLIReport(t, code, raw, diagnostic)
				if len(saved.Work) != 2 || saved.Budget == nil || !reflect.DeepEqual(saved.Budget, r.Budget) {
					t.Fatal("reopened saved state lost the finite step budget", raw)
				}
				for j, work := range saved.Work {
					wantOffset, wantSequence := int64(0), int64(0)
					if j == 0 {
						wantOffset, wantSequence = 1<<20, 1
						if i == 2 {
							wantOffset, wantSequence = 2<<20, 2
						}
					} else if i >= 1 {
						wantOffset, wantSequence = 1<<20, 1
					}
					if work.ID != strconv.Itoa(j+1) || work.Status != "pending" || work.LogicalBytes != logicalBytes || work.DurableOffset != wantOffset || work.Sequence != wantSequence || work.SHA256 != "" {
						t.Fatal("checkpoint reopen changed fair prefixes or completed a huge file", raw)
					}
				}
			}
			// The fourth explicit call must stop at the limiting cap. The error
			// envelope supplies no invented zero-usage or digest result; unchanged
			// durable work and budget prove that no extra charge/read was settled.
			code, raw, diagnostic = f.run(ctx, "hash", "--run", consent.ID, "--json")
			hashProposalFailure(t, code, raw, diagnostic, caps.deferredCode, 1)
			for _, forbidden := range []string{`"hash":`, `"usage"`, `"sha256"`, `"read_bytes"`, `"reserved_bytes"`} {
				if strings.Contains(raw, forbidden) {
					t.Fatal("cap refusal exposed a positive/partial result or invented usage", raw)
				}
			}
			code, raw, diagnostic = f.run(ctx, "hashes", "--json")
			after := hashCLIReport(t, code, raw, diagnostic)
			if !reflect.DeepEqual(after.Work, saved.Work) || !reflect.DeepEqual(after.Budget, saved.Budget) || after.ReadConsent == nil || !reflect.DeepEqual(after.ReadConsent.Approval, consent.Approval) {
				t.Fatal("cap refusal advanced huge-file progress or changed charges/limits", raw)
			}
			if !reflect.DeepEqual(inventoryBytes, hashCLIBytes(t, f.source)) {
				t.Fatal("bounded huge-file hashing changed saved inventory bytes")
			}
			for i, target := range proposal.Targets {
				var current unix.Stat_t
				if err := unix.Stat(string(target.File.PathBytes), &current); err != nil {
					t.Fatal(err)
				}
				before := initial[i]
				if current.Dev != before.Dev || current.Ino != before.Ino || current.Mode != before.Mode || current.Nlink != before.Nlink || current.Size != before.Size || current.Blocks != before.Blocks || current.Mtim != before.Mtim || current.Ctim != before.Ctim {
					t.Fatal("finite reads changed huge sparse source identity/content metadata")
				}
			}
		})
	}
}
