package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/config"
	"github.com/bjornarhagen/saga-rydd/internal/inventory"
	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func hashBoundarySaved(t *testing.T, f *hashProposalCLIFixture) HashReport {
	t.Helper()
	code, raw, stderr := f.run(context.Background(), "hashes", "--json")
	return hashCLIReport(t, code, raw, stderr)
}

func hashBoundaryApprove(t *testing.T, f *hashProposalCLIFixture, p inventory.HashProposal, day, total int64) inventory.HashReadConsent {
	t.Helper()
	code, raw, stderr := f.run(context.Background(), hashReadApproveArgs(p.SelectionID, day, total)...)
	return hashCLIConsentResult(t, code, raw, stderr, "approve")
}

func hashBoundaryUncharged(t *testing.T, saved HashReport) {
	t.Helper()
	if saved.Budget != nil {
		t.Fatal("refused preflight charged source work", saved)
	}
	for _, work := range saved.Work {
		if work.Sequence != 0 || work.DurableOffset != 0 || work.SHA256 != "" || work.LatestAttempt != nil {
			t.Fatal("refused preflight opened a source attempt or published content", saved)
		}
	}
}

func TestHashCLIBoundaryRunUsesFrozenDerivedInventoryWithoutFallback(t *testing.T) {
	for _, mode := range []string{"missing", "changed"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f, p := newHashReadCLIFixture(t, nil)
			consent := hashBoundaryApprove(t, f, p, 4096, 8192)
			before := hashCLIBytes(t, f.root)
			// This is a valid matching decoy at the configured/global location.
			// A dispatcher that silently falls back could hash the frozen files.
			body, err := os.ReadFile(filepath.Join(f.source, state.Filename))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(f.base, state.Filename), body, 0600); err != nil {
				t.Fatal(err)
			}
			decoy, err := state.OpenReader(ctx, f.base)
			if err != nil {
				t.Fatal(err)
			}
			report, err := decoy.SameSizeCandidates(ctx, 20, "", 1)
			closeErr := decoy.Close()
			if err != nil || closeErr != nil || report.InventoryID != p.InventoryID || len(report.Bands) != 1 || len(report.Bands[0].Files) != 2 {
				t.Fatal("decoy does not contain the complete matching inventory", report, err, closeErr)
			}
			want := "not_found"
			if mode == "missing" {
				if err = os.Rename(f.source, f.source+".offline"); err != nil {
					t.Fatal(err)
				}
			} else {
				// A production rescan changes saved observations at the actual
				// derived locator. The old matching decoy is left untouched.
				code, raw, stderr := f.run(ctx, "scan", "-d", f.root, "--now", "--detailed", "--json")
				if code != 0 || stderr != "" {
					t.Fatal(code, raw, stderr)
				}
				want = "hash_inventory_changed"
			}
			code, raw, stderr := f.run(ctx, "hash", "--run", consent.ID, "--json")
			hashProposalFailure(t, code, raw, stderr, want, 1)
			hashBoundaryUncharged(t, hashBoundarySaved(t, f))
			if mode == "missing" {
				if _, err = os.Stat(f.source); !os.IsNotExist(err) {
					t.Fatal("refused run recreated the frozen manual inventory", err)
				}
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root)) {
				t.Fatal("refused derived-inventory run changed selected source bytes")
			}
		})
	}
}

func TestHashCLIBoundaryRunAppliesExclusionsAddedAfterApproval(t *testing.T) {
	for _, mode := range []string{"direct_file", "physical_parent_alias"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f, p := newHashReadCLIFixture(t, nil)
			consent := hashBoundaryApprove(t, f, p, 4096, 8192)
			before := hashCLIBytes(t, f.root, f.source)
			excluded := string(p.Targets[0].File.PathBytes)
			if mode == "physical_parent_alias" {
				excluded = filepath.Join(t.TempDir(), "excluded-parent-alias")
				if err := os.Symlink(filepath.Dir(string(p.Targets[0].File.PathBytes)), excluded); err != nil {
					t.Fatal(err)
				}
			}
			cfg := config.Default()
			// Current configured roots do not replace the frozen manual root.
			cfg.Roots = []string{filepath.Join(t.TempDir(), "unrelated-root")}
			cfg.Excludes = []string{excluded}
			home, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			if err = config.Create(filepath.Join(f.base, "config.toml"), home, cfg); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(ctx, "hash", "--run", consent.ID, "--json")
			hashProposalFailure(t, code, raw, stderr, "command_failed", 1)
			if !strings.Contains(raw, "exclusion") {
				t.Fatal("the current exclusion did not cause refusal", raw)
			}
			saved := hashBoundarySaved(t, f)
			if saved.Budget == nil || saved.Budget.TotalReservedBytes != p.Targets[0].File.Size || saved.Budget.TotalRequestedBytes != 0 || saved.Budget.TotalReadBytes != 0 || saved.Budget.TotalUnknownReservedBytes != 0 || saved.Work[0].Status != "invalidated" || saved.Work[0].Code != "scope_excluded" || saved.Work[0].Sequence != 1 || saved.Work[0].DurableOffset != 0 || saved.Work[0].SHA256 != "" || saved.Work[0].LatestAttempt == nil || saved.Work[0].LatestAttempt.ReadBytes == nil || *saved.Work[0].LatestAttempt.ReadBytes != 0 || saved.Work[1].Sequence != 0 || saved.Work[1].LatestAttempt != nil {
				t.Fatal("excluded work read contents, refunded its charge, or expanded to another target", saved)
			}
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.source)) {
				t.Fatal("current exclusion run changed source or inventory bytes")
			}
		})
	}
}

func TestHashCLIBoundaryPrivateFileAliasesRefusedBeforeParsingOrReservation(t *testing.T) {
	for _, mode := range []string{"config", "source_database"} {
		t.Run(mode, func(t *testing.T) {
			f, p := newHashReadCLIFixture(t, nil)
			consent := hashBoundaryApprove(t, f, p, 4096, 8192)
			before := hashCLIBytes(t, f.root)
			alias := filepath.Join(f.base, "config.toml")
			if mode == "source_database" {
				alias = filepath.Join(f.source, state.Filename)
				if err := os.Rename(alias, alias+".offline"); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Link(string(p.Targets[0].File.PathBytes), alias); err != nil {
				t.Fatal(err)
			}
			code, raw, stderr := f.run(context.Background(), "hash", "--run", consent.ID, "--json")
			hashProposalFailure(t, code, raw, stderr, "command_failed", 1)
			if !strings.Contains(raw, "cannot alias selected") || strings.Contains(raw, "decode config") || strings.Contains(raw, "not a database") {
				t.Fatal("private alias reached content parsing instead of the metadata guard", raw)
			}
			hashBoundaryUncharged(t, hashBoundarySaved(t, f))
			if !reflect.DeepEqual(before, hashCLIBytes(t, f.root)) {
				t.Fatal("private alias refusal changed selected source bytes")
			}
		})
	}
}

func TestHashCLIBoundaryShortRepliesRecoverImmutableCapsWithoutRetry(t *testing.T) {
	ctx := context.Background()
	f, p := newHashReadCLIFixture(t, nil)
	limit := p.Targets[0].File.Size
	before := hashCLIBytes(t, f.root, f.source)
	var diagnostic bytes.Buffer
	code := Run(ctx, append([]string{"--data-dir", f.base}, hashReadApproveArgs(p.SelectionID, limit, limit)...), hashFailWriter{short: true}, &diagnostic)
	if code != 1 || !strings.Contains(diagnostic.String(), "short write") {
		t.Fatal("silent short approval output reported success", code, diagnostic.String())
	}
	code, raw, stderr := f.run(ctx, "hash", "--show", p.SelectionID, "--json")
	shown := hashCLIProposal(t, code, raw, stderr)
	if shown.ReadConsent == nil || shown.ReadConsent.Approval.DailyReservedByteLimit != limit || shown.ReadConsent.Approval.LifetimeReservedByteLimit != limit {
		t.Fatal("short reply lost approved absolute limits", raw)
	}
	consent := *shown.ReadConsent
	hashBoundaryUncharged(t, hashBoundarySaved(t, f))
	diagnostic.Reset()
	code = Run(ctx, []string{"--data-dir", f.base, "hash", "--run", consent.ID, "--json"}, hashFailWriter{short: true}, &diagnostic)
	if code != 1 || !strings.Contains(diagnostic.String(), "short write") {
		t.Fatal("silent short step output reported success", code, diagnostic.String())
	}
	saved := hashBoundarySaved(t, f)
	if saved.Budget == nil || saved.Budget.TotalReservedBytes != limit || saved.Budget.TotalRequestedBytes != limit || saved.Budget.TotalReadBytes != limit || saved.Work[0].Status != "complete" || saved.Work[0].Sequence != 1 || saved.Work[0].DurableOffset != limit || saved.Work[1].Sequence != 0 || saved.Work[1].LatestAttempt != nil {
		t.Fatal("short reply erased progress or implicitly retried/expanded work", saved)
	}
	// Neither publication failure nor matching retry creates fresh limits.
	code, raw, stderr = f.run(ctx, hashReadApproveArgs(p.SelectionID, 2*limit, 2*limit)...)
	hashProposalFailure(t, code, raw, stderr, "already_exists", 1)
	code, raw, stderr = f.run(ctx, hashReadApproveArgs(p.SelectionID, limit, limit)...)
	retry := hashCLIConsentResult(t, code, raw, stderr, "approve")
	if retry.ID != consent.ID || !reflect.DeepEqual(retry.Approval, consent.Approval) {
		t.Fatal("retry after short output renewed the approval", raw)
	}
	code, raw, stderr = f.run(ctx, "hash", "--run", consent.ID, "--json")
	hashProposalFailure(t, code, raw, stderr, "lifetime_byte_limit", 1)
	after := hashBoundarySaved(t, f)
	if !reflect.DeepEqual(saved.Work, after.Work) || !reflect.DeepEqual(saved.Budget, after.Budget) || after.ReadConsent == nil || after.ReadConsent.ID != consent.ID {
		t.Fatal("exhausted retry refunded work or issued more source reads", after)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.root, f.source)) {
		t.Fatal("short output or cap refusal changed selected source/inventory bytes")
	}
}
