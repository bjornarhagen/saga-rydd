package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bjornarhagen/saga-rydd/internal/inventory"
)

// These refusal fixtures use only generated files and production observations.
// The request command itself must not open source, inventory or configuration.
func TestHashChoiceFreshRequestCLILegacyLocatorRefusal(t *testing.T) {
	f := newHashCLIFixture(t)
	for range 2 {
		if _, err := f.store.RunNext(context.Background(), f.source, f.scanner, inventory.FileHashStepByteLimit, 4096); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := f.store.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	preview, err := f.store.PreviewKeeper(context.Background(), snapshot.SelectionID, "2", []string{"1"})
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.store.SaveKeeperChoice(context.Background(), preview)
	if err != nil {
		t.Fatal(err)
	}
	// The existing fixture deliberately has malformed configuration. A source
	// locator refusal must win without consulting that unrelated configuration.
	before := hashCLIBytes(t, f.base, f.sourceDir, f.root)
	code, raw, stderr := f.run(context.Background(), "hash", "--request-choice", saved.ID, "--json")
	hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_choice_request_unavailable", 1)
	if !strings.Contains(raw, "manual inventory locator") || strings.Contains(raw, "decode config") || strings.Contains(raw, `"request_id":`) {
		t.Fatal("legacy request consulted configuration or exposed partial request scope", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.sourceDir, f.root)) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("legacy refusal changed source, configuration, historical records or accounting")
	}
}

func TestHashChoiceFreshRequestCLICorruptChoiceRefusal(t *testing.T) {
	f := newHashGuidedReviewFixture(t, 3)
	saved := hashMetadataCLIChoice(t, f.hashProposalCLIFixture, f.proposal.SelectionID, "2", "1")
	db, err := sql.Open("sqlite", filepath.Join(f.base, "hashes", "hashes.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP TRIGGER hash_keeper_choice_no_update"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	// Keep the requested canonical ID and a BLOB payload. This is a saved-choice
	// integrity failure, rather than missing storage or an invalid CLI argument.
	if _, err = db.Exec("UPDATE hash_keeper_choice SET payload=x'00' WHERE id=?", saved.ID); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(f.base, "config.toml"), []byte("not valid [ GENERATED TOML"), 0600); err != nil {
		t.Fatal(err)
	}
	hashChoiceOffline(t, f.hashProposalCLIFixture)
	before := hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")
	code, raw, stderr := f.run(context.Background(), "--json", "hash", "--request-choice", saved.ID)
	hashChoiceCLIError(t, code, raw, stderr, "hash", "hash_choice_invalid", 1)
	if strings.Contains(raw, "decode config") || strings.Contains(raw, `"request_id":`) {
		t.Fatal("corrupt request reached config parsing or exposed partial request scope", raw)
	}
	if !reflect.DeepEqual(before, hashCLIBytes(t, f.base, f.root+".offline", f.source+".offline")) || len(hashChoiceIDs(t, f.base)) != 1 {
		t.Fatal("corrupt-choice refusal recovered, replaced or changed saved/source records")
	}
}
