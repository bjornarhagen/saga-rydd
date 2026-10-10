package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/storeexport"
)

type backupValue struct {
	value string
	set   bool
}

func (v *backupValue) String() string { return v.value }
func (v *backupValue) Set(s string) error {
	if v.set {
		return errors.New("backup options cannot be repeated or combined with an alias")
	}
	v.value, v.set = s, true
	return nil
}

type backupBool struct{ value, set bool }

func (v *backupBool) String() string   { return strconv.FormatBool(v.value) }
func (v *backupBool) IsBoolFlag() bool { return true }
func (v *backupBool) Set(s string) error {
	if v.set {
		return errors.New("backup modes cannot be repeated")
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	v.value, v.set = b, true
	return nil
}

// These private operations allow output and argument tests without opening an
// owner store. Production always binds the two explicit storeexport operations.
type backupOperations struct {
	export func(context.Context, string, string) (storeexport.ExportResult, error)
	verify func(context.Context, string, string) (storeexport.VerificationResult, error)
}

type BackupReport struct {
	Contract                          string                          `json:"contract"`
	Mode                              string                          `json:"mode"`
	DataDirectory                     string                          `json:"data_directory,omitempty"`
	Artifact                          string                          `json:"artifact,omitempty"`
	ScratchParent                     string                          `json:"scratch_parent,omitempty"`
	Export                            *storeexport.ExportResult       `json:"export,omitempty"`
	Verification                      *storeexport.VerificationResult `json:"verification,omitempty"`
	HistoryScope                      string                          `json:"history_scope"`
	OmittedStores                     []string                        `json:"omitted_stores"`
	PrivateUnusedPageRemnantsPossible bool                            `json:"private_unused_page_remnants_possible"`
	FullInstanceBackup                bool                            `json:"full_instance_backup"`
	ImportAvailable                   bool                            `json:"import_available"`
	RestoreAvailable                  bool                            `json:"restore_available"`
	CleanupAuthority                  bool                            `json:"cleanup_authority"`
	CurrentPermissionEvaluated        bool                            `json:"current_permission_evaluated"`
	HardLatencyBound                  bool                            `json:"hard_latency_bound"`
}

func backupDataDirCount(args []string) int {
	n := 0
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--data-dir" || a == "-data-dir" {
			n++
			i++
			continue
		}
		if strings.HasPrefix(a, "--data-dir=") || strings.HasPrefix(a, "-data-dir=") {
			n++
			continue
		}
		if a == "--" || len(a) == 0 || a[0] != '-' {
			break
		}
	}
	return n
}

func backupPath(p string) bool {
	return len(p) > 0 && len(p) <= 4096 && utf8.ValidString(p) && strings.IndexByte(p, 0) < 0 && filepath.IsAbs(p) && filepath.Clean(p) == p && p != string(filepath.Separator)
}
func backupBeneath(parent, child string) bool {
	r, err := filepath.Rel(parent, child)
	return err == nil && r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator))
}

func dispatchBackup(ctx context.Context, args []string, dataDir string, dataDirCount int, ops backupOperations) (BackupReport, error) {
	r := BackupReport{Contract: storeexport.Contract, HistoryScope: "committed_history_supported_by_existing_plans_schema", OmittedStores: []string{"inventory", "hashes_and_read_consent_charges", "configuration", "services", "source_files", "quarantined_file_bodies"}, PrivateUnusedPageRemnantsPossible: true}
	if len(args) > 16 {
		return r, usageError{errors.New("backup takes at most 16 arguments")}
	}
	var plansMode backupBool
	var to, verify, scratch backupValue
	f := flag.NewFlagSet("backup", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	f.Var(&plansMode, "plans", "export the fixed existing plans store")
	f.Var(&to, "to", "absent absolute destination directory")
	f.Var(&verify, "verify", "existing absolute export artifact")
	f.Var(&scratch, "scratch-dir", "existing private absolute scratch parent")
	if err := f.Parse(args); err != nil {
		return r, usageError{err}
	}
	if f.NArg() != 0 || plansMode.set == verify.set || plansMode.set && !plansMode.value {
		return r, usageError{errors.New("backup requires exactly one of --plans --to ABSENT_DESTINATION or --verify ARTIFACT --scratch-dir EXISTING_PRIVATE_PARENT; positional arguments are unavailable")}
	}
	if plansMode.set {
		if !to.set || scratch.set || dataDirCount != 1 || !backupPath(dataDir) || !backupPath(to.value) || len(filepath.Base(to.value)) > 255 || len(filepath.Dir(to.value))+80 > 4096 || backupBeneath(dataDir, to.value) {
			return r, usageError{errors.New("plans export requires one explicit global --data-dir and --to, using clean absolute UTF-8 paths of at most 4096 bytes; destination must be outside the data directory; --scratch-dir is unavailable")}
		}
		r.Mode, r.DataDirectory, r.Artifact = "export_plans", dataDir, to.value
	} else {
		if to.set || !scratch.set || dataDirCount != 0 || !backupPath(verify.value) || !backupPath(scratch.value) || len(scratch.value)+80 > 4096 || backupBeneath(verify.value, scratch.value) {
			return r, usageError{errors.New("verification requires --verify and --scratch-dir, using clean absolute UTF-8 paths of at most 4096 bytes; scratch parent must be outside the artifact; --data-dir and --to are unavailable")}
		}
		r.Mode, r.Artifact, r.ScratchParent = "verify_artifact", verify.value, scratch.value
	}
	if ctx == nil {
		return r, storeexport.ErrProfile
	}
	work, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	if err := work.Err(); err != nil {
		return r, err
	}
	var err error
	if r.Mode == "export_plans" {
		if ops.export == nil {
			return r, storeexport.ErrProfile
		}
		var result storeexport.ExportResult
		result, err = ops.export(work, r.DataDirectory, r.Artifact)
		r.Export = &result
	} else {
		if ops.verify == nil {
			return r, storeexport.ErrProfile
		}
		var result storeexport.VerificationResult
		result, err = ops.verify(work, r.Artifact, r.ScratchParent)
		r.Verification = &result
	}
	return r, errors.Join(err, work.Err())
}

func backupReplyMessage(r BackupReport) string {
	if r.Export != nil {
		if r.Export.Published {
			return fmt.Sprintf("Export destination %q was published; inspect that exact artifact before any new export. Sync completion: %t", r.Export.Destination, r.Export.SyncCompleted)
		}
		if r.Export.CandidateRetained {
			if !r.Export.CandidateIdentityObserved {
				return fmt.Sprintf("Export created staging name %q, but its identity was not confirmed; preserve that name for inspection and inspect exact destination %q. Do not replace, prune or retry automatically", r.Export.Candidate, r.Artifact)
			}
			return fmt.Sprintf("Export candidate %q is retained; its identity was observed earlier. Inspect it and exact destination %q. Do not replace, prune or retry automatically", r.Export.Candidate, r.Artifact)
		}
		return fmt.Sprintf("Export did not report publication; inspect exact destination %q before any new export", r.Artifact)
	}
	if r.Verification != nil && r.Verification.ScratchRetained {
		if !r.Verification.ScratchIdentityObserved {
			return fmt.Sprintf("Verification created scratch name %q, but its identity was not confirmed; preserve that name for inspection. Artifact: %q", r.Verification.Scratch, r.Artifact)
		}
		return fmt.Sprintf("Verification scratch %q is retained; its identity was observed earlier. Preserve it for inspection. Artifact: %q", r.Verification.Scratch, r.Artifact)
	}
	return fmt.Sprintf("Inspect the exact selected artifact %q; no import, restore or cleanup permission was granted", r.Artifact)
}

func backupErrorCode(err error) (string, int) {
	var usage usageError
	if errors.As(err, &usage) {
		return "invalid_arguments", 2
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "canceled", 1
	case errors.Is(err, storeexport.ErrClock):
		return "backup_clock", 1
	case errors.Is(err, storeexport.ErrBounds):
		return "backup_bounds", 1
	case errors.Is(err, storeexport.ErrChanged):
		return "backup_changed", 1
	case errors.Is(err, storeexport.ErrProfile):
		return "backup_profile_unsupported", 1
	case errors.Is(err, os.ErrExist):
		return "already_exists", 1
	case errors.Is(err, os.ErrNotExist):
		return "not_found", 1
	case errors.Is(err, os.ErrPermission):
		return "permission_denied", 1
	default:
		return "command_failed", 1
	}
}

func printBackupResult(out io.Writer, r BackupReport, success bool) error {
	g := &reviewOutput{writer: out}
	title := "PLANS HISTORY EXPORT INCOMPLETE"
	if r.Mode == "verify_artifact" {
		title = "ARTIFACT VERIFICATION INCOMPLETE"
	}
	if success {
		if r.Mode == "export_plans" {
			title = "PLANS HISTORY EXPORT SAVED"
		} else {
			title = "ARTIFACT STRUCTURE VERIFIED"
		}
	}
	printResultBanner(g, title)
	fmt.Fprintf(g, "Artifact: %q\n", r.Artifact)
	if r.Export != nil {
		if r.Export.CandidateRetained {
			fmt.Fprintf(g, "Retained candidate: %q\n", r.Export.Candidate)
		}
		printField(g, "Candidate identity observed", r.Export.CandidateIdentityObserved)
		printField(g, "Published", r.Export.Published)
		printField(g, "Structure verified", r.Export.Verified)
		printField(g, "Sync API completed", r.Export.SyncCompleted)
		printField(g, "Engine closure known", r.Export.EngineClosureKnown)
		if r.Export.ExportID != "" {
			fmt.Fprintf(g, "Export: %s\n", r.Export.ExportID)
		}
		printWrapped(g, "This private export contains committed history supported by the existing plans-store schema. It omits inventory, hashes and read-consent charges, configuration, services, source files and quarantined file bodies. Unused database pages may retain private remnants. This is not a full-instance backup or a sanitized report.", "")
	} else if r.Verification != nil {
		printField(g, "Structure verified", r.Verification.Verified)
		printField(g, "Engine closure known", r.Verification.EngineClosureKnown)
		printField(g, "Scratch retained", r.Verification.ScratchRetained)
		printField(g, "Scratch identity observed", r.Verification.ScratchIdentityObserved)
		if r.Verification.ScratchRetained {
			fmt.Fprintf(g, "Retained scratch: %q\n", r.Verification.Scratch)
		}
		if r.Verification.ExportID != "" {
			fmt.Fprintf(g, "Export: %s\n", r.Verification.ExportID)
		}
		printWrapped(g, "Verification reads this artifact and opens SQLite only on a separate scratch copy. It checks the digest, supported database structure and integrity. It does not validate record meanings or permission for an action.", "")
	}
	printWrapped(g, "The 120-second bound is cooperative; an entered filesystem or driver call can take longer. Import, restore and cleanup are unavailable. No current permission is evaluated.", "")
	if !success {
		printWrapped(g, backupReplyMessage(r), "")
	}
	if g.err != nil {
		return fmt.Errorf("%s; reply did not finish: %w", backupReplyMessage(r), g.err)
	}
	return nil
}

func runBackupWithOperations(ctx context.Context, args []string, dataDir string, dataDirCount int, out, errOut io.Writer, machine bool, ops backupOperations) int {
	r, err := dispatchBackup(ctx, args, dataDir, dataDirCount, ops)
	exit, code := 0, ""
	if err != nil {
		code, exit = backupErrorCode(err)
	}
	if machine {
		envelope := map[string]any{"api_version": APIVersion, "ok": err == nil, "command": "backup", "backup": r}
		if err != nil {
			envelope["error"] = map[string]string{"code": code, "message": err.Error()}
		}
		if err != nil && r.Mode != "" {
			envelope["next_action"] = backupReplyMessage(r)
		}
		if writeErr := json.NewEncoder(&reviewOutput{writer: out}).Encode(envelope); writeErr != nil {
			fmt.Fprintf(errOut, "write JSON: %v\n", writeErr)
			if r.Mode != "" {
				fmt.Fprintln(errOut, backupReplyMessage(r))
			}
			return 1
		}
		if ctx != nil && ctx.Err() != nil && exit == 0 {
			fmt.Fprintf(errOut, "%s; reply was canceled: %v\n", backupReplyMessage(r), ctx.Err())
			return 1
		}
		return exit
	}
	if exit == 2 {
		fmt.Fprintln(errOut, err)
		return exit
	}
	if r.Mode != "" {
		err = errors.Join(err, printBackupResult(out, r, err == nil))
	}
	if ctx != nil {
		err = errors.Join(err, ctx.Err())
	}
	if err != nil {
		fmt.Fprintf(errOut, "%v\n%s.\n", err, backupReplyMessage(r))
		return 1
	}
	return 0
}

func runBackup(ctx context.Context, args []string, dataDir string, dataDirCount int, out, errOut io.Writer, machine bool) int {
	return runBackupWithOperations(ctx, args, dataDir, dataDirCount, out, errOut, machine, backupOperations{storeexport.ExportPlanStore, storeexport.VerifyPlanStoreExport})
}
