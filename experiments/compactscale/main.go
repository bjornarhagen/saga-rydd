// compactscale exercises production CLI processes on an owned disposable tree.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	_ "modernc.org/sqlite"
)

type snapshot struct {
	Entries    int64 `json:"entries"`
	Identities int64 `json:"compact_identities"`
	Jobs       int64 `json:"inventory_backlog"`
	Retirement int64 `json:"retirement_backlog"`
	Allocation int64 `json:"allocation_backlog"`
	Scratch    int64 `json:"scratch_identities"`
	Pages      int64 `json:"pages"`
	FreePages  int64 `json:"reusable_pages"`
	PageBytes  int64 `json:"page_bytes"`
}
type measurement struct {
	Name               string    `json:"name"`
	ElapsedMS          int64     `json:"elapsed_ms"`
	PeakRSS            int64     `json:"child_peak_rss_bytes"`
	PeakDB             int64     `json:"sampled_peak_database_bytes"`
	PeakWAL            int64     `json:"sampled_peak_wal_bytes"`
	PeakJobs           int64     `json:"sampled_peak_inventory_backlog"`
	PeakRetirement     int64     `json:"sampled_peak_retirement_backlog"`
	PeakAllocation     int64     `json:"sampled_peak_allocation_backlog"`
	Interrupted        bool      `json:"sigkill_verified"`
	AtKill             *snapshot `json:"before_kill,omitempty"`
	Final              snapshot  `json:"final"`
	ScanBatches        int       `json:"scan_batches"`
	MaintenanceBatches int       `json:"maintenance_batches"`
	Mode               string    `json:"mode,omitempty"`
	ReportMS           int64     `json:"report_ms"`
	Logical            *int64    `json:"logical_bytes,omitempty"`
	Allocated          *int64    `json:"allocated_bytes,omitempty"`
}
type runner struct {
	binary, base, root, db string
	results                []measurement
}

func main() {
	binary := flag.String("binary", "./dist/rydd", "native production CLI")
	files := flag.Int("files", 100000, "unique dependency files (minimum 1000)")
	cycles := flag.Int("cycles", 2, "disappearance/reappearance cycles (minimum 1)")
	flag.Parse()
	if *files < 1000 || *cycles < 1 {
		fmt.Fprintln(os.Stderr, "files must be >=1000; cycles must be >=1")
		os.Exit(2)
	}
	if err := run(*binary, *files, *cycles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(binary string, files, cycles int) error {
	binary, err := filepath.Abs(binary)
	if err != nil {
		return err
	}
	base, err := os.MkdirTemp("", "rydd-compact-scale-")
	if err != nil {
		return err
	}
	// Retain fixtures and logs for inspection. Never accept a user-owned scan root.
	fmt.Fprintln(os.Stderr, "Disposable fixture retained at", base)
	r := &runner{binary: binary, base: base, root: filepath.Join(base, "root")}
	nm := filepath.Join(r.root, "node_modules")
	if err = os.MkdirAll(nm, 0700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(r.root, "package.json"), []byte("{}\n"), 0600); err != nil {
		return err
	}
	var allocated, halfAllocated int64
	manifest, err := os.Stat(filepath.Join(r.root, "package.json"))
	if err != nil {
		return err
	}
	manifestAllocated := manifest.Sys().(*syscall.Stat_t).Blocks * 512
	for i := 0; i < files; i++ {
		dir := filepath.Join(nm, fmt.Sprintf("pkg-%03d", i%100))
		if i < 100 {
			if err = os.Mkdir(dir, 0700); err != nil {
				return err
			}
		}
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("file-%06d", i)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		if i < 100 {
			_, err = f.Write([]byte("x"))
		}
		if err == nil {
			err = f.Truncate(17)
		}
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		info, err := os.Stat(f.Name())
		if err != nil {
			return err
		}
		blocks := info.Sys().(*syscall.Stat_t).Blocks * 512
		allocated += blocks
		if i%100 < 50 {
			halfAllocated += blocks
		}
	}
	fmt.Fprintln(os.Stderr, "Fixture ready:", files, "identities in 100 packages")
	r.setState("compact")
	if err = r.scan("fresh-interrupted", "scan", true); err != nil {
		return err
	}
	if err = r.complete("fresh-resume", files, allocated+manifestAllocated, true); err != nil {
		return err
	}
	if err = r.complete("rescan", files, allocated+manifestAllocated, false); err != nil {
		return err
	}
	held := filepath.Join(base, "held")
	if err = os.Mkdir(held, 0700); err != nil {
		return err
	}
	removed := 0
	for i := 0; i < files; i++ {
		if i%100 < 50 {
			removed++
		}
	}
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("pkg-%03d", i)
		if err = os.Rename(filepath.Join(nm, name), filepath.Join(held, name)); err != nil {
			return err
		}
	}
	if err = r.scan("shrink-interrupted", "retirement", true); err != nil {
		return err
	}
	if err = r.complete("shrink-resume", files-removed, allocated-halfAllocated+manifestAllocated, true); err != nil {
		return err
	}
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("pkg-%03d", i)
		if err = os.Rename(filepath.Join(held, name), filepath.Join(nm, name)); err != nil {
			return err
		}
	}
	if err = r.scan("restore-interrupted", "allocation", true); err != nil {
		return err
	}
	if err = r.complete("restore-resume", files, allocated+manifestAllocated, true); err != nil {
		return err
	}
	for i := 1; i <= cycles; i++ {
		moved := filepath.Join(base, fmt.Sprintf("dependencies-%d", i))
		if err = os.Rename(nm, moved); err != nil {
			return err
		}
		if err = r.complete(fmt.Sprintf("disappear-%d", i), 0, manifestAllocated, false); err != nil {
			return err
		}
		if _, err = os.Stat(filepath.Join(moved, "pkg-000", "file-000000")); err != nil {
			return fmt.Errorf("fixture original lost: %w", err)
		}
		if err = os.Rename(moved, nm); err != nil {
			return err
		}
		if err = r.complete(fmt.Sprintf("reappear-%d", i), files, allocated+manifestAllocated, false); err != nil {
			return err
		}
	}
	r.setState("detailed")
	if err = r.scan("detailed-baseline", "", false); err != nil {
		return err
	}
	if got := r.results[len(r.results)-1].Final; got.Entries != int64(files+103) || got.Identities != 0 || got.Jobs != 0 {
		return fmt.Errorf("unexpected detailed baseline: %+v", got)
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Platform     string        `json:"platform"`
		Files        int           `json:"unique_dependency_files"`
		Cycles       int           `json:"cycles"`
		SamplesMS    int           `json:"sampling_interval_ms"`
		Measurements []measurement `json:"measurements"`
	}{runtime.GOOS + "/" + runtime.GOARCH, files, cycles, 100, r.results})
}
func (r *runner) setState(name string) {
	r.base = filepath.Join(filepath.Dir(r.root), name)
	sum := sha256.Sum256([]byte(r.root))
	r.db = filepath.Join(r.base, "manual", fmt.Sprintf("%x", sum), state.Filename)
}
func (r *runner) read() (snapshot, error) {
	var s snapshot
	u := url.URL{Scheme: "file", Path: r.db, RawQuery: "mode=ro&_pragma=busy_timeout(1000)"}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return s, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM entries), (SELECT COUNT(*) FROM compact_inodes),
 (SELECT COUNT(*) FROM jobs WHERE status IN ('pending','running')),
 (SELECT COUNT(*) FROM compact_retirement)+(SELECT COUNT(*) FROM subtree_reconcile)+(SELECT COUNT(*) FROM subtree_retirement),
 (SELECT COUNT(*) FROM allocation_cache WHERE phase!='done'),
 (SELECT COUNT(*) FROM allocation_identities),
 (SELECT page_count FROM pragma_page_count), (SELECT freelist_count FROM pragma_freelist_count), (SELECT page_size FROM pragma_page_size)`).Scan(&s.Entries, &s.Identities, &s.Jobs, &s.Retirement, &s.Allocation, &s.Scratch, &s.Pages, &s.FreePages, &s.PageBytes)
	return s, err
}
func (r *runner) scan(name, killStage string, compact bool) error {
	fmt.Fprintln(os.Stderr, "Running", name)
	var initial snapshot
	if killStage == "retirement" {
		var err error
		initial, err = r.read()
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	args := []string{"--data-dir", r.base, "scan", "-d", r.root, "--now", "--json"}
	if compact {
		args = append(args, "--compact")
	}
	cmd := exec.CommandContext(ctx, r.binary, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	start := time.Now()
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	m := measurement{Name: name}
	sample := func() {
		for _, p := range []struct {
			path string
			peak *int64
		}{{r.db, &m.PeakDB}, {r.db + "-wal", &m.PeakWAL}} {
			if st, err := os.Stat(p.path); err == nil {
				*p.peak = max(*p.peak, st.Size())
			}
		}
	}
	var waitErr error
loop:
	for {
		select {
		case waitErr = <-done:
			break loop
		case <-ticker.C:
			sample()
			s, err := r.read()
			if err != nil {
				continue
			} // Fresh process may still be initializing its database.
			m.PeakJobs = max(m.PeakJobs, s.Jobs)
			m.PeakRetirement = max(m.PeakRetirement, s.Retirement)
			m.PeakAllocation = max(m.PeakAllocation, s.Allocation)
			shouldKill := (killStage == "scan" && s.Jobs > 0 && s.Identities >= 128) || (killStage == "retirement" && s.Jobs == 0 && s.Retirement > 0 && s.Identities < initial.Identities) || (killStage == "allocation" && s.Jobs == 0 && s.Retirement == 0 && s.Scratch >= 128)
			if shouldKill && m.AtKill == nil {
				m.AtKill = &s
				if err = cmd.Process.Kill(); err != nil {
					return err
				}
			}
		}
	}
	m.ElapsedMS = time.Since(start).Milliseconds()
	sample()
	if usage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		m.PeakRSS = usage.Maxrss
		if runtime.GOOS == "linux" {
			m.PeakRSS *= 1024
		}
	}
	if killStage != "" {
		status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
		if m.AtKill == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			return fmt.Errorf("%s: intended interruption not observed: %v %s", name, waitErr, stderr.String())
		}
		m.Interrupted = true
	} else if waitErr != nil {
		return fmt.Errorf("%s failed: %w: %s %s", name, waitErr, stderr.String(), out.String())
	}
	if !m.Interrupted {
		var envelope struct {
			Scan struct {
				Outcome    string `json:"outcome"`
				Mode       string `json:"mode"`
				Batches    int    `json:"batches"`
				Retirement int    `json:"retirement_batches"`
				Allocation int    `json:"allocation_batches"`
				Subtree    int    `json:"subtree_retirement_batches"`
			} `json:"scan"`
		}
		if err := json.Unmarshal(out.Bytes(), &envelope); err != nil {
			return err
		}
		if envelope.Scan.Outcome != "queue_drained" {
			return fmt.Errorf("%s: unexpected scan outcome: %s", name, out.String())
		}
		m.Mode = envelope.Scan.Mode
		m.ScanBatches = envelope.Scan.Batches
		m.MaintenanceBatches = envelope.Scan.Retirement + envelope.Scan.Allocation + envelope.Scan.Subtree
	}
	var err error
	m.Final, err = r.read()
	if err != nil {
		return err
	}
	r.results = append(r.results, m)
	return nil
}
func (r *runner) complete(name string, files int, allocated int64, resume bool) error {
	if err := r.scan(name, "", true); err != nil {
		return err
	}
	m := &r.results[len(r.results)-1]
	expectedMode := "new_pass"
	if resume {
		expectedMode = "resume"
	}
	if m.Mode != expectedMode {
		return fmt.Errorf("%s: mode %q, want %q", name, m.Mode, expectedMode)
	}
	if m.Final.Jobs != 0 || m.Final.Retirement != 0 || m.Final.Allocation != 0 || m.Final.Scratch != 0 || m.Final.Identities != int64(files) {
		return fmt.Errorf("%s: undrained or incorrect saved inventory: %+v", name, m.Final)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	out, err := exec.CommandContext(ctx, r.binary, "--data-dir", r.base, "report", "-d", r.root, "--json").Output()
	m.ReportMS = time.Since(start).Milliseconds()
	if err != nil {
		return err
	}
	var envelope struct {
		Report struct {
			Directory state.DirectoryReport `json:"directory"`
		} `json:"report"`
	}
	if err = json.Unmarshal(out, &envelope); err != nil {
		return err
	}
	d := envelope.Report.Directory
	m.Logical = d.LogicalBytes
	m.Allocated = d.AllocatedBytes
	if d.Status != "recorded_complete" || d.LogicalBytes == nil || *d.LogicalBytes != int64(files)*17+3 || d.AllocatedBytes == nil || *d.AllocatedBytes != allocated || d.CompactedFiles != files || d.ExcludedEntries != 0 || d.InodeEntriesExamined != 0 || (files > 0 && d.AllocatedSizeSource != "cached_reduction") {
		return fmt.Errorf("%s: incorrect saved report: %s", name, out)
	}
	if m.Final.Entries > 103 {
		return errors.New("compact scan retained per-file ordinary entries")
	}
	return nil
}
