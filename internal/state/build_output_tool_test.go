package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

type cargoProducerToolchain struct{ cargo, rustc, bin, sysroot string }
type cargoProducerVersions struct{ cargo, rustc string }

// Only an explicitly declared preinstalled toolchain is eligible. In
// particular, PATH's Cargo/Rustup shims are never a fallback. The executable
// installation is trusted; version strings are compatibility evidence, not
// executable authentication.
func cargoProducerTools(cargo, rustc string) (cargoProducerToolchain, error) {
	var result cargoProducerToolchain
	for _, path := range []string{cargo, rustc} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 4096 || !utf8.ValidString(path) || strings.ContainsFunc(path, func(r rune) bool { return r < 0x20 || r == 0x7f }) {
			return result, errors.New("opt-in Cargo gate requires explicit absolute RYDD_TEST_CARGO_BIN and RYDD_TEST_RUSTC_BIN direct toolchain executables")
		}
	}
	resolved := make([]string, 2)
	infos := make([]os.FileInfo, 2)
	for i, path := range []string{cargo, rustc} {
		var err error
		resolved[i], err = filepath.EvalSymlinks(path)
		if err != nil {
			return result, fmt.Errorf("declared preinstalled Cargo toolchain unavailable: %w", err)
		}
		infos[i], err = os.Lstat(resolved[i])
		if err != nil || !infos[i].Mode().IsRegular() || infos[i].Mode().Perm()&0111 == 0 {
			return result, errors.New("declared Cargo toolchain requires regular executable files")
		}
	}
	bin := filepath.Dir(resolved[0])
	if filepath.Base(resolved[0]) != "cargo" || filepath.Base(resolved[1]) != "rustc" || filepath.Dir(resolved[1]) != bin || filepath.Base(bin) != "bin" || filepath.Base(filepath.Dir(filepath.Dir(bin))) != "toolchains" || os.SameFile(infos[0], infos[1]) {
		return result, errors.New("Cargo gate requires distinct direct cargo/rustc executables in one preinstalled Rustup toolchain bin directory; shims are unsupported")
	}
	sysroot := filepath.Dir(bin)
	lib, err := os.Stat(filepath.Join(sysroot, "lib", "rustlib"))
	if err != nil || !lib.IsDir() {
		return result, errors.New("declared preinstalled Rust sysroot is unavailable")
	}
	return cargoProducerToolchain{resolved[0], resolved[1], bin, sysroot}, nil
}

// Cargo also probes configuration in cwd's ancestors, even with CARGO_HOME
// set. Refuse any such name using metadata only; never read its contents. This
// is a controlled generated-runner fixture, not namespace authentication.
func cargoProducerNoAncestorConfig(ctx context.Context, cwd string) error {
	for depth, dir := 0, cwd; ; depth, dir = depth+1, filepath.Dir(dir) {
		if depth >= 128 {
			return errors.New("Cargo fixture ancestor configuration check exceeded 128 directories")
		}
		for _, name := range []string{"config", "config.toml"} {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := os.Lstat(filepath.Join(dir, ".cargo", name))
			if err == nil {
				return errors.New("Cargo fixture refuses existing ancestor Cargo configuration")
			}
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("Cargo fixture cannot exclude ancestor configuration: %w", err)
			}
		}
		if filepath.Dir(dir) == dir {
			return nil
		}
	}
}

func cargoProducerEnv(tool cargoProducerToolchain, base string) []string {
	// HOME is deliberately omitted, not replaced. Explicit generated Cargo,
	// Rustup and temporary paths prevent inherited caches/configuration, while
	// direct binaries and RUSTC avoid Rustup/toolchain selection. No wrapper,
	// credential, proxy, user flag, target or loader environment is inherited.
	return []string{
		"PATH=" + tool.bin,
		"CARGO_HOME=" + filepath.Join(base, "cargo-home"),
		"RUSTUP_HOME=" + filepath.Join(base, "rustup-home"),
		"RUSTUP_AUTO_INSTALL=0",
		"RUSTC=" + tool.rustc,
		"RUSTC_WRAPPER=",
		"RUSTC_WORKSPACE_WRAPPER=",
		"CARGO_ENCODED_RUSTFLAGS=--sysroot\x1f" + tool.sysroot,
		"CARGO_INCREMENTAL=0",
		"CARGO_NET_OFFLINE=true",
		"CARGO_TERM_COLOR=never",
		"CARGO_TERM_PROGRESS_WHEN=never",
		"TMPDIR=" + filepath.Join(base, "tool-tmp"),
		"TMP=" + filepath.Join(base, "tool-tmp"),
		"TEMP=" + filepath.Join(base, "tool-tmp"),
	}
}

type cargoProducerOutput struct {
	buffer   bytes.Buffer
	limit    int
	exceeded bool
	cancel   context.CancelFunc
}

func (b *cargoProducerOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > b.limit-b.buffer.Len() {
		p = p[:b.limit-b.buffer.Len()]
		b.exceeded = true
		b.cancel()
	}
	_, err := b.buffer.Write(p)
	return n, err
}

func (b *cargoProducerOutput) String() string { return b.buffer.String() }

func cargoProducerRun(parent context.Context, tool, cwd string, env []string, args ...string) (string, error) {
	if err := cargoProducerNoAncestorConfig(parent, cwd); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.Dir, cmd.Env = cwd, env
	// Use guarded direct-child cancellation; compiler descendants are outside scope.
	cmd.WaitDelay = 2 * time.Second
	output := &cargoProducerOutput{limit: 64 << 10, cancel: cancel}
	cmd.Stdout, cmd.Stderr = output, output
	err := cmd.Run() // Run waits/reaps the owned process, including cancellation.
	if output.exceeded {
		return output.String(), errors.New("generated Cargo producer exceeded 64 KiB output")
	}
	return output.String(), errors.Join(err, parent.Err())
}

func cargoProducerProbeVersion(ctx context.Context, tool, cwd string, env []string, name string) (string, error) {
	probe, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	output, err := cargoProducerRun(probe, tool, cwd, env, "--version")
	if err != nil {
		return "", fmt.Errorf("preinstalled %s version probe failed: %w", name, err)
	}
	line := strings.TrimSpace(output)
	if len(line) > 256 || strings.ContainsFunc(line, func(r rune) bool { return r < 0x20 || r == 0x7f }) || !strings.HasPrefix(line, name+" ") || !strings.HasSuffix(line, ")") {
		return "", fmt.Errorf("preinstalled %s version probe did not return a bounded single version line", name)
	}
	version, build, ok := strings.Cut(strings.TrimPrefix(line, name+" "), " (")
	parts := strings.Split(version, ".")
	if !ok || len(build) < 2 || len(version) > 32 || len(parts) != 3 {
		return "", fmt.Errorf("preinstalled %s version probe did not return a bounded semantic version", name)
	}
	for _, part := range parts {
		if len(part) == 0 || len(part) > 8 || strings.ContainsFunc(part, func(r rune) bool { return r < '0' || r > '9' }) {
			return "", fmt.Errorf("preinstalled %s version probe did not return a bounded semantic version", name)
		}
	}
	return version, nil
}

func cargoProducerCheckVersions(ctx context.Context, tool cargoProducerToolchain, cwd string, env []string) (cargoProducerVersions, error) {
	var versions cargoProducerVersions
	var err error
	if versions.cargo, err = cargoProducerProbeVersion(ctx, tool.cargo, cwd, env, "cargo"); err != nil {
		return versions, err
	}
	if versions.rustc, err = cargoProducerProbeVersion(ctx, tool.rustc, cwd, env, "rustc"); err != nil {
		return versions, err
	}
	if err = ctx.Err(); err != nil {
		return versions, err
	}
	// These explicit pairs come from declared runner image profiles. Never
	// infer a Cargo patch number from rustc or accept arbitrary/mixed releases.
	switch versions {
	case cargoProducerVersions{"1.98.1", "1.98.1"}, cargoProducerVersions{"1.99.0", "1.99.0"}:
		return versions, nil
	default:
		return versions, fmt.Errorf("Cargo producer gate requires a declared exact Cargo/Rust pair before project creation; observed cargo %q and rustc %q", versions.cargo, versions.rustc)
	}
}

func TestCargoBuildOutputProducerGuardsAndBounds(t *testing.T) {
	// Generated shell helpers exercise refusals/overflow only. They are never
	// accepted as tool-produced Cargo layout compatibility evidence.
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(base, "toolchains", "generated", "bin")
	for _, dir := range []string{bin, filepath.Join(filepath.Dir(bin), "lib", "rustlib"), filepath.Join(base, "cargo-home"), filepath.Join(base, "rustup-home"), filepath.Join(base, "tool-tmp")} {
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cargo, rustc := filepath.Join(bin, "cargo"), filepath.Join(bin, "rustc")
	if err = os.WriteFile(cargo, []byte("#!/bin/sh\nprintf '%s\\n' 'cargo 9.9.9 (generated)'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(rustc, []byte("#!/bin/sh\nprintf '%s\\n' 'rustc 9.9.9 (generated)'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	tool, err := cargoProducerTools(cargo, rustc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cargoProducerTools("", ""); err == nil {
		t.Fatal("missing explicit toolchain became implicit PATH/shim discovery")
	}
	env := cargoProducerEnv(tool, base)
	for _, value := range env {
		if strings.HasPrefix(value, "HOME=") || strings.HasPrefix(value, "CODEX_HOME=") {
			t.Fatal("fixture repurposed or inherited a home variable")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, versions := range []struct {
		cargo, rustc string
		accepted     bool
	}{
		{"9.9.9", "9.9.9", false},
		{"1.98.1", "1.99.0", false},
		{"1.99.0", "1.98.1", false},
		{"1.98.1", "1.98.1", true},
		{"1.99.0", "1.99.0", true},
	} {
		for _, probe := range []struct{ path, name, version string }{{cargo, "cargo", versions.cargo}, {rustc, "rustc", versions.rustc}} {
			if err = os.WriteFile(probe.path, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' '%s %s (generated)'\n", probe.name, probe.version)), 0700); err != nil {
				t.Fatal(err)
			}
		}
		observed, err := cargoProducerCheckVersions(ctx, tool, base, env)
		if (err == nil) != versions.accepted || observed != (cargoProducerVersions{versions.cargo, versions.rustc}) {
			t.Fatal("unknown/mixed version refusal or declared pair differs", observed, versions, err)
		}
		if err != nil && (!strings.Contains(err.Error(), "declared exact Cargo/Rust pair") || !strings.Contains(err.Error(), fmt.Sprintf("cargo %q and rustc %q", versions.cargo, versions.rustc))) {
			t.Fatal("version refusal omitted safe exact observed pair", err)
		}
	}
	for _, malformed := range []string{"cargo 1.99.0 (generated)\nprivate-generated-version-canary", "cargo 1.99.0-private-generated-version-canary (generated)"} {
		if err = os.WriteFile(cargo, []byte(fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' '%s'\n", malformed)), 0700); err != nil {
			t.Fatal(err)
		}
		if _, err = cargoProducerCheckVersions(ctx, tool, base, env); err == nil || strings.Contains(err.Error(), "private-generated-version-canary") {
			t.Fatal("malformed version output was accepted or disclosed", err)
		}
	}
	if _, err = os.Lstat(filepath.Join(base, "projects")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("version refusal created producer projects", err)
	}
	// An ancestor configuration must refuse before attempting to execute even
	// an unavailable command; its potentially private contents remain unopened.
	configured := filepath.Join(base, "configured")
	if err = os.MkdirAll(filepath.Join(configured, ".cargo"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(configured, ".cargo", "config.toml"), []byte("never read this generated config"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := cargoProducerRun(ctx, filepath.Join(base, "never-created-command"), configured, env, "--version"); err == nil || output != "" || !strings.Contains(err.Error(), "refuses existing ancestor") {
		t.Fatal("ancestor config reached command execution", output, err)
	}
	// Rustup shims can be hardlinks to the same executable. Distinct names alone
	// must not qualify them as separate direct Cargo/compiler binaries.
	if err = os.Remove(rustc); err != nil {
		t.Fatal(err)
	}
	if err = os.Link(cargo, rustc); err != nil {
		t.Fatal(err)
	}
	if _, err = cargoProducerTools(cargo, rustc); err == nil {
		t.Fatal("hardlinked Rustup-style shims passed direct toolchain guard")
	}
	overflow := filepath.Join(base, "generated-output-command")
	if err = os.WriteFile(overflow, []byte("#!/bin/sh\nwhile :; do printf '%s' 'generated-output'; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	output, err := cargoProducerRun(ctx, overflow, base, env)
	if err == nil || !strings.Contains(err.Error(), "exceeded 64 KiB") || len(output) != 64<<10 || time.Since(started) > 3*time.Second {
		t.Fatal("producer output overflow did not bound/kill/reap its child", len(output), err, time.Since(started))
	}
	quiet := filepath.Join(base, "generated-quiet-command")
	if err = os.WriteFile(quiet, []byte("#!/bin/sh\nwhile :; do :; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	limited, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	started = time.Now()
	output, err = cargoProducerRun(limited, quiet, base, env)
	if !errors.Is(err, context.DeadlineExceeded) || output != "" || time.Since(started) > 3*time.Second {
		t.Fatal("quiet producer cancellation did not kill/reap its child", len(output), err, time.Since(started))
	}
}

type cargoProducerObservation struct {
	relative, parent, kind string
	stat                   unix.Stat_t
}

func cargoProducerObserve(ctx context.Context, root string) ([]cargoProducerObservation, error) {
	result := []cargoProducerObservation{}
	var visit func(string, int) error
	visit = func(relative string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > 12 || len(result) >= 4096 {
			return errors.New("generated Cargo observation exceeded depth 12 or 4096 entries")
		}
		path := filepath.Join(root, relative)
		var st unix.Stat_t
		if err := unix.Lstat(path, &st); err != nil {
			return err
		}
		kind := "file"
		if st.Mode&unix.S_IFMT == unix.S_IFDIR {
			kind = "directory"
		} else if st.Mode&unix.S_IFMT != unix.S_IFREG {
			return errors.New("generated Cargo producer created an unsupported object kind")
		}
		old := time.Unix(0, buildOutputOld)
		if err := os.Chtimes(path, old, old); err != nil {
			return err
		}
		if err := unix.Lstat(path, &st); err != nil {
			return err
		}
		if st.Size < 0 || st.Size > 64<<20 || st.Blocks < 0 || st.Blocks > (128<<20)/512 {
			return errors.New("generated Cargo object exceeded fixture size/allocation bounds")
		}
		parent := filepath.Dir(relative)
		if relative == "." {
			parent = ""
		}
		result = append(result, cargoProducerObservation{relative, parent, kind, st})
		if kind != "directory" {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		names, readErr := f.Readdirnames(129)
		closeErr := f.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		if closeErr != nil {
			return closeErr
		}
		if len(names) > 128 {
			return errors.New("generated Cargo directory exceeded 128 immediate names")
		}
		sort.Strings(names)
		for _, name := range names {
			if err := visit(filepath.Join(relative, name), depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	err := visit(".", 0)
	return result, err
}

// The opted-in producer reads only a declared trusted compiler installation.
// Projects, Cargo/Rustup homes, temporary files, artifacts and saved metadata
// are disposable. Normal tests never invoke an installed Cargo/Rust tool.
func TestCargoBuildOutputToolProduced(t *testing.T) {
	if os.Getenv("RYDD_TEST_CARGO_TOOL") != "1" {
		t.Skip("tool-produced Cargo compatibility requires explicit RYDD_TEST_CARGO_TOOL=1 and direct preinstalled cargo/rustc paths")
	}
	tool, err := cargoProducerTools(os.Getenv("RYDD_TEST_CARGO_BIN"), os.Getenv("RYDD_TEST_RUSTC_BIN"))
	if err != nil {
		t.Fatal(err)
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"cargo-home", "rustup-home", "tool-tmp"} {
		if err = os.Mkdir(filepath.Join(base, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	env := cargoProducerEnv(tool, base)
	versions, err := cargoProducerCheckVersions(ctx, tool, base, env)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("declared exact preinstalled producer pair: cargo %s and rustc %s", versions.cargo, versions.rustc)
	root := filepath.Join(base, "projects")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"debug", "release"} {
		project := filepath.Join(root, "project-"+profile)
		if err = os.MkdirAll(filepath.Join(project, "src"), 0700); err != nil {
			t.Fatal(err)
		}
		// An explicit empty workspace makes this manifest its own root, rather
		// than discovering or joining a Cargo.toml in a temporary ancestor.
		manifest := fmt.Sprintf("[package]\nname = \"rydd_generated_cargo_%s\"\nversion = \"0.1.0\"\nedition = \"2021\"\npublish = false\nbuild = false\nautobins = false\nautoexamples = false\nautotests = false\nautobenches = false\n\n[lib]\npath = \"src/lib.rs\"\ntest = false\ndoctest = false\nbench = false\n\n[workspace]\nmembers = []\n", profile)
		if err = os.WriteFile(filepath.Join(project, "Cargo.toml"), []byte(manifest), 0600); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(project, "src", "lib.rs"), []byte("pub fn generated_value() -> u32 { 7 }\n"), 0600); err != nil {
			t.Fatal(err)
		}
		args := []string{"build", "--offline", "--lib", "--jobs", "1", "--color", "never", "--manifest-path", filepath.Join(project, "Cargo.toml"), "--target-dir", filepath.Join(project, "target")}
		if profile == "release" {
			args = append(args, "--release")
		}
		if output, err := cargoProducerRun(ctx, tool.cargo, project, env, args...); err != nil {
			t.Fatalf("generated %s library build failed: %v\n%s", profile, err, output)
		}
	}
	// Unrelated generated files force a genuinely empty continuing raw page.
	// They do not manufacture or modify any Cargo target-layout marker.
	for i := 0; i < 1001; i++ {
		dir := filepath.Join(root, "00-pagination", fmt.Sprintf("group-%02d", i/100))
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, fmt.Sprintf("entry-%04d", i)), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	observations, err := cargoProducerObserve(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	cargoProducerVerifySavedReports(t, ctx, root, base, observations, versions)
}

func cargoProducerVerifySavedReports(t *testing.T, ctx context.Context, root, base string, observations []cargoProducerObservation, versions cargoProducerVersions) {
	t.Helper()
	byPath := make(map[string]cargoProducerObservation, len(observations))
	for _, observation := range observations {
		byPath[observation.relative] = observation
	}
	type expectedTarget struct {
		profile            string
		logical, allocated int64
		files, repeated    int
		markers            []cargoProducerObservation
	}
	expected := map[string]expectedTarget{}
	roles := []string{"target", "manifest", "lockfile", "profile", "profile_lock", "dependencies", "fingerprints"}
	for _, profile := range []string{"debug", "release"} {
		project := "project-" + profile
		target := filepath.Join(project, "target")
		profilePath := filepath.Join(target, profile)
		want := expectedTarget{profile: profile}
		for _, spec := range []struct{ path, kind string }{
			{target, "directory"}, {filepath.Join(project, "Cargo.toml"), "file"}, {filepath.Join(project, "Cargo.lock"), "file"},
			{profilePath, "directory"}, {filepath.Join(profilePath, ".cargo-lock"), "file"},
			{filepath.Join(profilePath, "deps"), "directory"}, {filepath.Join(profilePath, ".fingerprint"), "directory"},
		} {
			observed, exists := byPath[spec.path]
			if !exists || observed.kind != spec.kind {
				t.Fatalf("preinstalled Cargo %s did not create the supported %s marker %q of kind %s", versions.cargo, profile, spec.path, spec.kind)
			}
			want.markers = append(want.markers, observed)
		}
		inodes := map[[2]uint64]bool{}
		for _, observed := range observations {
			if observed.kind != "file" || !strings.HasPrefix(observed.relative, target+string(filepath.Separator)) {
				continue
			}
			want.files++
			want.logical += observed.stat.Size
			key := [2]uint64{uint64(observed.stat.Dev), observed.stat.Ino}
			if inodes[key] {
				want.repeated++
			} else {
				want.allocated += observed.stat.Blocks * 512
				inodes[key] = true
			}
		}
		if want.files == 0 || want.logical <= 0 || want.logical > 256<<20 || want.allocated > 256<<20 {
			t.Fatal("generated Cargo target has no bounded nonempty independent file oracle", profile, want)
		}
		expected[filepath.Join(root, target)] = want
	}

	// Import only independently Lstat-observed generated objects and completed
	// bounded listings. This exercises the production state/report reader, not
	// a second detector or a fabricated producer layout.
	stateDir := filepath.Join(base, "private-state")
	writer, err := OpenWriter(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if err = writer.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	tx, err := writer.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	observedAt := time.Now().UnixNano()
	for _, observed := range observations {
		if err = ctx.Err(); err != nil {
			t.Fatal(err)
		}
		st := observed.stat
		if _, err = tx.ExecContext(ctx, `INSERT INTO entries(root_id,path,parent,kind,size,allocated,mtime_ns,ctime_ns,device,inode,generation,observed_at_ns)
 VALUES(1,?,?,?,?,?,?,?,?,?,1,?)`, []byte(observed.relative), []byte(observed.parent), observed.kind, st.Size, st.Blocks*512, st.Mtim.Sec*int64(time.Second)+st.Mtim.Nsec, st.Ctim.Sec*int64(time.Second)+st.Ctim.Nsec, fmt.Sprint(st.Dev), fmt.Sprint(st.Ino), observedAt); err != nil {
			t.Fatal(err)
		}
		if observed.kind == "directory" {
			if _, err = tx.ExecContext(ctx, `INSERT INTO directories(root_id,path,generation,complete,checked_at_ns) VALUES(1,?,1,1,?)`, []byte(observed.relative), observedAt); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	// Park, rather than delete/recreate, producer objects. All subsequent
	// reports must work without their saved source path and change no DB bytes.
	if err = os.Rename(root, filepath.Join(base, "parked-generated-projects")); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(stateDir, Filename))
	if err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReader(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen, cursors := map[string]bool{}, map[string]bool{}
	cursor, emptyContinuation, pages := "", false, 0
	for ; pages < 8; pages++ {
		report, err := reader.CargoBuildOutputs(ctx, cursor, 90)
		if err != nil {
			t.Fatal(err)
		}
		assertBuildOutputQualified(t, report)
		if len(report.Findings) == 0 && report.NextCursor != "" {
			emptyContinuation = true
		}
		for _, finding := range report.Findings {
			want, exists := expected[string(finding.PathBytes)]
			if !exists || seen[finding.Path] || finding.Profile != want.profile || !bytes.Equal(finding.PathBytes, []byte(finding.Path)) {
				t.Fatal("tool-produced Cargo finding differs from independent exact scope", finding)
			}
			measurement := finding.Measurement
			if measurement.Status != "recorded_complete" || measurement.LogicalBytes == nil || measurement.AllocatedBytes == nil || *measurement.LogicalBytes != want.logical || *measurement.AllocatedBytes != want.allocated || measurement.FilePaths != want.files || measurement.RepeatedInodes != want.repeated || measurement.UnknownInodes != 0 {
				t.Fatal("tool-produced Cargo size/hardlink metadata oracle differs", finding, want)
			}
			for i, marker := range finding.Markers {
				observed := want.markers[i]
				path := filepath.Join(root, observed.relative)
				if marker.Role != roles[i] || marker.Path != path || !bytes.Equal(marker.PathBytes, []byte(path)) || marker.Kind != observed.kind || marker.Device != fmt.Sprint(observed.stat.Dev) || marker.Inode != fmt.Sprint(observed.stat.Ino) || marker.CtimeNS != observed.stat.Ctim.Sec*int64(time.Second)+observed.stat.Ctim.Nsec || marker.Generation != 1 || marker.ParentGeneration != 1 || marker.ModifiedAt.UnixNano() != buildOutputOld || marker.ObservedAt.UnixNano() != observedAt {
					t.Fatal("tool-produced Cargo marker evidence differs", marker, observed)
				}
			}
			seen[finding.Path] = true
		}
		if report.NextCursor == "" {
			pages++
			break
		}
		if report.NextCursor == cursor || cursors[report.NextCursor] || pages == 7 {
			t.Fatal("tool-produced Cargo report exceeded finite pagination or repeated a cursor")
		}
		cursors[report.NextCursor], cursor = true, report.NextCursor
	}
	if len(seen) != len(expected) || !emptyContinuation || pages < 2 {
		t.Fatal("tool-produced Cargo exact target coverage or empty pagination differs", len(seen), len(expected), emptyContinuation, pages)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(stateDir, Filename))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("saved-only Cargo reader changed database bytes", err)
	}
	if _, err = os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("saved-only Cargo report recreated its absent source", err)
	}
	t.Logf("preinstalled exact cargo %s and rustc %s: genuine debug/release seven-marker layouts; %d independent metadata observations and %d saved-only report pages, including an empty continuing page; no regeneration or cleanup claim", versions.cargo, versions.rustc, len(observations), pages)
}
