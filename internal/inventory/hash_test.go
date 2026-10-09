package inventory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"golang.org/x/sys/unix"
)

func fullHashContents(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func fullHashSession(t *testing.T, s *Scanner, target SavedFileTarget) *FullHashSession {
	t.Helper()
	session, err := s.NewFullHashSession(target)
	if err != nil || session == nil {
		t.Fatal("valid fixture did not create a hash session", err)
	}
	return session
}

func requireFullHashClaims(t *testing.T, p FullHashProgress) {
	t.Helper()
	if p.Contract != FileHashContract || p.Source == "" || p.ProvenanceVerified || p.ContentVerified || p.CurrentStateVerified || p.DuplicatesVerified || p.Executable || p.EstimatedReclaimableBytes != nil {
		t.Fatal("full-hash observation made a stronger claim", p)
	}
}

func requireFullHashDigest(t *testing.T, p FullHashProgress, data []byte) {
	t.Helper()
	// Compute from the complete fixture independently of continuation offsets,
	// streaming state, chunk selection and any private production serializer.
	want := sha256.Sum256(data)
	if p.Status != "hash_observed" || p.Offset != int64(len(data)) || p.LogicalBytes != int64(len(data)) || p.SHA256 != hex.EncodeToString(want[:]) {
		t.Fatal("full-file digest differs from independent expected bytes", p)
	}
	requireFullHashClaims(t, p)
}

func requireFullHashInvalid(t *testing.T, p FullHashProgress, usage FileReadUsage, err error) {
	t.Helper()
	if err == nil || p.Status != "invalidated" || p.SHA256 != "" || usage.RequestedBytes < usage.ReadBytes || usage.ReadBytes < 0 || usage.RequestedBytes > FileHashStepByteLimit {
		t.Fatal("uncertain file published usable hash state", p, usage, err)
	}
	requireFullHashClaims(t, p)
}

func TestFullHashKnownDigestsAndBlockBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 63, 64, 65, 127, 128, 129, 32767, 32768, 32769, 1024*1024 + 65} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := fullHashContents(size)
			s, targets := sampleFixture(t, data)
			session := fullHashSession(t, s, targets[0])
			allowances := []int64{1, 7, 31, 33, 65, 16385, 32767, 65539, FileHashStepByteLimit}
			var offset, requested, read int64
			for step := 0; step < 80; step++ {
				allowance := allowances[min(step, len(allowances)-1)]
				p, usage, err := session.Step(context.Background(), allowance)
				if err != nil || p.Offset < offset || p.Offset-offset > allowance || usage.RequestedBytes != p.Offset-offset || usage.ReadBytes != usage.RequestedBytes || usage.Elapsed < 0 {
					t.Fatal("irregular allowance skipped/repeated bytes or misreported usage", p, usage, err)
				}
				requireFullHashClaims(t, p)
				requested += usage.RequestedBytes
				read += usage.ReadBytes
				offset = p.Offset
				if p.Status == "hash_observed" {
					requireFullHashDigest(t, p, data)
					if requested != int64(size) || read != int64(size) || p.CheckedAt.IsZero() || !bytes.Equal(p.PathBytes, targets[0].File.PathBytes) {
						t.Fatal("wrong final evidence or cumulative usage", p, requested, read)
					}
					encoded, err := json.Marshal(p)
					if err != nil || bytes.Contains(encoded, []byte("hash_state")) || bytes.Contains(encoded, []byte("source_bytes")) {
						t.Fatal("opaque state escaped through progress", string(encoded), err)
					}
					return
				}
				if p.Status != "partial" || p.SHA256 != "" {
					t.Fatal("partial progress published a digest", p)
				}
			}
			t.Fatal("bounded known-digest fixture did not finish")
		})
	}
	// These public SHA-256 vectors also guard against accidental sample framing.
	for _, vector := range []struct{ data, digest string }{
		{"", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
		{"abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
	} {
		s, targets := sampleFixture(t, []byte(vector.data))
		p, _, err := fullHashSession(t, s, targets[0]).Step(context.Background(), 64)
		if err != nil || p.SHA256 != vector.digest {
			t.Fatal("standard unframed digest changed", p, err)
		}
	}
}

func TestFullHashStepByteAndChunkLimits(t *testing.T) {
	s, targets := sampleFixture(t, fullHashContents(2*1024*1024+1))
	session := fullHashSession(t, s, targets[0])
	for _, allowance := range []int64{0, -1, FileHashStepByteLimit + 1, math.MaxInt64} {
		p, usage, err := session.Step(context.Background(), allowance)
		if err == nil || p.Offset != 0 || p.SHA256 != "" || usage.RequestedBytes != 0 || usage.ReadBytes != 0 {
			t.Fatal("out-of-range allowance was accepted or changed state", allowance, p, usage, err)
		}
	}
	actual, calls := int64(0), 0
	p, usage, err := session.step(context.Background(), FileHashStepByteLimit, fileHashHooks{afterRead: func(n int) {
		if n > 32*1024 {
			t.Fatal("hash read exceeded 32 KiB", n)
		}
		actual += int64(n)
		calls++
	}})
	if err != nil || p.Status != "partial" || p.Offset != FileHashStepByteLimit || p.SHA256 != "" || actual != FileHashStepByteLimit || calls != 32 || usage.RequestedBytes != FileHashStepByteLimit || usage.ReadBytes != FileHashStepByteLimit {
		t.Fatal("actual reads exceeded or failed to meet fixed limits", p, usage, actual, calls, err)
	}
}

func TestFullHashAlternatingSessionsAndCopiedEvidence(t *testing.T) {
	large, small := fullHashContents(2*1024*1024+65), []byte("a small independent fixture")
	s, targets := sampleFixture(t, large, small)
	first := fullHashSession(t, s, targets[0])
	second := fullHashSession(t, s, targets[1])
	wantPath := bytes.Clone(targets[0].File.PathBytes)
	// Constructor evidence belongs to the session, not the caller's mutable
	// root/file/ancestor byte slices or display string.
	targets[0].Root.PathBytes[0] = 'x'
	targets[0].File.PathBytes[0] = 'x'
	targets[0].Ancestors[0].Path[0] = 'x'
	targets[0].File.Path = "changed display"
	p, _, err := first.Step(context.Background(), 63)
	if err != nil || p.Offset != 63 || !bytes.Equal(p.PathBytes, wantPath) {
		t.Fatal("constructor did not clone exact evidence", p, err)
	}
	encoded, err := json.Marshal(first)
	if err != nil || string(encoded) != "{}" {
		t.Fatal("private in-memory SHA continuation was serializable", string(encoded), err)
	}
	// Mutating returned display evidence must not rewrite the private session.
	p.PathBytes[0] = 'x'
	short, _, err := second.Step(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	requireFullHashDigest(t, short, small)
	for i := 0; i < 4; i++ {
		p, _, err = first.Step(context.Background(), FileHashStepByteLimit)
		if err != nil || !bytes.Equal(p.PathBytes, wantPath) {
			t.Fatal("alternating session lost checked continuation", p, err)
		}
		if p.Status == "hash_observed" {
			requireFullHashDigest(t, p, large)
			return
		}
	}
	t.Fatal("large session did not finish after small session")
}

func TestFullHashCopiedWrappersAndConcurrentCalls(t *testing.T) {
	data := fullHashContents(256 * 1024)
	s, targets := sampleFixture(t, data)
	session := fullHashSession(t, s, targets[0])
	copy := *session
	p, _, err := copy.Step(context.Background(), 63)
	if err != nil || p.Offset != 63 {
		t.Fatal(p, err)
	}
	p, _, err = session.Step(context.Background(), 65)
	if err != nil || p.Offset != 128 {
		t.Fatal("wrapper copy forked the hash state", p, err)
	}
	const parallel = 8
	type result struct {
		progress FullHashProgress
		usage    FileReadUsage
		err      error
	}
	results := make(chan result, parallel)
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wrapper := *session
			p, u, err := wrapper.Step(context.Background(), 4096)
			results <- result{p, u, err}
		}()
	}
	wg.Wait()
	close(results)
	seen := map[int64]bool{}
	for r := range results {
		if r.err != nil || r.usage.RequestedBytes != 4096 || r.usage.ReadBytes != 4096 || r.progress.SHA256 != "" || seen[r.progress.Offset] || r.progress.Offset < 128+4096 || r.progress.Offset > 128+parallel*4096 || (r.progress.Offset-128)%4096 != 0 {
			t.Fatal("concurrent wrappers forked/repeated progress", r)
		}
		seen[r.progress.Offset] = true
	}
	p, _, err = copy.Step(context.Background(), FileHashStepByteLimit)
	if err != nil {
		t.Fatal(err)
	}
	requireFullHashDigest(t, p, data)
}

func TestFullHashCancellationUsageAndRollback(t *testing.T) {
	for _, phase := range []string{"before", "after_open", "during", "final", "deadline"} {
		t.Run(phase, func(t *testing.T) {
			data := fullHashContents(256 * 1024)
			s, targets := sampleFixture(t, data)
			session := fullHashSession(t, s, targets[0])
			if p, _, err := session.Step(context.Background(), 65); err != nil || p.Offset != 65 {
				t.Fatal(p, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reads := 0
			hooks := fileHashHooks{afterRead: func(int) {
				reads++
				if phase == "during" {
					cancel()
				}
			}}
			switch phase {
			case "before":
				cancel()
			case "after_open":
				hooks.afterOpen = cancel
			case "final":
				hooks.beforeFinalCheck = cancel
			case "deadline":
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			}
			p, usage, err := session.step(ctx, 64*1024, hooks)
			wantErr := context.Canceled
			if phase == "deadline" {
				wantErr = context.DeadlineExceeded
			}
			if !errors.Is(err, wantErr) || p.Offset != 65 || p.SHA256 != "" || usage.RequestedBytes != usage.ReadBytes {
				t.Fatal("cancellation committed tentative hash state or lost usage", p, usage, err)
			}
			wantReads := 0
			if phase == "during" {
				wantReads = 1
			}
			if phase == "final" {
				wantReads = 2
			}
			if reads != wantReads || usage.ReadBytes != int64(wantReads)*32*1024 {
				t.Fatal("cancellation read accounting is incorrect", reads, usage)
			}
			p, _, err = session.Step(context.Background(), FileHashStepByteLimit)
			if err != nil {
				t.Fatal(err)
			}
			requireFullHashDigest(t, p, data)
		})
	}
}

func TestFullHashSoftReadDeadlinePublishesCheckedProgress(t *testing.T) {
	s, targets := sampleFixture(t, fullHashContents(128*1024))
	session := fullHashSession(t, s, targets[0])
	p, usage, err := session.step(context.Background(), FileHashStepByteLimit, fileHashHooks{readPhaseLimit: 100 * time.Millisecond, afterRead: func(int) { time.Sleep(150 * time.Millisecond) }})
	if err != nil || p.Status != "partial" || p.Offset != 32*1024 || usage.RequestedBytes != p.Offset || usage.ReadBytes != p.Offset || p.CheckedAt.IsZero() || p.SHA256 != "" {
		t.Fatal("soft deadline did not reserve time for checked partial progress", p, usage, err)
	}
}

func TestFullHashCanceledConcurrentWaiterDoesNotChangeState(t *testing.T) {
	s, targets := sampleFixture(t, fullHashContents(256*1024))
	session := fullHashSession(t, s, targets[0])
	prior, _, err := session.Step(context.Background(), 65)
	if err != nil || prior.Offset != 65 {
		t.Fatal(prior, err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	type result struct {
		p     FullHashProgress
		usage FileReadUsage
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		p, usage, err := session.step(context.Background(), 64, fileHashHooks{afterOpen: func() { close(entered); <-release }})
		finished <- result{p, usage, err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("hash step did not enter deterministic gate fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waiter := *session
	p, usage, err := waiter.Step(ctx, 64)
	if !errors.Is(err, context.DeadlineExceeded) || p.Offset != 65 || p.SHA256 != "" || usage.RequestedBytes != 0 || usage.ReadBytes != 0 || !bytes.Equal(p.PathBytes, targets[0].File.PathBytes) || p.InventoryID != prior.InventoryID || p.FileID != prior.FileID || p.CheckedAt != prior.CheckedAt {
		t.Fatal("canceled gate waiter read or changed shared hash state", p, usage, err)
	}
	requireFullHashClaims(t, p)
	p.PathBytes[0] = 'x'
	unblock()
	select {
	case r := <-finished:
		if r.err != nil || r.p.Offset != 129 || r.usage.ReadBytes != 64 || !bytes.Equal(r.p.PathBytes, targets[0].File.PathBytes) {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("held step did not finish after canceled waiter")
	}
	p, _, err = session.Step(context.Background(), 64)
	if err != nil || p.Offset != 193 {
		t.Fatal("canceled waiter forked progress", p, err)
	}
}

func TestFullHashScannerCloseDuringReadInvalidates(t *testing.T) {
	s, targets := sampleFixture(t, fullHashContents(256*1024))
	session := fullHashSession(t, s, targets[0])
	p, usage, err := session.step(context.Background(), 64*1024, fileHashHooks{afterRead: func(int) { s.Close() }})
	requireFullHashInvalid(t, p, usage, err)
	if p.Offset != 0 || usage.RequestedBytes != 32*1024 || usage.ReadBytes != 32*1024 {
		t.Fatal("close lost usage or committed unchecked progress", p, usage)
	}
}

func mutateFullHashFixture(t *testing.T, kind, path, root string, modified time.Time) {
	t.Helper()
	switch kind {
	case "edit", "restored_mtime":
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteAt([]byte{255}, 65535)
		_ = f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if kind == "restored_mtime" {
			if err := os.Chtimes(path, modified, modified); err != nil {
				t.Fatal(err)
			}
		}
	case "truncate":
		if err := os.Truncate(path, 0); err != nil {
			t.Fatal(err)
		}
	case "mode":
		if err := os.Chmod(path, 0400); err != nil {
			t.Fatal(err)
		}
	default:
		location := path
		if kind == "parent_replace" || kind == "parent_symlink" {
			location = filepath.Dir(path)
		}
		if kind == "root_replace" || kind == "root_symlink" {
			location = root
		}
		if err := os.Rename(location, location+".old"); err != nil {
			t.Fatal(err)
		}
		switch kind {
		case "file_replace":
			if err := os.WriteFile(location, fullHashContents(256*1024), 0600); err != nil {
				t.Fatal(err)
			}
		case "file_fifo":
			if err := unix.Mkfifo(location, 0600); err != nil {
				t.Fatal(err)
			}
		case "parent_replace", "root_replace":
			if err := os.Mkdir(location, 0700); err != nil {
				t.Fatal(err)
			}
		case "missing":
		default:
			if err := os.Symlink(location+".old", location); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestFullHashChangesBetweenAndDuringSteps(t *testing.T) {
	for _, phase := range []string{"between", "during"} {
		for _, kind := range []string{"edit", "restored_mtime", "truncate", "mode", "file_replace", "file_symlink", "file_fifo", "missing", "parent_replace", "parent_symlink", "root_replace", "root_symlink"} {
			t.Run(phase+"/"+kind, func(t *testing.T) {
				s, targets := sampleFixture(t, fullHashContents(256*1024))
				target := targets[0]
				session := fullHashSession(t, s, target)
				if p, _, err := session.Step(context.Background(), 65); err != nil || p.Offset != 65 {
					t.Fatal(p, err)
				}
				change := func() {
					mutateFullHashFixture(t, kind, string(target.File.PathBytes), string(target.Root.PathBytes), target.File.ModifiedAt)
				}
				hooks := fileHashHooks{}
				if phase == "between" {
					change()
				} else {
					hooks.beforeFinalCheck = change
				}
				p, usage, err := session.step(context.Background(), 64*1024, hooks)
				requireFullHashInvalid(t, p, usage, err)
				if p.Offset != 65 || (phase == "between" && usage.RequestedBytes != 0) || (phase == "during" && usage.ReadBytes != 64*1024) {
					t.Fatal("changed file committed new progress", p, usage)
				}
				p, usage, err = session.Step(context.Background(), 64)
				requireFullHashInvalid(t, p, usage, err)
				if usage.RequestedBytes != 0 {
					t.Fatal("invalidated session resumed reads", usage)
				}
			})
		}
	}
}

func TestFullHashShortReadChargesAttemptAndInvalidates(t *testing.T) {
	s, targets := sampleFixture(t, fullHashContents(256*1024))
	session := fullHashSession(t, s, targets[0])
	p, usage, err := session.step(context.Background(), 64*1024, fileHashHooks{afterOpen: func() {
		if err := os.Truncate(string(targets[0].File.PathBytes), 0); err != nil {
			t.Fatal(err)
		}
	}})
	requireFullHashInvalid(t, p, usage, err)
	if usage.RequestedBytes != 32*1024 || usage.ReadBytes != 0 || p.Offset != 0 {
		t.Fatal("failed read was not charged or committed progress", p, usage)
	}
}

func TestFullHashCompletedSessionRevalidates(t *testing.T) {
	s, targets := sampleFixture(t, []byte("abc"))
	session := fullHashSession(t, s, targets[0])
	p, _, err := session.Step(context.Background(), 64)
	if err != nil {
		t.Fatal(err)
	}
	requireFullHashDigest(t, p, []byte("abc"))
	p, usage, err := session.Step(context.Background(), 64)
	if err != nil || usage.ReadBytes != 0 || usage.RequestedBytes != 0 {
		t.Fatal(p, usage, err)
	}
	requireFullHashDigest(t, p, []byte("abc"))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	p, usage, err = session.Step(canceled, 64)
	if !errors.Is(err, context.Canceled) || p.SHA256 != "" || usage.RequestedBytes != 0 || usage.ReadBytes != 0 {
		t.Fatal("canceled completed-session query returned a cached digest", p, usage, err)
	}
	if err := os.WriteFile(string(targets[0].File.PathBytes), []byte("xyz"), 0600); err != nil {
		t.Fatal(err)
	}
	p, usage, err = session.Step(context.Background(), 64)
	requireFullHashInvalid(t, p, usage, err)
	if usage.RequestedBytes != 0 {
		t.Fatal("stale completed hash caused content reads", usage)
	}
}

func TestFullHashEvidenceScopeAndUnsupportedPaths(t *testing.T) {
	for _, kind := range []string{"bad_inventory", "long_path", "unknown_ancestor", "excluded", "protected_alias", "scope_removed", "generated_parent", "generated_root", "file_symlink", "file_fifo", "closed_scanner"} {
		t.Run(kind, func(t *testing.T) {
			s, targets := sampleFixture(t, fullHashContents(256*1024))
			target := targets[0]
			path, root := string(target.File.PathBytes), string(target.Root.PathBytes)
			switch kind {
			case "bad_inventory":
				target.InventoryID = "unknown"
			case "long_path":
				target.File.PathBytes = bytes.Repeat([]byte{'x'}, 4097)
			case "unknown_ancestor":
				target.Ancestors[0].Inode = ""
			case "excluded":
				s.excludes = append(s.excludes, path)
			case "protected_alias":
				s.protectedIDs[target.File.Device+":"+target.File.Inode] = true
			case "scope_removed":
				s.roots = map[string]bool{}
			case "closed_scanner":
				s.Close()
			case "generated_parent", "generated_root":
				parent := filepath.Join(root, "node_modules")
				if err := os.Rename(filepath.Dir(path), parent); err != nil {
					t.Fatal(err)
				}
				path = filepath.Join(parent, filepath.Base(path))
				if kind == "generated_root" {
					root = parent
					s.roots = map[string]bool{root: true}
				}
				target = captureSampleTargets(t, s, root, []string{path})[0]
			default:
				mutateFullHashFixture(t, kind, path, root, target.File.ModifiedAt)
			}
			session, err := s.NewFullHashSession(target)
			if err != nil {
				return
			}
			p, usage, err := session.Step(context.Background(), 64)
			requireFullHashInvalid(t, p, usage, err)
			if usage.ReadBytes != 0 || usage.RequestedBytes != 0 {
				t.Fatal("unsupported source was read", usage)
			}
		})
	}
}

func TestFullHashFromProductionInventory(t *testing.T) {
	ctx := context.Background()
	data := fullHashContents(1024*1024 + 65)
	s, targets := sampleFixture(t, data, bytes.Clone(data))
	root := string(targets[0].Root.PathBytes)
	stateDir := filepath.Join(t.TempDir(), "state")
	writer, err := state.OpenWriter(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })
	if err := writer.SyncRoots(ctx, []string{root}); err != nil {
		t.Fatal(err)
	}
	compact := false
	if _, err := writer.ConfigureCompact(ctx, &compact); err != nil {
		t.Fatal(err)
	}
	if err := writer.SeedInventory(ctx); err != nil {
		t.Fatal(err)
	}
	finished := false
	for chunk := 0; chunk < 12; chunk++ {
		job, err := writer.ClaimJob(ctx, []string{state.ScanKind}, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if job == nil {
			finished = true
			break
		}
		batch, err := s.Next(ctx, *job)
		if err != nil || batch.Fault != "" {
			t.Fatal(batch, err)
		}
		if err := writer.CommitScan(ctx, *job, batch); err != nil {
			t.Fatal(err)
		}
	}
	if !finished {
		t.Fatal("bounded production hash fixture did not finish scanning")
	}
	finishSelectionFixtureMaintenance(t, ctx, writer, s)
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := state.OpenReader(ctx, stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	saved, err := reader.SameSizeCandidates(ctx, 20, "", state.SameSizeMinimumBytes)
	if err != nil || len(saved.Bands) != 1 || len(saved.Bands[0].Files) != 2 {
		t.Fatal(saved, err)
	}
	captured, err := reader.PrepareFileSampleSelection(ctx, saved.InventoryID, []state.SameSizeFile{saved.Bands[0].Files[0]})
	if err != nil || len(captured) != 1 {
		t.Fatal(captured, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	session := fullHashSession(t, s, captured[0])
	var p FullHashProgress
	for step := 0; step < 4; step++ {
		p, _, err = session.Step(ctx, FileHashStepByteLimit)
		if err != nil {
			t.Fatal(err)
		}
		if p.Status == "hash_observed" {
			requireFullHashDigest(t, p, data)
			return
		}
	}
	t.Fatal("bounded production capture/hash fixture did not finish", p)
}

func TestFullHashAuthoritativeUnixNameJSON(t *testing.T) {
	for _, name := range []string{"node_modules", "ordinary-ø\n\"\t", "ordinary-\xff"} {
		t.Run(fmt.Sprintf("%x", []byte(name)), func(t *testing.T) {
			s, targets := sampleFixture(t, []byte("abc"))
			path := filepath.Join(filepath.Dir(string(targets[0].File.PathBytes)), name)
			requireSampleFixtureFilename(t, name, os.Rename(string(targets[0].File.PathBytes), path))
			target := captureSampleTargets(t, s, string(targets[0].Root.PathBytes), []string{path})[0]
			encoded, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &target); err != nil {
				t.Fatal(err)
			}
			target.File.Path = "ignored display path"
			p, _, err := fullHashSession(t, s, target).Step(context.Background(), 64)
			if err != nil || !bytes.Equal(p.PathBytes, []byte(path)) {
				t.Fatal("full hash used a lossy/display Unix path", p, err)
			}
			requireFullHashDigest(t, p, []byte("abc"))
		})
	}
}
