package packaging

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

// Synthetic trusted hashes exercise the same private engine. They do not stand
// in for an actual public fixed-pin success or authenticated upstream texts.
func rootNoticeSynthetic(t *testing.T) (RootNoticeCatalog, [][]byte) {
	t.Helper()
	catalog := PinnedRootNoticeCatalog()
	bodies := make([][]byte, rootNoticeCount)
	for i := range catalog.Files {
		bodies[i] = bytes.Repeat([]byte{byte('A' + i)}, int(catalog.Files[i].Bytes))
		if i == 0 {
			copy(bodies[i], []byte("This paragraph shall stay with this work.\r\n"))
			bodies[i][len(bodies[i])-1] = 0xff
		}
		digest := sha256.Sum256(bodies[i])
		catalog.Files[i].SHA256 = hex.EncodeToString(digest[:])
	}
	return catalog, bodies
}

func rootNoticeFixedNow() rootNoticeTime {
	t := time.Unix(1700000000, 0)
	return rootNoticeTime{wall: t, elapsed: t}
}

func TestRootNoticeCandidatePreservesWholeRawBytesAndDeterminism(t *testing.T) {
	catalog, originals := rootNoticeSynthetic(t)
	var first RootNoticeCandidate
	for repeat := 0; repeat < 2; repeat++ {
		got, err := assembleRootNoticeCandidate(context.Background(), catalog, originals, catalog, rootNoticeFixedNow)
		if err != nil || got.Contract != RootNoticeContract || got.OriginalCount != 20 || got.MemberCount != 22 || got.Bytes > RootNoticeArchiveLimit || got.Bytes != int64(len(got.Archive)) {
			t.Fatal(got, err)
		}
		if !got.Qualifications.RootCatalogOnly || !got.Qualifications.HistoricalRetainedCopiesOnly || !got.Qualifications.RetainedCopyBytesVerified || got.Qualifications.ReleaseAccepted || got.Qualifications.UpstreamSourceAuthenticated || got.Qualifications.DistributionNoticeBundleComplete || got.Qualifications.WholeRootClauseAssessmentAccepted {
			t.Fatal(got.Qualifications)
		}
		checkRootNoticeArchive(t, got, catalog, originals)
		if repeat == 0 {
			first = got
		} else if !reflect.DeepEqual(first, got) {
			t.Fatal("same byte inputs produced different candidates")
		}
	}
	if !bytes.Contains(first.Archive, []byte("This paragraph shall stay with this work.\r\n")) {
		t.Fatal("complete nonmatching clause was lost")
	}
}

func checkRootNoticeArchive(t *testing.T, got RootNoticeCandidate, catalog RootNoticeCatalog, originals [][]byte) {
	t.Helper()
	digest := sha256.Sum256(got.Archive)
	if hex.EncodeToString(digest[:]) != got.SHA256 {
		t.Fatal("archive digest mismatch")
	}
	r := tar.NewReader(bytes.NewReader(got.Archive))
	var expectedSize int64 = (22 + 2) * 512
	for i := 0; i < 22; i++ {
		h, err := r.Next()
		if err != nil || h.Typeflag != tar.TypeReg || h.Format != tar.FormatUSTAR || h.Mode != 0644 || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" || !h.ModTime.Equal(time.Unix(0, 0)) || !h.AccessTime.IsZero() || !h.ChangeTime.IsZero() || h.Linkname != "" {
			t.Fatal(i, h, err)
		}
		body, err := io.ReadAll(io.LimitReader(r, RootNoticeFileLimit+1))
		if err != nil || int64(len(body)) != h.Size {
			t.Fatal(i, err)
		}
		expectedSize += (h.Size + 511) / 512 * 512
		if i < 20 {
			if h.Name != catalog.Files[i].Member || !bytes.Equal(body, originals[i]) {
				t.Fatal("original bytes/order changed", i, h.Name)
			}
		} else if i == 20 {
			if h.Name != "INDEX.json" || len(body) > RootNoticeIndexLimit || !bytes.HasSuffix(body, []byte{'\n'}) {
				t.Fatal(h)
			}
			var index rootNoticeIndex
			if err := json.Unmarshal(body, &index); err != nil || index.Contract != RootNoticeContract || index.InventoryRevision != RootNoticeInventoryRevision || index.SourceKind != "historical_retained_root_text_copies" || !reflect.DeepEqual(index.Files, catalog.Files) || !reflect.DeepEqual(index.TargetLabels, []string{"darwin-amd64", "darwin-arm64", "linux-amd64", "linux-arm64"}) || !reflect.DeepEqual(index.Qualifications, got.Qualifications) {
				t.Fatal(index, err)
			}
		} else if h.Name != "LIMITS.txt" || string(body) != rootNoticeLimitsText || len(body) > RootNoticeLimitsTextLimit {
			t.Fatal(h)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatal("unexpected member", err)
	}
	if int64(len(got.Archive)) != expectedSize || len(got.Archive) < 1024 || !bytes.Equal(got.Archive[len(got.Archive)-1024:], make([]byte, 1024)) {
		t.Fatal("noncanonical padding/trailer")
	}
}

func TestRootNoticePublicCatalogAndPinAdmission(t *testing.T) {
	first := PinnedRootNoticeCatalog()
	second := PinnedRootNoticeCatalog()
	*first.Files[2].ModuleSum = "changed"
	first.Files[0].Name = "changed"
	if !reflect.DeepEqual(second, PinnedRootNoticeCatalog()) {
		t.Fatal("caller mutated pinned admission")
	}
	for _, test := range []struct {
		name   string
		change func(*RootNoticeCatalog)
	}{
		{"contract", func(c *RootNoticeCatalog) { c.Contract = "other" }},
		{"revision", func(c *RootNoticeCatalog) { c.InventoryRevision = "other" }},
		{"toolchain", func(c *RootNoticeCatalog) { c.Toolchain = "other" }},
		{"module", func(c *RootNoticeCatalog) { c.Files[2].Component = "other" }},
		{"version", func(c *RootNoticeCatalog) { c.Files[2].Version = "other" }},
		{"sum", func(c *RootNoticeCatalog) { c.Files[2].ModuleSum = nil }},
		{"root_name", func(c *RootNoticeCatalog) { c.Files[0].Name = "other" }},
		{"member", func(c *RootNoticeCatalog) { c.Files[0].Member = "../LICENSE" }},
		{"duplicate", func(c *RootNoticeCatalog) { c.Files[1] = c.Files[0] }},
		{"order", func(c *RootNoticeCatalog) { c.Files[0], c.Files[1] = c.Files[1], c.Files[0] }},
		{"length", func(c *RootNoticeCatalog) { c.Files[0].Bytes++ }},
		{"digest", func(c *RootNoticeCatalog) { c.Files[0].SHA256 = "00" }},
		{"missing", func(c *RootNoticeCatalog) { c.Files = c.Files[:19] }},
		{"extra", func(c *RootNoticeCatalog) { c.Files = append(c.Files, c.Files[0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := PinnedRootNoticeCatalog()
			test.change(&catalog)
			got, err := AssembleRootNoticeCandidate(context.Background(), catalog, make([][]byte, 20))
			if !errors.Is(err, ErrRootNoticeInput) || len(got.Archive) != 0 {
				t.Fatal(got, err)
			}
		})
	}
	_, synthetics := rootNoticeSynthetic(t)
	if got, err := AssembleRootNoticeCandidate(context.Background(), PinnedRootNoticeCatalog(), synthetics); !errors.Is(err, ErrRootNoticeInput) || len(got.Archive) != 0 {
		t.Fatal("synthetic bodies were accepted as fixed originals", got, err)
	}
}

func TestRootNoticeInputAndMetadataBoundsRefuseWholeCandidate(t *testing.T) {
	for _, delta := range []int{-1, 1} {
		t.Run(map[int]string{-1: "short", 1: "extra"}[delta], func(t *testing.T) {
			catalog, bodies := rootNoticeSynthetic(t)
			bodies[0] = append(bodies[0], byte(0))[:len(bodies[0])+delta]
			got, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, rootNoticeFixedNow)
			if !errors.Is(err, ErrRootNoticeBounds) || len(got.Archive) != 0 {
				t.Fatal(got, err)
			}
		})
	}
	t.Run("file_cap", func(t *testing.T) {
		catalog, bodies := rootNoticeSynthetic(t)
		catalog.Files[0].Bytes = RootNoticeFileLimit + 1
		bodies[0] = make([]byte, RootNoticeFileLimit+1)
		got, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, rootNoticeFixedNow)
		if !errors.Is(err, ErrRootNoticeBounds) || len(got.Archive) != 0 {
			t.Fatal(got, err)
		}
	})
	t.Run("aggregate_cap", func(t *testing.T) {
		catalog, bodies := rootNoticeSynthetic(t)
		for i := range bodies {
			bodies[i] = make([]byte, 4096)
			catalog.Files[i].Bytes = 4096
			d := sha256.Sum256(bodies[i])
			catalog.Files[i].SHA256 = hex.EncodeToString(d[:])
		}
		got, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, rootNoticeFixedNow)
		if !errors.Is(err, ErrRootNoticeBounds) || len(got.Archive) != 0 {
			t.Fatal(got, err)
		}
	})
	t.Run("mutated_body", func(t *testing.T) {
		catalog, bodies := rootNoticeSynthetic(t)
		bodies[0][0] ^= 1
		got, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, rootNoticeFixedNow)
		if !errors.Is(err, ErrRootNoticeInput) || len(got.Archive) != 0 {
			t.Fatal(got, err)
		}
	})
	catalog, bodies := rootNoticeSynthetic(t)
	for _, sizes := range [][2]int{{RootNoticeIndexLimit + 1, 1}, {1, RootNoticeLimitsTextLimit + 1}} {
		g, err := newRootNoticeFence(context.Background(), rootNoticeFixedNow)
		if err != nil {
			t.Fatal(err)
		}
		if err := writeRootNoticeTar(g, catalog.Files, bodies, make([]byte, sizes[0]), make([]byte, sizes[1]), io.Discard); !errors.Is(err, ErrRootNoticeBounds) {
			t.Fatal(err)
		}
	}
}

func TestRootNoticeSnapshotDoesNotFollowLaterInputMutation(t *testing.T) {
	catalog, bodies := rootNoticeSynthetic(t)
	want := append([]byte(nil), bodies[0]...)
	calls := 0
	now := func() rootNoticeTime {
		calls++
		if calls == 22 {
			bodies[0][0] ^= 1
		}
		return rootNoticeFixedNow()
	}
	got, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, now)
	if err != nil {
		t.Fatal(err)
	}
	r := tar.NewReader(bytes.NewReader(got.Archive))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(body, want) || bytes.Equal(body, bodies[0]) {
		t.Fatal("snapshot was replaced by caller's later bytes", err)
	}
}

func TestRootNoticeDualChronologyAndCancellationRemainDenied(t *testing.T) {
	base := rootNoticeFixedNow()
	for _, domain := range []string{"wall", "elapsed"} {
		t.Run(domain+"_rollback_above_start", func(t *testing.T) {
			calls := 0
			now := func() rootNoticeTime {
				calls++
				n := base
				if calls > 1 {
					n.wall = n.wall.Add(time.Second)
					n.elapsed = n.elapsed.Add(time.Second)
				}
				if calls == 3 {
					if domain == "wall" {
						n.wall = base.wall.Add(time.Millisecond)
					} else {
						n.elapsed = base.elapsed.Add(time.Millisecond)
					}
				}
				return n
			}
			g, err := newRootNoticeFence(context.Background(), now)
			if err != nil || g.check() != nil {
				t.Fatal(err)
			}
			if !errors.Is(g.check(), ErrRootNoticeClock) || !errors.Is(g.check(), ErrRootNoticeClock) {
				t.Fatal("rollback denial was not retained")
			}
		})
		t.Run(domain+"_independent_expiry", func(t *testing.T) {
			calls := 0
			now := func() rootNoticeTime {
				calls++
				n := base
				if calls > 1 {
					if domain == "wall" {
						n.wall = n.wall.Add(5 * time.Second)
					} else {
						n.elapsed = n.elapsed.Add(5 * time.Second)
					}
				}
				return n
			}
			g, err := newRootNoticeFence(context.Background(), now)
			if err != nil || !errors.Is(g.check(), ErrRootNoticeBounds) {
				t.Fatal(err)
			}
		})
	}
	for _, invalid := range []rootNoticeTime{{}, {wall: time.Unix(0, 0), elapsed: time.Time{}}, {wall: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), elapsed: base.elapsed}} {
		if _, err := newRootNoticeFence(context.Background(), func() rootNoticeTime { return invalid }); !errors.Is(err, ErrRootNoticeClock) {
			t.Fatal(err)
		}
	}
	catalog, bodies := rootNoticeSynthetic(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := assembleRootNoticeCandidate(ctx, catalog, bodies, catalog, rootNoticeFixedNow); !errors.Is(err, context.Canceled) || len(got.Archive) != 0 {
		t.Fatal(got, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	g, err := newRootNoticeFence(ctx, rootNoticeFixedNow)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(g.check(), context.Canceled) || !errors.Is(g.check(), context.Canceled) {
		t.Fatal("cancellation denial was not retained")
	}
}

type rootNoticeFaultWriter struct {
	kind   string
	cancel context.CancelFunc
	calls  int
}

func (w *rootNoticeFaultWriter) Write(p []byte) (int, error) {
	w.calls++
	switch w.kind {
	case "error":
		return 0, io.ErrClosedPipe
	case "short":
		return len(p) - 1, nil
	case "cancel":
		w.cancel()
	}
	return len(p), nil
}

func TestRootNoticeOutputCapsErrorsAndTerminalVeto(t *testing.T) {
	g, err := newRootNoticeFence(context.Background(), rootNoticeFixedNow)
	if err != nil {
		t.Fatal(err)
	}
	b := &rootNoticeBuffer{fence: g}
	if n, err := b.Write(make([]byte, RootNoticeArchiveLimit)); n != RootNoticeArchiveLimit || err != nil {
		t.Fatal(n, err)
	}
	if n, err := b.Write([]byte{0}); n != 0 || !errors.Is(err, ErrRootNoticeBounds) || len(b.body) != RootNoticeArchiveLimit {
		t.Fatal(n, err)
	}
	catalog, bodies := rootNoticeSynthetic(t)
	for _, kind := range []string{"error", "short", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g, err := newRootNoticeFence(ctx, rootNoticeFixedNow)
			if err != nil {
				t.Fatal(err)
			}
			w := &rootNoticeFaultWriter{kind: kind, cancel: cancel}
			err = writeRootNoticeTar(g, catalog.Files, bodies, []byte("{}\n"), []byte("fixture\n"), w)
			want := map[string]error{"error": io.ErrClosedPipe, "short": io.ErrShortWrite, "cancel": context.Canceled}[kind]
			if !errors.Is(err, want) || w.calls == 0 {
				t.Fatal(err, w.calls)
			}
		})
	}
	baselineCalls := 0
	baselineNow := func() rootNoticeTime { baselineCalls++; return rootNoticeFixedNow() }
	baseline, err := assembleRootNoticeCandidate(context.Background(), catalog, bodies, catalog, baselineNow)
	if err != nil || baseline.MemberCount != 22 || len(baseline.Archive) == 0 || baselineCalls <= rootNoticeCount+1 {
		t.Fatal("completed baseline absent", baseline, err, baselineCalls)
	}
	terminalCall := baselineCalls
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	now := func() rootNoticeTime {
		calls++
		if calls == terminalCall {
			cancel()
		}
		return rootNoticeFixedNow()
	}
	if got, err := assembleRootNoticeCandidate(ctx, catalog, bodies, catalog, now); !errors.Is(err, context.Canceled) || len(got.Archive) != 0 || calls != terminalCall {
		t.Fatal("completed archive survived terminal denial", got, err, calls, terminalCall)
	}
}
