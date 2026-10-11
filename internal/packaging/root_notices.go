// Package packaging keeps this root-text review candidate separate from product archives.
package packaging

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"time"
)

const (
	RootNoticeContract          = "root_notice_bundle_candidate_v1"
	RootNoticeCatalogContract   = "root_notice_candidate_catalog_v1"
	RootNoticeInventoryRevision = "d9f0bf3a046e89eda616d118bdd7f77fb00a8b84"
	RootNoticeInputLimit        = 65536
	RootNoticeFileLimit         = 16384
	RootNoticeIndexLimit        = 16384
	RootNoticeLimitsTextLimit   = 4096
	RootNoticeArchiveLimit      = 131072
	rootNoticeCount             = 20
	rootNoticeExactBytes        = 34954
	rootNoticeOperationLimit    = 5 * time.Second
)

var (
	ErrRootNoticeInput  = errors.New("root notice candidate requires the exact pinned twenty-row catalog and complete retained bytes")
	ErrRootNoticeBounds = errors.New("root notice candidate exceeds its fixed bounds")
	ErrRootNoticeClock  = errors.New("root notice candidate clock chronology is unavailable")
)

// RootNoticeSource contains attribution metadata only. It contains no source path or URL.
type RootNoticeSource struct {
	Component string  `json:"component"`
	Version   string  `json:"version"`
	ModuleSum *string `json:"module_sum"`
	Name      string  `json:"name"`
	Bytes     int64   `json:"bytes"`
	SHA256    string  `json:"sha256"`
	Member    string  `json:"member"`
}

type RootNoticeCatalog struct {
	Contract          string             `json:"contract"`
	InventoryRevision string             `json:"inventory_source_revision"`
	Toolchain         string             `json:"toolchain"`
	Files             []RootNoticeSource `json:"files"`
}

type RootNoticeQualifications struct {
	RootCatalogOnly                         bool `json:"root_catalog_only"`
	HistoricalRetainedCopiesOnly            bool `json:"historical_retained_copies_only"`
	RetainedCopyBytesVerified               bool `json:"retained_copy_bytes_verified"`
	CurrentDependencyCacheBodyAuthenticated bool `json:"current_dependency_cache_body_authenticated"`
	CurrentTargetClosureVerified            bool `json:"current_target_closure_verified"`
	CodeRetainedByLinkerVerified            bool `json:"code_retained_by_linker_verified"`
	CompleteEmbeddedNoticeAudit             bool `json:"complete_embedded_notice_audit"`
	WholeRootClauseAssessmentAccepted       bool `json:"whole_root_clause_assessment_accepted"`
	UpstreamSourceAuthenticated             bool `json:"upstream_source_authenticated"`
	ProjectLicenseSelected                  bool `json:"project_license_selected"`
	LicenseCompatibilityAccepted            bool `json:"license_compatibility_accepted"`
	DistributionMaterialsOwnerApproved      bool `json:"distribution_materials_owner_approved"`
	DistributionNoticeBundleComplete        bool `json:"distribution_notice_bundle_complete"`
	ReleaseAccepted                         bool `json:"release_accepted"`
}

// RootNoticeCandidate is an in-memory review candidate. The caller gets independent
// archive bytes. This function does not read sources, write files or publish a release.
type RootNoticeCandidate struct {
	Contract       string
	Archive        []byte
	SHA256         string
	Bytes          int64
	OriginalCount  int
	MemberCount    int
	Qualifications RootNoticeQualifications
}

type rootNoticeIndex struct {
	Contract          string                   `json:"contract"`
	InventoryRevision string                   `json:"inventory_source_revision"`
	Toolchain         string                   `json:"toolchain"`
	SourceKind        string                   `json:"source_kind"`
	TargetLabels      []string                 `json:"target_labels_association_only"`
	Files             []RootNoticeSource       `json:"files"`
	Qualifications    RootNoticeQualifications `json:"qualifications"`
}

const rootNoticeLimitsText = "Root notice review candidate only.\n" +
	"These twenty complete text byte slices are historical retained copies identified by the dated inventory.\n" +
	"The digest checks identify retained bytes, not authenticated upstream provenance or current dependency-cache contents.\n" +
	"This uniform root catalog does not prove current target closure, retained compiled code, asset applicability, or complete embedded/per-file notices.\n" +
	"Whole-root clause assessment, project license selection, compatibility, approved distribution materials and release acceptance remain open.\n" +
	"The ordinary candidate_package_v1 archive remains exactly rydd and BUILDINFO.json.\n"

func rootNoticeSum(s string) *string { return &s }

// PinnedRootNoticeCatalog returns a new fixed catalog, including independent sum
// pointers. Changing the returned metadata cannot change future pinned admission.
func PinnedRootNoticeCatalog() RootNoticeCatalog {
	return RootNoticeCatalog{Contract: RootNoticeCatalogContract, InventoryRevision: RootNoticeInventoryRevision, Toolchain: "go1.27.1", Files: fixedRootNoticeSources()}
}

func fixedRootNoticeSources() []RootNoticeSource {
	return []RootNoticeSource{
		{Component: "go-toolchain", Version: "go1.27.1", ModuleSum: nil, Name: "LICENSE", Bytes: 1453, SHA256: "911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad", Member: "THIRD-PARTY-NOTICES/01/LICENSE"},
		{Component: "go-toolchain", Version: "go1.27.1", ModuleSum: nil, Name: "PATENTS", Bytes: 1303, SHA256: "96f408bfae65bf137fc2525d3ecb030271c50c1e90799f87abf8846d8dd505cc", Member: "THIRD-PARTY-NOTICES/02/PATENTS"},
		{Component: "github.com/dustin/go-humanize", Version: "v1.0.1", ModuleSum: rootNoticeSum("h1:GzkhY7T5VNhEkwH0PVJgjz+fX1rhBrR7pRT3mDkpeCY="), Name: "LICENSE", Bytes: 1136, SHA256: "a973b4498c13eb74baa2a8e5c351426a6826f2fcdd909916dbe53ee2e755fd71", Member: "THIRD-PARTY-NOTICES/03/LICENSE"},
		{Component: "github.com/google/uuid", Version: "v1.6.0", ModuleSum: rootNoticeSum("h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0="), Name: "LICENSE", Bytes: 1480, SHA256: "0a8d61ed3cbfd5312326e8126c31ce9c627a283adc99131b56896d29ada04b2d", Member: "THIRD-PARTY-NOTICES/04/LICENSE"},
		{Component: "github.com/mattn/go-isatty", Version: "v0.0.24", ModuleSum: rootNoticeSum("h1:tGZZoVgT/KiqK1c8ocVLeDS8BSWMRd47J3Lbz7vsReI="), Name: "LICENSE", Bytes: 1099, SHA256: "08eab1118c80885fa1fa6a6dd7303f65a379fcb3733e063d20d1bbc2c76e6fa1", Member: "THIRD-PARTY-NOTICES/05/LICENSE"},
		{Component: "github.com/ncruces/go-strftime", Version: "v1.0.0", ModuleSum: rootNoticeSum("h1:HMFp8mLCTPp341M/ZnA4qaf7ZlsbTc+miZjCLOFAw7w="), Name: "LICENSE", Bytes: 1068, SHA256: "38ae43959daf953a393a585b2988672cb65a5a541aca0d0be5e72595a0a16883", Member: "THIRD-PARTY-NOTICES/06/LICENSE"},
		{Component: "github.com/pelletier/go-toml/v2", Version: "v2.4.3", ModuleSum: rootNoticeSum("h1:GTRvJQutkOSftxIFD5xw9aepkYNuPWmVJpffdDPYVpY="), Name: "LICENSE", Bytes: 1101, SHA256: "26844e4b53c5adec04e557fd7dfef281cc0205a7d355626b1c68b778b99e9e7b", Member: "THIRD-PARTY-NOTICES/07/LICENSE"},
		{Component: "github.com/remyoudompheng/bigfft", Version: "v0.0.0-20230129092748-24d4a6f8daec", ModuleSum: rootNoticeSum("h1:W09IVJc94icq4NjY3clb7Lk8O1qJ8BdBEF8z0ibU0rE="), Name: "LICENSE", Bytes: 1479, SHA256: "dd26a7abddd02e2d0aba97805b31f248ef7835d9e10da289b22e3b8ab78b324d", Member: "THIRD-PARTY-NOTICES/08/LICENSE"},
		{Component: "golang.org/x/sys", Version: "v0.47.0", ModuleSum: rootNoticeSum("h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs="), Name: "LICENSE", Bytes: 1453, SHA256: "911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad", Member: "THIRD-PARTY-NOTICES/09/LICENSE"},
		{Component: "golang.org/x/sys", Version: "v0.47.0", ModuleSum: rootNoticeSum("h1:o7XGOvZQCADBQQ4Y7VNq2dRWQR7JmOUW8Kxx4ZsNgWs="), Name: "PATENTS", Bytes: 1303, SHA256: "96f408bfae65bf137fc2525d3ecb030271c50c1e90799f87abf8846d8dd505cc", Member: "THIRD-PARTY-NOTICES/10/PATENTS"},
		{Component: "modernc.org/libc", Version: "v1.75.7", ModuleSum: rootNoticeSum("h1:o3DTP9/0p9pKmY2WCKQaySW6wIiZhNM7wc2lUoyhfew="), Name: "LICENSE", Bytes: 1482, SHA256: "95ff867eb55a56935fa7492406cfa953fb7c13ca73f4c0a86ae05756b4605600", Member: "THIRD-PARTY-NOTICES/11/LICENSE"},
		{Component: "modernc.org/libc", Version: "v1.75.7", ModuleSum: rootNoticeSum("h1:o3DTP9/0p9pKmY2WCKQaySW6wIiZhNM7wc2lUoyhfew="), Name: "LICENSE-3RD-PARTY.md", Bytes: 10505, SHA256: "f597097efe3d97021f89170746bd3a0fb9a8b6fb26b82043ed68a4e0283bee6c", Member: "THIRD-PARTY-NOTICES/12/LICENSE-3RD-PARTY.md"},
		{Component: "modernc.org/mathutil", Version: "v1.7.1", ModuleSum: rootNoticeSum("h1:GCZVGXdaN8gTqB1Mf/usp1Y/hSqgI2vAGGP4jZMCxOU="), Name: "LICENSE", Bytes: 1486, SHA256: "bfa9bf72a72ca009fd62a8f84fca3dca67e51d93af96352723646599898b6cf5", Member: "THIRD-PARTY-NOTICES/13/LICENSE"},
		{Component: "modernc.org/memory", Version: "v1.12.1", ModuleSum: rootNoticeSum("h1:nFMiWrpStgZczNl6XI9GnIk/rWhYIyHGUaR04pGbp9g="), Name: "LICENSE", Bytes: 1484, SHA256: "59895e669f48f168b6b858358f6005779cdf40a265f7828813061b56af67b496", Member: "THIRD-PARTY-NOTICES/14/LICENSE"},
		{Component: "modernc.org/memory", Version: "v1.12.1", ModuleSum: rootNoticeSum("h1:nFMiWrpStgZczNl6XI9GnIk/rWhYIyHGUaR04pGbp9g="), Name: "LICENSE-GO", Bytes: 1479, SHA256: "2d36597f7117c38b006835ae7f537487207d8ec407aa9d9980794b2030cbc067", Member: "THIRD-PARTY-NOTICES/15/LICENSE-GO"},
		{Component: "modernc.org/memory", Version: "v1.12.1", ModuleSum: rootNoticeSum("h1:nFMiWrpStgZczNl6XI9GnIk/rWhYIyHGUaR04pGbp9g="), Name: "LICENSE-LOGO", Bytes: 62, SHA256: "5ae5bee3072a841376451b48d8cfcec7188e10543926d5870828d36c8a750dc5", Member: "THIRD-PARTY-NOTICES/16/LICENSE-LOGO"},
		{Component: "modernc.org/memory", Version: "v1.12.1", ModuleSum: rootNoticeSum("h1:nFMiWrpStgZczNl6XI9GnIk/rWhYIyHGUaR04pGbp9g="), Name: "LICENSE-MMAP-GO", Bytes: 1518, SHA256: "c2eba69f20d05414538c3a5df7694dde392e065ff70882e1625e90f5d6659fff", Member: "THIRD-PARTY-NOTICES/17/LICENSE-MMAP-GO"},
		{Component: "modernc.org/sqlite", Version: "v1.59.0", ModuleSum: rootNoticeSum("h1:X1es1GpqBlS/5T+vbM4HLUdaa8OtQx468DF2vrx+38A="), Name: "LICENSE", Bytes: 1489, SHA256: "c6fe05491a60ae13bcd223088d2705e36dede24e5587226231d2459ada5c4822", Member: "THIRD-PARTY-NOTICES/18/LICENSE"},
		{Component: "modernc.org/sqlite", Version: "v1.59.0", ModuleSum: rootNoticeSum("h1:X1es1GpqBlS/5T+vbM4HLUdaa8OtQx468DF2vrx+38A="), Name: "LICENSE-SQLITE", Bytes: 1506, SHA256: "8438c9c89b849131ead81d5435cb97fcf052df5b0b286dda8a2d4c29e6cb3fd0", Member: "THIRD-PARTY-NOTICES/19/LICENSE-SQLITE"},
		{Component: "modernc.org/sqlite", Version: "v1.59.0", ModuleSum: rootNoticeSum("h1:X1es1GpqBlS/5T+vbM4HLUdaa8OtQx468DF2vrx+38A="), Name: "LICENSE-SQLITE_VEC", Bytes: 1068, SHA256: "6ce72bbe12d975bd5286e5ab0a064c069693300c47bccbc57bec18485f1621ea", Member: "THIRD-PARTY-NOTICES/20/LICENSE-SQLITE_VEC"},
	}
}

// AssembleRootNoticeCandidate admits exactly the fixed historical catalog. Inputs
// must not be changed concurrently with this call. It snapshots every admitted
// byte slice before serializing. It neither selects a license nor grants any action.
func AssembleRootNoticeCandidate(ctx context.Context, catalog RootNoticeCatalog, originals [][]byte) (RootNoticeCandidate, error) {
	return assembleRootNoticeCandidate(ctx, catalog, originals, PinnedRootNoticeCatalog(), rootNoticeProductionNow)
}

type rootNoticeTime struct{ wall, elapsed time.Time }
type rootNoticeNow func() rootNoticeTime

func rootNoticeProductionNow() rootNoticeTime {
	t := time.Now()
	return rootNoticeTime{wall: t.Round(0), elapsed: t}
}

type rootNoticeFence struct {
	ctx         context.Context
	now         rootNoticeNow
	start, high rootNoticeTime
	denial      error
}

func newRootNoticeFence(ctx context.Context, now rootNoticeNow) (*rootNoticeFence, error) {
	if ctx == nil || now == nil {
		return nil, ErrRootNoticeInput
	}
	g := &rootNoticeFence{ctx: ctx, now: now}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.start = now()
	g.high = g.start
	if !rootNoticeValidTime(g.start) {
		return nil, ErrRootNoticeClock
	}
	return g, nil
}

func rootNoticeValidTime(t rootNoticeTime) bool {
	return !t.wall.IsZero() && !t.elapsed.IsZero() && t.wall.Year() >= 1970 && t.wall.Year() <= 9999 && t.elapsed.Year() >= 1970 && t.elapsed.Year() <= 9999
}

func (g *rootNoticeFence) check() error {
	if g.denial != nil {
		return g.denial
	}
	if err := g.ctx.Err(); err != nil {
		g.denial = err
		return err
	}
	n := g.now()
	if err := g.ctx.Err(); err != nil {
		g.denial = err
		return err
	}
	if !rootNoticeValidTime(n) || n.wall.Before(g.high.wall) || n.elapsed.Before(g.high.elapsed) {
		g.denial = ErrRootNoticeClock
		return g.denial
	}
	g.high = n
	if n.wall.Sub(g.start.wall) >= rootNoticeOperationLimit || n.elapsed.Sub(g.start.elapsed) >= rootNoticeOperationLimit {
		g.denial = ErrRootNoticeBounds
		return g.denial
	}
	return nil
}

// This private trusted-pin seam lets generated tests exercise serialization with
// synthetic byte hashes. The public function always supplies the hardcoded catalog.
func assembleRootNoticeCandidate(ctx context.Context, catalog RootNoticeCatalog, originals [][]byte, trusted RootNoticeCatalog, now rootNoticeNow) (RootNoticeCandidate, error) {
	g, err := newRootNoticeFence(ctx, now)
	if err != nil {
		return RootNoticeCandidate{}, err
	}
	if len(catalog.Files) != rootNoticeCount || len(originals) != rootNoticeCount || !reflect.DeepEqual(catalog, trusted) {
		return RootNoticeCandidate{}, ErrRootNoticeInput
	}
	if catalog.Contract != RootNoticeCatalogContract || catalog.InventoryRevision != RootNoticeInventoryRevision || catalog.Toolchain != "go1.27.1" {
		return RootNoticeCandidate{}, ErrRootNoticeInput
	}
	var total int64
	bodies := make([][]byte, rootNoticeCount)
	rows := make([]RootNoticeSource, rootNoticeCount)
	for i, row := range catalog.Files {
		if err := g.check(); err != nil {
			return RootNoticeCandidate{}, err
		}
		if row.Bytes <= 0 || row.Bytes > RootNoticeFileLimit || int64(len(originals[i])) != row.Bytes || total > RootNoticeInputLimit-row.Bytes {
			return RootNoticeCandidate{}, ErrRootNoticeBounds
		}
		bodies[i] = append([]byte(nil), originals[i]...)
		digest := sha256.Sum256(bodies[i])
		if hex.EncodeToString(digest[:]) != row.SHA256 {
			return RootNoticeCandidate{}, ErrRootNoticeInput
		}
		total += row.Bytes
		rows[i] = row
		if row.ModuleSum != nil {
			rows[i].ModuleSum = rootNoticeSum(*row.ModuleSum)
		}
	}
	if total != rootNoticeExactBytes {
		return RootNoticeCandidate{}, ErrRootNoticeInput
	}
	qualifications := RootNoticeQualifications{RootCatalogOnly: true, HistoricalRetainedCopiesOnly: true, RetainedCopyBytesVerified: true}
	index, err := json.Marshal(rootNoticeIndex{Contract: RootNoticeContract, InventoryRevision: catalog.InventoryRevision, Toolchain: catalog.Toolchain, SourceKind: "historical_retained_root_text_copies", TargetLabels: []string{"darwin-amd64", "darwin-arm64", "linux-amd64", "linux-arm64"}, Files: rows, Qualifications: qualifications})
	if err != nil || len(index)+1 > RootNoticeIndexLimit || len(rootNoticeLimitsText) > RootNoticeLimitsTextLimit {
		return RootNoticeCandidate{}, errors.Join(ErrRootNoticeBounds, err)
	}
	index = append(index, '\n')
	buffer := &rootNoticeBuffer{body: make([]byte, 0, RootNoticeArchiveLimit), fence: g}
	if err := writeRootNoticeTar(g, rows, bodies, index, []byte(rootNoticeLimitsText), buffer); err != nil {
		return RootNoticeCandidate{}, err
	}
	if err := g.check(); err != nil {
		return RootNoticeCandidate{}, err
	}
	digest := sha256.Sum256(buffer.body)
	if err := g.check(); err != nil {
		return RootNoticeCandidate{}, err
	}
	return RootNoticeCandidate{Contract: RootNoticeContract, Archive: buffer.body, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(buffer.body)), OriginalCount: rootNoticeCount, MemberCount: rootNoticeCount + 2, Qualifications: qualifications}, nil
}

type rootNoticeBuffer struct {
	body  []byte
	fence *rootNoticeFence
}

func (b *rootNoticeBuffer) Write(p []byte) (int, error) {
	if err := b.fence.check(); err != nil {
		return 0, err
	}
	if len(p) > RootNoticeArchiveLimit-len(b.body) {
		return 0, ErrRootNoticeBounds
	}
	b.body = append(b.body, p...)
	return len(p), nil
}

func writeRootNoticeTar(g *rootNoticeFence, rows []RootNoticeSource, bodies [][]byte, index, limits []byte, output io.Writer) error {
	if len(rows) != rootNoticeCount || len(bodies) != rootNoticeCount || len(index) > RootNoticeIndexLimit || len(limits) > RootNoticeLimitsTextLimit || output == nil {
		return ErrRootNoticeBounds
	}
	tw := tar.NewWriter(output)
	var failure error
	write := func(name string, body []byte) error {
		if err := g.check(); err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(body)), Typeflag: tar.TypeReg, Uid: 0, Gid: 0, ModTime: time.Unix(0, 0), Format: tar.FormatUSTAR}); err != nil {
			return err
		}
		n, err := tw.Write(body)
		if err == nil && n != len(body) {
			err = io.ErrShortWrite
		}
		return errors.Join(err, g.check())
	}
	for i, row := range rows {
		if failure = write(row.Member, bodies[i]); failure != nil {
			break
		}
	}
	if failure == nil {
		failure = write("INDEX.json", index)
	}
	if failure == nil {
		failure = write("LIMITS.txt", limits)
	}
	closeErr := tw.Close()
	return errors.Join(failure, closeErr, g.check())
}
