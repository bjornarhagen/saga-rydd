package worker

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

func TestAdaptiveRevisitDigestBindsLexicalPolicyWithoutFilesystem(t *testing.T) {
	roots, excludes, private := []string{"/generated/project"}, []string{"/generated/project/cache"}, []string{"/generated/state", "/generated/config"}
	beforeRoots, beforeExcludes, beforePrivate := append([]string(nil), roots...), append([]string(nil), excludes...), append([]string(nil), private...)
	digest, err := adaptiveRevisitDigest(roots, excludes, private, "/generated/user")
	if err != nil || len(digest) != 64 {
		t.Fatal(digest, err)
	}
	if !reflect.DeepEqual(roots, beforeRoots) || !reflect.DeepEqual(excludes, beforeExcludes) || !reflect.DeepEqual(private, beforePrivate) {
		t.Fatal("digest changed caller-owned slices")
	}
	protected := append(append([]string(nil), private...), "/proc", "/sys", "/dev", "/run", "/System", "/Library", "/usr", "/bin", "/sbin", "/etc", "/private/etc", "/generated/user/Library")
	expected, err := state.AdaptiveRevisitScopeDigest(roots, excludes, protected)
	if err != nil || expected != digest {
		t.Fatal("effective protection profile was omitted", expected, digest, err)
	}
	canonical, err := adaptiveRevisitDigest([]string{"/generated/project/."}, append(excludes, excludes[0]), []string{private[1], private[0], private[0]}, "/generated/user")
	if err != nil || canonical != digest {
		t.Fatal("equivalent lexical scope changed digest", canonical, err)
	}
	for _, mode := range []string{"root", "exclude", "private", "home"} {
		r, e, p, home := append([]string(nil), roots...), append([]string(nil), excludes...), append([]string(nil), private...), "/generated/user"
		switch mode {
		case "root":
			r[0] = "/generated/other"
		case "exclude":
			e[0] = filepath.Join(r[0], "different")
		case "private":
			p[0] = "/generated/other-state"
		case "home":
			home = "/generated/other-user"
		}
		other, err := adaptiveRevisitDigest(r, e, p, home)
		if err != nil || other == digest {
			t.Fatal("scope change did not reset identity", mode, other, err)
		}
	}
}

func TestAdaptiveRevisitSnapshotIsCachedAndIndependent(t *testing.T) {
	at := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	p := adaptiveRevisitPolicy{enabled: true, cached: &AdaptiveRevisitSnapshot{PolicyEnabled: true, CachedAt: at, AdaptiveRevisitStatus: state.AdaptiveRevisitStatus{Contract: state.AdaptiveRevisitContract, TrackedRoots: 2, WeeklyRoots: 1, DailyRoots: 1, HistoricalMetadataOnly: true}}}
	one, two := p.snapshot(), p.snapshot()
	one.WeeklyRoots, one.CachedAt = 0, at.Add(time.Hour)
	if two.WeeklyRoots != 1 || !two.CachedAt.Equal(at) || p.cached.WeeklyRoots != 1 || p.snapshot() == p.cached {
		t.Fatal("snapshot changed cached evidence")
	}
	if (&adaptiveRevisitPolicy{}).snapshot() != nil || p.startupPending() {
		t.Fatal("unconfigured profile invented state or startup work")
	}
}

func TestAdaptiveRevisitSnapshotHasBoundedControlProjection(t *testing.T) {
	p := AdaptiveRevisitSnapshot{PolicyEnabled: true, CachedAt: time.Unix(0, 1<<63-1).UTC(), AdaptiveRevisitStatus: state.AdaptiveRevisitStatus{Contract: state.AdaptiveRevisitContract, TrackedRoots: 32, ActiveEpochs: 32, ChangedEpochs: 32, UnknownEpochs: 32, StableRoots: 32, DailyRoots: 32, WeeklyRoots: 32, HistoricalMetadataOnly: true}}
	encoded, err := json.Marshal(map[string]*AdaptiveRevisitSnapshot{"adaptive_revisits": &p})
	if err != nil || len(encoded) > 356 {
		t.Fatal("adaptive control summary exceeded its bounded allocation", len(encoded), err)
	}
	var fields map[string]map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for key, value := range fields["adaptive_revisits"] {
		if len(value) > 0 && (value[0] == '[' || value[0] == '{') {
			t.Fatal("adaptive status added a per-root collection", key)
		}
	}
}
