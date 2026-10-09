package dockerinfo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

const CacheScope = "engine_embedded_cache"

// CacheRecord is a projection of Engine API 1.44 BuildCache metadata. Missing
// or null optional observations remain unknown. SizeBytes is always nil: the
// daemon may calculate sizes, but this profile does not report them.
type CacheRecord struct {
	ID         string     `json:"id"`
	Type       string     `json:"type"`
	InUse      *bool      `json:"in_use"`
	Shared     *bool      `json:"shared"`
	CreatedAt  *time.Time `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
	UsageCount *int64     `json:"usage_count"`
	SizeBytes  *int64     `json:"size_bytes"`
}

// CacheReport covers only the selected Engine's embedded cache. The API
// supplies no separately named builder instance or generation. Internal daemon
// accounting may change even though Rydd sends only the fixed GET requests.
type CacheReport struct {
	ContextName               string        `json:"context_name"`
	Endpoint                  string        `json:"endpoint"`
	APIVersion                string        `json:"api_version"`
	DaemonID                  string        `json:"daemon_id"`
	ServerVersion             string        `json:"server_version"`
	StartedAt                 time.Time     `json:"started_at"`
	CompletedAt               time.Time     `json:"completed_at"`
	Scope                     string        `json:"scope"`
	Records                   []CacheRecord `json:"records"`
	SequentialObservations    bool          `json:"sequential_observations"`
	DaemonAccountingMayChange bool          `json:"daemon_accounting_may_change"`
	AtomicSnapshot            bool          `json:"atomic_snapshot"`
	PhysicalLocalityVerified  bool          `json:"physical_locality_verified"`
	NamespaceAuthenticated    bool          `json:"namespace_authenticated"`
	SizesMeasured             bool          `json:"sizes_measured"`
	CurrentStateVerified      bool          `json:"current_state_verified"`
	CleanupApproved           bool          `json:"cleanup_approved"`
	Executable                bool          `json:"executable"`
	SavingsBytes              *int64        `json:"savings_bytes"`
	Persisted                 bool          `json:"persisted"`
	BuilderPinned             bool          `json:"builder_pinned"`
	CacheChecked              bool          `json:"cache_checked"`
	ImagesChecked             bool          `json:"images_checked"`
	ContainersChecked         bool          `json:"containers_checked"`
	VolumesChecked            bool          `json:"volumes_checked"`
}

func DiscoverCache(ctx context.Context, name string) (CacheReport, error) {
	return DiscoverCacheWithResolver(ctx, name, resolveContext)
}

func DiscoverCacheWithResolver(ctx context.Context, name string, resolver Resolver) (CacheReport, error) {
	return discoverCache(ctx, name, resolver, (&net.Dialer{}).DialContext)
}

func discoverCache(parent context.Context, name string, resolver Resolver, dial func(context.Context, string, string) (net.Conn, error)) (CacheReport, error) {
	ctx, cancel := context.WithTimeout(parent, OperationLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return CacheReport{}, err
	}
	if !ValidContextName(name) || resolver == nil {
		return CacheReport{}, ErrContext
	}
	started := time.Now().UTC()
	resolved, err := resolver(ctx, name)
	if err != nil {
		return CacheReport{}, errors.Join(ErrResolver, err, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return CacheReport{}, err
	}
	if resolved.ContextName != name {
		return CacheReport{}, fmt.Errorf("%w: selected context changed", ErrResolver)
	}
	path, err := socketPath(resolved.Endpoint)
	if err != nil {
		return CacheReport{}, err
	}
	conn, err := dial(ctx, "unix", path)
	if err != nil {
		return CacheReport{}, errors.Join(ErrProtocol, err, ctx.Err())
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return CacheReport{}, errors.Join(ErrProtocol, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	w := newWire(conn)
	before, err := w.info(ctx)
	if err != nil {
		return CacheReport{}, errors.Join(err, ctx.Err())
	}
	records, err := w.cache(ctx)
	if err != nil {
		return CacheReport{}, errors.Join(err, ctx.Err())
	}
	after, err := w.info(ctx)
	if err != nil {
		return CacheReport{}, errors.Join(err, ctx.Err())
	}
	if before.ID != after.ID || before.ServerVersion != after.ServerVersion {
		return CacheReport{}, ErrDaemonChanged
	}
	if err := ctx.Err(); err != nil {
		return CacheReport{}, err
	}
	return CacheReport{ContextName: name, Endpoint: resolved.Endpoint, APIVersion: APIVersion, DaemonID: before.ID, ServerVersion: before.ServerVersion, StartedAt: started, CompletedAt: time.Now().UTC(), Scope: CacheScope, Records: records, SequentialObservations: true, DaemonAccountingMayChange: true, CacheChecked: true}, nil
}

func validCacheID(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	for i, c := range []byte(id) {
		alpha := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		digit := c >= '0' && c <= '9'
		if !(alpha || digit || i > 0 && (c == '-' || c == '_' || c == '.' || c == ':')) {
			return false
		}
	}
	return true
}

func cacheDate(raw *string) (*time.Time, error) {
	if raw == nil {
		return nil, nil
	}
	s := *raw
	// Go's time parser accepts a few forms outside this declared RFC3339
	// profile. Bound the fraction and numeric offset explicitly before parsing.
	if len(s) < 20 || len(s) > 35 || s[10] != 'T' {
		return nil, ErrProtocol
	}
	end := len(s)
	if s[end-1] == 'Z' {
		end--
	} else {
		if end < 25 {
			return nil, ErrProtocol
		}
		zone := s[end-6:]
		if (zone[0] != '+' && zone[0] != '-') || zone[3] != ':' || zone[1] < '0' || zone[1] > '2' || zone[2] < '0' || zone[2] > '9' || zone[4] < '0' || zone[4] > '5' || zone[5] < '0' || zone[5] > '9' || (zone[1] == '2' && zone[2] > '3') {
			return nil, ErrProtocol
		}
		end -= 6
	}
	if end > 19 {
		if s[19] != '.' || end-20 < 1 || end-20 > 9 {
			return nil, ErrProtocol
		}
		for _, c := range []byte(s[20:end]) {
			if c < '0' || c > '9' {
				return nil, ErrProtocol
			}
		}
	} else if end != 19 {
		return nil, ErrProtocol
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return nil, ErrProtocol
	}
	if t.IsZero() {
		return nil, nil
	}
	t = t.UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return nil, ErrProtocol
	}
	return &t, nil
}

func (w *wire) cache(ctx context.Context) ([]CacheRecord, error) {
	b, err := w.get(ctx, "/system/df?type=build-cache")
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil || fields == nil {
		return nil, ErrProtocol
	}
	raw, exists := fields["BuildCache"]
	// The pinned v25 handler explicitly turns a nil builder result into [],
	// so absent/null cannot stand in for a successfully observed empty cache.
	if !exists || strings.TrimSpace(string(raw)) == "null" {
		return nil, ErrProtocol
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(raw, &rows); err != nil || rows == nil {
		return nil, ErrProtocol
	}
	if len(rows) > MaxObjects {
		return nil, ErrBounds
	}
	result, seen := make([]CacheRecord, 0, len(rows)), make(map[string]bool, len(rows))
	for _, raw := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		selected, err := projectedObject(raw, "ID", "Type", "InUse", "Shared", "CreatedAt", "LastUsedAt", "UsageCount")
		if err != nil {
			return nil, err
		}
		var row struct {
			ID         string
			Type       string
			InUse      *bool
			Shared     *bool
			CreatedAt  *string
			LastUsedAt *string
			UsageCount *int64
		}
		if err := json.Unmarshal(selected, &row); err != nil {
			return nil, ErrProtocol
		}
		if len(row.ID) > 128 {
			return nil, ErrBounds
		}
		if !validCacheID(row.ID) || seen[row.ID] {
			return nil, ErrProtocol
		}
		switch row.Type {
		case "internal", "frontend", "source.local", "source.git.checkout", "exec.cachemount", "regular":
		default:
			return nil, ErrProtocol
		}
		if row.UsageCount != nil && *row.UsageCount < 0 {
			return nil, ErrProtocol
		}
		created, err := cacheDate(row.CreatedAt)
		if err != nil {
			return nil, err
		}
		lastUsed, err := cacheDate(row.LastUsedAt)
		if err != nil {
			return nil, err
		}
		seen[row.ID] = true
		result = append(result, CacheRecord{ID: row.ID, Type: row.Type, InUse: row.InUse, Shared: row.Shared, CreatedAt: created, LastUsedAt: lastUsed, UsageCount: row.UsageCount})
	}
	return result, nil
}
