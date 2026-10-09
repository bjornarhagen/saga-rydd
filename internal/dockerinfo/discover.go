// Package dockerinfo reads a finite, explicitly selected Docker metadata scope.
// The installed Docker CLI, its configuration, and the selected daemon remain
// trust boundaries. A Unix endpoint does not authenticate physical locality.
package dockerinfo

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	APIVersion             = "1.44"
	OperationLimit         = 5 * time.Second
	MaxObjects             = 128
	MaxBodyBytes           = 1 << 20
	MaxHeaderBytes         = 16 << 10
	MaxResolverStdoutBytes = 64 << 10
	MaxResolverStderrBytes = 8 << 10
)

var (
	ErrContext       = errors.New("Docker context name is unsupported")
	ErrEndpoint      = errors.New("Docker endpoint is not a canonical Unix socket URI")
	ErrResolver      = errors.New("Docker context resolution is unavailable")
	ErrBounds        = errors.New("Docker metadata exceeds the bounded profile")
	ErrProtocol      = errors.New("Docker metadata protocol is unsupported or malformed")
	ErrDaemonChanged = errors.New("Docker daemon identity changed during metadata discovery")
)

type ResolvedContext struct {
	ContextName string `json:"context_name"`
	Endpoint    string `json:"endpoint"`
}

// Resolver is a test/integration seam. Discovery still validates the returned
// name and endpoint, and never resolves them again after its single dial.
type Resolver func(context.Context, string) (ResolvedContext, error)

type Image struct {
	ID        string    `json:"id"`
	Tags      []string  `json:"tags"`
	CreatedAt time.Time `json:"created_at"`
	SizeBytes *int64    `json:"size_bytes"`
}

type Container struct {
	ID        string    `json:"id"`
	Names     []string  `json:"names"`
	CreatedAt time.Time `json:"created_at"`
	State     string    `json:"state"`
	SizeBytes *int64    `json:"size_bytes"`
}

type Report struct {
	ContextName              string      `json:"context_name"`
	Endpoint                 string      `json:"endpoint"`
	APIVersion               string      `json:"api_version"`
	DaemonID                 string      `json:"daemon_id"`
	ServerVersion            string      `json:"server_version"`
	StartedAt                time.Time   `json:"started_at"`
	CompletedAt              time.Time   `json:"completed_at"`
	Images                   []Image     `json:"images"`
	Containers               []Container `json:"containers"`
	SequentialObservations   bool        `json:"sequential_observations"`
	AtomicSnapshot           bool        `json:"atomic_snapshot"`
	PhysicalLocalityVerified bool        `json:"physical_locality_verified"`
	NamespaceAuthenticated   bool        `json:"namespace_authenticated"`
	SizesMeasured            bool        `json:"sizes_measured"`
	CurrentStateVerified     bool        `json:"current_state_verified"`
	CleanupApproved          bool        `json:"cleanup_approved"`
	Executable               bool        `json:"executable"`
	SavingsBytes             *int64      `json:"savings_bytes"`
	Persisted                bool        `json:"persisted"`
}

func Discover(ctx context.Context, contextName string) (Report, error) {
	return DiscoverWithResolver(ctx, contextName, resolveContext)
}

func DiscoverWithResolver(ctx context.Context, contextName string, resolver Resolver) (Report, error) {
	return discover(ctx, contextName, resolver, (&net.Dialer{}).DialContext)
}

// ValidContextName deliberately accepts a finite ASCII subset. In particular,
// completion/help tokens cannot reach the installed CLI's early dispatch.
func ValidContextName(name string) bool {
	if len(name) < 1 || len(name) > 128 {
		return false
	}
	for i, c := range []byte(name) {
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		digit := c >= '0' && c <= '9'
		if !(letter || digit || i > 0 && (c == '_' || c == '-' || c == '.')) {
			return false
		}
	}
	return true
}

func socketPath(endpoint string) (string, error) {
	if !safeText(endpoint, 4096) || !strings.HasPrefix(endpoint, "unix:///") {
		return "", ErrEndpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "unix" || u.Host != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" || !safeText(u.Path, 4096) || !filepath.IsAbs(u.Path) || u.Path == "/" || filepath.Clean(u.Path) != u.Path || endpoint != "unix://"+(&url.URL{Path: u.Path}).EscapedPath() {
		return "", ErrEndpoint
	}
	return u.Path, nil
}

func safeText(s string, max int) bool {
	if len(s) > max || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Cs) {
			return false
		}
	}
	return true
}

func discover(parent context.Context, name string, resolver Resolver, dial func(context.Context, string, string) (net.Conn, error)) (Report, error) {
	ctx, cancel := context.WithTimeout(parent, OperationLimit)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if !ValidContextName(name) || resolver == nil {
		return Report{}, ErrContext
	}
	started := time.Now().UTC()
	resolved, err := resolver(ctx, name)
	if err != nil {
		return Report{}, errors.Join(ErrResolver, err, ctx.Err())
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if resolved.ContextName != name {
		return Report{}, fmt.Errorf("%w: selected context changed", ErrResolver)
	}
	path, err := socketPath(resolved.Endpoint)
	if err != nil {
		return Report{}, err
	}
	conn, err := dial(ctx, "unix", path)
	if err != nil {
		return Report{}, errors.Join(ErrProtocol, err, ctx.Err())
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return Report{}, errors.Join(ErrProtocol, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	wire := newWire(conn)
	before, err := wire.info(ctx)
	if err != nil {
		return Report{}, errors.Join(err, ctx.Err())
	}
	images, err := wire.images(ctx)
	if err != nil {
		return Report{}, errors.Join(err, ctx.Err())
	}
	containers, err := wire.containers(ctx)
	if err != nil {
		return Report{}, errors.Join(err, ctx.Err())
	}
	after, err := wire.info(ctx)
	if err != nil {
		return Report{}, errors.Join(err, ctx.Err())
	}
	if before.ID != after.ID {
		return Report{}, ErrDaemonChanged
	}
	if before.ServerVersion != after.ServerVersion {
		return Report{}, ErrDaemonChanged
	}
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	return Report{ContextName: name, Endpoint: resolved.Endpoint, APIVersion: APIVersion, DaemonID: before.ID, ServerVersion: before.ServerVersion, StartedAt: started, CompletedAt: time.Now().UTC(), Images: images, Containers: containers, SequentialObservations: true}, nil
}

func validDigest(id string, image bool) bool {
	if image {
		if !strings.HasPrefix(id, "sha256:") {
			return false
		}
		id = strings.TrimPrefix(id, "sha256:")
	}
	if len(id) != 64 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func versionAtLeast44(s string) bool {
	parts := strings.Split(s, ".")
	if len(parts) != 2 || parts[0] != "1" {
		return false
	}
	n, err := strconv.Atoi(parts[1])
	return err == nil && n >= 44 && s == "1."+strconv.Itoa(n)
}
