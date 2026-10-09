package inventory

import (
	"errors"
	"path/filepath"
	"strings"

	"github.com/bjornarhagen/saga-rydd/internal/state"
)

const MaxRootStreams = 32
const MaxRetainedNameBytes = 4096
const MaxPendingNameBytes = state.MaxBatchEntries * MaxRetainedNameBytes

// WithRootStreams opts into one retained stream per exact configured root.
// A full admitted set is required: no stream is evicted to admit another root.
// Pacing and API counters remain shared by this Scanner. Ordinary constructors
// without this option retain their existing single-stream behavior.
func WithRootStreams(limit int) Option {
	return func(s *Scanner) error {
		if limit < 1 || limit > MaxRootStreams {
			return errors.New("root stream limit must be 1–32")
		}
		s.rootStreamLimit = limit
		s.rootStreams = make(map[string]*stream)
		return nil
	}
}

func (s *Scanner) validateRootStreams(roots []string) error {
	if s.rootStreamLimit == 0 {
		return nil
	}
	if len(roots) < 1 || len(roots) > s.rootStreamLimit {
		return errors.New("configured roots exceed the admitted root stream set")
	}
	seen := make(map[string]bool, len(roots))
	for _, root := range roots {
		if len(root) > 4096 || !filepath.IsAbs(root) || filepath.Clean(root) != root || strings.ContainsRune(root, 0) || seen[root] {
			return errors.New("root streams require distinct bounded absolute configured paths")
		}
		seen[root] = true
	}
	return nil
}

// selectRoot and stream mutations run under mu. current remains the selected
// stream, including for the default single-stream implementation.
func (s *Scanner) selectRoot(root string) {
	if s.rootStreams != nil {
		s.current = s.rootStreams[root]
	}
	s.currentRoot = root
}

func (s *Scanner) retainStream(current *stream) {
	s.current = current
	if s.rootStreams != nil {
		s.rootStreams[s.currentRoot] = current
	}
}

func (s *Scanner) resetAll() {
	if s.rootStreams == nil {
		s.reset()
		return
	}
	for root, current := range s.rootStreams {
		_ = current.file.Close()
		delete(s.rootStreams, root)
	}
	s.current = nil
}

// ReleaseRoot closes only this root's parked stream. The owning worker calls
// it between chunks after saved work drains or the root is disabled. Quota
// waits do not release unfinished streams. It reads no source metadata.
func (s *Scanner) ReleaseRoot(root string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rootStreams != nil {
		if current := s.rootStreams[root]; current != nil {
			_ = current.file.Close()
			delete(s.rootStreams, root)
			if s.currentRoot == root {
				s.current = nil
			}
		}
	} else if s.currentRoot == root {
		s.reset()
	}
}

func supportedPendingNames(path string, names []string) bool {
	if len(names) > state.MaxBatchEntries {
		return false
	}
	bytes := 0
	for _, name := range names {
		if len(name) == 0 || len(name) > MaxRetainedNameBytes || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || !validPath(filepath.Join(path, name)) {
			return false
		}
		bytes += len(name)
		if bytes > MaxPendingNameBytes {
			return false
		}
	}
	return true
}
