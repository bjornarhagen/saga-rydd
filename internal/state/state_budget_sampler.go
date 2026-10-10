//go:build darwin || linux

package state

import (
	"context"
	"path/filepath"
	"strings"
)

// InventoryStateBudgetSampler is an immutable metadata-only capability. It
// carries a copied database name and an identity, but no Store, connection or
// writer lock. A copy is the same scope; a newly constructed sampler is a new
// scope even when its name is equal. Neither identity authenticates a namespace.
type InventoryStateBudgetSampler struct {
	path     string
	identity *inventoryStateSamplerIdentity
}

// A nonzero-size allocation has distinct identity under Go's pointer rules.
type inventoryStateSamplerIdentity struct{ value byte }

// InventoryStateBudgetSampler copies only the existing in-memory database name.
// It performs no filesystem or SQL operation and remains usable after Close.
func (s *Store) InventoryStateBudgetSampler() (InventoryStateBudgetSampler, error) {
	if s == nil || len(s.path) == 0 || len(s.path) > 4096 || !filepath.IsAbs(s.path) || filepath.Clean(s.path) != s.path || strings.ContainsRune(s.path, 0) {
		return InventoryStateBudgetSampler{}, ErrInventoryStateBudgetInput
	}
	return InventoryStateBudgetSampler{path: s.path, identity: &inventoryStateSamplerIdentity{value: 1}}, nil
}

// SameScope compares capabilities, not current objects at the copied names.
// The zero capability never matches, including another zero capability.
func (s InventoryStateBudgetSampler) SameScope(other InventoryStateBudgetSampler) bool {
	return s.identity != nil && other.identity != nil && s == other
}

// Observe uses the unchanged sequential DB/WAL observation contract. At most
// four Lstat calls occur; no connection, lock, open, body read or recovery occurs.
// Cooperative guards cannot interrupt an already-entered metadata call.
func (s InventoryStateBudgetSampler) Observe(ctx context.Context, limit int64) (InventoryStateBudget, error) {
	if s.identity == nil {
		return InventoryStateBudget{}, ErrInventoryStateBudgetInput
	}
	return inventoryStateBudgetAtPath(ctx, s.path, limit, inventoryStateBudgetHooks{})
}
