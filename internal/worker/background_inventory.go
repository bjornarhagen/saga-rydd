package worker

// InventoryModeSnapshot is the storage mode selected at worker startup. It is
// cached configuration evidence, not a claim that a source pass has completed.
// A nil snapshot means this worker does not run background inventory.
type InventoryModeSnapshot struct {
	Compact bool `json:"compact"`
}
