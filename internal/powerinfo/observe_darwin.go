package powerinfo

import "context"

// No helper, filesystem lookup or unavailable cgo bridge is attempted.
func platformOpen(context.Context) (source, error) {
	return nil, sourceError{"unsupported_platform"}
}
