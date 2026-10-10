//go:build !darwin && !linux

package doctor

import "context"

func platformSupported() bool { return false }

// The new package has a no-I/O fallback. This does not extend the existing
// whole CLI's native build/support scope beyond macOS and Linux.
func readExisting(context.Context, Request, ConfigValidator, *Report) error { return ErrPlatform }
