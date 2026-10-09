package main

import "context"

// Darwin has no proc profile here. This path performs no filesystem probe.
func openKernelIO(context.Context, *child) (kernelIOSource, string, string) {
	return nil, "unsupported", "platform_unsupported"
}
