//go:build race

package inventory

// Race instrumentation adds its own shadow memory. Its RSS is recorded, but
// cannot validate the ordinary native fixture's application memory limit.
const rootStreamRaceInstrumented = true
