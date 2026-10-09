package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/buildmetadata"
)

func versionReply(ctx context.Context, out, errOut io.Writer, machine bool) int {
	if err := ctx.Err(); err != nil {
		if machine {
			return machineFailure(out, errOut, "version", "canceled", "Version reply canceled.", 1)
		}
		fmt.Fprintln(errOut, "Version reply canceled.")
		return 1
	}
	r := buildmetadata.Current()
	code := 0
	if machine {
		code = emit(out, errOut, map[string]any{"api_version": APIVersion, "ok": true, "command": "version", "version": r.Version, "build_metadata": r}, 0)
	} else {
		guard := &reviewOutput{writer: out}
		fmt.Fprintf(guard, "rydd %s (experimental inventory)\n", r.Version)
		if guard.err != nil {
			fmt.Fprintln(errOut, "Version reply could not be written.")
			code = 1
		}
	}
	if code == 0 && ctx.Err() != nil {
		fmt.Fprintln(errOut, "Version reply canceled during output.")
		return 1
	}
	return code
}
