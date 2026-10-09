package cli

import (
	"fmt"
	"io"

	"github.com/bjornarhagen/saga-rydd/internal/worker"
)

func printWorkerInventoryMode(out io.Writer, mode *worker.InventoryModeSnapshot) {
	if mode == nil {
		return
	}
	if !mode.Compact {
		fmt.Fprintln(out, "Background inventory storage: DETAILED")
		return
	}
	fmt.Fprintln(out, "Background inventory storage: COMPACT")
	printWrapped(out, "This mode saves directory totals and file identities inside node_modules. Other files keep detailed records. After saved work drains, root listings use a fixed daily interval. The selected mode does not prove that a scan or saved-data work has finished. Status does not inspect source folders.", "")
}
