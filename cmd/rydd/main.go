package main

import (
	"fmt"
	"os"
)

const help = `Saga — Rydd
A quiet storage cleanup companion for macOS and Linux.

Usage: rydd [--help | --version]

Development scaffold: scanning, cleanup and background services are not yet
implemented. See PLAN.md and PROGRESS.md for the roadmap.
`

func main() {
	if len(os.Args) == 1 || (len(os.Args) == 2 && (os.Args[1] == "--help" || os.Args[1] == "-h")) {
		fmt.Print(help)
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("rydd dev (scaffold)")
		return
	}
	fmt.Fprintln(os.Stderr, "Unsupported command. Run rydd --help; application features are not implemented yet.")
	os.Exit(2)
}
