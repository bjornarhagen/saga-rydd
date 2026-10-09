package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/bjornarhagen/saga-rydd/internal/packaging"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: rydd-package VERSION")
		os.Exit(2)
	}
	repo, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Repository directory is unavailable.")
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	result, err := packaging.Build(ctx, repo, os.Args[1])
	if result.Contract != "" {
		if outputErr := json.NewEncoder(os.Stdout).Encode(result); outputErr != nil {
			fmt.Fprintln(os.Stderr, "Candidate reply could not be written; inspect the candidate path.")
			os.Exit(1)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "Candidate reply canceled; inspect the candidate path.")
		os.Exit(1)
	}
}
