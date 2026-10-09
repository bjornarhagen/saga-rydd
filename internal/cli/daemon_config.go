package cli

import (
	"context"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/config"
)

func loadDaemonConfiguration(ctx context.Context, paths config.Paths, home string) (config.Config, func(context.Context) error, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	cfg, snapshot, err := config.LoadSnapshot(readCtx, paths.ConfigFile, home)
	cancel()
	if err != nil {
		return config.Config{}, nil, err
	}
	check := func(ctx context.Context) error {
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return snapshot.Check(checkCtx)
	}
	return cfg, check, nil
}
