package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/crisog/postgres-migrator/internal/config"
	"github.com/crisog/postgres-migrator/pkg/migration"
	"github.com/crisog/postgres-migrator/pkg/validation"
)

func main() {
	exitCode := run()
	os.Exit(exitCode)
}

func run() int {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("\nReceived signal: %v. Cancelling operation...\n", sig)
		cancel()
	}()

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)
	if err := migration.Run(ctx, cfg, logger); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}

	if cfg.ValidateAfter {
		logger.Println("\nRunning post-migration validation...")
		if err := validation.ValidateAllTablesFromURLs(ctx, cfg.SourceDatabaseURL, cfg.TargetDatabaseURL, logger); err != nil {
			fmt.Fprintf(os.Stderr, "Validation failed: %v\n", err)
			return 1
		}
	}

	return 0
}
