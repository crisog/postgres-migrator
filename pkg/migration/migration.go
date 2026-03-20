package migration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/crisog/postgres-migrator/internal/config"
	"github.com/crisog/postgres-migrator/internal/database"
	"github.com/crisog/postgres-migrator/internal/migrator"
)

func Run(ctx context.Context, cfg *config.Config, logger *log.Logger) (skipMigration bool, err error) {
	logger.Println("postgres-migrator starting...")
	if cfg.ParallelJobs > 1 {
		logger.Printf("Parallel jobs: %d\n", cfg.ParallelJobs)
	}

	targetTableCount, err := database.ValidateBothConnections(logger, cfg.SourceDatabaseURL, cfg.TargetDatabaseURL, cfg.SkipVersionCheck)
	if err != nil {
		return false, fmt.Errorf("connection validation failed: %w", err)
	}

	if targetTableCount > 0 {
		if cfg.DataOnly {
			logger.Printf("Target database already has %d tables, proceeding with data-only restore (DATA_ONLY is enabled)...\n", targetTableCount)
		} else {
			logger.Printf("Target database already has %d tables, skipping migration and running validation only...\n", targetTableCount)
			return true, nil
		}
	}

	tmpDir, err := os.MkdirTemp("", "postgres-migrator-*")
	if err != nil {
		return false, fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			logger.Printf("Warning: failed to clean up temporary directory: %v\n", err)
		}
	}()

	dumpFile := filepath.Join(tmpDir, "db.dump")

	dumper := migrator.NewDumper(cfg, logger)
	dumpStart := time.Now()

	if err := dumper.Dump(ctx, dumpFile); err != nil {
		return false, fmt.Errorf("dump failed: %w", err)
	}

	dumpDuration := time.Since(dumpStart)
	fileInfo, err := os.Stat(dumpFile)
	if err == nil {
		logger.Printf("Dump completed in %v (size: %d bytes)\n", dumpDuration, fileInfo.Size())
	} else {
		logger.Printf("Dump completed in %v\n", dumpDuration)
	}

	if ctx.Err() != nil {
		return false, fmt.Errorf("operation cancelled before restore")
	}

	restorer := migrator.NewRestorer(cfg, logger)
	restoreStart := time.Now()

	if err := restorer.Restore(ctx, dumpFile); err != nil {
		return false, fmt.Errorf("restore failed: %w", err)
	}

	restoreDuration := time.Since(restoreStart)
	logger.Printf("Restore completed in %v\n", restoreDuration)

	totalDuration := time.Since(dumpStart)
	logger.Printf("\nMigration completed successfully in %v\n", totalDuration)

	return false, nil
}
