package helpers

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/crisog/postgres-migrator/internal/config"
	"github.com/crisog/postgres-migrator/pkg/migration"
	"github.com/stretchr/testify/require"
)

func RunMigration(t *testing.T, ctx context.Context, sourceURL, targetURL string, parallelJobs int, noOwner, noACL bool) {
	t.Helper()

	cfg := &config.Config{
		SourceDatabaseURL: sourceURL,
		TargetDatabaseURL: targetURL,
		ParallelJobs:      parallelJobs,
		NoOwner:           noOwner,
		NoACL:             noACL,
	}

	logger := log.New(io.Discard, "", 0)
	err := migration.Run(ctx, cfg, logger)
	require.NoError(t, err)
}

func RunMigrationExpectError(t *testing.T, ctx context.Context, sourceURL, targetURL string, parallelJobs int, noOwner, noACL bool, expectedError string) {
	t.Helper()

	cfg := &config.Config{
		SourceDatabaseURL: sourceURL,
		TargetDatabaseURL: targetURL,
		ParallelJobs:      parallelJobs,
		NoOwner:           noOwner,
		NoACL:             noACL,
	}

	logger := log.New(io.Discard, "", 0)
	err := migration.Run(ctx, cfg, logger)
	require.Error(t, err)
	require.Contains(t, err.Error(), expectedError)
}
