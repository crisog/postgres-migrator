package tests

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/crisog/postgres-migrator/tests/helpers"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func getPostgresImage(version string) string {
	if version == "" {
		version = "16"
	}
	return fmt.Sprintf("postgres:%s-alpine", version)
}

func getDefaultPostgresVersion() string {
	if v := os.Getenv("POSTGRES_VERSION"); v != "" {
		return v
	}
	return "16"
}

func TestMigration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 1, true, true)

	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	helpers.ValidateBasicMigration(t, ctx, targetConn)

	var name, email string
	err = targetConn.QueryRow(ctx, "SELECT name, email FROM users WHERE id = 1").Scan(&name, &email)
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
	require.Equal(t, "alice@example.com", email)

	var publishedCount int
	err = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM posts WHERE published = TRUE").Scan(&publishedCount)
	require.NoError(t, err)
	require.Equal(t, 2, publishedCount)

	var alicePostCount int
	err = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM posts WHERE user_id = 1").Scan(&alicePostCount)
	require.NoError(t, err)
	require.Equal(t, 2, alicePostCount)
}

func TestVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage("15"),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage("16"),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigrationExpectError(t, ctx, sourceConnStr, targetConnStr, 1, true, true, "major version mismatch")
}

func TestParallelJobs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 4, true, true)

	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	helpers.ValidateBasicMigration(t, ctx, targetConn)
}

func TestNonCleanTarget(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigrationExpectError(t, ctx, sourceConnStr, targetConnStr, 1, true, true, "already exists")
}

func TestNoOwner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("sourceuser"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("targetuser"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 1, true, true)

	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	var tableOwner string
	err = targetConn.QueryRow(ctx, "SELECT tableowner FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users'").Scan(&tableOwner)
	require.NoError(t, err)
	require.Equal(t, "targetuser", tableOwner)
}

func TestNoOwnerDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("sourceuser"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-source.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("targetuser"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigrationExpectError(t, ctx, sourceConnStr, targetConnStr, 1, false, true, "restore failed")
}

func TestNoACL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-with-acl.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 1, true, true)

	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	var userCount int
	err = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	require.NoError(t, err)
	require.Equal(t, 2, userCount)

	var roleExists bool
	err = targetConn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'app_user')").Scan(&roleExists)
	require.NoError(t, err)
	require.False(t, roleExists)
}

func TestNoACLDisabled(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-with-acl.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 1, true, false)

	var count int
	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	err = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count)
}

func TestLargeDataset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	sourceContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("sourcedb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.WithInitScripts("testdata/init-large-dataset.sql"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, sourceContainer)
	require.NoError(t, err)

	targetContainer, err := postgres.Run(
		ctx,
		getPostgresImage(getDefaultPostgresVersion()),
		postgres.WithDatabase("targetdb"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	testcontainers.CleanupContainer(t, targetContainer)
	require.NoError(t, err)

	sourceConnStr, err := sourceContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	targetConnStr, err := targetContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	sourceConn, err := pgx.Connect(ctx, sourceConnStr)
	require.NoError(t, err)
	defer sourceConn.Close(ctx)

	var sourceCount int
	err = sourceConn.QueryRow(ctx, "SELECT COUNT(*) FROM random_data").Scan(&sourceCount)
	require.NoError(t, err)
	require.Equal(t, 1000000, sourceCount, "Source database should have 1 million records")

	helpers.RunMigration(t, ctx, sourceConnStr, targetConnStr, 4, true, true)

	targetConn, err := pgx.Connect(ctx, targetConnStr)
	require.NoError(t, err)
	defer targetConn.Close(ctx)

	t.Log("Running comprehensive validation for large dataset migration...")
	helpers.ValidateTableMigration(t, ctx, sourceConn, targetConn, "random_data", true)

	helpers.ValidateIDsInRange(t, ctx, sourceConn, targetConn, "random_data", 1, 1000000)
}
