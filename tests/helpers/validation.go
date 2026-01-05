package helpers

import (
	"context"
	"testing"

	"github.com/crisog/postgres-migrator/pkg/validation"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func ValidateSchemaColumns(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateSchemaColumns(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Column definitions should match between source and target")
}

func ValidateSchemaConstraints(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateSchemaConstraints(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Constraints should match between source and target")
}

func ValidateRowCount(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) (int, int) {
	t.Helper()
	sourceCount, targetCount, err := validation.ValidateRowCount(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Row counts should match between source and target")
	return sourceCount, targetCount
}

func ValidatePrimaryKey(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidatePrimaryKey(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Primary keys should match between source and target")
}

func ValidateTableMigration(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string, validateChecksum bool) {
	t.Helper()

	t.Log("Validating schema columns...")
	ValidateSchemaColumns(t, ctx, sourceConn, targetConn, tableName)

	t.Log("Validating schema constraints...")
	ValidateSchemaConstraints(t, ctx, sourceConn, targetConn, tableName)

	t.Log("Validating row count...")
	sourceCount, _ := ValidateRowCount(t, ctx, sourceConn, targetConn, tableName)
	t.Logf("Row count verified: %d records", sourceCount)

	t.Log("Validating primary key...")
	ValidatePrimaryKey(t, ctx, sourceConn, targetConn, tableName)

	t.Logf("✓ All validations passed for table '%s'", tableName)
}

func ValidateBasicMigration(t *testing.T, ctx context.Context, targetConn *pgx.Conn) {
	t.Helper()

	var userCount int
	err := targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&userCount)
	require.NoError(t, err)
	require.Equal(t, 3, userCount)

	var postCount int
	err = targetConn.QueryRow(ctx, "SELECT COUNT(*) FROM posts").Scan(&postCount)
	require.NoError(t, err)
	require.Equal(t, 4, postCount)
}

func ValidateIDsInRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string, minID, maxID int) {
	t.Helper()

	quotedTable := `"` + tableName + `"`
	query := "SELECT COUNT(*) FROM " + quotedTable + " WHERE id >= $1 AND id <= $2"

	var sourceCount int
	err := sourceConn.QueryRow(ctx, query, minID, maxID).Scan(&sourceCount)
	require.NoError(t, err, "Failed to count IDs in source database")

	var targetCount int
	err = targetConn.QueryRow(ctx, query, minID, maxID).Scan(&targetCount)
	require.NoError(t, err, "Failed to count IDs in target database")

	expectedCount := maxID - minID + 1
	require.Equal(t, expectedCount, sourceCount, "Source database should have all IDs in range")
	require.Equal(t, expectedCount, targetCount, "Target database should have all IDs in range")
	require.Equal(t, sourceCount, targetCount, "Source and target should have the same number of IDs in range")
}
