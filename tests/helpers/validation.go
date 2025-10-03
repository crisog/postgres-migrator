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

func ValidateIDRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateIDRange(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "ID ranges should match between source and target")
}

func ValidateIDsInRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string, minID, maxID int) {
	t.Helper()

	query := "SELECT COUNT(*) AS rows_in_range FROM " + tableName + " WHERE id BETWEEN $1 AND $2"

	var sourceCount int
	err := sourceConn.QueryRow(ctx, query, minID, maxID).Scan(&sourceCount)
	require.NoError(t, err)

	var targetCount int
	err = targetConn.QueryRow(ctx, query, minID, maxID).Scan(&targetCount)
	require.NoError(t, err)

	require.Equal(t, sourceCount, targetCount, "Row counts in ID range should match")
	expectedCount := maxID - minID + 1
	require.Equal(t, expectedCount, targetCount, "All IDs in range should be present")
}

func ValidateAggregateStats(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateAggregateStats(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Aggregate statistics should match between source and target")
}

func ValidateTimestampRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateTimestampRange(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Timestamp ranges should match between source and target")
}

func ValidateDataChecksum(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()
	err := validation.ValidateDataChecksum(ctx, sourceConn, targetConn, tableName)
	require.NoError(t, err, "Data checksums should match - data integrity verified")
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

	t.Log("Validating ID range...")
	ValidateIDRange(t, ctx, sourceConn, targetConn, tableName)

	t.Log("Validating aggregate statistics...")
	ValidateAggregateStats(t, ctx, sourceConn, targetConn, tableName)

	t.Log("Validating timestamp range...")
	ValidateTimestampRange(t, ctx, sourceConn, targetConn, tableName)

	if validateChecksum {
		t.Log("Validating data checksum (this may take a while on large datasets)...")
		ValidateDataChecksum(t, ctx, sourceConn, targetConn, tableName)
	}

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
