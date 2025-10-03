package helpers

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

type ColumnDefinition struct {
	ColumnName    string
	DataType      string
	IsNullable    string
	ColumnDefault *string
}

type ConstraintDefinition struct {
	ConstraintName string
	ConstraintType string
}

type DataStats struct {
	MinID        int
	MaxID        int
	UniqueIDs    int
	TotalAge     *int64
	TotalSalary  *float64
	UniqueNames  int
	UniqueEmails int
}

func ValidateSchemaColumns(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()

	query := `
		SELECT
			column_name,
			data_type,
			is_nullable,
			column_default
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`

	sourceRows, err := sourceConn.Query(ctx, query, tableName)
	require.NoError(t, err)
	defer sourceRows.Close()

	var sourceColumns []ColumnDefinition
	for sourceRows.Next() {
		var col ColumnDefinition
		err := sourceRows.Scan(&col.ColumnName, &col.DataType, &col.IsNullable, &col.ColumnDefault)
		require.NoError(t, err)
		sourceColumns = append(sourceColumns, col)
	}
	require.NoError(t, sourceRows.Err())

	targetRows, err := targetConn.Query(ctx, query, tableName)
	require.NoError(t, err)
	defer targetRows.Close()

	var targetColumns []ColumnDefinition
	for targetRows.Next() {
		var col ColumnDefinition
		err := targetRows.Scan(&col.ColumnName, &col.DataType, &col.IsNullable, &col.ColumnDefault)
		require.NoError(t, err)
		targetColumns = append(targetColumns, col)
	}
	require.NoError(t, targetRows.Err())

	require.Equal(t, sourceColumns, targetColumns, "Column definitions should match between source and target")
}

func ValidateSchemaConstraints(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()

	query := `
		SELECT
			constraint_type
		FROM information_schema.table_constraints
		WHERE table_name = $1
		AND constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
		ORDER BY constraint_type`

	sourceRows, err := sourceConn.Query(ctx, query, tableName)
	require.NoError(t, err)
	defer sourceRows.Close()

	var sourceConstraints []string
	for sourceRows.Next() {
		var constraintType string
		err := sourceRows.Scan(&constraintType)
		require.NoError(t, err)
		sourceConstraints = append(sourceConstraints, constraintType)
	}
	require.NoError(t, sourceRows.Err())

	targetRows, err := targetConn.Query(ctx, query, tableName)
	require.NoError(t, err)
	defer targetRows.Close()

	var targetConstraints []string
	for targetRows.Next() {
		var constraintType string
		err := targetRows.Scan(&constraintType)
		require.NoError(t, err)
		targetConstraints = append(targetConstraints, constraintType)
	}
	require.NoError(t, targetRows.Err())

	require.Equal(t, sourceConstraints, targetConstraints, "Constraints should match between source and target")
}

func ValidateRowCount(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) (int, int) {
	t.Helper()

	query := "SELECT COUNT(*) FROM " + tableName

	var sourceCount int
	err := sourceConn.QueryRow(ctx, query).Scan(&sourceCount)
	require.NoError(t, err)

	var targetCount int
	err = targetConn.QueryRow(ctx, query).Scan(&targetCount)
	require.NoError(t, err)

	require.Equal(t, sourceCount, targetCount, "Row counts should match between source and target")
	return sourceCount, targetCount
}

func ValidateIDRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()

	query := "SELECT MIN(id) AS min_id, MAX(id) AS max_id, COUNT(DISTINCT id) AS unique_ids FROM " + tableName

	var sourceStats DataStats
	err := sourceConn.QueryRow(ctx, query).Scan(&sourceStats.MinID, &sourceStats.MaxID, &sourceStats.UniqueIDs)
	require.NoError(t, err)

	var targetStats DataStats
	err = targetConn.QueryRow(ctx, query).Scan(&targetStats.MinID, &targetStats.MaxID, &targetStats.UniqueIDs)
	require.NoError(t, err)

	require.Equal(t, sourceStats.MinID, targetStats.MinID, "Min ID should match")
	require.Equal(t, sourceStats.MaxID, targetStats.MaxID, "Max ID should match")
	require.Equal(t, sourceStats.UniqueIDs, targetStats.UniqueIDs, "Unique ID count should match")
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

	var sourceAge, targetAge *int64
	err := sourceConn.QueryRow(ctx, "SELECT SUM(age) FROM "+tableName).Scan(&sourceAge)
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, "SELECT SUM(age) FROM "+tableName).Scan(&targetAge)
	require.NoError(t, err)
	require.Equal(t, sourceAge, targetAge, "Sum of ages should match")

	var sourceSalary, targetSalary *float64
	err = sourceConn.QueryRow(ctx, "SELECT SUM(salary) FROM "+tableName).Scan(&sourceSalary)
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, "SELECT SUM(salary) FROM "+tableName).Scan(&targetSalary)
	require.NoError(t, err)
	require.Equal(t, sourceSalary, targetSalary, "Sum of salaries should match")

	var sourceNames, targetNames int
	err = sourceConn.QueryRow(ctx, "SELECT COUNT(DISTINCT name) FROM "+tableName).Scan(&sourceNames)
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, "SELECT COUNT(DISTINCT name) FROM "+tableName).Scan(&targetNames)
	require.NoError(t, err)
	require.Equal(t, sourceNames, targetNames, "Unique name count should match")

	var sourceEmails, targetEmails int
	err = sourceConn.QueryRow(ctx, "SELECT COUNT(DISTINCT email) FROM "+tableName).Scan(&sourceEmails)
	require.NoError(t, err)
	err = targetConn.QueryRow(ctx, "SELECT COUNT(DISTINCT email) FROM "+tableName).Scan(&targetEmails)
	require.NoError(t, err)
	require.Equal(t, sourceEmails, targetEmails, "Unique email count should match")
}

func ValidateTimestampRange(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()

	query := "SELECT MIN(created_at)::text AS earliest_created, MAX(created_at)::text AS latest_created FROM " + tableName

	var sourceMin, sourceMax *string
	err := sourceConn.QueryRow(ctx, query).Scan(&sourceMin, &sourceMax)
	require.NoError(t, err)

	var targetMin, targetMax *string
	err = targetConn.QueryRow(ctx, query).Scan(&targetMin, &targetMax)
	require.NoError(t, err)

	require.Equal(t, sourceMin, targetMin, "Earliest timestamp should match")
	require.Equal(t, sourceMax, targetMax, "Latest timestamp should match")
}

func ValidateDataChecksum(t *testing.T, ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) {
	t.Helper()

	query := `SELECT MD5(STRING_AGG(name || '|' || email || '|' || COALESCE(age::text, '') || '|' ||
		COALESCE(salary::text, '') || '|' || COALESCE(created_at::text, ''), '|' ORDER BY id))
		AS data_checksum FROM ` + tableName

	var sourceChecksum string
	err := sourceConn.QueryRow(ctx, query).Scan(&sourceChecksum)
	require.NoError(t, err)

	var targetChecksum string
	err = targetConn.QueryRow(ctx, query).Scan(&targetChecksum)
	require.NoError(t, err)

	require.Equal(t, sourceChecksum, targetChecksum, "Data checksums should match - data integrity verified")
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
