package validation

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/jackc/pgx/v5"
)

type ColumnDefinition struct {
	ColumnName    string
	DataType      string
	IsNullable    string
	ColumnDefault *string
}

func ValidateSchemaColumns(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateSchemaColumns(ctx, sourceConn, targetConn, tableName)
}

func ValidateSchemaConstraints(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateSchemaConstraints(ctx, sourceConn, targetConn, tableName)
}

func ValidateRowCount(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) (int, int, error) {
	count, err := validateRowCount(ctx, sourceConn, targetConn, tableName)
	return count, count, err
}

func ValidateIDRange(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateIDRange(ctx, sourceConn, targetConn, tableName)
}

func ValidateAggregateStats(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateAggregateStats(ctx, sourceConn, targetConn, tableName)
}

func ValidateTimestampRange(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateTimestampRange(ctx, sourceConn, targetConn, tableName)
}

func ValidateDataChecksum(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validateDataChecksum(ctx, sourceConn, targetConn, tableName)
}

func ValidateTableMigrationFromURLs(ctx context.Context, sourceURL, targetURL, tableName string, validateChecksum bool, logger *log.Logger) error {
	sourceConn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer sourceConn.Close(ctx)

	targetConn, err := pgx.Connect(ctx, targetURL)
	if err != nil {
		return fmt.Errorf("failed to connect to target database: %w", err)
	}
	defer targetConn.Close(ctx)

	return ValidateTableMigration(ctx, sourceConn, targetConn, tableName, validateChecksum, logger)
}

func ValidateAllTablesFromURLs(ctx context.Context, sourceURL, targetURL string, logger *log.Logger) error {
	sourceConn, err := pgx.Connect(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer sourceConn.Close(ctx)

	targetConn, err := pgx.Connect(ctx, targetURL)
	if err != nil {
		return fmt.Errorf("failed to connect to target database: %w", err)
	}
	defer targetConn.Close(ctx)

	// Get all tables from target database
	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
		ORDER BY tablename`

	rows, err := targetConn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating tables: %w", err)
	}

	if len(tables) == 0 {
		logger.Println("No tables found to validate")
		return nil
	}

	logger.Printf("Found %d tables to validate\n", len(tables))

	// Validate each table (without checksum for speed)
	for _, tableName := range tables {
		logger.Printf("\n=== Validating table: %s ===", tableName)
		if err := ValidateTableMigration(ctx, sourceConn, targetConn, tableName, false, logger); err != nil {
			return fmt.Errorf("validation failed for table %s: %w", tableName, err)
		}
	}

	logger.Printf("\n✓ All %d tables validated successfully", len(tables))
	return nil
}

func ValidateTableMigration(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string, validateChecksum bool, logger *log.Logger) error {
	logger.Println("Validating schema columns...")
	if err := validateSchemaColumns(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("schema columns validation failed: %w", err)
	}
	logger.Println("✓ Schema columns match")

	logger.Println("Validating schema constraints...")
	if err := validateSchemaConstraints(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("schema constraints validation failed: %w", err)
	}
	logger.Println("✓ Schema constraints match")

	logger.Println("Validating row count...")
	sourceCount, err := validateRowCount(ctx, sourceConn, targetConn, tableName)
	if err != nil {
		return fmt.Errorf("row count validation failed: %w", err)
	}
	logger.Printf("✓ Row count matches: %d records", sourceCount)

	logger.Println("Validating ID range...")
	if err := validateIDRange(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("ID range validation failed: %w", err)
	}
	logger.Println("✓ ID range matches")

	logger.Println("Validating aggregate statistics...")
	if err := validateAggregateStats(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("aggregate statistics validation failed: %w", err)
	}
	logger.Println("✓ Aggregate statistics match")

	logger.Println("Validating timestamp range...")
	if err := validateTimestampRange(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("timestamp range validation failed: %w", err)
	}
	logger.Println("✓ Timestamp range matches")

	if validateChecksum {
		logger.Println("Validating data checksum (this may take a while on large datasets)...")
		if err := validateDataChecksum(ctx, sourceConn, targetConn, tableName); err != nil {
			return fmt.Errorf("data checksum validation failed: %w", err)
		}
		logger.Println("✓ Data checksum matches")
	}

	return nil
}

func validateSchemaColumns(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
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
	if err != nil {
		return fmt.Errorf("source query failed: %w", err)
	}
	defer sourceRows.Close()

	var sourceColumns []ColumnDefinition
	for sourceRows.Next() {
		var col ColumnDefinition
		if err := sourceRows.Scan(&col.ColumnName, &col.DataType, &col.IsNullable, &col.ColumnDefault); err != nil {
			return fmt.Errorf("source scan failed: %w", err)
		}
		sourceColumns = append(sourceColumns, col)
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("source rows error: %w", err)
	}

	targetRows, err := targetConn.Query(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("target query failed: %w", err)
	}
	defer targetRows.Close()

	var targetColumns []ColumnDefinition
	for targetRows.Next() {
		var col ColumnDefinition
		if err := targetRows.Scan(&col.ColumnName, &col.DataType, &col.IsNullable, &col.ColumnDefault); err != nil {
			return fmt.Errorf("target scan failed: %w", err)
		}
		targetColumns = append(targetColumns, col)
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("target rows error: %w", err)
	}

	if len(sourceColumns) != len(targetColumns) {
		return fmt.Errorf("column count mismatch: source=%d, target=%d", len(sourceColumns), len(targetColumns))
	}

	for i := range sourceColumns {
		if !reflect.DeepEqual(sourceColumns[i], targetColumns[i]) {
			return fmt.Errorf("column definition mismatch at position %d: source=%+v, target=%+v", i, sourceColumns[i], targetColumns[i])
		}
	}

	return nil
}

func validateSchemaConstraints(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	query := `
		SELECT
			constraint_type
		FROM information_schema.table_constraints
		WHERE table_name = $1
		AND constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
		ORDER BY constraint_type`

	sourceRows, err := sourceConn.Query(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("source query failed: %w", err)
	}
	defer sourceRows.Close()

	var sourceConstraints []string
	for sourceRows.Next() {
		var constraintType string
		if err := sourceRows.Scan(&constraintType); err != nil {
			return fmt.Errorf("source scan failed: %w", err)
		}
		sourceConstraints = append(sourceConstraints, constraintType)
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("source rows error: %w", err)
	}

	targetRows, err := targetConn.Query(ctx, query, tableName)
	if err != nil {
		return fmt.Errorf("target query failed: %w", err)
	}
	defer targetRows.Close()

	var targetConstraints []string
	for targetRows.Next() {
		var constraintType string
		if err := targetRows.Scan(&constraintType); err != nil {
			return fmt.Errorf("target scan failed: %w", err)
		}
		targetConstraints = append(targetConstraints, constraintType)
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("target rows error: %w", err)
	}

	if len(sourceConstraints) != len(targetConstraints) {
		return fmt.Errorf("constraint count mismatch: source=%v, target=%v", sourceConstraints, targetConstraints)
	}

	for i := range sourceConstraints {
		if sourceConstraints[i] != targetConstraints[i] {
			return fmt.Errorf("constraint mismatch at position %d", i)
		}
	}

	return nil
}

func validateRowCount(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) (int, error) {
	query := "SELECT COUNT(*) FROM " + tableName

	var sourceCount int
	if err := sourceConn.QueryRow(ctx, query).Scan(&sourceCount); err != nil {
		return 0, fmt.Errorf("source count failed: %w", err)
	}

	var targetCount int
	if err := targetConn.QueryRow(ctx, query).Scan(&targetCount); err != nil {
		return 0, fmt.Errorf("target count failed: %w", err)
	}

	if sourceCount != targetCount {
		return 0, fmt.Errorf("row count mismatch: source=%d, target=%d", sourceCount, targetCount)
	}

	return sourceCount, nil
}

func validateIDRange(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	query := "SELECT MIN(id) AS min_id, MAX(id) AS max_id, COUNT(DISTINCT id) AS unique_ids FROM " + tableName

	var sourceMinID, sourceMaxID, sourceUniqueIDs int
	if err := sourceConn.QueryRow(ctx, query).Scan(&sourceMinID, &sourceMaxID, &sourceUniqueIDs); err != nil {
		return fmt.Errorf("source query failed: %w", err)
	}

	var targetMinID, targetMaxID, targetUniqueIDs int
	if err := targetConn.QueryRow(ctx, query).Scan(&targetMinID, &targetMaxID, &targetUniqueIDs); err != nil {
		return fmt.Errorf("target query failed: %w", err)
	}

	if sourceMinID != targetMinID || sourceMaxID != targetMaxID || sourceUniqueIDs != targetUniqueIDs {
		return fmt.Errorf("ID range mismatch")
	}

	return nil
}

func validateAggregateStats(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	var sourceAge, targetAge *int64
	if err := sourceConn.QueryRow(ctx, "SELECT SUM(age) FROM "+tableName).Scan(&sourceAge); err != nil {
		return fmt.Errorf("source age sum failed: %w", err)
	}
	if err := targetConn.QueryRow(ctx, "SELECT SUM(age) FROM "+tableName).Scan(&targetAge); err != nil {
		return fmt.Errorf("target age sum failed: %w", err)
	}
	if (sourceAge == nil) != (targetAge == nil) || (sourceAge != nil && *sourceAge != *targetAge) {
		return fmt.Errorf("sum of ages mismatch")
	}

	var sourceSalary, targetSalary *float64
	if err := sourceConn.QueryRow(ctx, "SELECT SUM(salary) FROM "+tableName).Scan(&sourceSalary); err != nil {
		return fmt.Errorf("source salary sum failed: %w", err)
	}
	if err := targetConn.QueryRow(ctx, "SELECT SUM(salary) FROM "+tableName).Scan(&targetSalary); err != nil {
		return fmt.Errorf("target salary sum failed: %w", err)
	}
	if (sourceSalary == nil) != (targetSalary == nil) || (sourceSalary != nil && *sourceSalary != *targetSalary) {
		return fmt.Errorf("sum of salaries mismatch")
	}

	var sourceNames, targetNames int
	if err := sourceConn.QueryRow(ctx, "SELECT COUNT(DISTINCT name) FROM "+tableName).Scan(&sourceNames); err != nil {
		return fmt.Errorf("source unique names failed: %w", err)
	}
	if err := targetConn.QueryRow(ctx, "SELECT COUNT(DISTINCT name) FROM "+tableName).Scan(&targetNames); err != nil {
		return fmt.Errorf("target unique names failed: %w", err)
	}
	if sourceNames != targetNames {
		return fmt.Errorf("unique name count mismatch")
	}

	var sourceEmails, targetEmails int
	if err := sourceConn.QueryRow(ctx, "SELECT COUNT(DISTINCT email) FROM "+tableName).Scan(&sourceEmails); err != nil {
		return fmt.Errorf("source unique emails failed: %w", err)
	}
	if err := targetConn.QueryRow(ctx, "SELECT COUNT(DISTINCT email) FROM "+tableName).Scan(&targetEmails); err != nil {
		return fmt.Errorf("target unique emails failed: %w", err)
	}
	if sourceEmails != targetEmails {
		return fmt.Errorf("unique email count mismatch")
	}

	return nil
}

func validateTimestampRange(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	query := "SELECT MIN(created_at)::text AS earliest_created, MAX(created_at)::text AS latest_created FROM " + tableName

	var sourceMin, sourceMax *string
	if err := sourceConn.QueryRow(ctx, query).Scan(&sourceMin, &sourceMax); err != nil {
		return fmt.Errorf("source query failed: %w", err)
	}

	var targetMin, targetMax *string
	if err := targetConn.QueryRow(ctx, query).Scan(&targetMin, &targetMax); err != nil {
		return fmt.Errorf("target query failed: %w", err)
	}

	if (sourceMin == nil) != (targetMin == nil) || (sourceMin != nil && *sourceMin != *targetMin) {
		return fmt.Errorf("earliest timestamp mismatch")
	}
	if (sourceMax == nil) != (targetMax == nil) || (sourceMax != nil && *sourceMax != *targetMax) {
		return fmt.Errorf("latest timestamp mismatch")
	}

	return nil
}

func validateDataChecksum(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	query := `SELECT MD5(STRING_AGG(name || '|' || email || '|' || COALESCE(age::text, '') || '|' ||
		COALESCE(salary::text, '') || '|' || COALESCE(created_at::text, ''), '|' ORDER BY id))
		AS data_checksum FROM ` + tableName

	var sourceChecksum string
	if err := sourceConn.QueryRow(ctx, query).Scan(&sourceChecksum); err != nil {
		return fmt.Errorf("source checksum failed: %w", err)
	}

	var targetChecksum string
	if err := targetConn.QueryRow(ctx, query).Scan(&targetChecksum); err != nil {
		return fmt.Errorf("target checksum failed: %w", err)
	}

	if sourceChecksum != targetChecksum {
		return fmt.Errorf("data checksums mismatch")
	}

	return nil
}
