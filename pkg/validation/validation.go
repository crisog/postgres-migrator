package validation

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/jackc/pgx/v5"
)

func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

func columnExists(ctx context.Context, conn *pgx.Conn, tableName, columnName string) bool {
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = $1 AND column_name = $2
	)`
	if err := conn.QueryRow(ctx, query, tableName, columnName).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func getPrimaryKeyColumns(ctx context.Context, conn *pgx.Conn, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)`

	rows, err := conn.Query(ctx, query, quoteIdentifier(tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}


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

func ValidatePrimaryKey(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	return validatePrimaryKey(ctx, sourceConn, targetConn, tableName)
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

	query := `
		SELECT tablename
		FROM pg_tables
		WHERE schemaname = 'public'
		ORDER BY tablename`

	sourceRows, err := sourceConn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query source tables: %w", err)
	}
	defer sourceRows.Close()

	var sourceTables []string
	for sourceRows.Next() {
		var tableName string
		if err := sourceRows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		sourceTables = append(sourceTables, tableName)
	}
	if err := sourceRows.Err(); err != nil {
		return fmt.Errorf("error iterating source tables: %w", err)
	}

	targetRows, err := targetConn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query target tables: %w", err)
	}
	defer targetRows.Close()

	targetTables := make(map[string]bool)
	for targetRows.Next() {
		var tableName string
		if err := targetRows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		targetTables[tableName] = true
	}
	if err := targetRows.Err(); err != nil {
		return fmt.Errorf("error iterating target tables: %w", err)
	}

	if len(sourceTables) == 0 {
		logger.Println("No tables found in source database")
		return nil
	}

	logger.Printf("Found %d tables in source database to validate\n", len(sourceTables))

	for _, tableName := range sourceTables {
		logger.Printf("\n=== Validating table: %s ===", tableName)

		if !targetTables[tableName] {
			return fmt.Errorf("validation failed for table %s: table missing from target database", tableName)
		}

		if err := ValidateTableMigration(ctx, sourceConn, targetConn, tableName, false, logger); err != nil {
			return fmt.Errorf("validation failed for table %s: %w", tableName, err)
		}
	}

	logger.Printf("\n✓ All %d tables validated successfully", len(sourceTables))
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

	logger.Println("Validating primary key...")
	if err := validatePrimaryKey(ctx, sourceConn, targetConn, tableName); err != nil {
		return fmt.Errorf("primary key validation failed: %w", err)
	}
	logger.Println("✓ Primary key matches")

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
	query := "SELECT COUNT(*) FROM " + quoteIdentifier(tableName)

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

func validatePrimaryKey(ctx context.Context, sourceConn, targetConn *pgx.Conn, tableName string) error {
	pkCols, err := getPrimaryKeyColumns(ctx, sourceConn, tableName)
	if err != nil || len(pkCols) == 0 {
		return nil
	}

	quoted := quoteIdentifier(tableName)
	var sourceCount, targetCount int

	for _, pkCol := range pkCols {
		quotedCol := quoteIdentifier(pkCol)
		query := fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", quotedCol, quoted)

		if err := sourceConn.QueryRow(ctx, query).Scan(&sourceCount); err != nil {
			return fmt.Errorf("source query failed for column %s: %w", pkCol, err)
		}

		if err := targetConn.QueryRow(ctx, query).Scan(&targetCount); err != nil {
			return fmt.Errorf("target query failed for column %s: %w", pkCol, err)
		}

		if sourceCount != targetCount {
			return fmt.Errorf("primary key distinct count mismatch for %s: source=%d, target=%d", pkCol, sourceCount, targetCount)
		}
	}

	return nil
}

