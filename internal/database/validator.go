package database

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func ValidateConnection(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("unable to query database: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected query result: got %d, expected 1", result)
	}

	return nil
}

func ValidateTargetIsClean(ctx context.Context, targetURL string) error {
	conn, err := pgx.Connect(ctx, targetURL)
	if err != nil {
		return fmt.Errorf("failed to connect to target database: %w", err)
	}
	defer conn.Close(ctx)

	var tableCount int
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'").Scan(&tableCount)
	if err != nil {
		return fmt.Errorf("failed to check for existing tables: %w", err)
	}
	if tableCount > 0 {
		return fmt.Errorf("target database already exists with %d tables in public schema", tableCount)
	}

	return nil
}

func GetVersion(ctx context.Context, databaseURL string) (string, error) {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return "", fmt.Errorf("unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	var version string
	err = conn.QueryRow(ctx, "SHOW server_version").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("unable to get version: %w", err)
	}

	return version, nil
}

func extractMajorVersion(version string) (int, error) {
	parts := strings.Split(version, ".")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid version format: %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("unable to parse major version from %s: %w", version, err)
	}

	return major, nil
}

func ValidateBothConnections(logger *log.Logger, sourceURL, targetURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger.Println("Validating source database connection...")

	if err := ValidateConnection(ctx, sourceURL); err != nil {
		return fmt.Errorf("source database validation failed: %w", err)
	}

	sourceVersion, err := GetVersion(ctx, sourceURL)
	if err != nil {
		return fmt.Errorf("unable to get source database version: %w", err)
	}
	logger.Printf("Source database: PostgreSQL %s\n", sourceVersion)

	logger.Println("Validating target database connection...")

	if err := ValidateConnection(ctx, targetURL); err != nil {
		return fmt.Errorf("target database validation failed: %w", err)
	}

	targetVersion, err := GetVersion(ctx, targetURL)
	if err != nil {
		return fmt.Errorf("unable to get target database version: %w", err)
	}
	logger.Printf("Target database: PostgreSQL %s\n", targetVersion)

	sourceMajor, err := extractMajorVersion(sourceVersion)
	if err != nil {
		return fmt.Errorf("failed to parse source version: %w", err)
	}

	targetMajor, err := extractMajorVersion(targetVersion)
	if err != nil {
		return fmt.Errorf("failed to parse target version: %w", err)
	}

	if sourceMajor != targetMajor {
		return fmt.Errorf("major version mismatch: source is PostgreSQL %d, target is PostgreSQL %d (must be same major version)", sourceMajor, targetMajor)
	}

	logger.Printf("Version check passed: both databases are PostgreSQL %d\n", sourceMajor)

	if err := ValidateTargetIsClean(ctx, targetURL); err != nil {
		return err
	}

	return nil
}
