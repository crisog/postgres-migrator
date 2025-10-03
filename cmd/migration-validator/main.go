package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/crisog/postgres-migrator/pkg/validation"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime)

	sourceURL := flag.String("source", "", "Source database connection URL")
	targetURL := flag.String("target", "", "Target database connection URL")
	tableName := flag.String("table", "", "Optional: specific table name to validate (validates all tables if not specified)")
	validateChecksum := flag.Bool("checksum", false, "Perform data checksum validation (slower)")
	flag.Parse()

	if *sourceURL == "" || *targetURL == "" {
		fmt.Println("Usage: migration-validator -source <source-url> -target <target-url> [-table <table-name>] [-checksum]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	if *tableName != "" {
		logger.Printf("Starting validation for table '%s'...\n", *tableName)
		if err := validation.ValidateTableMigrationFromURLs(ctx, *sourceURL, *targetURL, *tableName, *validateChecksum, logger); err != nil {
			logger.Fatalf("❌ Validation failed: %v", err)
		}
		logger.Printf("\n✓ All validations passed for table '%s'\n", *tableName)
	} else {
		logger.Println("Starting validation for all tables...")
		if err := validation.ValidateAllTablesFromURLs(ctx, *sourceURL, *targetURL, logger); err != nil {
			logger.Fatalf("❌ Validation failed: %v", err)
		}
	}
}
