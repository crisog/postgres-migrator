package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/crisog/postgres-migrator/pkg/validation"
	"github.com/jackc/pgx/v5"
)

func main() {
	sourceURL := flag.String("source", "", "Source database connection URL")
	targetURL := flag.String("target", "", "Target database connection URL")
	tableName := flag.String("table", "", "Table name to validate")
	validateChecksum := flag.Bool("checksum", false, "Perform data checksum validation (slower)")
	flag.Parse()

	if *sourceURL == "" || *targetURL == "" || *tableName == "" {
		fmt.Println("Usage: migration-validator -source <source-url> -target <target-url> -table <table-name> [-checksum]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	log.Printf("Connecting to source database...")
	sourceConn, err := pgx.Connect(ctx, *sourceURL)
	if err != nil {
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	defer sourceConn.Close(ctx)

	log.Printf("Connecting to target database...")
	targetConn, err := pgx.Connect(ctx, *targetURL)
	if err != nil {
		log.Fatalf("Failed to connect to target database: %v", err)
	}
	defer targetConn.Close(ctx)

	log.Printf("Starting validation for table '%s'...\n", *tableName)

	if err := validation.ValidateTableMigration(ctx, sourceConn, targetConn, *tableName, *validateChecksum); err != nil {
		log.Fatalf("❌ Validation failed: %v", err)
	}

	log.Printf("\n✓ All validations passed for table '%s'\n", *tableName)
}
