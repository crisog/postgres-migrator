package migrator

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/crisog/postgres-migrator/internal/config"
)

type Dumper struct {
	config *config.Config
	logger *log.Logger
}

func NewDumper(cfg *config.Config, logger *log.Logger) *Dumper {
	return &Dumper{
		config: cfg,
		logger: logger,
	}
}

func (d *Dumper) Dump(ctx context.Context, outputFile string) error {
	d.logger.Println("Starting database dump...")

	if _, err := exec.LookPath("pg_dump"); err != nil {
		return fmt.Errorf("pg_dump not found in PATH: %w", err)
	}

	args := d.buildDumpArgs(outputFile)

	d.logger.Printf("Executing: pg_dump %s\n", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "pg_dump", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", extractPassword(d.config.SourceDatabaseURL)))

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dump: %w", err)
	}

	errOutput := make(chan string, 1)
	go func() {
		stderrBytes, _ := io.ReadAll(stderr)
		errOutput <- string(stderrBytes)
	}()

	if err := cmd.Wait(); err != nil {
		stderrStr := <-errOutput
		return fmt.Errorf("pg_dump failed: %w\nStderr: %s", err, stderrStr)
	}

	d.logger.Printf("Database dump completed successfully: %s\n", outputFile)

	return nil
}

func (d *Dumper) buildDumpArgs(outputFile string) []string {
	args := []string{}

	args = append(args, d.config.SourceDatabaseURL)

	args = append(args, "-Fc")

	args = append(args, "-f", outputFile)

	args = append(args, "-v")

	if d.config.NoOwner {
		args = append(args, "--no-owner")
	}

	if d.config.NoACL {
		args = append(args, "--no-acl")
	}

	return args
}

func extractPassword(connStr string) string {
	if strings.Contains(connStr, "password=") {
		parts := strings.Split(connStr, "password=")
		if len(parts) > 1 {
			passwordPart := parts[1]
			if idx := strings.Index(passwordPart, " "); idx != -1 {
				return passwordPart[:idx]
			}
			return passwordPart
		}
	}

	if strings.HasPrefix(connStr, "postgres://") || strings.HasPrefix(connStr, "postgresql://") {
		withoutScheme := strings.TrimPrefix(connStr, "postgres://")
		withoutScheme = strings.TrimPrefix(withoutScheme, "postgresql://")

		if strings.Contains(withoutScheme, "@") {
			userInfo := strings.Split(withoutScheme, "@")[0]
			if strings.Contains(userInfo, ":") {
				parts := strings.Split(userInfo, ":")
				if len(parts) > 1 {
					return parts[1]
				}
			}
		}
	}

	return ""
}
