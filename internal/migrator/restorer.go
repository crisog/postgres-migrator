package migrator

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"

	"github.com/crisog/postgres-migrator/internal/config"
)

type Restorer struct {
	config *config.Config
	logger *log.Logger
}

func NewRestorer(cfg *config.Config, logger *log.Logger) *Restorer {
	return &Restorer{
		config: cfg,
		logger: logger,
	}
}

func (r *Restorer) Restore(ctx context.Context, inputFile string) error {
	r.logger.Println("Starting database restore...")

	return r.restoreCustomFormat(ctx, inputFile)
}

func (r *Restorer) restoreCustomFormat(ctx context.Context, inputFile string) error {
	if _, err := exec.LookPath("pg_restore"); err != nil {
		return fmt.Errorf("pg_restore not found in PATH: %w", err)
	}

	args := r.buildRestoreArgs(inputFile)

	r.logger.Println("Executing pg_restore...")

	cmd := exec.CommandContext(ctx, "pg_restore", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", extractPassword(r.config.TargetDatabaseURL)))

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	cmd.Stdout = os.Stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_restore: %w", err)
	}

	errOutput := make(chan string, 1)
	go func() {
		stderrBytes, _ := io.ReadAll(stderr)
		errOutput <- string(stderrBytes)
	}()

	waitErr := cmd.Wait()
	stderrStr := <-errOutput

	if waitErr != nil {
		// exit-on-error flag causes pg_restore to exit immediately on error
		// Without it, exit code 1 just means there were warnings
		if r.config.NoOwner {
			// When using --no-owner, we tolerate exit code 1 (warnings)
			if exitErr, ok := waitErr.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
				r.logger.Println("Database restore completed with warnings (some non-fatal errors were ignored)")
				return nil
			}
		}
		return fmt.Errorf("pg_restore failed: %w\nStderr: %s", waitErr, stderrStr)
	}

	r.logger.Println("Database restore completed successfully")

	return nil
}

func (r *Restorer) buildRestoreArgs(inputFile string) []string {
	args := []string{}

	args = append(args, "-d", r.config.TargetDatabaseURL)

	args = append(args, inputFile)

	args = append(args, "-v")

	if r.config.NoOwner {
		args = append(args, "--no-owner")
	} else {
		// When preserving ownership, fail on errors (e.g., missing roles)
		args = append(args, "--exit-on-error")
	}

	if r.config.NoACL {
		args = append(args, "--no-acl")
	}

	if r.config.ParallelJobs > 1 {
		args = append(args, "-j", fmt.Sprintf("%d", r.config.ParallelJobs))
	}

	return args
}
