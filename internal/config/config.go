package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	SourceDatabaseURL string
	TargetDatabaseURL string
	ParallelJobs      int
	NoOwner           bool
	NoACL             bool
}

func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		SourceDatabaseURL: os.Getenv("SOURCE_DATABASE_URL"),
		TargetDatabaseURL: os.Getenv("TARGET_DATABASE_URL"),
		ParallelJobs:      getEnvAsIntOrDefault("PARALLEL_JOBS", 1),
		NoOwner:           os.Getenv("NO_OWNER") != "false",
		NoACL:             os.Getenv("NO_ACL") != "false",
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.SourceDatabaseURL == "" {
		return fmt.Errorf("SOURCE_DATABASE_URL is required")
	}

	if c.TargetDatabaseURL == "" {
		return fmt.Errorf("TARGET_DATABASE_URL is required")
	}

	if c.ParallelJobs < 1 {
		return fmt.Errorf("PARALLEL_JOBS must be at least 1, got: %d", c.ParallelJobs)
	}

	return nil
}

func getEnvAsIntOrDefault(key string, defaultValue int) int {
	valueStr := os.Getenv(key)
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}

	return value
}
