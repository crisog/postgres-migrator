# postgres-migrator

PostgreSQL database migration tool using native `pg_dump` and `pg_restore`.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.com/deploy/postgres-migrator?referralCode=crisog)

<p align="center">
  <img src="template.jpeg" alt="Deploy Template">
</p>

## Installation

```bash
go install github.com/crisog/postgres-migrator/cmd/postgres-migrator@latest
```

Or build from source:

```bash
git clone https://github.com/crisog/postgres-migrator.git
cd postgres-migrator
go build -o postgres-migrator ./cmd/postgres-migrator
```

## Prerequisites

- PostgreSQL client tools (`pg_dump` and `pg_restore`) must be installed and in your `PATH`
- Source and target databases must have the same PostgreSQL major version
- Target database must be empty (no existing tables in `public` schema)

## Usage

### Basic Migration

```bash
export SOURCE_DATABASE_URL="postgres://user:password@source-host:5432/sourcedb"
export TARGET_DATABASE_URL="postgres://user:password@target-host:5432/targetdb"

postgres-migrator
```

### Configuration

All configuration is done via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_DATABASE_URL` | Yes | - | Source database connection string |
| `TARGET_DATABASE_URL` | Yes | - | Target database connection string |
| `PARALLEL_JOBS` | No | `1` | Number of parallel jobs for restore (recommended: number of CPU cores) |
| `NO_OWNER` | No | `false` | Skip ownership preservation |
| `NO_ACL` | No | `false` | Skip ACL/permissions |
| `VALIDATE_AFTER` | No | `true` | Run validation on all tables after migration completes (set to `false` to skip) |

### With Validation

```bash
export SOURCE_DATABASE_URL="postgres://user:password@source-host:5432/sourcedb"
export TARGET_DATABASE_URL="postgres://user:password@target-host:5432/targetdb"
export VALIDATE_AFTER=true

postgres-migrator
```

This will automatically validate all migrated tables after the migration completes, checking:
- Schema columns and constraints match
- Row counts are identical
- ID ranges are correct
- Aggregate statistics match
- Timestamp ranges are preserved

## Connection String Format

PostgreSQL connection strings can be in URL or keyword format:

**URL format:**
```
postgres://username:password@hostname:port/database?sslmode=disable
postgresql://username:password@hostname:port/database?sslmode=require
```

**Keyword format:**
```
host=hostname port=5432 user=username password=password dbname=database sslmode=disable
```

## Migration Validator

Standalone tool for validating database migrations:

```bash
# Validate all tables
migration-validator \
  -source "postgres://user:pass@source-host:5432/sourcedb" \
  -target "postgres://user:pass@target-host:5432/targetdb"

# Validate a specific table with checksum (slower but thorough)
migration-validator \
  -source "postgres://user:pass@source-host:5432/sourcedb" \
  -target "postgres://user:pass@target-host:5432/targetdb" \
  -table users \
  -checksum
```

The validator checks:
- Schema columns and data types
- Constraints (primary keys, foreign keys, unique)
- Row counts
- ID ranges and uniqueness
- Aggregate statistics (sums, distinct counts)
- Timestamp ranges
- Data checksums (optional, slower)

## How It Works

1. **Validation** - Checks both database connections and verifies version compatibility
2. **Pre-flight checks** - Ensures target database is clean (no existing tables)
3. **Dump** - Creates a compressed custom-format dump of the source database
4. **Restore** - Restores the dump to the target database (optionally in parallel)
5. **Cleanup** - Removes temporary dump file
6. **Post-migration validation** (optional) - Validates all tables were migrated correctly

## Error Handling

The tool will fail and exit with an error if:

- Source or target database is unreachable
- Database versions don't match (different major versions)
- Target database is not empty
- `pg_dump` or `pg_restore` commands fail
- Required roles/users don't exist (when `NO_OWNER=false`)

## License

MIT
