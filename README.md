# postgres-migrator

PostgreSQL database migration tool using native `pg_dump` and `pg_restore`.

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.com/deploy/postgres-migrator?referralCode=crisog)

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
| `NO_OWNER` | No | `true` | Skip ownership preservation (`false` to preserve owners) |
| `NO_ACL` | No | `true` | Skip ACL/permissions (`false` to preserve permissions) |

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

## How It Works

1. **Validation** - Checks both database connections and verifies version compatibility
2. **Pre-flight checks** - Ensures target database is clean (no existing tables)
3. **Dump** - Creates a compressed custom-format dump of the source database
4. **Restore** - Restores the dump to the target database (optionally in parallel)
5. **Cleanup** - Removes temporary dump file

## Error Handling

The tool will fail and exit with an error if:

- Source or target database is unreachable
- Database versions don't match (different major versions)
- Target database is not empty
- `pg_dump` or `pg_restore` commands fail
- Required roles/users don't exist (when `NO_OWNER=false`)

## License

MIT
