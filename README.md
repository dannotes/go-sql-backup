# Parallel Database Backup and Restore Tool

This is a Go-based tool designed to backup and restore Microsoft SQL Server databases to/from Azure Blob Storage. It supports striped backups, configurable parallelism, compression, and detailed error handling with retry logic.

## Features

- **Backup**: Backs up SQL Server databases to Azure Blob Storage with dynamic striping based on database size (smallest first).
- **Restore**: Restores databases from Azure Blob Storage, prioritizing smaller backup sizes first.
- **Compression**: Optional backup compression to reduce storage footprint.
- **Parallel Processing**: Configurable parallelism threshold for concurrent operations.
- **Dry Run**: Simulate backup/restore operations without executing them.
- **Debug Mode**: Detailed logging for troubleshooting.
- **Exclusion List**: Skip specific databases during backup/restore.
- **Copy-Only Backups**: Option to perform copy-only backups that don't affect the backup chain.
- **Colored Output**: ANSI-colored console output for better visibility.
- **Detailed Summary**: Tabular summary of operations with success/failure indicators.

## Prerequisites

- Go 1.18 or higher
- Microsoft SQL Server
- Azure Blob Storage account with SAS token access
- Required Go packages:
  - `github.com/Azure/azure-storage-blob-go/azblob`
  - `github.com/denisenkom/go-mssqldb`
  - `github.com/jedib0t/go-pretty/v6`

Install dependencies:
```bash
go get github.com/Azure/azure-storage-blob-go/azblob
go get github.com/denisenkom/go-mssqldb
go get github.com/jedib0t/go-pretty/v6
```

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Build the application:
   ```bash
   go build -o go-sql-backup main.go
   ```

## Configuration

Create a `config.json` file in the same directory as the executable with the following structure:

```json
{
  "storageAccountName": "yourstorageaccount",
  "containerName": "backups",
  "sqlConnectionString": "server=localhost\\INSTANCE;Integrated Security=SSPI;TrustServerCertificate=true;port=1433;",
  "sasToken": "your-sas-token",
  "dataPath": "D:\\Database",
  "logPath": "D:\\Database",
  "parallelismThreshold": 4,
  "excludedDatabases": "db1,db2,db3",
  "copyOnly": true,
  "compression": true,
  "queryTimeoutInHours": 5
}
```

### Configuration Fields

- `storageAccountName`: Azure Storage Account name.
- `containerName`: Blob container name for backups.
- `sqlConnectionString`: SQL Server connection string.
- `sasToken`: Shared Access Signature token for Azure Blob Storage.
- `dataPath`: Absolute path for restored data files.
- `logPath`: Absolute path for restored log files.
- `parallelismThreshold`: Maximum concurrent backup/restore operations (1-100).
- `excludedDatabases`: Comma-separated list of databases to exclude.
- `copyOnly`: Boolean to enable copy-only backups.
- `compression`: Boolean to enable backup compression.
- `queryTimeoutInHours`: Timeout for SQL queries in hours (1-24).

## Usage

Run the tool with one of the following commands:

### Backup
```bash
./go-sql-backup --backup [--debug] [--dry-run]
```
- Backs up all non-system databases (excluding those in `excludedDatabases`) in order of increasing size.
- Outputs progress and a summary table.

### Restore
```bash
./go-sql-backup --restore [timestamp] [--debug] [--dry-run] [--restore-timestamp <timestamp>]
```
- Restores databases from the latest backup folder or a specified timestamp, smallest first.
- Drops existing databases before restoration.
- Example timestamp: `20250224155736` (YYYYMMDDHHMMSS).

### Flags
- `--backup`: Perform a backup operation.
- `--restore`: Perform a restore operation.
- `--restore-timestamp`: Specify a backup folder timestamp for restore.
- `--debug`: Enable detailed logging.
- `--dry-run`: Simulate operations without executing them.

### Examples
```bash
# Backup all databases
./go-sql-backup --backup

# Restore from a specific timestamp with debug output
./go-sql-backup --restore 20250224155736 --debug

# Simulate a backup
./go-sql-backup --backup --dry-run
```

## Output

- Console logs with timestamps and color-coded status (yellow for info, green for success, red for errors, orange for warnings).
- A summary table at the end showing database names, folder paths, stripe counts, results, time taken, and error details (if any).

## Notes

- The tool creates an Azure credential in SQL Server using the provided SAS token.
- Backups are striped (3, 6, or 12 stripes) based on database size (<100GB, 100-500GB, >500GB).
- Operations can be cancelled with Ctrl+C, with graceful shutdown handling.
- Databases are processed in order of size (smallest first) for both backup and restore.
