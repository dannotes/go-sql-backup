package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text" // Added for color customization
)

// ANSI color codes
const (
	ColorReset  = "\033[0m"
	ColorYellow = "\033[33m"       // Info/Warning
	ColorOrange = "\033[38;5;208m" // Warning
	ColorGreen  = "\033[32m"       // Success
	ColorRed    = "\033[31m"       // Error
)

// Config holds configuration parameters.
type Config struct {
	StorageAccountName   string `json:"storageAccountName"`
	ContainerName        string `json:"containerName"`
	SQLConnectionString  string `json:"sqlConnectionString"`
	SASToken             string `json:"sasToken"`
	DataPath             string `json:"dataPath"`
	LogPath              string `json:"logPath"`
	ParallelismThreshold int    `json:"parallelismThreshold"`
	ExcludedDatabases    string `json:"excludedDatabases"`
	CopyOnly             bool   `json:"copyOnly"`
	QueryTimeoutInHours  int    `json:"queryTimeoutInHours"`
	Compression          bool   `json:"compression"`
}

// DatabaseSize holds database name and size
type DatabaseSize struct {
	Name   string
	SizeGB float64
}

// OperationResult holds the result of a backup or restore operation.
type OperationResult struct {
	DBName      string
	FolderPath  string
	StripeCount int
	Result      string
	TimeTaken   time.Duration
	Error       error
}

// SQLDBError represents an individual MSSQL error structure
type SQLDBError struct {
	Number     int32
	State      uint8
	Class      uint8
	Message    string
	ServerName string
	ProcName   string
	LineNo     int32
}

// SQLDBErrorWrapper holds all SQL errors in the `All` property
type SQLDBErrorWrapper struct {
	All []SQLDBError
}

// App encapsulates the application state.
type App struct {
	config       Config
	db           *sql.DB
	logger       *slog.Logger
	debug        bool
	dryRun       bool
	runTimestamp string
}

// customLogHandler implements slog.Handler for simple colored output.
type customLogHandler struct {
	writer io.Writer
	debug  bool
}

func (h *customLogHandler) Enabled(_ context.Context, level slog.Level) bool {
	if level == slog.LevelDebug {
		return h.debug
	}
	return true
}

func (h *customLogHandler) Handle(_ context.Context, r slog.Record) error {
	var color string
	switch r.Level {
	case slog.LevelInfo, slog.LevelDebug:
		color = ColorYellow
	case slog.LevelWarn:
		color = ColorOrange
	case slog.LevelError:
		color = ColorRed
	default:
		color = ColorReset
	}

	msg := fmt.Sprintf("%s%s %s %s%s\n", color, r.Time.Format("2006-01-02T15:04:05.000-07:00"), r.Level, r.Message, ColorReset)
	_, err := h.writer.Write([]byte(msg))
	return err
}

func (h *customLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *customLogHandler) WithGroup(name string) slog.Handler {
	return h
}

// extractSQLErrors extracts detailed error information from mssql.Error
func extractSQLErrors(err error) SQLDBErrorWrapper {
	var errorList []SQLDBError

	errValue := reflect.ValueOf(err)
	if errValue.Kind() == reflect.Ptr {
		errValue = errValue.Elem()
	}

	if errValue.Type().String() == "mssql.Error" {
		primaryErr := SQLDBError{Message: err.Error()}

		if numField := errValue.FieldByName("Number"); numField.IsValid() {
			primaryErr.Number = int32(numField.Int())
		}
		if stateField := errValue.FieldByName("State"); stateField.IsValid() {
			primaryErr.State = uint8(stateField.Uint())
		}
		if classField := errValue.FieldByName("Class"); classField.IsValid() {
			primaryErr.Class = uint8(classField.Uint())
		}
		if msgField := errValue.FieldByName("Message"); msgField.IsValid() {
			primaryErr.Message = msgField.String()
		}
		if serverField := errValue.FieldByName("ServerName"); serverField.IsValid() {
			primaryErr.ServerName = serverField.String()
		}
		if procField := errValue.FieldByName("ProcName"); procField.IsValid() {
			primaryErr.ProcName = procField.String()
		}
		if lineField := errValue.FieldByName("LineNo"); lineField.IsValid() {
			primaryErr.LineNo = int32(lineField.Int())
		}

		errorList = append(errorList, primaryErr)

		allField := errValue.FieldByName("All")
		if allField.IsValid() && allField.Kind() == reflect.Slice {
			for i := 0; i < allField.Len(); i++ {
				errItem := allField.Index(i)
				if errItem.Kind() == reflect.Struct {
					dbErr := SQLDBError{}

					if numField := errItem.FieldByName("Number"); numField.IsValid() {
						dbErr.Number = int32(numField.Int())
					}
					if stateField := errItem.FieldByName("State"); stateField.IsValid() {
						dbErr.State = uint8(stateField.Uint())
					}
					if classField := errItem.FieldByName("Class"); classField.IsValid() {
						dbErr.Class = uint8(classField.Uint())
					}
					if msgField := errItem.FieldByName("Message"); msgField.IsValid() {
						dbErr.Message = msgField.String()
					}
					if serverField := errItem.FieldByName("ServerName"); serverField.IsValid() {
						dbErr.ServerName = serverField.String()
					}
					if procField := errItem.FieldByName("ProcName"); procField.IsValid() {
						dbErr.ProcName = procField.String()
					}
					if lineField := errItem.FieldByName("LineNo"); lineField.IsValid() {
						dbErr.LineNo = int32(lineField.Int())
					}

					if dbErr.Number == primaryErr.Number && dbErr.Message == primaryErr.Message {
						continue
					}

					errorList = append(errorList, dbErr)
				}
			}
		}
	} else {
		errorList = append(errorList, SQLDBError{Message: err.Error()})
	}

	return SQLDBErrorWrapper{All: errorList}
}

// NewApp initializes the application with simplified logging for production.
func NewApp(debug, dryRun bool) (*App, error) {
	logger := slog.New(&customLogHandler{
		writer: os.Stdout,
		debug:  debug,
	})

	config, err := readConfig("config.json")
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to read config: %v", err))
		return nil, fmt.Errorf("reading config: %w", err)
	}

	db, err := connectToSQL(config.SQLConnectionString)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to connect to SQL Server: %v", err))
		return nil, fmt.Errorf("connecting to SQL: %w", err)
	}

	return &App{config: config, db: db, logger: logger, debug: debug, dryRun: dryRun}, nil
}

// readConfig reads and validates the config file with stricter checks.
func readConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("opening config: %w", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("parsing config: %w", err)
	}

	switch {
	case config.StorageAccountName == "":
		return Config{}, fmt.Errorf("storageAccountName is required")
	case config.ContainerName == "":
		return Config{}, fmt.Errorf("containerName is required")
	case config.SQLConnectionString == "":
		return Config{}, fmt.Errorf("sqlConnectionString is required")
	case config.SASToken == "":
		return Config{}, fmt.Errorf("sasToken is required")
	case config.DataPath == "":
		return Config{}, fmt.Errorf("dataPath is required")
	case !filepath.IsAbs(config.DataPath):
		return Config{}, fmt.Errorf("dataPath must be an absolute path")
	case config.LogPath == "":
		return Config{}, fmt.Errorf("logPath is required")
	case !filepath.IsAbs(config.LogPath):
		return Config{}, fmt.Errorf("logPath must be an absolute path")
	case config.ParallelismThreshold <= 0 || config.ParallelismThreshold > 100:
		slog.Info("parallelismThreshold out of range (1-100), defaulting to 1")
		config.ParallelismThreshold = 1
	case config.QueryTimeoutInHours <= 0 || config.QueryTimeoutInHours > 24:
		slog.Info("queryTimeoutInHours out of range (1-24), defaulting to 5")
		config.QueryTimeoutInHours = 5
	}

	return config, nil
}

// connectToSQL establishes a SQL Server connection with retry logic.
func connectToSQL(connString string) (*sql.DB, error) {
	const maxRetries = 3
	const retryDelay = 5 * time.Second

	var db *sql.DB
	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		db, err = sql.Open("sqlserver", connString)
		if err != nil {
			return nil, fmt.Errorf("opening SQL connection: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			slog.Info(fmt.Sprintf("Connected to SQL Server (attempt %d)", attempt))
			return db, nil
		}

		db.Close()
		if attempt < maxRetries {
			dbErrors := extractSQLErrors(err)
			var errDetails []string
			for _, dbErr := range dbErrors.All {
				errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
					dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
				errDetails = append(errDetails, errMsg)
			}
			slog.Warn(fmt.Sprintf("Failed to ping SQL Server, retrying (attempt %d): %s", attempt, strings.Join(errDetails, "; ")))
			time.Sleep(retryDelay)
		}
	}

	dbErrors := extractSQLErrors(err)
	var errDetails []string
	for _, dbErr := range dbErrors.All {
		errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
			dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
		errDetails = append(errDetails, errMsg)
	}
	return nil, fmt.Errorf("failed to connect to SQL Server after %d attempts: %s", maxRetries, strings.Join(errDetails, "; "))
}

// contains checks if a string exists in a slice, case-insensitively.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if strings.EqualFold(s, strings.TrimSpace(item)) {
			return true
		}
	}
	return false
}

// getDatabasesToBackup retrieves databases excluding system and excluded ones.
func (a *App) getDatabasesToBackup(ctx context.Context) ([]string, error) {
	query := `
        SELECT d.name, SUM(CAST(mf.size AS BIGINT)) * 8.0 / 1024 / 1024 AS size_gb
        FROM sys.databases d
        LEFT JOIN sys.master_files mf ON d.database_id = mf.database_id
        WHERE d.name NOT IN ('master', 'tempdb', 'model', 'msdb')
        GROUP BY d.name
    `
	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		dbErrors := extractSQLErrors(err)
		var errDetails []string
		for _, dbErr := range dbErrors.All {
			errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
				dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
			errDetails = append(errDetails, errMsg)
		}
		return nil, fmt.Errorf("querying databases: %s", strings.Join(errDetails, "; "))
	}
	defer rows.Close()

	var dbSizes []DatabaseSize
	excluded := strings.Split(a.config.ExcludedDatabases, ",")
	for rows.Next() {
		var db DatabaseSize
		if err := rows.Scan(&db.Name, &db.SizeGB); err != nil {
			dbErrors := extractSQLErrors(err)
			var errDetails []string
			for _, dbErr := range dbErrors.All {
				errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
					dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
				errDetails = append(errDetails, errMsg)
			}
			return nil, fmt.Errorf("scanning database info: %s", strings.Join(errDetails, "; "))
		}
		if !contains(excluded, db.Name) {
			dbSizes = append(dbSizes, db)
		}
	}

	if err := rows.Err(); err != nil {
		dbErrors := extractSQLErrors(err)
		var errDetails []string
		for _, dbErr := range dbErrors.All {
			errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
				dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
			errDetails = append(errDetails, errMsg)
		}
		return nil, fmt.Errorf("reading database rows: %s", strings.Join(errDetails, "; "))
	}

	// Sort by size ascending
	sort.Slice(dbSizes, func(i, j int) bool {
		return dbSizes[i].SizeGB < dbSizes[j].SizeGB
	})

	// Extract just the names in sorted order
	databases := make([]string, len(dbSizes))
	for i, db := range dbSizes {
		databases[i] = db.Name
	}
	return databases, nil
}

// Modified listLatestBackupsFromBlob to include size sorting
func (a *App) listLatestBackupsFromBlob(ctx context.Context, restoreTimestamp string) (map[string][]string, string, error) {
	credential := azblob.NewAnonymousCredential()
	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	baseURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.config.StorageAccountName, a.config.ContainerName)
	sasToken := a.config.SASToken
	if !strings.HasPrefix(sasToken, "?") {
		sasToken = "?" + sasToken
	}
	urlStr := baseURL + sasToken
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return nil, "", fmt.Errorf("parsing container URL: %w", err)
	}
	containerURL := azblob.NewContainerURL(*parsedURL, pipeline)

	var selectedTimestamp string
	if restoreTimestamp != "" {
		if _, err := time.Parse("20060102150405", restoreTimestamp); err != nil {
			return nil, "", fmt.Errorf("invalid restore timestamp format: %s (expected YYYYMMDDHHMMSS)", restoreTimestamp)
		}
		selectedTimestamp = restoreTimestamp

		hasBlobs := false
		for marker := (azblob.Marker{}); marker.NotDone() && !hasBlobs; {
			listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
				Prefix: selectedTimestamp + "/",
			})
			if err != nil {
				return nil, "", fmt.Errorf("checking restore folder %s: %w", selectedTimestamp, err)
			}
			marker = listBlob.NextMarker
			if len(listBlob.Segment.BlobItems) > 0 {
				hasBlobs = true
			}
		}
		if !hasBlobs {
			return nil, "", fmt.Errorf("restore folder %s not found in %s/%s", selectedTimestamp, a.config.StorageAccountName, a.config.ContainerName)
		}
	} else {
		timestampFolders := make(map[string]time.Time)
		for marker := (azblob.Marker{}); marker.NotDone(); {
			listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{})
			if err != nil {
				return nil, "", fmt.Errorf("listing blobs: %w", err)
			}
			marker = listBlob.NextMarker

			for _, blob := range listBlob.Segment.BlobItems {
				parts := strings.Split(blob.Name, "/")
				if len(parts) < 3 {
					continue
				}
				timestamp := parts[0]
				if timestamp == "" || contains([]string{"master", "tempdb", "model", "msdb"}, parts[1]) {
					continue
				}
				parsedTime, err := time.Parse("20060102150405", timestamp)
				if err != nil {
					continue
				}
				timestampFolders[timestamp] = parsedTime
			}
		}

		if len(timestampFolders) == 0 {
			return nil, "", fmt.Errorf("no valid timestamped backup folders found in %s/%s", a.config.StorageAccountName, a.config.ContainerName)
		}

		latestTimestamp := ""
		latestTime := time.Time{}
		for ts, t := range timestampFolders {
			if t.After(latestTime) {
				latestTime = t
				latestTimestamp = ts
			}
		}
		selectedTimestamp = latestTimestamp
	}

	backupFiles := make(map[string][]string)
	backupTimes := make(map[string]time.Time)
	fileSizes := make(map[string]int64) // Track total size per database

	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
			Prefix: selectedTimestamp + "/",
			Details: azblob.BlobListingDetails{
				Metadata: true,
			},
		})
		if err != nil {
			return nil, "", fmt.Errorf("listing blobs in %s: %w", selectedTimestamp, err)
		}
		marker = listBlob.NextMarker

		for _, blob := range listBlob.Segment.BlobItems {
			parts := strings.Split(blob.Name, "/")
			if len(parts) != 3 {
				continue
			}
			dbName := parts[1]
			fileName := parts[2]

			if !strings.HasSuffix(fileName, ".bak") {
				continue
			}
			timeStr := strings.TrimSuffix(strings.TrimPrefix(fileName, dbName+"-"), ".bak")
			timeStr = strings.Split(timeStr, "_")[0]
			backupTime, err := time.Parse("20060102150405", timeStr)
			if err != nil {
				continue
			}

			if existingTime, exists := backupTimes[dbName]; !exists || backupTime.After(existingTime) {
				backupTimes[dbName] = backupTime
				backupFiles[dbName] = []string{}
				fileSizes[dbName] = 0
			}
			if backupTimes[dbName].Equal(backupTime) {
				backupFiles[dbName] = append(backupFiles[dbName], blob.Name)
				if blob.Properties.ContentLength != nil {
					fileSizes[dbName] += *blob.Properties.ContentLength
				}
			}
		}
	}

	if len(backupFiles) == 0 {
		return nil, "", fmt.Errorf("no valid backups found in folder %s/%s/%s", a.config.StorageAccountName, a.config.ContainerName, selectedTimestamp)
	}

	// Sort databases by total backup size
	type dbSize struct {
		Name string
		Size int64
	}
	var dbSizes []dbSize
	for dbName := range backupFiles {
		dbSizes = append(dbSizes, dbSize{Name: dbName, Size: fileSizes[dbName]})
	}
	sort.Slice(dbSizes, func(i, j int) bool {
		return dbSizes[i].Size < dbSizes[j].Size
	})

	sortedBackupFiles := make(map[string][]string)
	for _, db := range dbSizes {
		sortedBackupFiles[db.Name] = backupFiles[db.Name]
		sort.Strings(sortedBackupFiles[db.Name])
	}

	return sortedBackupFiles, selectedTimestamp, nil
}

// setupCredential manages SQL Server credential for Azure Blob storage.
func (a *App) setupCredential(ctx context.Context) error {
	credentialName := fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.config.StorageAccountName, a.config.ContainerName)

	dropQuery := fmt.Sprintf("IF EXISTS (SELECT * FROM sys.credentials WHERE name = '%s') DROP CREDENTIAL [%s]", credentialName, credentialName)
	if a.debug {
		a.logger.Debug(fmt.Sprintf("Executing drop credential query: %s", dropQuery))
	}
	if _, err := a.db.ExecContext(ctx, dropQuery); err != nil {
		dbErrors := extractSQLErrors(err)
		var errDetails []string
		for _, dbErr := range dbErrors.All {
			errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
				dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
			errDetails = append(errDetails, errMsg)
		}
		detailedErr := fmt.Errorf("dropping existing credential: %s", strings.Join(errDetails, "; "))
		a.logger.Error(fmt.Sprintf("Failed to drop credential: %v", detailedErr))
		return detailedErr
	}

	secret := strings.TrimPrefix(a.config.SASToken, "?")
	createQuery := fmt.Sprintf("CREATE CREDENTIAL [%s] WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '%s'",
		credentialName, secret)
	if a.debug {
		a.logger.Debug(fmt.Sprintf("Executing create credential query: %s", createQuery))
	}
	if _, err := a.db.ExecContext(ctx, createQuery); err != nil {
		dbErrors := extractSQLErrors(err)
		var errDetails []string
		for _, dbErr := range dbErrors.All {
			errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
				dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
			errDetails = append(errDetails, errMsg)
		}
		detailedErr := fmt.Errorf("creating credential: %s", strings.Join(errDetails, "; "))
		a.logger.Error(fmt.Sprintf("Failed to create credential: %v", detailedErr))
		return detailedErr
	}

	a.logger.Info(fmt.Sprintf("Azure credential setup completed for %s", credentialName))
	return nil
}

// Modified backupDatabase to include compression
func (a *App) backupDatabase(ctx context.Context, dbName string, results chan<- OperationResult) {
	start := time.Now()

	timeout := time.Duration(a.config.QueryTimeoutInHours) * time.Hour
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Determine database size (in GB)
	querySize := fmt.Sprintf(`
        SELECT SUM(CAST(size AS BIGINT)) * 8.0 / 1024 / 1024 AS size_gb
        FROM sys.master_files
        WHERE database_id = DB_ID('%s')
    `, dbName)
	var sizeGB float64
	err := a.db.QueryRowContext(ctx, querySize).Scan(&sizeGB)
	if err != nil {
		dbErrors := extractSQLErrors(err)
		var errDetails []string
		for _, dbErr := range dbErrors.All {
			errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
				dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
			errDetails = append(errDetails, errMsg)
		}
		detailedErr := fmt.Errorf("determining size: %s", strings.Join(errDetails, "; "))
		a.logger.Error(fmt.Sprintf("Failed to determine size of %s: %v", dbName, detailedErr))
		results <- OperationResult{DBName: dbName, FolderPath: "", StripeCount: 0, Result: "Failure", TimeTaken: time.Since(start), Error: detailedErr}
		return
	}

	// Calculate stripes based on size
	stripeCount := 1
	if sizeGB >= 500 {
		stripeCount = 6
	} else if sizeGB >= 100 {
		stripeCount = 3
	}

	// Generate backup files
	fileTimestamp := start.Format("20060102150405")
	folderPath := fmt.Sprintf("%s/%s", a.runTimestamp, dbName)
	var urls []string
	for i := 1; i <= stripeCount; i++ {
		backupFile := fmt.Sprintf("%s/%s-%s_%d.bak", folderPath, dbName, fileTimestamp, i)
		url := fmt.Sprintf("URL = 'https://%s.blob.core.windows.net/%s/%s'", a.config.StorageAccountName, a.config.ContainerName, backupFile)
		urls = append(urls, url)
	}
	backupURL := strings.Join(urls, ", ")

	// Add COPY_ONLY, COMPRESSION and MAXTRANSFERSIZE if configured
	withClause := "WITH FORMAT, STATS = 10, MAXTRANSFERSIZE = 4194304"
	if a.config.CopyOnly {
		withClause += ", COPY_ONLY"
	}
	if a.config.Compression {
		withClause += ", COMPRESSION"
	}

	// Full backup command
	backupCommand := fmt.Sprintf("BACKUP DATABASE [%s] TO %s %s", dbName, backupURL, withClause)

	prefix := ""
	if a.dryRun {
		prefix = "Dry run: "
	}
	fmt.Printf("%s %s%sStarting backup of %s to %s (%d stripes)%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, prefix, dbName, folderPath, stripeCount, ColorReset)
	if a.debug {
		a.logger.Debug(fmt.Sprintf("Executing backup command: %s", backupCommand))
	}

	// Retry logic
	const maxRetries = 3
	const retryDelay = 5 * time.Second
	var detailedErr error
	result := "Failure"
	if a.dryRun {
		result = "Success (simulated)"
		time.Sleep(1 * time.Second)
	} else {
		for attempt := 1; attempt <= maxRetries; attempt++ {
			_, err := a.db.ExecContext(ctx, backupCommand)
			if err == nil {
				result = "Success"
				detailedErr = nil
				break
			}

			dbErrors := extractSQLErrors(err)
			var errDetails []string
			for _, dbErr := range dbErrors.All {
				errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
					dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
				errDetails = append(errDetails, errMsg)
			}
			detailedErr = fmt.Errorf("backup failed: %s", strings.Join(errDetails, "; "))

			a.logger.Warn(fmt.Sprintf("Backup attempt %d failed for %s, retrying: %v", attempt, dbName, detailedErr))
			if attempt < maxRetries {
				time.Sleep(retryDelay)
			}
		}
	}

	duration := time.Since(start)
	if result == "Success" || result == "Success (simulated)" {
		fmt.Printf("%s %s%sBackup of %s completed successfully in %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorGreen, prefix, dbName, duration, ColorReset)
	} else {
		conciseErr := "Backup failed"
		if detailedErr != nil {
			conciseErr += ": " + detailedErr.Error()
		}
		fmt.Printf("%s %s%sBackup of %s failed: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, prefix, dbName, conciseErr, ColorReset)
		if a.debug {
			a.logger.Error(fmt.Sprintf("Backup of %s failed: %v", dbName, detailedErr))
		} else {
			a.logger.Error(fmt.Sprintf("Backup of %s failed: %s", dbName, conciseErr))
		}
	}

	if a.debug {
		a.logger.Info(fmt.Sprintf("Backup of %s completed with result %s in %s", dbName, result, duration), "folder", folderPath, "stripes", stripeCount, "error", detailedErr)
	}
	results <- OperationResult{DBName: dbName, FolderPath: folderPath, StripeCount: stripeCount, Result: result, TimeTaken: duration, Error: detailedErr}
}

// restoreDatabase restores a database from a striped backup.
// restoreDatabase restores a database from a striped backup.
func (a *App) restoreDatabase(ctx context.Context, dbName string, blobPaths []string, results chan<- OperationResult) {
	start := time.Now()

	timeout := time.Duration(a.config.QueryTimeoutInHours) * time.Hour
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dropQuery := fmt.Sprintf(`
        IF EXISTS (SELECT * FROM sys.databases WHERE name = '%s')
        BEGIN
            ALTER DATABASE [%s] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE [%s];
        END
    `, dbName, dbName, dbName)
	if a.debug {
		a.logger.Debug(fmt.Sprintf("Executing drop database query: %s", dropQuery))
	}

	prefix := ""
	if a.dryRun {
		prefix = "Dry run: "
	}

	folderPath := strings.Split(blobPaths[0], "/")[0] + "/" + dbName
	fmt.Printf("%s %s%sStarting restore of %s from %s (%d stripes)%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, prefix, dbName, folderPath, len(blobPaths), ColorReset)
	if a.debug {
		a.logger.Debug(fmt.Sprintf("Starting restore of %s from %s with %d stripes", dbName, folderPath, len(blobPaths)))
	}

	result := "Failure"
	var detailedErr error
	if a.dryRun {
		result = "Success (simulated)"
		time.Sleep(1 * time.Second)
	} else {
		if _, err := a.db.ExecContext(ctx, dropQuery); err != nil {
			dbErrors := extractSQLErrors(err)
			var errDetails []string
			for _, dbErr := range dbErrors.All {
				errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
					dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
				errDetails = append(errDetails, errMsg)
			}
			detailedErr = fmt.Errorf("dropping database: %s", strings.Join(errDetails, "; "))
			a.logger.Error(fmt.Sprintf("Failed to drop database %s: %v", dbName, detailedErr))
			results <- OperationResult{DBName: dbName, FolderPath: "", StripeCount: 0, Result: "Failure", TimeTaken: time.Since(start), Error: detailedErr}
			return
		}

		dataDir := filepath.Join(a.config.DataPath, dbName)
		logDir := filepath.Join(a.config.LogPath, dbName)
		for _, dir := range []string{dataDir, logDir} {
			// Check if directory exists, create if it doesn't
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				if err := os.MkdirAll(dir, 0755); err != nil {
					detailedErr = fmt.Errorf("creating directory %s: %w", dir, err)
					a.logger.Error(fmt.Sprintf("Failed to create directory %s: %v", dir, detailedErr))
					results <- OperationResult{DBName: dbName, FolderPath: "", StripeCount: 0, Result: "Failure", TimeTaken: time.Since(start), Error: detailedErr}
					return
				}
			} else if err != nil {
				// Handle other stat errors (e.g., permission issues)
				detailedErr = fmt.Errorf("checking directory %s: %w", dir, err)
				a.logger.Error(fmt.Sprintf("Failed to check directory %s: %v", dir, detailedErr))
				results <- OperationResult{DBName: dbName, FolderPath: "", StripeCount: 0, Result: "Failure", TimeTaken: time.Since(start), Error: detailedErr}
				return
			}
			// If directory exists, no action needed
		}

		urls := make([]string, len(blobPaths))
		for i, path := range blobPaths {
			urls[i] = fmt.Sprintf("URL = 'https://%s.blob.core.windows.net/%s/%s'", a.config.StorageAccountName, a.config.ContainerName, path)
		}
		restoreURL := strings.Join(urls, ", ")
		restoreCommand := fmt.Sprintf(`
            RESTORE DATABASE [%s]
            FROM %s
            WITH
                MOVE '%s' TO '%s/%s.mdf',
                MOVE '%s_log' TO '%s/%s_log.ldf',
                RECOVERY
        `, dbName, restoreURL, dbName, dataDir, dbName, dbName, logDir, dbName)

		_, err := a.db.ExecContext(ctx, restoreCommand)
		if err != nil {
			dbErrors := extractSQLErrors(err)
			var errDetails []string
			for _, dbErr := range dbErrors.All {
				errMsg := fmt.Sprintf("Error #%d (Severity %d, State %d, Line %d, Proc %s): %s",
					dbErr.Number, dbErr.Class, dbErr.State, dbErr.LineNo, dbErr.ProcName, dbErr.Message)
				errDetails = append(errDetails, errMsg)
			}
			detailedErr = fmt.Errorf("restore failed: %s", strings.Join(errDetails, "; "))
			a.logger.Error(fmt.Sprintf("Restore of %s failed: %v", dbName, detailedErr))
			results <- OperationResult{DBName: dbName, FolderPath: folderPath, StripeCount: len(blobPaths), Result: "Failure", TimeTaken: time.Since(start), Error: detailedErr}
			return
		}

		result = "Success"
		detailedErr = nil
	}

	duration := time.Since(start)
	if result == "Success" || result == "Success (simulated)" {
		fmt.Printf("%s %s%sRestore of %s completed successfully in %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorGreen, prefix, dbName, duration, ColorReset)
	} else {
		conciseErr := "Restore failed"
		if detailedErr != nil {
			conciseErr += ": " + detailedErr.Error()
		}
		fmt.Printf("%s %s%sRestore of %s failed: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, prefix, dbName, conciseErr, ColorReset)
		if a.debug {
			a.logger.Error(fmt.Sprintf("Restore of %s failed: %v", dbName, detailedErr))
		} else {
			a.logger.Error(fmt.Sprintf("Restore of %s failed: %s", dbName, conciseErr))
		}
	}
	if a.debug {
		a.logger.Info(fmt.Sprintf("Restore of %s completed with result %s in %s", dbName, result, duration), "folder", folderPath, "stripes", len(blobPaths), "error", detailedErr)
	}
	results <- OperationResult{DBName: dbName, FolderPath: folderPath, StripeCount: len(blobPaths), Result: result, TimeTaken: duration, Error: detailedErr}
}

// redactSASToken redacts the SAS token for display.
func redactSASToken(sasToken string) string {
	parts := strings.Split(sasToken, "&")
	for i, part := range parts {
		if strings.HasPrefix(part, "sig=") {
			parts[i] = "sig=****"
		}
	}
	return strings.Join(parts, "&")
}

// displaySummary prints the operation summary with colors, borders, grid lines, and custom symbols.
func displaySummary(results []OperationResult, operation string) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{
		"Database Name",
		"Folder Path",
		"Stripe Count",
		"Result",
		"Time Taken",
		"Error Details",
	})

	// Customize StyleColoredDark with borders and grid lines
	t.SetStyle(table.Style{
		Name: "StyleColoredDark",
		Box:  table.StyleBoxDefault,
		Color: table.ColorOptions{
			Header: text.Colors{text.FgHiCyan},  // Cyan header for contrast
			Row:    text.Colors{text.FgHiWhite}, // White rows for readability
		},
		Format: table.FormatOptions{
			Header: text.FormatUpper, // Uppercase header text
		},
		Options: table.Options{
			DrawBorder:      true,  // Add borders around the table
			SeparateColumns: true,  // Add vertical lines between columns
			SeparateHeader:  true,  // Add a line under the header
			SeparateRows:    false, // No horizontal lines between rows (can change to true if desired)
		},
	})

	sort.Slice(results, func(i, j int) bool { return results[i].DBName < results[j].DBName })
	for _, r := range results {
		errStr := ""
		if r.Error != nil {
			errStr = text.WrapSoft(r.Error.Error(), 50) // Wrap long error messages
		}
		resultStr := ""
		if r.Result == "Success" || r.Result == "Success (simulated)" {
			resultStr = fmt.Sprintf("%s✅ %s%s", ColorGreen, r.Result, ColorReset) // Green check mark
		} else {
			resultStr = fmt.Sprintf("%s❌ Failure%s", ColorRed, ColorReset) // Red cross
		}
		t.AppendRow(table.Row{
			r.DBName,
			r.FolderPath,
			r.StripeCount,
			resultStr,
			r.TimeTaken.String(),
			errStr,
		})
	}

	fmt.Printf("%s %sBackup Summary:%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, ColorReset)
	t.Render()
}

// Modified confirm to show compression status
func (a *App) confirm(operation string, databases []string, restoreTimestamp string) bool {
	var selectedTimestamp string
	if operation == "restore" {
		backupFiles, ts, err := a.listLatestBackupsFromBlob(context.Background(), restoreTimestamp)
		if err != nil {
			fmt.Printf("%s %sError determining restore folder: %v%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, err, ColorReset)
			return false
		}
		selectedTimestamp = ts
		databases = []string{}
		for dbName := range backupFiles {
			databases = append(databases, dbName)
		}
		sort.Strings(databases)
	}

	fmt.Printf("%s %sOperation: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, operation, ColorReset)
	if operation == "restore" && selectedTimestamp != "" {
		fmt.Printf("%s %sSelected restore folder: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, selectedTimestamp, ColorReset)
	}
	fmt.Printf("%s %sNumber of databases affected: %d%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, len(databases), ColorReset)
	fmt.Printf("%s %sDatabases: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, strings.Join(databases, ", "), ColorReset)
	fmt.Printf("%s %sConfig:%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, ColorReset)

	redactedSASToken := redactSASToken(a.config.SASToken)
	fmt.Printf("%s   %sStorageAccountName: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.StorageAccountName, ColorReset)
	fmt.Printf("%s   %sContainerName: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.ContainerName, ColorReset)
	fmt.Printf("%s   %sSASToken: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, redactedSASToken, ColorReset)
	fmt.Printf("%s   %sParallelismThreshold: %d%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.ParallelismThreshold, ColorReset)
	if a.config.ExcludedDatabases == "" {
		fmt.Printf("%s   %sExcludedDatabases: none%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, ColorReset)
	} else {
		fmt.Printf("%s   %sExcludedDatabases: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.ExcludedDatabases, ColorReset)
	}
	fmt.Printf("%s   %sCopyOnly: %t%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.CopyOnly, ColorReset)
	fmt.Printf("%s   %sCompression: %t%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.Compression, ColorReset)
	fmt.Printf("%s   %sQueryTimeoutInHours: %d%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.QueryTimeoutInHours, ColorReset)
	if operation == "restore" {
		fmt.Printf("%s   %sDataPath: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.DataPath, ColorReset)
		fmt.Printf("%s   %sLogPath: %s%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorYellow, a.config.LogPath, ColorReset)
	}

	if operation == "restore" {
		fmt.Printf("%s %sWarning: Existing databases will be dropped before restoration.%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
	}
	fmt.Printf("%s %sProceed? (yes/no): %s", time.Now().Format("2006-01-02 15:04:05"), ColorGreen, ColorReset)
	var response string
	fmt.Scanln(&response)
	return strings.ToLower(response) == "yes"
}

func main() {
	backupFlag := flag.Bool("backup", false, "Perform database backup")
	restoreFlag := flag.Bool("restore", false, "Perform database restore (optional timestamp follows, e.g., --restore 20250224155736)")
	restoreTimestampFlag := flag.String("restore-timestamp", "", "Specify a timestamp folder for restore (e.g., 20250224155736)")
	debugFlag := flag.Bool("debug", false, "Enable debug logging")
	dryRunFlag := flag.Bool("dry-run", false, "Simulate backup/restore operations without executing them")
	flag.Parse()

	var restoreTimestamp string
	if *restoreFlag {
		args := flag.Args()
		if len(args) > 0 {
			restoreTimestamp = args[0]
		} else if *restoreTimestampFlag != "" {
			restoreTimestamp = *restoreTimestampFlag
		}
	} else if *backupFlag {
		if len(flag.Args()) > 0 {
			fmt.Printf("%s %sError: Unexpected arguments after --backup%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, ColorReset)
			fmt.Println("Usage:")
			fmt.Println("  go run main.go --backup [--debug] [--dry-run]")
			fmt.Println("    Perform a full database backup to Azure Blob Storage.")
			fmt.Println("    --debug: Enable detailed logging.")
			fmt.Println("    --dry-run: Simulate the backup without executing it.")
			fmt.Println("  go run main.go --restore [timestamp] [--debug] [--dry-run] [--restore-timestamp <timestamp>]")
			fmt.Println("    Restore databases from Azure Blob Storage.")
			fmt.Println("    [timestamp]: Optional folder timestamp (e.g., 20250224155736).")
			fmt.Println("    --restore-timestamp: Alternative way to specify timestamp.")
			fmt.Println("    --debug: Enable detailed logging.")
			fmt.Println("    --dry-run: Simulate the restore without executing it.")
			fmt.Println("Configuration is read from config.json:")
			fmt.Println("  - storageAccountName: Azure storage account name")
			fmt.Println("  - containerName: Blob container name")
			fmt.Println("  - sqlConnectionString: SQL Server connection string")
			fmt.Println("  - sasToken: Azure SAS token")
			fmt.Println("  - dataPath: Local path for restore data files")
			fmt.Println("  - logPath: Local path for restore log files")
			fmt.Println("  - parallelismThreshold: Max concurrent operations (1-100)")
			fmt.Println("  - excludedDatabases: Comma-separated list of databases to exclude")
			fmt.Println("  - copyOnly: Boolean to enable COPY_ONLY backup (true/false)")
			fmt.Println("  - queryTimeoutInHours: Timeout for SQL queries in hours (1-24)")
			fmt.Println("Examples:")
			fmt.Println("  go run main.go --backup")
			fmt.Println("  go run main.go --restore 20250224155736 --debug")
			fmt.Println("  go run main.go --backup --dry-run")
			os.Exit(1)
		}
	} else {
		fmt.Printf("%s %sError: Must specify exactly one of --backup or --restore%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, ColorReset)
		fmt.Println("Usage:")
		fmt.Println("  go run main.go --backup [--debug] [--dry-run]")
		fmt.Println("    Perform a full database backup to Azure Blob Storage.")
		fmt.Println("    --debug: Enable detailed logging.")
		fmt.Println("    --dry-run: Simulate the backup without executing it.")
		fmt.Println("  go run main.go --restore [timestamp] [--debug] [--dry-run] [--restore-timestamp <timestamp>]")
		fmt.Println("    Restore databases from Azure Blob Storage.")
		fmt.Println("    [timestamp]: Optional folder timestamp (e.g., 20250224155736).")
		fmt.Println("    --restore-timestamp: Alternative way to specify timestamp.")
		fmt.Println("    --debug: Enable detailed logging.")
		fmt.Println("    --dry-run: Simulate the restore without executing it.")
		fmt.Println("Configuration is read from config.json:")
		fmt.Println("  - storageAccountName: Azure storage account name")
		fmt.Println("  - containerName: Blob container name")
		fmt.Println("  - sqlConnectionString: SQL Server connection string")
		fmt.Println("  - sasToken: Azure SAS token")
		fmt.Println("  - dataPath: Local path for restore data files")
		fmt.Println("  - logPath: Local path for restore log files")
		fmt.Println("  - parallelismThreshold: Max concurrent operations (1-100)")
		fmt.Println("  - excludedDatabases: Comma-separated list of databases to exclude")
		fmt.Println("  - copyOnly: Boolean to enable COPY_ONLY backup (true/false)")
		fmt.Println("  - queryTimeoutInHours: Timeout for SQL queries in hours (1-24)")
		fmt.Println("Examples:")
		fmt.Println("  go run main.go --backup")
		fmt.Println("  go run main.go --restore 20250224155736 --debug")
		fmt.Println("  go run main.go --backup --dry-run")
		os.Exit(1)
	}

	app, err := NewApp(*debugFlag, *dryRunFlag)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to initialize application: %v", err))
		os.Exit(1)
	}
	defer func() {
		if err := app.db.Close(); err != nil {
			app.logger.Error(fmt.Sprintf("Failed to close SQL connection: %v", err))
		}
	}()

	if *backupFlag {
		app.runTimestamp = time.Now().Format("20060102150405")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Printf("%s %sReceived shutdown signal, cancelling operations...%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
		cancel()
	}()

	operation := "backup"
	if *restoreFlag {
		operation = "restore"
	}

	if err := app.setupCredential(ctx); err != nil {
		app.logger.Error(fmt.Sprintf("Failed to setup Azure credential: %v", err))
		os.Exit(1)
	}

	var count int
	results := make(chan OperationResult, app.config.ParallelismThreshold)
	var wg sync.WaitGroup
	sem := make(chan struct{}, app.config.ParallelismThreshold)

	if *backupFlag {
		databases, err := app.getDatabasesToBackup(ctx)
		if err != nil {
			app.logger.Error(fmt.Sprintf("Failed to get databases for backup: %v", err))
			os.Exit(1)
		}
		if len(databases) == 0 {
			app.logger.Info("No databases to backup")
			fmt.Printf("%s %sNo databases to backup%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
			return
		}
		count = len(databases)
		results = make(chan OperationResult, count)

		if !app.confirm(operation, databases, "") {
			fmt.Printf("%s %sOperation cancelled%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
			return
		}

		for _, dbName := range databases {
			select {
			case <-ctx.Done():
				app.logger.Warn("Backup operation cancelled due to shutdown")
				fmt.Printf("%s %sBackup operation cancelled due to shutdown%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
				return
			default:
				wg.Add(1)
				sem <- struct{}{}
				go func(dbName string) {
					defer wg.Done()
					defer func() { <-sem }()
					app.backupDatabase(ctx, dbName, results)
				}(dbName)
			}
		}
	} else {
		databases := []string{}
		if !app.confirm(operation, databases, restoreTimestamp) {
			fmt.Printf("%s %sOperation cancelled%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
			return
		}

		backupFiles, _, err := app.listLatestBackupsFromBlob(ctx, restoreTimestamp)
		if err != nil {
			app.logger.Error(fmt.Sprintf("Failed to list backup files from blob storage: %v", err))
			os.Exit(1)
		}
		excluded := strings.Split(app.config.ExcludedDatabases, ",")
		databases = []string{}
		for dbName := range backupFiles {
			if !contains(excluded, dbName) {
				databases = append(databases, dbName)
			} else {
				delete(backupFiles, dbName)
			}
		}
		if len(backupFiles) == 0 {
			app.logger.Info("No databases to restore")
			fmt.Printf("%s %sNo databases to restore%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
			return
		}
		count = len(backupFiles)
		results = make(chan OperationResult, count)

		for dbName, blobPaths := range backupFiles {
			select {
			case <-ctx.Done():
				app.logger.Warn("Restore operation cancelled due to shutdown")
				fmt.Printf("%s %sRestore operation cancelled due to shutdown%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
				return
			default:
				wg.Add(1)
				sem <- struct{}{}
				go func(dbName string, blobPaths []string) {
					defer wg.Done()
					defer func() { <-sem }()
					app.restoreDatabase(ctx, dbName, blobPaths, results)
				}(dbName, blobPaths)
			}
		}
	}

	var operationResults []OperationResult
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(results)
		close(done)
	}()

	// Remove overall timeout to prevent 5-hour limit issues with large databases
	for r := range results {
		operationResults = append(operationResults, r)
	}
	// If you want a longer timeout (e.g., 12 hours), use:
	// timeout := time.After(12 * time.Hour)
	// for {
	//     select {
	//     case <-timeout:
	//         app.logger.Error("Timeout waiting for operations to complete")
	//         fmt.Printf("%s %sTimeout: Operations did not complete within 12 hours%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorRed, 12, ColorReset)
	//         os.Exit(1)
	//     case <-ctx.Done():
	//         app.logger.Warn("Operation cancelled due to shutdown")
	//         fmt.Printf("%s %sOperation cancelled due to shutdown%s\n", time.Now().Format("2006-01-02 15:04:05"), ColorOrange, ColorReset)
	//         return
	//     case r, ok := <-results:
	//         if !ok {
	//             break
	//         }
	//         operationResults = append(operationResults, r)
	//     }
	// }

	displaySummary(operationResults, operation)
	if app.debug {
		app.logger.Info(fmt.Sprintf("Operation %s completed, processed %d databases", operation, len(operationResults)))
	}
}
