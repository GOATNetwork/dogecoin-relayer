package models

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	log "github.com/sirupsen/logrus"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

type LogrusWriter struct {
	Logger *log.Logger
	Level  log.Level
}

func (w *LogrusWriter) Printf(format string, args ...interface{}) {
	w.Logger.Logf(w.Level, format, args...)
}

type DBConnection struct {
	DB *gorm.DB
}

func NewDBConnection(sqliteCfg *config.SqliteConfig, gormCfg *config.GormConfig) (*DBConnection, error) {
	conn := &DBConnection{}
	if err := conn.initDB(sqliteCfg, gormCfg); err != nil {
		return nil, err
	}

	log.Info("Start to migrate")
	migrateRepo := NewMigrateRepository(conn.DB)
	if err := migrateRepo.DoMigrate(); err != nil {
		return nil, fmt.Errorf("migrate failed: %w", err)
	}
	log.Info("Migrate done")

	return conn, nil
}

func (conn *DBConnection) GetDB() *gorm.DB {
	return conn.DB
}

func (conn *DBConnection) initDB(sqliteCfg *config.SqliteConfig, gormCfg *config.GormConfig) error {
	folder := sqliteCfg.Folder
	if folder == "" {
		folder = "/app/data"
	}
	if err := os.MkdirAll(folder, 0o755); err != nil {
		return fmt.Errorf("failed to create sqlite folder %s: %w", folder, err)
	}

	dbPath := filepath.Join(folder, "relayer.db")

	// Check if database exists and verify integrity before opening
	if _, err := os.Stat(dbPath); err == nil {
		log.Info("Existing database found, checking integrity...")
		if err := conn.checkAndRepairDatabase(dbPath, folder); err != nil {
			log.Warnf("Database integrity check failed: %v", err)
			// Attempt to recover by backing up and creating fresh database
			if err := conn.backupCorruptedDatabase(dbPath, folder); err != nil {
				log.Errorf("Failed to backup corrupted database: %v", err)
			}
			log.Warn("Creating fresh database after corruption detected")
		}
	}

	// Configure GORM logger level from config
	logMode := parseGormLogLevel(gormCfg.LogLevel)
	gLogger := gormlogger.Default.LogMode(logMode)

	// Configure SQLite with optimized settings for concurrent access
	// Increase timeout to 30s to avoid "database is locked" errors during heavy contention
	dsn := dbPath + "?cache=shared&mode=rwc&_journal_mode=WAL&_synchronous=NORMAL&_timeout=30000&_busy_timeout=30000"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: gLogger})
	if err != nil {
		// If open fails due to corruption, try recovery
		log.Errorf("Failed to open database: %v", err)
		if err := conn.backupCorruptedDatabase(dbPath, folder); err != nil {
			log.Errorf("Failed to backup corrupted database: %v", err)
		}
		// Try opening again with a fresh database
		db, err = gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: gLogger})
		if err != nil {
			return fmt.Errorf("failed to open sqlite db after recovery attempt: %w", err)
		}
		log.Info("Successfully opened fresh database after corruption recovery")
	}

	// Execute additional PRAGMA statements for better concurrency and corruption resistance
	db.Exec("PRAGMA journal_mode=WAL")
	db.Exec("PRAGMA synchronous=NORMAL")
	db.Exec("PRAGMA cache_size=10000")
	db.Exec("PRAGMA temp_store=memory")
	db.Exec("PRAGMA mmap_size=268435456") // 256MB
	db.Exec("PRAGMA busy_timeout=30000")
	db.Exec("PRAGMA wal_autocheckpoint=1000") // Checkpoint every 1000 pages

	// Run integrity check after opening
	var integrityResult string
	if err := db.Raw("PRAGMA integrity_check").Scan(&integrityResult).Error; err == nil {
		if integrityResult != "ok" {
			log.Errorf("Database integrity check failed after opening: %s", integrityResult)
			return fmt.Errorf("database integrity compromised: %s", integrityResult)
		}
		log.Info("Database integrity check passed")
	}

	// Tune connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql DB: %w", err)
	}
	// Set optimized connection pool settings for SQLite
	if gormCfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(gormCfg.MaxIdleConns)
	} else {
		sqlDB.SetMaxIdleConns(5) // Default for SQLite
	}

	if gormCfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(gormCfg.MaxOpenConns)
	} else {
		sqlDB.SetMaxOpenConns(10) // Conservative limit for SQLite
	}

	if gormCfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(gormCfg.ConnMaxLifetime) * time.Second)
	} else {
		sqlDB.SetConnMaxLifetime(30 * time.Minute) // Default connection lifetime
	}

	conn.DB = db
	log.Infof("SQLite database initialized at %s", dbPath)
	return nil
}

// checkAndRepairDatabase checks database integrity and attempts quick repair
func (conn *DBConnection) checkAndRepairDatabase(dbPath, folder string) error {
	// Try to open database directly with SQLite
	dsn := dbPath + "?mode=ro&_timeout=5000" // Read-only mode for integrity check
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("cannot open database for integrity check: %w", err)
	}
	defer func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			sqlDB.Close()
		}
	}()

	// Run integrity check
	var result string
	if err := db.Raw("PRAGMA integrity_check").Scan(&result).Error; err != nil {
		return fmt.Errorf("integrity check query failed: %w", err)
	}

	if result != "ok" {
		return fmt.Errorf("integrity check failed: %s", result)
	}

	return nil
}

// backupCorruptedDatabase creates a backup of corrupted database and removes it
func (conn *DBConnection) backupCorruptedDatabase(dbPath, folder string) error {
	backupDir := filepath.Join(folder, "corrupted_backups")
	if err := os.MkdirAll(backupDir, 0o755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	timestamp := time.Now().Format("20060102_150405")
	backupPath := filepath.Join(backupDir, fmt.Sprintf("relayer_%s.db.corrupted", timestamp))

	// Try to copy the corrupted database
	if err := copyFile(dbPath, backupPath); err != nil {
		log.Warnf("Failed to backup corrupted database: %v", err)
	} else {
		log.Infof("Corrupted database backed up to: %s", backupPath)
	}

	// Remove corrupted files
	os.Remove(dbPath)
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	log.Info("Removed corrupted database files")
	return nil
}

// copyFile copies a file from src to dst
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = destFile.ReadFrom(sourceFile)
	return err
}

func (conn *DBConnection) Close() {
	if conn == nil || conn.DB == nil {
		return
	}
	sqlDB, err := conn.DB.DB()
	if err != nil {
		log.Errorf("Failed to get sql db for close: %v", err)
		return
	}
	sqlDB.Close()
}

// parseGormLogLevel converts a string level into gorm logger level
func parseGormLogLevel(level string) gormlogger.LogLevel {
	switch level {
	case "silent", "Silent", "SILENT":
		return gormlogger.Silent
	case "error", "Error", "ERROR":
		return gormlogger.Error
	case "warn", "Warn", "WARNING", "WARN":
		return gormlogger.Warn
	case "info", "Info", "INFO", "":
		return gormlogger.Info
	default:
		return gormlogger.Info
	}
}
