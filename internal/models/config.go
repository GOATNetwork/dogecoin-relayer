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

	// Configure GORM logger level from config
	logMode := parseGormLogLevel(gormCfg.LogLevel)
	gLogger := gormlogger.Default.LogMode(logMode)

	db, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{Logger: gLogger})
	if err != nil {
		return fmt.Errorf("failed to open sqlite db: %w", err)
	}

	// Tune connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get underlying sql DB: %w", err)
	}
	if gormCfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(gormCfg.MaxIdleConns)
	}
	if gormCfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(gormCfg.MaxOpenConns)
	}
	if gormCfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(gormCfg.ConnMaxLifetime) * time.Second)
	}

	conn.DB = db
	log.Infof("SQLite database initialized at %s", dbPath)
	return nil
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
