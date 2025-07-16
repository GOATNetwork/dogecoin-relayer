package models

import (
	"fmt"
	"os"
	"path/filepath"

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

	l2SyncDb    *gorm.DB
	l2InfoDb    *gorm.DB
	dogeLightDb *gorm.DB
	walletDb    *gorm.DB
	dogeCacheDb *gorm.DB
}

func NewDBConnection(sqliteCfg *config.SqliteConfig, gormCfg *config.GormConfig) (*DBConnection, error) {
	conn := &DBConnection{}
	// init db
	err := conn.initDB(sqliteCfg, gormCfg)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (conn *DBConnection) GetDB() *gorm.DB {
	return conn.DB
}

func (conn *DBConnection) GetL2SyncDB() *gorm.DB {
	return conn.l2SyncDb
}

func (conn *DBConnection) GetL2InfoDB() *gorm.DB {
	return conn.l2InfoDb
}

func (conn *DBConnection) GetDogeLightDB() *gorm.DB {
	return conn.dogeLightDb
}

func (conn *DBConnection) GetWalletDB() *gorm.DB {
	return conn.walletDb
}

func (conn *DBConnection) GetDogeCacheDB() *gorm.DB {
	return conn.dogeCacheDb
}

func (conn *DBConnection) initDB(sqliteCfg *config.SqliteConfig, gormCfg *config.GormConfig) error {
	// create database directory
	dbDir := sqliteCfg.Folder
	if err := os.MkdirAll(dbDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create database directory: %v", err)
	}

	// config DOGE/L1 related databases
	databaseConfigs := []struct {
		dbPath string
		dbRef  **gorm.DB
		dbName string
	}{
		{filepath.Join(dbDir, "l2_sync.db"), &conn.l2SyncDb, "Database 1"},
		{filepath.Join(dbDir, "l2_info.db"), &conn.l2InfoDb, "Database 2"},
		{filepath.Join(dbDir, "doge_light.db"), &conn.dogeLightDb, "Database 3"},
		{filepath.Join(dbDir, "wallet_order.db"), &conn.walletDb, "Database 4"},
		{filepath.Join(dbDir, "doge_cache.db"), &conn.dogeCacheDb, "Database 5"},
	}

	// connect all databases
	for _, dbConfig := range databaseConfigs {
		if err := conn.connectDatabase(dbConfig.dbPath, dbConfig.dbRef, dbConfig.dbName); err != nil {
			log.Fatalf("Failed to connect to %s: %v", dbConfig.dbName, err)
		}
	}

	// first run auto migrate to create all necessary tables
	conn.autoMigrate()

	// then run data migration
	if err := conn.runMigrations(); err != nil {
		log.Fatalf("Failed to run migrations: %v", err)
	}

	log.Debugf("Database migration completed successfully")
	return nil
}

func (conn *DBConnection) connectDatabase(dbPath string, dbRef **gorm.DB, dbName string) error {
	// config SQLite, use WAL mode and busy timeout
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL", dbPath)

	// open database with config
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
		// enable auto retry for database lock
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", dbName, err)
	}

	*dbRef = db
	log.Debugf("%s connected successfully with WAL mode, path: %s", dbName, dbPath)
	return nil
}

func (conn *DBConnection) autoMigrate() {
	if err := conn.l2SyncDb.AutoMigrate(&L1SyncStatus{}); err != nil {
		log.Fatalf("Failed to migrate database 1: %v", err)
	}
	if err := conn.l2InfoDb.AutoMigrate(&L1Block{}, &Voter{}, &EpochVoter{}, &VoterQueue{}, &DepositPubKey{}); err != nil {
		log.Fatalf("Failed to migrate database 2: %v", err)
	}
	if err := conn.dogeLightDb.AutoMigrate(&DogeBlock{}); err != nil {
		log.Fatalf("Failed to migrate database 3: %v", err)
	}
	if err := conn.walletDb.AutoMigrate(&UTXO{}, &Withdraw{}, &SendOrder{}, &VIN{}, &VOUT{}, &DepositResult{}); err != nil {
		log.Fatalf("Failed to migrate database 4: %v", err)
	}
	if err := conn.dogeCacheDb.AutoMigrate(&DogeSyncStatus{}, &DogeBlockData{}, &DogeTXOutput{}, &Deposit{}); err != nil {
		log.Fatalf("Failed to migrate database 5: %v", err)
	}
}

func (conn *DBConnection) runMigrations() error {
	// init migration manager for each database
	migrationManagers := map[string]*MigrationManager{
		"wallet": NewMigrationManager(conn.walletDb),
	}

	// ensure migration table exists
	for name, manager := range migrationManagers {
		log.Debugf("Ensuring migration table exists for %s database", name)
		if err := manager.EnsureMigrationTable(); err != nil {
			return fmt.Errorf("failed to create migration table for %s: %w", name, err)
		}
	}

	// run migration for wallet database
	log.Debugf("Running UTXO records migration")
	migrates := make(map[string](func(*gorm.DB) error), 0)

	// run UTXO records migration (can add condition judgment according to needs)
	// note: here we need to pass in the doge configuration to determine whether to run the migration
	// since the current function does not access the configuration context, we temporarily run the migration
	// in actual use, the configuration can be passed in as a parameter or use the global configuration
	migrates["20241220_2345_add_utxo_records"] = AddUtxoRecords

	for name, migrate := range migrates {
		if err := migrationManagers["wallet"].RunMigration(name, migrate); err != nil {
			return fmt.Errorf("failed to run UTXO records migration: %w", err)
		}
	}

	return nil
}

func (conn *DBConnection) Close() {
	sqlDB, err := conn.DB.DB()
	if err != nil {
		log.Errorf("Failed to get sql db for close: %v", err)
		return
	}
	sqlDB.Close()
}
