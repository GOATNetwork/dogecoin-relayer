package models

import (
	"github.com/goat-network/dogecoin-relayer/config"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
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
	// TODO: init db
	// err := conn.initDB(sqliteCfg, gormCfg)
	// if err != nil {
	// 	return nil, err
	// }
	log.Info("Start to migrate")
	// do migrate
	// migrateRepo := NewMigrateRepository(conn.DB)
	// err = migrateRepo.DoMigrate()
	// if err != nil {
	// 	log.Fatalf("Migrate failed: %v", err)
	// }
	log.Info("Migrate done")

	// TODO: init all repositories

	return conn, nil
}

func (conn *DBConnection) GetDB() *gorm.DB {
	return conn.DB
}

func (conn *DBConnection) initDB(sqliteCfg *config.SqliteConfig, gormCfg *config.GormConfig) error {
	// logLevel, err := log.ParseLevel(gormCfg.LogLevel) // Convert string to logger.LogLevel
	// if err != nil {
	// 	return fmt.Errorf("invalid log level: %w", err)
	// }

	// logrusLogger := log.New()
	// logrusLogger.SetLevel(log.InfoLevel)

	// gormLogger := logger.New(
	// 	&LogrusWriter{Logger: logrusLogger, Level: logLevel},
	// 	logger.Config{
	// 		SlowThreshold:             time.Second,
	// 		LogLevel:                  logger.LogLevel(logLevel),
	// 		IgnoreRecordNotFoundError: true,
	// 	},
	// )
	// dsn := fmt.Sprintf(
	// 	"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=%s client_encoding=UTF8",
	// 	postgresCfg.Host,
	// 	postgresCfg.Port,
	// 	postgresCfg.User,
	// 	postgresCfg.Password,
	// 	postgresCfg.DBName,
	// 	postgresCfg.SSLMode,
	// 	postgresCfg.TimeZone,
	// )
	// db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
	// 	Logger: gormLogger,
	// 	NamingStrategy: schema.NamingStrategy{
	// 		TablePrefix: "tb_", // Table prefix for all tables, e.g., "tb_migrate_logs"
	// 	},
	// })

	// if err != nil {
	// 	return fmt.Errorf("failed to connect to db: %w", err)
	// }
	// log.Debug("Db connected successfully")

	// // AutoMigrate types
	// err = db.AutoMigrate(&MigrateLog{})
	// if err != nil {
	// 	return fmt.Errorf("failed to migrate db: %w", err)
	// }

	// conn.DB = db
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
