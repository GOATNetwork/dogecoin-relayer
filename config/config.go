package config

import (
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Log    LogConfig    `yaml:"log"`
	Sqlite SqliteConfig `yaml:"sqlite"`
	Gorm   GormConfig   `yaml:"gorm"`
	Doge   DogeConfig   `yaml:"doge"`
	P2P    P2PConfig    `yaml:"p2p"`
	Http   HttpConfig   `yaml:"http"`
	Scan   ScanConfig   `yaml:"scan"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

type SqliteConfig struct {
	Folder string `yaml:"folder"`
}

type GormConfig struct {
	LogLevel        string `yaml:"log_level"`
	MaxIdleConns    int    `yaml:"max_idle_conns"`
	MaxOpenConns    int    `yaml:"max_open_conns"`
	ConnMaxLifetime int    `yaml:"conn_max_lifetime"`
}

type ScanConfig struct {
	Enabled  bool `yaml:"enabled"`
	Interval int  `yaml:"interval"` // in seconds
	Timeout  int  `yaml:"timeout"`  // in seconds
}

type P2PConfig struct {
	Enabled               bool     `yaml:"enabled"`
	ListenAddr            string   `yaml:"listen_addr"`
	BootstrapPeers        []string `yaml:"bootstrap_peers"`
	EnableMDNS            bool     `yaml:"enable_mdns"`
	ListenWaitSeconds     int      `yaml:"listen_wait_seconds"`
	ConnectionWaitSeconds int      `yaml:"connection_wait_seconds"`
}

type HttpConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type DogeConfig struct {
	RpcUrl        string `yaml:"rpc_url"`
	RpcUser       string `yaml:"rpc_user"`
	RpcPassword   string `yaml:"rpc_password"`
	StartHeight   int    `yaml:"start_height"`
	Confirmations int    `yaml:"confirmations"`
	NetworkType   string `yaml:"network_type"`
}

func LoadConfig(filePath string) (*Config, error) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Failed to open config file: %v", err)
	}
	defer file.Close()

	var config Config
	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		log.Fatalf("Failed to decode config file: %v", err)
	}
	// set log level
	logLevel, err := log.ParseLevel(config.Log.Level)
	if err != nil {
		log.Warnf("Auto set log level to info, parse log level error: %v", err)
		log.SetLevel(log.InfoLevel)
	} else {
		log.Infof("Set log level to %s", config.Log.Level)
		log.SetLevel(logLevel)
	}
	log.SetOutput(os.Stdout)

	return &config, nil
}
