package config

import (
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Log       LogConfig       `yaml:"log"`
	Sqlite    SqliteConfig    `yaml:"sqlite"`
	Gorm      GormConfig      `yaml:"gorm"`
	Doge      DogeConfig      `yaml:"doge"`
	Withdraw  WithdrawConfig  `yaml:"withdraw"`
	P2P       P2PConfig       `yaml:"p2p"`
	Http      HttpConfig      `yaml:"http"`
	Grpc      GrpcConfig      `yaml:"grpc"`
	Rpc       RpcConfig       `yaml:"rpc"`
	Scan      ScanConfig      `yaml:"scan"`
	Consensus ConsensusConfig `yaml:"consensus"`
	Tss       TssConfig       `yaml:"tss"`
	Metrics   MetricsConfig   `yaml:"metrics"`
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
	Range    int  `yaml:"range"`    // number of blocks to scan at once

	// Electrs-based scanning configuration
	// When enabled, uses electrs API instead of RPC for block scanning
	// This is more efficient as it allows filtering blocks by tx_count
	ElectrsEnabled bool   `yaml:"electrs_enabled"`
	ElectrsUrl     string `yaml:"electrs_url"` // e.g., "https://doge-electrs-testnet-demo.qed.me"
}

type P2PConfig struct {
	Enabled               bool     `yaml:"enabled"`
	ListenAddr            string   `yaml:"listen_addr"`
	ExternalAddr          string   `yaml:"external_addr"` // broadcast factory address, such as "/ip4/52.88.11.99/tcp/4001,/ip4/54.91.64.7/tcp/4001"
	BootstrapPeers        []string `yaml:"bootstrap_peers"`
	EnableMDNS            bool     `yaml:"enable_mdns"`
	ListenWaitSeconds     int      `yaml:"listen_wait_seconds"`
	ConnectionWaitSeconds int      `yaml:"connection_wait_seconds"`
	KeyDir                string   `yaml:"key_dir"`

	// hex from environment variable: PROPOSER_PRIVATE_KEY
	ProposerPrivateKey string
}

type HttpConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type RpcConfig struct {
	Enabled bool `yaml:"enabled"`
	Port    int  `yaml:"port"`
}

type GrpcConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Port        int    `yaml:"port"`
	UseTLS      bool   `yaml:"use_tls"`
	CertFile    string `yaml:"cert_file"`
	KeyFile     string `yaml:"key_file"`
	MaxRecvSize int    `yaml:"max_recv_size"`
	MaxSendSize int    `yaml:"max_send_size"`
	MaxConnAge  int    `yaml:"max_conn_age"`
	MaxConnIdle int    `yaml:"max_conn_idle"`
	PingTime    int    `yaml:"ping_time"`
	Timeout     int    `yaml:"timeout"`
}

type DogeConfig struct {
	RpcUrl        string            `yaml:"rpc_url"`
	RpcUser       string            `yaml:"rpc_user"`
	RpcPassword   string            `yaml:"rpc_password"`
	RpcHeaders    map[string]string `yaml:"rpc_headers"`
	StartHeight   int               `yaml:"start_height"`
	Confirmations int               `yaml:"confirmations"`
	NetworkType   string            `yaml:"network_type"`

	// Optional: addresses or pubkey to watch
	// If WatchAddresses is set, relayer will match against these addresses directly.
	// If WatchPubkeyBase64 is set, relayer will derive P2PKH/P2WPKH addresses from it.
	// When both are empty, relayer will scan without address filtering for ownership-specific actions.
	WatchAddresses    []string `yaml:"watch_addresses"`
	WatchPubkeyBase64 string   `yaml:"watch_pubkey_base64"`

	// Optional: deposit detection parameters
	// Magic bytes (hex string, e.g. "0xfeedbeef" or "feedbeef") and minimum deposit amount in satoshis
	DepositMagicBytes string `yaml:"deposit_magic_bytes"`
	MinDepositAmount  int64  `yaml:"min_deposit_amount"`
}

type ConsensusConfig struct {
	Enabled bool   `yaml:"enabled"`
	Rpc     string `yaml:"rpc"`
	ChainId uint64 `yaml:"chain_id"`

	// Event detection configuration
	EventDetection EventDetectionConfig `yaml:"event_detection"`

	// UTXO processor polling configuration
	UtxoPollingIntervalSec int `yaml:"utxo_polling_interval_sec"`

	// hex from environment variable: PROPOSER_PRIVATE_KEY
	ProposerPrivateKey string
}

type EventDetectionConfig struct {
	Enabled            bool   `yaml:"enabled"`
	ContractBridge     string `yaml:"contract_bridge"`
	ContractEntryPoint string `yaml:"contract_entry_point"`
	AbiPath            string `yaml:"abi_path"`
	LastScannedBlock   int    `yaml:"last_scanned_block"`
	ConfirmationBlocks int    `yaml:"confirmation_blocks"`
	BatchSize          int    `yaml:"batch_size"`
	ScanIntervalSec    int    `yaml:"scan_interval_sec"`
}

type TssConfig struct {
	Enabled bool   `yaml:"enabled"`
	Url     string `yaml:"url"`
	Kdd     string `yaml:"kdd"`
	Timeout int    `yaml:"timeout"` // unit: second
}

type MetricsConfig struct {
	Enabled               bool `yaml:"enabled"`
	CollectSystemMetrics  bool `yaml:"collect_system_metrics"`
	SystemCollectInterval int  `yaml:"system_collect_interval"` // in seconds
	EnableDetailedMetrics bool `yaml:"enable_detailed_metrics"`
}

type FireblocksConfig struct {
	ApiKey       string `yaml:"api_key"`
	Secret       string `yaml:"secret"`
	BaseURL      string `yaml:"base_url"`
	VaultAccount string `yaml:"vault_account"`
	AssetId      string `yaml:"asset_id"`
	CallbackPriv string `yaml:"callback_private"`
	CallbackPub  string `yaml:"callback_public"`
}

type WithdrawConfig struct {
	Enabled          bool             `yaml:"enabled"`
	Mode             string           `yaml:"mode"` // local | fireblocks
	ChangeAddress    string           `yaml:"change_address"`
	FeeRate          int64            `yaml:"fee_rate"`
	MinConfirmations int              `yaml:"min_confirmations"`
	Fireblocks       FireblocksConfig `yaml:"fireblocks"`
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

	if os.Getenv("PROPOSER_PRIVATE_KEY") == "" {
		log.Fatalf("PROPOSER_PRIVATE_KEY is not set")
	}

	// set private key from environment variable
	config.P2P.ProposerPrivateKey = os.Getenv("PROPOSER_PRIVATE_KEY")
	config.Consensus.ProposerPrivateKey = os.Getenv("PROPOSER_PRIVATE_KEY")

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
