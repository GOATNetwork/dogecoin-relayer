package doge

import (
	"context"
	"fmt"
	"time"

	"github.com/dogecoinw/doged/chaincfg"
	"github.com/dogecoinw/doged/chaincfg/chainhash"
	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/metrics"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
)

type DogeModule struct {
    cfg           config.DogeConfig
    scanCfg       config.ScanConfig
    conn          *models.DBConnection
    logger        *log.Entry
    client        *DogeClient
    state         *models.StateRepository
    blockCh       chan *types.DogeBlockExt
    currentHeight int64
}

var _ module.Module = (*DogeModule)(nil)

func (m *DogeModule) Name() string {
	return "doge"
}

func (m *DogeModule) Init(cfg any, conn *models.DBConnection) error {
	// Initialize configurations
	m.cfg = cfg.(config.DogeConfig)
	m.conn = conn
	m.logger = types.InitLogEntry(m.Name())

	// Initialize Dogecoin client
	client, err := NewDogeClient(m.cfg)
	if err != nil {
		metrics.RecordError("doge", "init_failed")
		return fmt.Errorf("failed to create Dogecoin client: %w", err)
	}
	m.client = client

	// Initialize state repository
	m.state = models.NewStateRepository(conn.GetDB())

	// Initialize block channel
	m.blockCh = make(chan *types.DogeBlockExt, 100)

	// Set current height from config
	m.currentHeight = int64(m.cfg.StartHeight)

	// Set scan config from global config
	globalCfg := global.GetConfig()
	if globalCfg != nil {
		m.scanCfg = globalCfg.Scan
	}

	return nil
}

func (m *DogeModule) Run(ctx context.Context) error {
	m.logger.Info("Doge module running")
	metrics.RecordModuleStart("doge")

	// Start block scanning loop
	go m.blockScanLoop(ctx)

	// Start block fetching loop
	go m.blockFetchLoop(ctx)

	return nil
}

func (m *DogeModule) Shutdown(ctx context.Context) error {
	m.logger.Info("Doge module shutting down")
	close(m.blockCh)
	if m.client != nil {
		m.client.Close()
	}
	metrics.RecordModuleStop("doge")
	return nil
}

// blockFetchLoop continuously fetches new blocks from Dogecoin node
func (m *DogeModule) blockFetchLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(m.scanCfg.Interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Block fetch loop stopping...")
			return
		case <-ticker.C:
			if err := m.fetchNewBlocks(ctx); err != nil {
				m.logger.Errorf("Failed to fetch new blocks: %v", err)
			}
		}
	}
}

// fetchNewBlocks fetches new blocks from the current height
func (m *DogeModule) fetchNewBlocks(ctx context.Context) error {
	// Get current block count from node
	currentBlockCount, err := m.client.GetBlockCount()
	if err != nil {
		return err
	}

	// Calculate end height (current height + range, but not exceeding current block count)
	endHeight := m.currentHeight + int64(m.scanCfg.Range)
	if endHeight > currentBlockCount {
		endHeight = currentBlockCount
	}

	// Fetch blocks in range
	for height := m.currentHeight; height <= endHeight; height++ {
		block, err := m.client.GetBlockByHeight(height)
		if err != nil {
			m.logger.Errorf("Failed to get block at height %d: %v", height, err)
			continue
		}

		// Send block to processing channel
		select {
		case m.blockCh <- block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Update current height
	m.currentHeight = endHeight + 1

	return nil
}

// blockScanLoop processes blocks from the channel
func (m *DogeModule) blockScanLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Block scan loop stopping...")
			return
		case block := <-m.blockCh:
			if block != nil {
				m.processBlock(*block)
			}
		}
	}
}

// processBlock processes a single block and extracts P2PKH transactions
func (m *DogeModule) processBlock(dogeBlock types.DogeBlockExt) {
    m.logger.Debugf("Processing Dogecoin block, height: %d, hash: %s", dogeBlock.BlockNumber, dogeBlock.GetBlockHash().String())

    // Verify SPV (simplified)
    if err := types.VerifyBlockSPV(dogeBlock); err != nil {
        m.logger.Errorf("Verify block SPV err %v", err)
        return
    }

    // Get network parameters
    network := types.GetDogeNetwork(m.cfg.NetworkType)

    // Prepare watch addresses
    watchAddrSet := make(map[string]struct{})
    var pubkeyBytes []byte

    // Prefer explicit watch addresses if provided
    if len(m.cfg.WatchAddresses) > 0 {
        for _, a := range m.cfg.WatchAddresses {
            if a != "" {
                watchAddrSet[a] = struct{}{}
            }
        }
        m.logger.Debugf("Using configured watch addresses: %d", len(watchAddrSet))
    } else if m.cfg.WatchPubkeyBase64 != "" {
        // Derive standard addresses from configured pubkey
        var err error
        pubkeyBytes, err = types.DecodeBase64Pubkey(m.cfg.WatchPubkeyBase64)
        if err != nil {
            m.logger.Errorf("Failed to decode configured pubkey: %v", err)
            return
        }
        p2pkhAddress, err := types.GenerateP2PKHAddress(pubkeyBytes, network)
        if err != nil {
            m.logger.Errorf("Failed to generate P2PKH address: %v", err)
            return
        }
        p2wpkhAddress, err := types.GenerateP2WPKHAddress(pubkeyBytes, network)
        if err != nil {
            m.logger.Errorf("Failed to generate P2WPKH address: %v", err)
            return
        }
        watchAddrSet[p2pkhAddress] = struct{}{}
        watchAddrSet[p2wpkhAddress] = struct{}{}
        m.logger.Debugf("Using derived watch addresses - P2PKH: %s, P2WPKH: %s", p2pkhAddress, p2wpkhAddress)
    } else {
        // No addresses configured; continue scanning but skip ownership-based classification
        m.logger.Debug("No watch addresses or pubkey configured; scanning without address filter")
    }

    // Process each transaction in the block
    for _, tx := range dogeBlock.Transactions {
        m.processTransaction(tx, dogeBlock, watchAddrSet, network, pubkeyBytes)
    }

	m.logger.Debugf("Processed Dogecoin block %d", dogeBlock.BlockNumber)
}

// processTransaction processes a single transaction
func (m *DogeModule) processTransaction(tx *wire.MsgTx, dogeBlock types.DogeBlockExt, watchAddrSet map[string]struct{}, network *chaincfg.Params, pubkeyBytes []byte) {
	var utxos []*models.UTXO
	var vins []*models.VIN
	var vouts []*models.VOUT

	isUtxo, isVin := false, false
	isWithdrawal, isSafebox, isDeposit, isConsolidation := false, false, false, false

	// Get transaction ID
	txid := tx.TxHash().String()

	// Query database for send order
	sendOrder, err := m.state.GetSendOrderByTxIdOrExternalId(txid)
	if err != nil {
		m.logger.Errorf("Get send order by txid %s err %v", txid, err)
		return
	}
	if sendOrder != nil {
		switch sendOrder.OrderType {
		case models.ORDER_TYPE_WITHDRAWAL:
			isWithdrawal = true
		case models.ORDER_TYPE_CONSOLIDATION:
			isConsolidation = true
		case models.ORDER_TYPE_SAFEBOX:
			isSafebox = true
		}
	}

	sender, receiver := "", ""

	// Process inputs (VINs) - extract real addresses
	for _, vin := range tx.TxIn {
		// Check for coinbase transaction
		if (vin.PreviousOutPoint.Hash == chainhash.Hash{} && vin.PreviousOutPoint.Index == 0xffffffff) {
			sender = "coinbase"
			m.logger.Debugf("Detect coinbase tx")
			break
		}

		// Extract address from signature script
		if len(vin.SignatureScript) > 0 {
			// For P2PKH inputs, the address is in the signature script
			// The last 20 bytes after the signature and public key
			if len(vin.SignatureScript) >= 23 {
				// Extract the public key from the signature script
				// P2PKH signature script format: <sig> <pubkey>
				sigLen := int(vin.SignatureScript[0])
				if sigLen > 0 && len(vin.SignatureScript) > sigLen+1 {
					pubKeyLen := int(vin.SignatureScript[sigLen+1])
					if pubKeyLen > 0 && len(vin.SignatureScript) >= sigLen+2+pubKeyLen {
						pubKey := vin.SignatureScript[sigLen+2 : sigLen+2+pubKeyLen]
						// Generate address from public key
						addr, err := types.GenerateP2PKHAddress(pubKey, network)
						if err == nil {
							sender = addr
						}
					}
				}
			}
		}

    // Check if sender matches our addresses
    if _, ok := watchAddrSet[sender]; ok {
        isVin = true
        if len(tx.TxOut) == 1 {
            isConsolidation = true
        } else {
            isWithdrawal = true
        }
			vins = append(vins, &models.VIN{
				OrderId:   "",
				BtcHeight: dogeBlock.BlockNumber,
				Txid:      vin.PreviousOutPoint.Hash.String(),
				OutIndex:  int(vin.PreviousOutPoint.Index),
				SigScript: vin.SignatureScript,
				Sender:    sender,
				Source:    models.UTXO_SOURCE_UNKNOWN,
				Status:    models.UTXO_STATUS_CONFIRMED,
				UpdatedAt: time.Now(),
			})
		}
	}

	// Check for deposit
    var receiverType, evmAddr string
    // Decode magic bytes from config (hex string); ignore error and use empty if not provided
    magicBytes := parseHexOrRaw(m.cfg.DepositMagicBytes)
    minDepositAmount := m.cfg.MinDepositAmount
    if minDepositAmount <= 0 {
        minDepositAmount = 1_000_000 // default 1 DOGE
    }
    // Build watch address slice for deposit detection
    watchList := make([]string, 0, len(watchAddrSet))
    for a := range watchAddrSet {
        watchList = append(watchList, a)
    }
    isDeposit, evmAddr, _ = types.IsUtxoDogeDepositV1(tx, watchList, network, minDepositAmount, magicBytes)

	// Process outputs (VOUTs) - extract real addresses
	for idx, vout := range tx.TxOut {
		// Extract address from pkScript
		receiver, err = types.ExtractAddressFromScript(vout.PkScript, network)
		if err != nil {
			m.logger.Debugf("Failed to extract address from script: %v", err)
			receiver = "unknown"
		}

		// Determine receiver type based on script type
		if types.IsP2PKHScript(vout.PkScript) {
			receiverType = models.WALLET_TYPE_P2PKH
		} else if types.IsP2WPKHScript(vout.PkScript) {
			receiverType = models.WALLET_TYPE_P2WPKH
		} else {
			receiverType = models.WALLET_TYPE_UNKNOWN
		}

        // Check if it's our address
        if _, ok := watchAddrSet[receiver]; ok {
            isUtxo = true
            m.logger.Infof("Detected UTXO for watch address %s: txid=%s vout=%d amount=%d", receiver, txid, idx, vout.Value)
            utxos = append(utxos, &models.UTXO{
                Uid:           "",
                Txid:          txid,
                PkScript:      vout.PkScript,
                OutIndex:      idx,
                Amount:        vout.Value,
                Receiver:      receiver,
                WalletVersion: "1",
                Sender:        sender,
                EvmAddr:       evmAddr,
                Source:        models.UTXO_SOURCE_UNKNOWN,
                ReceiverType:  receiverType,
                Status:        models.UTXO_STATUS_CONFIRMED,
                ReceiveBlock:  dogeBlock.BlockNumber,
                SpentBlock:    0,
                UpdatedAt:     time.Now(),
            })
        }
		vouts = append(vouts, &models.VOUT{
			OrderId:    "",
			BtcHeight:  dogeBlock.BlockNumber,
			Txid:       txid,
			OutIndex:   idx,
			WithdrawId: "",
			Amount:     vout.Value,
			Receiver:   receiver,
			Sender:     sender,
			Source:     models.UTXO_SOURCE_UNKNOWN,
			Status:     models.UTXO_STATUS_CONFIRMED,
			UpdatedAt:  time.Now(),
		})
	}

	// Save UTXOs to database
    if isUtxo {
        for _, utxo := range utxos {
            if isDeposit {
                utxo.Source = models.UTXO_SOURCE_DEPOSIT
            } else if isConsolidation {
                utxo.Source = models.UTXO_SOURCE_CONSOLIDATION
            } else if isWithdrawal {
                utxo.Source = models.UTXO_SOURCE_WITHDRAWAL
            }

            noWitnessTx, _ := types.SerializeTransactionNoWitness(tx)
            merkleRoot, proofBytes, txIndex, err := types.GenerateSPVProof(utxo.Txid, []string{txid})
            if err != nil {
                m.logger.Errorf("GenerateSPVProof err %v, txid: %s", err, utxo.Txid)
                continue
            }

            err = m.state.AddUtxo(utxo, pubkeyBytes, dogeBlock.GetBlockHash().String(), dogeBlock.BlockNumber, noWitnessTx, merkleRoot, proofBytes, txIndex, isDeposit)
            if err != nil {
                m.logger.Errorf("Add utxo %v err %v", utxo, err)
            }
        }
    }

	// Save VINs and VOUTs to database
    if isVin {
        for _, vin := range vins {
			if isDeposit {
				vin.Source = models.UTXO_SOURCE_DEPOSIT
			} else if isConsolidation {
				vin.Source = models.UTXO_SOURCE_CONSOLIDATION
			} else if isWithdrawal {
				vin.Source = models.UTXO_SOURCE_WITHDRAWAL
			}
			err = m.state.AddOrUpdateVin(vin)
			if err != nil {
				m.logger.Errorf("Add vin %v err %v", vin, err)
			}
		}
        for _, vout := range vouts {
			if isDeposit {
				vout.Source = models.UTXO_SOURCE_DEPOSIT
			} else if isConsolidation {
				vout.Source = models.UTXO_SOURCE_CONSOLIDATION
			} else if isWithdrawal {
				vout.Source = models.UTXO_SOURCE_WITHDRAWAL
			}
			err = m.state.AddOrUpdateVout(vout)
			if err != nil {
				m.logger.Errorf("Add vout %v err %v", vout, err)
			}
		}
	}

	// Update send order status
    if (isWithdrawal || isSafebox || isConsolidation) {
        m.logger.Debugf("Update send order confirmed, txid: %s", txid)
		err = m.state.UpdateSendOrderConfirmed(txid, dogeBlock.BlockNumber)
		if err != nil {
			m.logger.Debugf("Update send order confirmed %v err %v", txid, err)
		}

		// update utxo status spent by vin
		if len(vins) > 0 {
			err = m.state.UpdateUtxoStatusSpentByVins(vins, dogeBlock.BlockNumber)
			if err != nil {
				m.logger.Errorf("Update utxo status spent by vins failed, err: %v", err)
			}
		}
	}
}

// parseHexOrRaw parses a hex string like "0xdeadbeef" or "deadbeef" into bytes.
// If s is empty or invalid hex, it returns the raw bytes of s.
func parseHexOrRaw(s string) []byte {
    if s == "" {
        return []byte{}
    }
    // trim 0x/0X prefix
    if len(s) > 2 && (s[:2] == "0x" || s[:2] == "0X") {
        s = s[2:]
    }
    // hex decode
    dst := make([]byte, len(s)/2)
    n := 0
    for i := 0; i+1 < len(s); i += 2 {
        var b byte
        for k := 0; k < 2; k++ {
            c := s[i+k]
            var v byte
            switch {
            case '0' <= c && c <= '9':
                v = c - '0'
            case 'a' <= c && c <= 'f':
                v = c - 'a' + 10
            case 'A' <= c && c <= 'F':
                v = c - 'A' + 10
            default:
                // not hex
                return []byte(s)
            }
            b = (b << 4) | v
        }
        dst[n] = b
        n++
    }
    return dst[:n]
}

func init() {
	log.Info("Registering doge module")
	module.RegisterModule(&DogeModule{})
}
