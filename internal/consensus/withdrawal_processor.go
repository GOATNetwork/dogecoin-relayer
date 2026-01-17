package consensus

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dogecoinw/doged/wire"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/doge"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/internal/p2p"
	"github.com/goat-network/dogecoin-relayer/internal/wallet"
	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/goat-network/dogecoin-relayer/pkg/module"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
	log "github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

// WithdrawalProcessor coordinates Dogecoin L1 payouts and triggers bridgeOutFinish on L2.
// It supports both local signing and Fireblocks-backed signing via the wallet package.
type WithdrawalProcessor struct {
	conn             *models.DBConnection
	eventRepo        *models.EventRepository
	dogeClient       *doge.DogeClient
	utxoProcessor    *UtxoProcessor
	logger           *log.Entry
	changeAddr       string
	requiredConfs    int64
	pollInterval     time.Duration
	ctx              context.Context
	cancel           context.CancelFunc
	eventBus         *eventbus.Bus
	withdrawEnable   bool
	fireblocksMode   bool
	fireblocksClient *fireblocksClient

	// Concurrency protection
	processingMu       sync.Mutex            // Protects withdrawal processing operations
	processingMap      map[string]time.Time  // Tracks withdrawals currently being processed (reqTaskId -> startTime)
	utxoSelectionMu    sync.Mutex            // Protects UTXO selection to prevent double-spend
}

type fireblocksClient struct {
	apiKey       string
	baseURL      string
	vaultAccount string
	assetId      string
	privateKey   *rsa.PrivateKey
	httpClient   *http.Client
}

type fireblocksCreateTxRequest struct {
	Operation       string                    `json:"operation"`
	Note            string                    `json:"note,omitempty"`
	AssetID         string                    `json:"assetId"`
	Source          fireblocksSource          `json:"source"`
	Destination     *fireblocksDestination    `json:"destination,omitempty"`
	Amount          string                    `json:"amount,omitempty"`
	ExtraParameters fireblocksExtraParameters `json:"extraParameters,omitempty"`
}

type fireblocksSource struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type fireblocksDestination struct {
	Type           string                    `json:"type"`
	OneTimeAddress *fireblocksOneTimeAddress `json:"oneTimeAddress,omitempty"`
}

type fireblocksOneTimeAddress struct {
	Address string `json:"address"`
	Tag     string `json:"tag,omitempty"`
}

type fireblocksExtraParameters struct {
	RawMessageData fireblocksRawMessageData `json:"rawMessageData"`
}

type fireblocksRawMessageData struct {
	Messages []fireblocksUnsignedMessage `json:"messages"`
}

type fireblocksUnsignedMessage struct {
	Content string `json:"content"`
}

type fireblocksCreateTxResponse struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type fireblocksTransactionDetails struct {
	ID             string                           `json:"id"`
	Status         string                           `json:"status"`
	SubStatus      string                           `json:"subStatus"`
	TxHash         string                           `json:"txHash"`
	SignedMessages []wallet.FireblocksSignedMessage `json:"signedMessages"`
}

func NewWithdrawalProcessor(conn *models.DBConnection, up *UtxoProcessor) (*WithdrawalProcessor, error) {
	cfg := global.GetConfig()
	if cfg == nil {
		return nil, fmt.Errorf("global config not initialized")
	}

	changeAddr := cfg.Withdraw.ChangeAddress
	if changeAddr == "" && len(cfg.Doge.WatchAddresses) > 0 {
		changeAddr = cfg.Doge.WatchAddresses[0]
	}
	if changeAddr == "" {
		return nil, fmt.Errorf("withdraw change address not configured")
	}

	confs := int64(cfg.Withdraw.MinConfirmations)
	if confs <= 0 {
		confs = int64(cfg.Doge.Confirmations)
	}
	if confs <= 0 {
		confs = 1
	}

	client, err := doge.NewDogeClient(cfg.Doge)
	if err != nil {
		return nil, fmt.Errorf("create doge client for withdrawal: %w", err)
	}

	poll := time.Duration(cfg.Scan.Interval)
	if poll <= 0 {
		poll = 15
	}

	withdrawEnable := cfg.Withdraw.Enabled || cfg.Withdraw.Mode == "" || cfg.Withdraw.Mode == "local" || cfg.Withdraw.Mode == "fireblocks"
	fireblocksMode := cfg.Withdraw.Mode == "fireblocks"
	var fireblocksClient *fireblocksClient
	if fireblocksMode {
		client, err := newFireblocksClient(cfg.Withdraw.Fireblocks)
		if err != nil {
			return nil, fmt.Errorf("init fireblocks client: %w", err)
		}
		fireblocksClient = client
	}

	return &WithdrawalProcessor{
		conn:             conn,
		eventRepo:        models.NewEventRepository(conn.GetDB()),
		dogeClient:       client,
		utxoProcessor:    up,
		logger:           log.WithField("component", "withdrawal-processor"),
		changeAddr:       changeAddr,
		requiredConfs:    confs,
		pollInterval:     poll * time.Second,
		eventBus:         global.GetEventBus(),
		withdrawEnable:   withdrawEnable,
		fireblocksMode:   fireblocksMode,
		fireblocksClient: fireblocksClient,
		processingMap:    make(map[string]time.Time),
	}, nil
}

func newFireblocksClient(cfg config.FireblocksConfig) (*fireblocksClient, error) {
	if cfg.ApiKey == "" {
		return nil, fmt.Errorf("fireblocks api key not configured")
	}
	if cfg.Secret == "" {
		return nil, fmt.Errorf("fireblocks secret not configured")
	}
	if cfg.VaultAccount == "" {
		return nil, fmt.Errorf("fireblocks vault account not configured")
	}
	if cfg.AssetId == "" {
		return nil, fmt.Errorf("fireblocks asset id not configured")
	}
	privateKey, err := parseRSAPrivateKey(cfg.Secret)
	if err != nil {
		return nil, err
	}
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	if baseURL == "" {
		baseURL = "https://api.fireblocks.io"
	}
	return &fireblocksClient{
		apiKey:       cfg.ApiKey,
		baseURL:      baseURL,
		vaultAccount: cfg.VaultAccount,
		assetId:      cfg.AssetId,
		privateKey:   privateKey,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func parseRSAPrivateKey(pemValue string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemValue))
	if block == nil {
		return nil, fmt.Errorf("fireblocks secret is not valid pem")
	}
	parsedKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err == nil {
		return parsedKey, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse fireblocks private key: %w", err)
	}
	privateKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("fireblocks private key is not rsa")
	}
	return privateKey, nil
}

func (client *fireblocksClient) postRawSigningRequest(messages []string, note string) (string, error) {
	payload := make([]fireblocksUnsignedMessage, len(messages))
	for index, message := range messages {
		payload[index] = fireblocksUnsignedMessage{Content: message}
	}
	request := fireblocksCreateTxRequest{
		Operation: "RAW",
		Note:      note,
		AssetID:   client.assetId,
		Source: fireblocksSource{
			Type: "VAULT_ACCOUNT",
			ID:   client.vaultAccount,
		},
		ExtraParameters: fireblocksExtraParameters{
			RawMessageData: fireblocksRawMessageData{
				Messages: payload,
			},
		},
	}
	responseBody, err := client.doRequest("POST", "/v1/transactions", request)
	if err != nil {
		return "", err
	}
	var response fireblocksCreateTxResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", fmt.Errorf("decode fireblocks response: %w", err)
	}
	if response.ID == "" {
		if response.Message != "" {
			return "", fmt.Errorf("fireblocks create transaction failed: %s", response.Message)
		}
		return "", fmt.Errorf("fireblocks create transaction returned empty id")
	}
	return response.ID, nil
}

func (client *fireblocksClient) queryTransaction(id string) (*fireblocksTransactionDetails, error) {
	responseBody, err := client.doRequest("GET", fmt.Sprintf("/v1/transactions/%s", id), nil)
	if err != nil {
		return nil, err
	}
	var details fireblocksTransactionDetails
	if err := json.Unmarshal(responseBody, &details); err != nil {
		return nil, fmt.Errorf("decode fireblocks transaction: %w", err)
	}
	return &details, nil
}

func (client *fireblocksClient) doRequest(method, path string, body any) ([]byte, error) {
	var requestBody []byte
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode fireblocks request: %w", err)
		}
		requestBody = encoded
	}
	hash := sha256.Sum256(requestBody)
	token, err := client.buildJWT(path, hex.EncodeToString(hash[:]))
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(method, client.baseURL+path, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("build fireblocks request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-API-Key", client.apiKey)
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fireblocks request: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read fireblocks response: %w", err)
	}
	if response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("fireblocks response %d: %s", response.StatusCode, string(responseBody))
	}
	return responseBody, nil
}

func (client *fireblocksClient) buildJWT(path string, bodyHash string) (string, error) {
	header := map[string]string{
		"alg": "RS256",
		"typ": "JWT",
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("encode fireblocks jwt header: %w", err)
	}
	issuedAt := time.Now().Unix()
	claims := map[string]any{
		"uri":      path,
		"nonce":    randomHex(16),
		"iat":      issuedAt,
		"exp":      issuedAt + 60,
		"sub":      client.apiKey,
		"bodyHash": bodyHash,
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("encode fireblocks jwt claims: %w", err)
	}
	headerEncoded := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsEncoded := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := headerEncoded + "." + claimsEncoded
	hash := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, client.privateKey, crypto.SHA256, hash[:])
	if err != nil {
		return "", fmt.Errorf("sign fireblocks jwt: %w", err)
	}
	signatureEncoded := base64.RawURLEncoding.EncodeToString(signature)
	return signingInput + "." + signatureEncoded, nil
}

func randomHex(length int) string {
	buffer := make([]byte, length)
	if _, err := rand.Read(buffer); err != nil {
		return hex.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	}
	return hex.EncodeToString(buffer)
}

func (wp *WithdrawalProcessor) Start(ctx context.Context) error {
	if !wp.withdrawEnable {
		wp.logger.Info("Withdrawal processor disabled via config")
		return nil
	}
	wp.ctx, wp.cancel = context.WithCancel(ctx)
	// Clean up aggregating send orders and their associated PENDING UTXOs
	if resetCount, err := wallet.CleanProcessingSendOrders(wp.conn.GetDB()); err != nil {
		wp.logger.Warnf("Failed to clean processing send orders: %v", err)
	} else if resetCount > 0 {
		wp.logger.Infof("Cleaned up processing send orders, reset %d pending UTXOs on startup", resetCount)
	}
	if !wp.fireblocksMode {
		if err := wp.resetFireblocksWithdrawals(); err != nil {
			wp.logger.Warnf("Failed to reset fireblocks withdrawals: %v", err)
		}
	}
	wp.eventBus.Subscribe(eventbus.EventBridgeOutProposed, wp.handleBridgeOutProposed)
	wp.registerP2PHandler()
	go wp.loop()
	wp.logger.Infof("Withdrawal processor started (change=%s, confirmations=%d)", wp.changeAddr, wp.requiredConfs)
	return nil
}

func (wp *WithdrawalProcessor) Shutdown() {
	if wp.cancel != nil {
		wp.cancel()
	}
	if wp.dogeClient != nil {
		wp.dogeClient.Close()
	}
	wp.eventBus.Unsubscribe(eventbus.EventBridgeOutProposed, wp.handleBridgeOutProposed)
}

func (wp *WithdrawalProcessor) loop() {
	ticker := time.NewTicker(wp.pollInterval)
	defer ticker.Stop()

	wp.logger.Infof("Withdrawal processor loop started with interval %s", wp.pollInterval)

	for {
		select {
		case <-wp.ctx.Done():
			wp.logger.Info("Withdrawal processor stopped")
			return
		case <-ticker.C:
			if err := wp.processRetryWithdrawals(); err != nil {
				wp.logger.Errorf("processRetryWithdrawals error: %v", err)
			}
			if err := wp.processPendingWithdrawals(); err != nil {
				wp.logger.Errorf("processPendingWithdrawals error: %v", err)
			}
			if wp.fireblocksMode {
				if err := wp.processSigningWithdrawals(); err != nil {
					wp.logger.Errorf("processSigningWithdrawals error: %v", err)
				}
			}
			if err := wp.processBroadcastedWithdrawals(); err != nil {
				wp.logger.Errorf("processBroadcastedWithdrawals error: %v", err)
			}
			if err := wp.processStatusSync(); err != nil {
				wp.logger.Errorf("processStatusSync error: %v", err)
			}
		}
	}
}

func (wp *WithdrawalProcessor) handleBridgeOutProposed(data any) {
	if err := wp.processPendingWithdrawals(); err != nil {
		wp.logger.Errorf("handleBridgeOutProposed process error: %v", err)
	}
}

func (wp *WithdrawalProcessor) processPendingWithdrawals() error {
	if wp.utxoProcessor == nil {
		return fmt.Errorf("utxo processor not ready")
	}
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		return err
	}
	if !isProposer {
		wp.logger.Debug("Not proposer, skip pending withdrawal processing")
		return nil
	}

	// Use mutex to prevent concurrent processing
	wp.processingMu.Lock()
	defer wp.processingMu.Unlock()

	// Clean up stale processing entries (older than 5 minutes)
	wp.cleanupStaleProcessing()

	statuses := []string{
		models.WITHDRAW_STATUS_CREATE,
		models.WITHDRAW_STATUS_AGGREGATING,
		models.WITHDRAW_STATUS_INIT,
	}
	for _, status := range statuses {
		list, err := wp.eventRepo.ListWithdrawalsByStatus(nil, status, 20)
		if err != nil {
			return fmt.Errorf("list %s withdrawals: %w", status, err)
		}
		for i := range list {
			reqTaskId := list[i].ReqTaskId
			// Skip if already being processed
			if _, isProcessing := wp.processingMap[reqTaskId]; isProcessing {
				wp.logger.Debugf("Withdrawal %s is already being processed, skipping", reqTaskId)
				continue
			}
			// Mark as being processed
			wp.processingMap[reqTaskId] = time.Now()
			if err := wp.broadcastWithdrawal(&list[i]); err != nil {
				wp.logger.Errorf("broadcast withdrawal %s failed: %v", reqTaskId, err)
				wp.markWithdrawalRetry(&list[i])
			}
			// Remove from processing map after completion
			delete(wp.processingMap, reqTaskId)
		}
	}
	return nil
}

// cleanupStaleProcessing removes entries that have been processing for too long
func (wp *WithdrawalProcessor) cleanupStaleProcessing() {
	staleThreshold := 5 * time.Minute
	now := time.Now()
	for reqTaskId, startTime := range wp.processingMap {
		if now.Sub(startTime) > staleThreshold {
			wp.logger.Warnf("Cleaning up stale processing entry for withdrawal %s (started at %v)", reqTaskId, startTime)
			delete(wp.processingMap, reqTaskId)
		}
	}
}

func (wp *WithdrawalProcessor) processSigningWithdrawals() error {
	if !wp.fireblocksMode {
		return nil
	}
	if wp.fireblocksClient == nil {
		return fmt.Errorf("fireblocks client not initialized")
	}

	var signing []models.Withdrawal
	if err := wp.conn.GetDB().
		Where("status = ?", models.WITHDRAW_STATUS_PENDING).
		Where("external_id != ''").
		Where("tx_id = '' OR tx_id IS NULL").
		Limit(20).
		Find(&signing).Error; err != nil {
		return fmt.Errorf("list signing withdrawals: %w", err)
	}
	for index := range signing {
		if err := wp.finishFireblocksWithdrawal(&signing[index]); err != nil {
			wp.logger.Errorf("finish fireblocks withdrawal %s failed: %v", signing[index].ReqTaskId, err)
		}
	}
	return nil
}

func (wp *WithdrawalProcessor) processBroadcastedWithdrawals() error {
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		wp.logger.Errorf("processBroadcastedWithdrawals: isCurrentProposer error: %v", err)
		return err
	}
	if !isProposer {
		return nil
	}

	var pending []models.Withdrawal
	if err := wp.conn.GetDB().
		Where("status = ?", models.WITHDRAW_STATUS_PENDING).
		Where("tx_id != ''").
		Limit(50).
		Find(&pending).Error; err != nil {
		return fmt.Errorf("list pending withdrawals: %w", err)
	}

	if len(pending) > 0 {
		wp.logger.Infof("processBroadcastedWithdrawals: found %d pending withdrawals with tx_id", len(pending))
	}

	for i := range pending {
		w := &pending[i]
		wp.logger.Debugf("processBroadcastedWithdrawals: checking withdrawal %s with tx_id %s", w.ReqTaskId, w.TxId)
		if err := wp.checkAndSubmit(w); err != nil {
			wp.logger.Errorf("checkAndSubmit withdrawal %s failed: %v", w.ReqTaskId, err)
		}
	}
	return nil
}

func (wp *WithdrawalProcessor) processRetryWithdrawals() error {
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		return err
	}
	if !isProposer {
		return nil
	}

	retryDelay := wp.pollInterval * 2
	if retryDelay < 2*time.Minute {
		retryDelay = 2 * time.Minute
	}

	var pending []models.Withdrawal
	if err := wp.conn.GetDB().
		Where("status = ?", models.WITHDRAW_STATUS_PENDING).
		Where("tx_id = '' OR tx_id IS NULL").
		Limit(20).
		Find(&pending).Error; err != nil {
		return fmt.Errorf("list pending withdrawals: %w", err)
	}

	for i := range pending {
		w := &pending[i]
		if time.Since(w.UpdatedAt) < retryDelay {
			continue
		}
		closedOrders, err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId)
		if err != nil {
			return fmt.Errorf("close send orders for %s: %w", w.ReqTaskId, err)
		}
		// Broadcast SendOrder status updates for closed orders
		for _, order := range closedOrders {
			wp.broadcastSendOrderStatusUpdate(order.OrderId, order.Txid, order.OldStatus, "closed", "withdrawal_retry")
		}
		w.Status = models.WITHDRAW_STATUS_INIT
		w.ExternalId = ""
		w.UnsignedTx = nil
		w.TxId = ""
		w.TxBytes = nil
		if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, w); err != nil {
			wp.logger.Warnf("Failed to reset withdrawal %s to init: %v", w.ReqTaskId, err)
			continue
		}
		wp.broadcastWithdrawalStatus(w)
	}

	return nil
}

func (wp *WithdrawalProcessor) processStatusSync() error {
	isProposer, err := wp.utxoProcessor.isCurrentProposer()
	if err != nil {
		return err
	}
	if !isProposer {
		return nil
	}

	var recent []models.Withdrawal
	if err := wp.conn.GetDB().Order("updated_at desc").Limit(20).Find(&recent).Error; err != nil {
		return fmt.Errorf("load recent withdrawals: %w", err)
	}
	for i := range recent {
		wp.broadcastWithdrawalStatus(&recent[i])
	}
	return nil
}

func (wp *WithdrawalProcessor) resetFireblocksWithdrawals() error {
	var withdrawals []models.Withdrawal
	if err := wp.conn.GetDB().
		Where("status = ?", models.WITHDRAW_STATUS_PENDING).
		Where("external_id != ''").
		Where("tx_id = '' OR tx_id IS NULL").
		Find(&withdrawals).Error; err != nil {
		return fmt.Errorf("load pending fireblocks withdrawals: %w", err)
	}
	for i := range withdrawals {
		w := &withdrawals[i]
		closedOrders, err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId)
		if err != nil {
			return fmt.Errorf("close send orders for %s: %w", w.ReqTaskId, err)
		}
		// Broadcast SendOrder status updates for closed orders
		for _, order := range closedOrders {
			wp.broadcastSendOrderStatusUpdate(order.OrderId, order.Txid, order.OldStatus, "closed", "fireblocks_reset")
		}
		w.Status = models.WITHDRAW_STATUS_INIT
		w.ExternalId = ""
		w.UnsignedTx = nil
		w.TxId = ""
		w.TxBytes = nil
		if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, w); err != nil {
			return fmt.Errorf("reset withdrawal %s: %w", w.ReqTaskId, err)
		}
		wp.broadcastWithdrawalStatus(w)
	}
	return nil
}

func (wp *WithdrawalProcessor) markWithdrawalRetry(w *models.Withdrawal) {
	closedOrders, err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId)
	if err != nil {
		wp.logger.Warnf("Failed to close send orders for %s: %v", w.ReqTaskId, err)
	}
	// Broadcast SendOrder status updates for closed orders
	for _, order := range closedOrders {
		wp.broadcastSendOrderStatusUpdate(order.OrderId, order.Txid, order.OldStatus, "closed", "withdrawal_retry")
	}
	w.Status = models.WITHDRAW_STATUS_INIT
	w.ExternalId = ""
	w.UnsignedTx = nil
	w.TxId = ""
	w.TxBytes = nil
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, w); err != nil {
		wp.logger.Warnf("Failed to reset withdrawal %s to init: %v", w.ReqTaskId, err)
		return
	}
	wp.broadcastWithdrawalStatus(w)
}

func (wp *WithdrawalProcessor) registerP2PHandler() {
	go func() {
		time.Sleep(30 * time.Second)
		maxRetries := 30
		for i := 0; i < maxRetries; i++ {
			p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
			if !ok {
				wp.logger.Debugf("P2P module not found, retrying in 2 seconds... (%d/%d)", i+1, maxRetries)
				time.Sleep(2 * time.Second)
				continue
			}
			network := p2pModule.(*p2p.P2PModule).GetNetwork()
			if network == nil {
				wp.logger.Debugf("P2P network not initialized, retrying in 2 seconds... (%d/%d)", i+1, maxRetries)
				time.Sleep(2 * time.Second)
				continue
			}

			// Register withdrawal status handler
			err := p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeWithdrawalStatus, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleWithdrawalStatusMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register withdrawal status handler: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			// Register send_order broadcast handler
			err = p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeSendOrderBroadcasted, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleSendOrderBroadcastMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register send_order broadcast handler: %v", err)
				// Continue anyway, withdrawal status handler is more important
			}

			// Register send_order txid update handler
			err = p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeSendOrderTxidUpdate, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleSendOrderTxidUpdateMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register send_order txid update handler: %v", err)
			}

			// Register UTXO status update handler
			err = p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeUTXOStatusUpdate, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleUTXOStatusUpdateMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register UTXO status update handler: %v", err)
			}

			// Register SendOrder status update handler
			err = p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeSendOrderStatusUpdate, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleSendOrderStatusUpdateMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register SendOrder status update handler: %v", err)
			}

			// Register PendingBatch status update handler
			err = p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypePendingBatchStatus, func(msg *types.P2PBroadcastMessage) error {
				return wp.handlePendingBatchStatusMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register PendingBatch status handler: %v", err)
			}

			wp.logger.Info("Registered P2P handlers for withdrawal status, send_order, UTXO, pending_batch and status updates")
			return
		}
		wp.logger.Errorf("Failed to register P2P handlers after %d retries", maxRetries)
	}()
}

func (wp *WithdrawalProcessor) broadcastSendOrder(sendOrder *models.SendOrder, vins []*models.VIN, vouts []*models.VOUT) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		wp.logger.Debug("P2P module not available for send_order broadcast")
		return
	}

	vinPayloads := make([]types.SendOrderVIN, len(vins))
	for i, vin := range vins {
		vinPayloads[i] = types.SendOrderVIN{
			Txid:     vin.Txid,
			OutIndex: vin.OutIndex,
			Source:   vin.Source,
		}
	}

	voutPayloads := make([]types.SendOrderVOUT, len(vouts))
	for i, vout := range vouts {
		voutPayloads[i] = types.SendOrderVOUT{
			Txid:       vout.Txid,
			OutIndex:   vout.OutIndex,
			WithdrawId: vout.WithdrawId,
			Amount:     vout.Amount,
			Receiver:   vout.Receiver,
			Source:     vout.Source,
		}
	}

	payload := types.SendOrderBroadcastPayload{
		TxId:       sendOrder.Txid,
		ExternalId: sendOrder.ExternalId,
		OrderId:    sendOrder.OrderId,
		OrderType:  sendOrder.OrderType,
		Status:     sendOrder.Status,
		VINs:       vinPayloads,
		VOUTs:      voutPayloads,
		UpdatedAt:  time.Now().Unix(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		wp.logger.Warnf("Failed to marshal send_order broadcast payload: %v", err)
		return
	}

	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeSendOrderBroadcasted,
		SessionID: sendOrder.Txid,
		Payload:   payloadBytes,
	}

	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		wp.logger.Warnf("Failed to broadcast send_order %s: %v", sendOrder.OrderId, err)
	} else {
		wp.logger.Infof("Broadcasted send_order %s (txid=%s) to P2P network", sendOrder.OrderId, sendOrder.Txid)
	}
}

func (wp *WithdrawalProcessor) broadcastWithdrawalStatus(w *models.Withdrawal) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		return
	}
	payload := types.WithdrawalStatusPayload{
		ReqTaskId:      w.ReqTaskId,
		Status:         w.Status,
		ReqTxHash:      w.ReqTxHash,
		ReqBlock:       w.ReqBlock,
		ReqLogIndex:    w.ReqLogIndex,
		DestAddress:    w.DestAddress,
		DestAmount:     w.DestAmount,
		TxId:           w.TxId,
		ExternalId:     w.ExternalId,
		Vout:           w.Vout,
		TxBytes:        w.TxBytes,
		UnsignedTx:     w.UnsignedTx,
		FinishTxHash:   w.FinishTxHash,
		FinishBlock:    w.FinishBlock,
		FinishLogIndex: w.FinishLogIndex,
		UpdatedAt:      time.Now().Unix(),
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		wp.logger.Warnf("Failed to marshal withdrawal status payload: %v", err)
		return
	}
	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeWithdrawalStatus,
		SessionID: w.ReqTaskId,
		Payload:   payloadBytes,
	}
	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		wp.logger.Warnf("Failed to broadcast withdrawal status %s: %v", w.ReqTaskId, err)
	}
}

func (wp *WithdrawalProcessor) handleWithdrawalStatusMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.WithdrawalStatusPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal withdrawal status payload: %w", err)
	}
	if payload.ReqTaskId == "" {
		return fmt.Errorf("withdrawal status payload missing req_task_id")
	}
	current, err := wp.eventRepo.GetWithdrawalByTask(nil, payload.ReqTaskId)
	if err == nil && !shouldApplyWithdrawalUpdate(current, &payload) {
		return nil
	}

	w := &models.Withdrawal{
		ReqTaskId:      payload.ReqTaskId,
		ReqTxHash:      payload.ReqTxHash,
		ReqBlock:       payload.ReqBlock,
		ReqLogIndex:    payload.ReqLogIndex,
		Status:         payload.Status,
		DestAddress:    payload.DestAddress,
		DestAmount:     payload.DestAmount,
		TxId:           payload.TxId,
		ExternalId:     payload.ExternalId,
		Vout:           payload.Vout,
		TxBytes:        payload.TxBytes,
		UnsignedTx:     payload.UnsignedTx,
		FinishTxHash:   payload.FinishTxHash,
		FinishBlock:    payload.FinishBlock,
		FinishLogIndex: payload.FinishLogIndex,
	}
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, w); err != nil {
		return fmt.Errorf("update withdrawal from p2p: %w", err)
	}
	return nil
}

func (wp *WithdrawalProcessor) handleSendOrderBroadcastMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.SendOrderBroadcastPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal send_order broadcast payload: %w", err)
	}

	if payload.TxId == "" || payload.OrderId == "" {
		return fmt.Errorf("send_order broadcast payload missing tx_id or order_id")
	}

	wp.logger.Infof("Received send_order broadcast: orderId=%s, txId=%s, externalId=%s",
		payload.OrderId, payload.TxId, payload.ExternalId)

	// Check if send_order already exists
	stateRepo := models.NewStateRepository(wp.conn.GetDB())
	existing, err := stateRepo.GetSendOrderByTxIdOrExternalId(payload.TxId)
	if err == nil && existing != nil {
		wp.logger.Debugf("Send order %s already exists, skipping", payload.TxId)
		return nil
	}

	// Create the send_order and associated VINs/VOUTs
	err = wp.conn.GetDB().Transaction(func(tx *gorm.DB) error {
		// Create SendOrder
		sendOrder := &models.SendOrder{
			OrderId:    payload.OrderId,
			OrderType:  payload.OrderType,
			Txid:       payload.TxId,
			ExternalId: payload.ExternalId,
			Status:     payload.Status,
		}
		if err := tx.Create(sendOrder).Error; err != nil {
			return fmt.Errorf("create send_order: %w", err)
		}

		// Create VINs
		for _, vinPayload := range payload.VINs {
			vin := &models.VIN{
				OrderId:  payload.OrderId,
				Txid:     vinPayload.Txid,
				OutIndex: vinPayload.OutIndex,
				Source:   vinPayload.Source,
				Status:   payload.Status,
			}
			if err := tx.Create(vin).Error; err != nil {
				return fmt.Errorf("create vin: %w", err)
			}
		}

		// Create VOUTs
		for _, voutPayload := range payload.VOUTs {
			vout := &models.VOUT{
				OrderId:    payload.OrderId,
				Txid:       voutPayload.Txid,
				OutIndex:   voutPayload.OutIndex,
				WithdrawId: voutPayload.WithdrawId,
				Amount:     voutPayload.Amount,
				Receiver:   voutPayload.Receiver,
				Source:     voutPayload.Source,
				Status:     payload.Status,
			}
			if err := tx.Create(vout).Error; err != nil {
				return fmt.Errorf("create vout: %w", err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("save send_order from p2p: %w", err)
	}

	wp.logger.Infof("Created send_order %s (txId=%s) from P2P broadcast", payload.OrderId, payload.TxId)
	return nil
}

// broadcastSendOrderTxidUpdate broadcasts a txid update to other nodes after Fireblocks signing
func (wp *WithdrawalProcessor) broadcastSendOrderTxidUpdate(externalId, oldTxid, newTxid string) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		wp.logger.Debug("P2P module not available for txid update broadcast")
		return
	}

	payload := types.SendOrderTxidUpdatePayload{
		ExternalId: externalId,
		OldTxid:    oldTxid,
		NewTxid:    newTxid,
		UpdatedAt:  time.Now().Unix(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		wp.logger.Warnf("Failed to marshal txid update payload: %v", err)
		return
	}

	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeSendOrderTxidUpdate,
		SessionID: externalId,
		Payload:   payloadBytes,
	}

	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		wp.logger.Warnf("Failed to broadcast txid update for %s: %v", externalId, err)
	} else {
		wp.logger.Infof("Broadcasted txid update: externalId=%s, oldTxid=%s, newTxid=%s", externalId, oldTxid, newTxid)
	}
}

// handleSendOrderTxidUpdateMessage handles P2P messages for txid updates after signing
func (wp *WithdrawalProcessor) handleSendOrderTxidUpdateMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.SendOrderTxidUpdatePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal txid update payload: %w", err)
	}

	if payload.ExternalId == "" || payload.NewTxid == "" {
		return fmt.Errorf("txid update payload missing external_id or new_txid")
	}

	wp.logger.Infof("Received txid update: externalId=%s, oldTxid=%s, newTxid=%s",
		payload.ExternalId, payload.OldTxid, payload.NewTxid)

	// Update send_order.txid by external_id
	result := wp.conn.GetDB().Model(&models.SendOrder{}).
		Where("external_id = ?", payload.ExternalId).
		Updates(map[string]interface{}{
			"txid":       payload.NewTxid,
			"updated_at": time.Now(),
		})

	if result.Error != nil {
		return fmt.Errorf("update send_order txid: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		wp.logger.Infof("Updated send_order txid to %s for externalId=%s", payload.NewTxid, payload.ExternalId)
	} else {
		wp.logger.Debugf("No send_order found for externalId=%s", payload.ExternalId)
	}

	return nil
}

// handleUTXOStatusUpdateMessage handles P2P messages for UTXO status changes
func (wp *WithdrawalProcessor) handleUTXOStatusUpdateMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.UTXOStatusUpdatePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal utxo status update payload: %w", err)
	}

	if payload.Txid == "" {
		return fmt.Errorf("utxo status update payload missing txid")
	}

	wp.logger.Infof("Received UTXO status update: txid=%s:%d, %s->%s, reason=%s",
		payload.Txid, payload.OutIndex, payload.OldStatus, payload.NewStatus, payload.Reason)

	// Check current status and apply hierarchy rules
	var utxo models.UTXO
	err := wp.conn.GetDB().Where("txid = ? AND out_index = ?", payload.Txid, payload.OutIndex).First(&utxo).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			wp.logger.Debugf("UTXO %s:%d not found, skipping update", payload.Txid, payload.OutIndex)
			return nil
		}
		return fmt.Errorf("query utxo: %w", err)
	}

	// Apply status hierarchy: only allow forward transitions or same rank updates
	if !shouldApplyUTXOStatusUpdate(utxo.Status, payload.NewStatus) {
		wp.logger.Debugf("Skipping UTXO status update %s:%d: current=%s, incoming=%s (hierarchy violation)",
			payload.Txid, payload.OutIndex, utxo.Status, payload.NewStatus)
		return nil
	}

	// Update UTXO status
	result := wp.conn.GetDB().Model(&models.UTXO{}).
		Where("txid = ? AND out_index = ?", payload.Txid, payload.OutIndex).
		Update("status", payload.NewStatus)

	if result.Error != nil {
		return fmt.Errorf("update utxo status: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		wp.logger.Infof("Updated UTXO %s:%d status to %s via P2P", payload.Txid, payload.OutIndex, payload.NewStatus)
	}

	return nil
}

// handleSendOrderStatusUpdateMessage handles P2P messages for SendOrder status changes
func (wp *WithdrawalProcessor) handleSendOrderStatusUpdateMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.SendOrderStatusUpdatePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal send_order status update payload: %w", err)
	}

	if payload.OrderId == "" && payload.Txid == "" {
		return fmt.Errorf("send_order status update payload missing order_id and txid")
	}

	wp.logger.Infof("Received SendOrder status update: orderId=%s, txid=%s, %s->%s, reason=%s",
		payload.OrderId, payload.Txid, payload.OldStatus, payload.NewStatus, payload.Reason)

	// Build query based on available identifiers
	query := wp.conn.GetDB().Model(&models.SendOrder{})
	if payload.OrderId != "" {
		query = query.Where("order_id = ?", payload.OrderId)
	} else {
		query = query.Where("txid = ?", payload.Txid)
	}

	// Check current status
	var sendOrder models.SendOrder
	if err := query.First(&sendOrder).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			wp.logger.Debugf("SendOrder not found (orderId=%s, txid=%s), skipping update", payload.OrderId, payload.Txid)
			return nil
		}
		return fmt.Errorf("query send_order: %w", err)
	}

	// Apply status hierarchy for send orders
	if !shouldApplySendOrderStatusUpdate(sendOrder.Status, payload.NewStatus) {
		wp.logger.Debugf("Skipping SendOrder status update: current=%s, incoming=%s (hierarchy violation)",
			sendOrder.Status, payload.NewStatus)
		return nil
	}

	// Update SendOrder and associated VINs/VOUTs
	err := wp.conn.GetDB().Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		if err := tx.Model(&models.SendOrder{}).
			Where("order_id = ?", sendOrder.OrderId).
			Updates(map[string]interface{}{
				"status":     payload.NewStatus,
				"updated_at": now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.VIN{}).
			Where("order_id = ?", sendOrder.OrderId).
			Updates(map[string]interface{}{
				"status":     payload.NewStatus,
				"updated_at": now,
			}).Error; err != nil {
			return err
		}

		if err := tx.Model(&models.VOUT{}).
			Where("order_id = ?", sendOrder.OrderId).
			Updates(map[string]interface{}{
				"status":     payload.NewStatus,
				"updated_at": now,
			}).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("update send_order status: %w", err)
	}

	wp.logger.Infof("Updated SendOrder %s status to %s via P2P", sendOrder.OrderId, payload.NewStatus)
	return nil
}

// handlePendingBatchStatusMessage handles P2P messages for pending_batch status changes
func (wp *WithdrawalProcessor) handlePendingBatchStatusMessage(msg *types.P2PBroadcastMessage) error {
	var payload types.PendingBatchStatusPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal pending_batch status payload: %w", err)
	}

	if payload.BaseSessionID == "" {
		return fmt.Errorf("pending_batch status payload missing base_session_id")
	}

	wp.logger.Infof("Received PendingBatch status update: baseSessionId=%s, batchType=%s, status=%s",
		payload.BaseSessionID, payload.BatchType, payload.Status)

	// Update pending_batch status in database
	result := wp.conn.GetDB().Model(&models.PendingBatch{}).
		Where("base_session_id = ?", payload.BaseSessionID).
		Update("status", payload.Status)

	if result.Error != nil {
		return fmt.Errorf("update pending_batch status: %w", result.Error)
	}

	if result.RowsAffected > 0 {
		wp.logger.Infof("Updated PendingBatch %s status to %s via P2P", payload.BaseSessionID, payload.Status)
	} else {
		wp.logger.Debugf("PendingBatch %s not found, skipping update", payload.BaseSessionID)
	}

	return nil
}

// broadcastUTXOStatusUpdate broadcasts a UTXO status change to other nodes
func (wp *WithdrawalProcessor) broadcastUTXOStatusUpdate(txid string, outIndex int, oldStatus, newStatus, reason string) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		wp.logger.Debug("P2P module not available for UTXO status broadcast")
		return
	}

	payload := types.UTXOStatusUpdatePayload{
		Txid:      txid,
		OutIndex:  outIndex,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Reason:    reason,
		UpdatedAt: time.Now().Unix(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		wp.logger.Warnf("Failed to marshal UTXO status update payload: %v", err)
		return
	}

	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeUTXOStatusUpdate,
		SessionID: fmt.Sprintf("%s:%d", txid, outIndex),
		Payload:   payloadBytes,
	}

	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		wp.logger.Warnf("Failed to broadcast UTXO status update %s:%d: %v", txid, outIndex, err)
	} else {
		wp.logger.Infof("Broadcasted UTXO status update: %s:%d %s->%s (%s)", txid, outIndex, oldStatus, newStatus, reason)
	}
}

// broadcastSendOrderStatusUpdate broadcasts a SendOrder status change to other nodes
func (wp *WithdrawalProcessor) broadcastSendOrderStatusUpdate(orderId, txid, oldStatus, newStatus, reason string) {
	p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
	if !ok {
		wp.logger.Debug("P2P module not available for SendOrder status broadcast")
		return
	}

	payload := types.SendOrderStatusUpdatePayload{
		OrderId:   orderId,
		Txid:      txid,
		OldStatus: oldStatus,
		NewStatus: newStatus,
		Reason:    reason,
		UpdatedAt: time.Now().Unix(),
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		wp.logger.Warnf("Failed to marshal SendOrder status update payload: %v", err)
		return
	}

	msg := types.P2PBroadcastMessage{
		Type:      types.P2PMessageTypeSendOrderStatusUpdate,
		SessionID: orderId,
		Payload:   payloadBytes,
	}

	if err := p2pModule.(p2p.P2PSender).BroadcastP2PMessage(msg); err != nil {
		wp.logger.Warnf("Failed to broadcast SendOrder status update %s: %v", orderId, err)
	} else {
		wp.logger.Infof("Broadcasted SendOrder status update: %s %s->%s (%s)", orderId, oldStatus, newStatus, reason)
	}
}

// shouldApplyUTXOStatusUpdate checks if the status update should be applied based on hierarchy
// Status hierarchy: unconfirmed (0) < confirmed (10) < processed (20) < pending (30) < spent (100)
func shouldApplyUTXOStatusUpdate(currentStatus, newStatus string) bool {
	currentRank := utxoStatusRank(currentStatus)
	newRank := utxoStatusRank(newStatus)
	// Allow forward transitions or same rank updates (for idempotency)
	return newRank >= currentRank
}

func utxoStatusRank(status string) int {
	switch status {
	case models.UTXO_STATUS_UNCONFIRMED:
		return 0
	case models.UTXO_STATUS_CONFIRMED:
		return 10
	case models.UTXO_STATUS_PROCESSED:
		return 20
	case models.UTXO_STATUS_PENDING:
		return 30
	case models.UTXO_STATUS_SPENT:
		return 100
	default:
		return 0
	}
}

// shouldApplySendOrderStatusUpdate checks if the status update should be applied based on hierarchy
// Status hierarchy: aggregating (0) < init (10) < pending (20) < confirmed (30) < processed (40) < closed (50)
func shouldApplySendOrderStatusUpdate(currentStatus, newStatus string) bool {
	currentRank := sendOrderStatusRank(currentStatus)
	newRank := sendOrderStatusRank(newStatus)
	// Allow forward transitions or same rank updates (for idempotency)
	return newRank >= currentRank
}

func sendOrderStatusRank(status string) int {
	switch status {
	case "aggregating":
		return 0
	case "init":
		return 10
	case "pending":
		return 20
	case "confirmed":
		return 30
	case "processed":
		return 40
	case "closed":
		return 50
	default:
		return 0
	}
}

func shouldApplyWithdrawalUpdate(current *models.Withdrawal, payload *types.WithdrawalStatusPayload) bool {
	if current == nil {
		return true
	}
	currentStatus := normalizeWithdrawalStatus(current.Status)
	incomingStatus := normalizeWithdrawalStatus(payload.Status)
	currentRank := withdrawalStatusRank(currentStatus)
	incomingRank := withdrawalStatusRank(incomingStatus)
	if incomingRank < currentRank {
		return false
	}
	if incomingRank == currentRank {
		return true
	}
	return true
}

func normalizeWithdrawalStatus(status string) string {
	return strings.ToLower(strings.TrimSpace(status))
}

func withdrawalStatusRank(status string) int {
	switch status {
	case models.WITHDRAW_STATUS_CREATE, models.WITHDRAW_STATUS_AGGREGATING:
		return 10
	case models.WITHDRAW_STATUS_INIT, models.WITHDRAW_STATUS_PENDING:
		return 20
	case models.WITHDRAW_STATUS_CONFIRMED:
		return 30
	case models.WITHDRAW_STATUS_PROCESSED:
		return 40
	default:
		return 0
	}
}

func (wp *WithdrawalProcessor) broadcastWithdrawal(w *models.Withdrawal) error {
	if w.DestAddress == "" {
		return fmt.Errorf("withdrawal %s missing dest address", w.ReqTaskId)
	}

	amountWei, err := parseAmountString(w.DestAmount)
	if err != nil || amountWei.Sign() <= 0 {
		return fmt.Errorf("withdrawal %s invalid dest amount: %v", w.ReqTaskId, err)
	}

	weiToSatoshiDivisor := big.NewInt(10000000000)
	amountSat := new(big.Int).Div(amountWei, weiToSatoshiDivisor)
	if amountSat.Sign() <= 0 {
		return fmt.Errorf("withdrawal %s amount too small after conversion: wei=%s sat=%s", w.ReqTaskId, amountWei.String(), amountSat.String())
	}

	if !amountSat.IsInt64() {
		return fmt.Errorf("withdrawal amount too large: %s", amountSat.String())
	}

	wp.logger.Infof("Withdrawal %s: converting amount wei=%s -> sat=%s",
		w.ReqTaskId, amountWei.String(), amountSat.String())

	aggregating := &models.Withdrawal{
		ReqTaskId:   w.ReqTaskId,
		ReqTxHash:   w.ReqTxHash,
		ReqBlock:    w.ReqBlock,
		ReqLogIndex: w.ReqLogIndex,
		Status:      models.WITHDRAW_STATUS_AGGREGATING,
		DestAddress: w.DestAddress,
		DestAmount:  w.DestAmount,
		TxId:        w.TxId,
		ExternalId:  w.ExternalId,
		Vout:        w.Vout,
		TxBytes:     w.TxBytes,
		UnsignedTx:  w.UnsignedTx,
	}
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, aggregating); err != nil {
		return fmt.Errorf("update withdrawal %s to aggregating: %w", w.ReqTaskId, err)
	}
	wp.broadcastWithdrawalStatus(aggregating)

	closedOrders, err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId)
	if err != nil {
		return fmt.Errorf("cleanup send orders for withdrawal %s: %w", w.ReqTaskId, err)
	}
	// Broadcast SendOrder status updates for closed orders
	for _, order := range closedOrders {
		wp.broadcastSendOrderStatusUpdate(order.OrderId, order.Txid, order.OldStatus, "closed", "withdrawal_cleanup")
	}

	var utxos []*models.UTXO
	addresses := []string{wp.changeAddr}
	if cfg := global.GetConfig(); cfg != nil {
		for _, addr := range cfg.Doge.WatchAddresses {
			if addr != "" && addr != wp.changeAddr {
				addresses = append(addresses, addr)
			}
		}
	}
	var spendableCount int64
	if err := wp.conn.GetDB().Raw(
		"SELECT COUNT(*) FROM utxos WHERE receiver = ? AND (status = ? OR status = ?)",
		wp.changeAddr,
		models.UTXO_STATUS_CONFIRMED,
		models.UTXO_STATUS_PROCESSED,
	).Scan(&spendableCount).Error; err != nil {
		return fmt.Errorf("count spendable utxos: %w", err)
	}
	wp.logger.Infof("Spendable UTXO count for %s: %d", wp.changeAddr, spendableCount)
	if spendableCount == 0 {
		return fmt.Errorf("no spendable utxos available for withdrawal")
	}

	if err := wp.conn.GetDB().Raw(
		"SELECT * FROM utxos WHERE receiver = ? AND (status = ? OR status = ?) ORDER BY amount asc",
		wp.changeAddr,
		models.UTXO_STATUS_CONFIRMED,
		models.UTXO_STATUS_PROCESSED,
	).Scan(&utxos).Error; err != nil {
		return fmt.Errorf("load spendable utxos: %w", err)
	}

	wp.logger.Debugf("Found %d confirmed UTXOs for withdrawal %s", len(utxos), w.ReqTaskId)

	utxoAdapters := make([]*wallet.UTXOAdapter, 0, len(utxos))
	for _, utxo := range utxos {
		utxoAdapters = append(utxoAdapters, wallet.ToUTXOAdapter(utxo))
	}

	// Get effective fee rate from network or config
	configFeeRate := int64(global.GetConfig().Withdraw.FeeRate)
	networkFee := doge.GetEffectiveFeeRate(wp.dogeClient, configFeeRate)
	wp.logger.Infof("Using fee rate: %d sat/byte", networkFee)

	selectedUTXOs, totalAmount, withdrawAmount, changeAmount, estimatedFee, witnessSize, err := wallet.SelectOptimalUTXOs(
		utxoAdapters,
		[]string{wallet.WALLET_TYPE_P2PKH},
		amountSat.Int64(),
		0,
		networkFee,
		1,
	)
	if err != nil {
		return fmt.Errorf("select optimal utxos: %w", err)
	}

	wp.logger.Infof("Selected %d UTXOs: total=%d, withdraw=%d, change=%d, fee=%f",
		len(selectedUTXOs), totalAmount, withdrawAmount, changeAmount, estimatedFee)

	dogeNet := &wallet.DogeNetworkParams{
		Params: types.GetDogeNetwork(global.GetConfig().Doge.NetworkType),
	}

	withdrawalAdapter := wallet.ToWithdrawalAdapter(w, amountSat.Int64(), networkFee)

	txParams := &wallet.TransactionParams{
		UTXOs:          selectedUTXOs,
		Withdrawals:    []*wallet.WithdrawalAdapter{withdrawalAdapter},
		ChangeAddress:  wp.changeAddr,
		ChangeAmount:   changeAmount,
		EstimatedFee:   estimatedFee,
		WitnessSize:    witnessSize,
		NetworkFee:     networkFee,
		Net:            dogeNet,
		UtxoAmount:     totalAmount,
		WithdrawAmount: withdrawAmount,
	}

	tx, actualFee, err := wallet.CreateRawTransaction(txParams)
	if err != nil {
		return fmt.Errorf("create raw transaction: %w", err)
	}

	wp.logger.Debugf("Created transaction %s with actual fee %d", tx.TxHash().String(), actualFee)

	sendOrder, vins, vouts, err := wallet.CreateSendOrder(
		tx,
		wallet.ORDER_TYPE_WITHDRAWAL,
		selectedUTXOs,
		[]*wallet.WithdrawalAdapter{withdrawalAdapter},
		wp.changeAddr,
		wp.conn.GetDB(),
	)
	if err != nil {
		return fmt.Errorf("create send order: %w", err)
	}

	wp.logger.Infof("Created send order %s for withdrawal %s", sendOrder.OrderId, w.ReqTaskId)

	initialized := &models.Withdrawal{
		ReqTaskId:   w.ReqTaskId,
		ReqTxHash:   w.ReqTxHash,
		ReqBlock:    w.ReqBlock,
		ReqLogIndex: w.ReqLogIndex,
		Status:      models.WITHDRAW_STATUS_INIT,
		DestAddress: w.DestAddress,
		DestAmount:  w.DestAmount,
		TxId:        w.TxId,
		ExternalId:  w.ExternalId,
		Vout:        w.Vout,
		TxBytes:     w.TxBytes,
		UnsignedTx:  w.UnsignedTx,
	}
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, initialized); err != nil {
		return fmt.Errorf("update withdrawal %s to init: %w", w.ReqTaskId, err)
	}
	wp.broadcastWithdrawalStatus(initialized)

	if wp.fireblocksMode {
		return wp.handleFireblocksSigning(w, tx, selectedUTXOs, sendOrder, vins, vouts)
	}

	return wp.handleLocalSigning(w, tx, sendOrder, vins, vouts)
}

func (wp *WithdrawalProcessor) handleLocalSigning(w *models.Withdrawal, tx *wire.MsgTx, sendOrder *models.SendOrder, vins []*models.VIN, vouts []*models.VOUT) error {
	var buf bytes.Buffer
	if err := tx.Serialize(&buf); err != nil {
		return fmt.Errorf("serialize transaction: %w", err)
	}
	unsignedHex := hex.EncodeToString(buf.Bytes())

	signedHex, err := wp.dogeClient.SignRawTransaction(unsignedHex)
	if err != nil {
		return fmt.Errorf("sign raw transaction: %w", err)
	}

	txid, err := wp.dogeClient.SendRawTransaction(signedHex)
	if err != nil {
		if errors.Is(err, doge.ErrEmptyRPCResponse) {
			// Empty response from RPC - compute txid from signed tx and proceed
			wp.logger.Warn("Empty RPC response for local signing, computing txid from signed tx")
			txid = tx.TxHash().String()
		} else {
			return fmt.Errorf("send raw transaction: %w", err)
		}
	}

	if err := wp.conn.GetDB().Transaction(func(db *gorm.DB) error {
		if err := wallet.UpdateSendOrderPending(txid, "", db, vins, vouts); err != nil {
			return err
		}

		// Mark UTXOs as pending (not spent yet - will be marked as spent by blockchain confirmation in doge/module.go)
		for _, vin := range vins {
			if err := db.Model(&models.UTXO{}).
				Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).
				Update("status", models.UTXO_STATUS_PENDING).Error; err != nil {
				return fmt.Errorf("update utxo status to pending: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("update send order and utxo status: %w", err)
	}

	// Broadcast UTXO status updates to other nodes (after transaction commits)
	for _, vin := range vins {
		wp.broadcastUTXOStatusUpdate(vin.Txid, vin.OutIndex, models.UTXO_STATUS_PROCESSED, models.UTXO_STATUS_PENDING, "selected_for_withdrawal")
	}

	updated := &models.Withdrawal{
		ReqTaskId:   w.ReqTaskId,
		ReqTxHash:   w.ReqTxHash,
		ReqBlock:    w.ReqBlock,
		ReqLogIndex: w.ReqLogIndex,
		Status:      models.WITHDRAW_STATUS_PENDING,
		DestAddress: w.DestAddress,
		DestAmount:  w.DestAmount,
		TxId:        txid,
		TxBytes:     buf.Bytes(),
	}

	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
		return fmt.Errorf("update withdrawal after broadcast: %w", err)
	}
	wp.broadcastWithdrawalStatus(updated)

	voutIndex := wp.findVoutIndex(tx, w.DestAddress)
	updated.Vout = voutIndex
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
		wp.logger.Warnf("Failed to update withdrawal vout index: %v", err)
	} else {
		wp.broadcastWithdrawalStatus(updated)
	}

	wp.logger.Infof("Broadcasted Dogecoin withdrawal tx %s for task %s (amount=%s)", txid, w.ReqTaskId, w.DestAmount)
	return nil
}

func (wp *WithdrawalProcessor) handleFireblocksSigning(w *models.Withdrawal, tx *wire.MsgTx, selectedUTXOs []*wallet.UTXOAdapter, sendOrder *models.SendOrder, vins []*models.VIN, vouts []*models.VOUT) error {
	dogeNet := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)

	rawMessages, err := wallet.GenerateRawMessageToFireblocks(tx, selectedUTXOs, dogeNet)
	if err != nil {
		return fmt.Errorf("generate raw message to fireblocks: %w", err)
	}

	messages := make([]string, len(rawMessages))
	for i, msg := range rawMessages {
		messages[i] = hex.EncodeToString(msg)
	}

	// Use simple note format for cosigner callback validation
	// Format: "withdrawal:txHash" - cosigner will look up send_order by txHash
	externalId, err := wp.fireblocksClient.postRawSigningRequest(messages, "withdrawal:"+sendOrder.Txid)
	if err != nil {
		return fmt.Errorf("fireblocks signing request: %w", err)
	}

	if err := wp.conn.GetDB().Transaction(func(db *gorm.DB) error {
		if err := wallet.UpdateSendOrderPending(tx.TxHash().String(), externalId, db, vins, vouts); err != nil {
			return err
		}

		// Mark UTXOs as pending (not spent yet - will be marked as spent after broadcast to Dogecoin network)
		for _, vin := range vins {
			if err := db.Model(&models.UTXO{}).
				Where("txid = ? AND out_index = ?", vin.Txid, vin.OutIndex).
				Update("status", models.UTXO_STATUS_PENDING).Error; err != nil {
				return fmt.Errorf("update utxo status to pending: %w", err)
			}
		}

		return nil
	}); err != nil {
		return fmt.Errorf("update send order and utxo status: %w", err)
	}

	// Broadcast UTXO status updates to other nodes (after transaction commits)
	for _, vin := range vins {
		wp.broadcastUTXOStatusUpdate(vin.Txid, vin.OutIndex, models.UTXO_STATUS_PROCESSED, models.UTXO_STATUS_PENDING, "selected_for_withdrawal")
	}

	var buf bytes.Buffer
	if err := tx.SerializeNoWitness(&buf); err != nil {
		return fmt.Errorf("serialize transaction without witness: %w", err)
	}

	updated := &models.Withdrawal{
		ReqTaskId:   w.ReqTaskId,
		ReqTxHash:   w.ReqTxHash,
		ReqBlock:    w.ReqBlock,
		ReqLogIndex: w.ReqLogIndex,
		Status:      models.WITHDRAW_STATUS_PENDING,
		DestAddress: w.DestAddress,
		DestAmount:  w.DestAmount,
		ExternalId:  externalId,
		UnsignedTx:  buf.Bytes(),
	}

	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
		return fmt.Errorf("update withdrawal after fireblocks request: %w", err)
	}
	wp.broadcastWithdrawalStatus(updated)

	// Broadcast send_order to other cosigner nodes so they can validate the Fireblocks callback
	// Update sendOrder with externalId before broadcasting
	sendOrder.ExternalId = externalId
	sendOrder.Status = "pending"
	wp.broadcastSendOrder(sendOrder, vins, vouts)

	wp.logger.Infof("Submitted Fireblocks signing request %s for withdrawal %s", externalId, w.ReqTaskId)
	return nil
}

func (wp *WithdrawalProcessor) findVoutIndex(tx *wire.MsgTx, destAddress string) int {
	network := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)
	for index, output := range tx.TxOut {
		receiver, err := types.ExtractAddressFromScript(output.PkScript, network)
		if err == nil && receiver == destAddress {
			return index
		}
	}
	return -1
}

func (wp *WithdrawalProcessor) finishFireblocksWithdrawal(w *models.Withdrawal) error {
	if wp.fireblocksClient == nil {
		return fmt.Errorf("fireblocks client not initialized")
	}
	if w.ExternalId == "" {
		return fmt.Errorf("withdrawal %s missing fireblocks external id", w.ReqTaskId)
	}

	details, err := wp.fireblocksClient.queryTransaction(w.ExternalId)
	if err != nil {
		return fmt.Errorf("query fireblocks transaction: %w", err)
	}

	wp.logger.Infof("Fireblocks tx %s status=%s subStatus=%s txHash=%s", w.ExternalId, details.Status, details.SubStatus, details.TxHash)
	status := strings.ToUpper(details.Status)
	if status == "BLOCKED" && strings.ToUpper(details.SubStatus) == "BLOCKED_BY_POLICY" {
		wp.logger.Warnf("Fireblocks tx %s blocked by policy for withdrawal %s (status=%s, subStatus=%s)", w.ExternalId, w.ReqTaskId, details.Status, details.SubStatus)
		return nil
	}
	if status == "COMPLETED" {
		if len(w.UnsignedTx) == 0 {
			return fmt.Errorf("withdrawal %s missing unsigned tx data", w.ReqTaskId)
		}

		if len(details.SignedMessages) == 0 {
			return fmt.Errorf("fireblocks returned no signatures for withdrawal %s", w.ReqTaskId)
		}

		tx, err := wallet.DeserializeTransactionFromBytes(w.UnsignedTx)
		if err != nil {
			// Reset UTXOs on permanent failure - can't deserialize means bad data
			wp.resetPendingUTXOsForWithdrawal(w)
			return fmt.Errorf("deserialize unsigned tx: %w", err)
		}

		dogeNet := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)

		utxoAdapters := make([]*wallet.UTXOAdapter, 0, len(tx.TxIn))
		for _, txIn := range tx.TxIn {
			var utxo models.UTXO
			if err := wp.conn.GetDB().Where("txid = ? AND out_index = ?", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index).First(&utxo).Error; err != nil {
				return fmt.Errorf("load utxo %s:%d: %w", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, err)
			}
			utxoAdapters = append(utxoAdapters, wallet.ToUTXOAdapter(&utxo))
		}

		if err := wallet.ApplyFireblocksSignaturesToTx(tx, utxoAdapters, details.SignedMessages, dogeNet); err != nil {
			// Reset UTXOs on signature application failure
			wp.resetPendingUTXOsForWithdrawal(w)
			return fmt.Errorf("apply fireblocks signatures: %w", err)
		}

		signedHex, signedBytes, err := serializeTx(tx)
		if err != nil {
			// Reset UTXOs on serialization failure
			wp.resetPendingUTXOsForWithdrawal(w)
			return fmt.Errorf("serialize signed tx: %w", err)
		}

		txid, err := wp.dogeClient.SendRawTransaction(signedHex)
		if err != nil {
			// If transaction is already in chain, compute txid from signed tx and proceed
			if errors.Is(err, doge.ErrTxAlreadyInChain) {
				wp.logger.Infof("Fireblocks withdrawal tx already in chain for %s, computing txid from signed tx", w.ReqTaskId)
				// Compute txid from signed transaction
				txid = tx.TxHash().String()
			} else if errors.Is(err, doge.ErrEmptyRPCResponse) {
				// Empty response from RPC (transient API issue)
				// Transaction was likely submitted, compute txid and proceed
				wp.logger.Warnf("Empty RPC response for withdrawal %s, computing txid from signed tx and proceeding", w.ReqTaskId)
				txid = tx.TxHash().String()
			} else {
				// Reset UTXOs from pending back to processed on broadcast failure
				for _, txIn := range tx.TxIn {
					utxoTxid := txIn.PreviousOutPoint.Hash.String()
					utxoOutIndex := int(txIn.PreviousOutPoint.Index)
					result := wp.conn.GetDB().Model(&models.UTXO{}).
						Where("txid = ? AND out_index = ? AND status = ?", utxoTxid, utxoOutIndex, models.UTXO_STATUS_PENDING).
						Update("status", models.UTXO_STATUS_PROCESSED)
					if result.Error != nil {
						wp.logger.Warnf("Failed to reset UTXO %s:%d to processed: %v", utxoTxid, utxoOutIndex, result.Error)
					} else if result.RowsAffected > 0 {
						// Broadcast UTXO status reset to other nodes
						wp.broadcastUTXOStatusUpdate(utxoTxid, utxoOutIndex, models.UTXO_STATUS_PENDING, models.UTXO_STATUS_PROCESSED, "broadcast_failed")
					}
				}
				return fmt.Errorf("send raw tx: %w", err)
			}
		}

		// Note: UTXOs remain in PENDING status until blockchain confirms the transaction
		// The doge/module.go handleSpendingTransaction() will mark them as SPENT when confirmed

		// Update send_order.txid from unsigned hash to signed hash
		// This is critical for blockchain scanner to correlate transactions
		var oldTxid string
		var sendOrder models.SendOrder
		if err := wp.conn.GetDB().Where("external_id = ?", w.ExternalId).First(&sendOrder).Error; err == nil {
			oldTxid = sendOrder.Txid
			if oldTxid != txid {
				if updateErr := wp.conn.GetDB().Model(&models.SendOrder{}).
					Where("external_id = ?", w.ExternalId).
					Updates(map[string]interface{}{
						"txid":       txid,
						"updated_at": time.Now(),
					}).Error; updateErr != nil {
					wp.logger.Warnf("Failed to update send_order txid from %s to %s: %v", oldTxid, txid, updateErr)
				} else {
					wp.logger.Infof("Updated send_order txid from %s (unsigned) to %s (signed)", oldTxid, txid)
					// Broadcast txid update to other nodes
					wp.broadcastSendOrderTxidUpdate(w.ExternalId, oldTxid, txid)
				}
			}
		}

		// Record change UTXO for future withdrawals (required when scan.enabled=false)
		for idx, output := range tx.TxOut {
			receiver, extractErr := types.ExtractAddressFromScript(output.PkScript, dogeNet)
			if extractErr != nil {
				continue
			}
			// If this is the change output (back to our change address)
			if receiver == wp.changeAddr {
				changeUid := fmt.Sprintf("%s:%d", txid, idx)
				changeUtxo := &models.UTXO{
					Uid:      changeUid,
					Txid:     txid,
					PkScript: output.PkScript,
					OutIndex: idx,
					Amount:   output.Value,
					Receiver: receiver,
					Source:   models.UTXO_SOURCE_WITHDRAWAL,
					Status:   models.UTXO_STATUS_PROCESSED,
				}
				if createErr := wp.conn.GetDB().Create(changeUtxo).Error; createErr != nil {
					wp.logger.Warnf("Failed to record change UTXO %s: %v", changeUid, createErr)
				} else {
					wp.logger.Infof("Recorded change UTXO %s with amount %d for withdrawal %s", changeUid, output.Value, w.ReqTaskId)
				}
				break // Only one change output expected
			}
		}

		voutIndex := wp.findVoutIndex(tx, w.DestAddress)
		updated := &models.Withdrawal{
			ReqTaskId:   w.ReqTaskId,
			ReqTxHash:   w.ReqTxHash,
			ReqBlock:    w.ReqBlock,
			ReqLogIndex: w.ReqLogIndex,
			Status:      models.WITHDRAW_STATUS_PENDING,
			DestAddress: w.DestAddress,
			DestAmount:  w.DestAmount,
			TxId:        txid,
			Vout:        voutIndex,
			TxBytes:     signedBytes,
			ExternalId:  w.ExternalId,
			UnsignedTx:  w.UnsignedTx,
		}
		if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
			return fmt.Errorf("update withdrawal after broadcast: %w", err)
		}
		wp.broadcastWithdrawalStatus(updated)
		wp.logger.Infof("Broadcasted Fireblocks-signed tx %s for withdrawal %s", txid, w.ReqTaskId)
		return nil
	}

	if isFireblocksFailureStatus(status) {
		wp.logger.Warnf("Fireblocks tx %s failed for withdrawal %s (status=%s, subStatus=%s)", w.ExternalId, w.ReqTaskId, details.Status, details.SubStatus)

		// Reset UTXOs from pending back to processed before resetting withdrawal
		wp.resetPendingUTXOsForWithdrawal(w)

		updated := &models.Withdrawal{
			ReqTaskId:   w.ReqTaskId,
			ReqTxHash:   w.ReqTxHash,
			ReqBlock:    w.ReqBlock,
			ReqLogIndex: w.ReqLogIndex,
			Status:      models.WITHDRAW_STATUS_INIT,
			DestAddress: w.DestAddress,
			DestAmount:  w.DestAmount,
			ExternalId:  "",
			UnsignedTx:  nil,
			TxId:        "",
			TxBytes:     nil,
		}
		if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
			wp.logger.Warnf("Failed to reset withdrawal %s status to init: %v", w.ReqTaskId, err)
		} else {
			wp.broadcastWithdrawalStatus(updated)
		}
		return nil
	}

	return nil
}

// resetPendingUTXOsForWithdrawal resets UTXOs associated with a withdrawal from pending back to processed
func (wp *WithdrawalProcessor) resetPendingUTXOsForWithdrawal(w *models.Withdrawal) {
	if len(w.UnsignedTx) == 0 {
		return
	}
	tx, err := wallet.DeserializeTransactionFromBytes(w.UnsignedTx)
	if err != nil {
		wp.logger.Warnf("Failed to deserialize unsigned tx for UTXO reset: %v", err)
		return
	}
	var resetUTXOs []struct {
		txid     string
		outIndex int
	}
	for _, txIn := range tx.TxIn {
		txid := txIn.PreviousOutPoint.Hash.String()
		outIndex := int(txIn.PreviousOutPoint.Index)
		result := wp.conn.GetDB().Model(&models.UTXO{}).
			Where("txid = ? AND out_index = ? AND status = ?", txid, outIndex, models.UTXO_STATUS_PENDING).
			Update("status", models.UTXO_STATUS_PROCESSED)
		if result.Error != nil {
			wp.logger.Warnf("Failed to reset UTXO %s:%d to processed: %v", txid, outIndex, result.Error)
		} else if result.RowsAffected > 0 {
			resetUTXOs = append(resetUTXOs, struct {
				txid     string
				outIndex int
			}{txid, outIndex})
		}
	}
	// Broadcast UTXO status updates for successfully reset UTXOs
	for _, utxo := range resetUTXOs {
		wp.broadcastUTXOStatusUpdate(utxo.txid, utxo.outIndex, models.UTXO_STATUS_PENDING, models.UTXO_STATUS_PROCESSED, "withdrawal_failed")
	}
	wp.logger.Infof("Reset %d pending UTXOs for withdrawal %s", len(resetUTXOs), w.ReqTaskId)
}

func (wp *WithdrawalProcessor) checkAndSubmit(w *models.Withdrawal) error {
	if w.TxId == "" {
		return fmt.Errorf("withdrawal %s missing txid", w.ReqTaskId)
	}

	wp.logger.Infof("checkAndSubmit: querying tx %s for withdrawal %s", w.TxId, w.ReqTaskId)
	rawHex, confs, err := wp.dogeClient.GetRawTransactionHex(w.TxId)
	if err != nil {
		wp.logger.Warnf("checkAndSubmit: GetRawTransactionHex error for tx %s: %v", w.TxId, err)
		// If transaction not found, try to re-broadcast it
		if errors.Is(err, doge.ErrTxNotFound) {
			wp.logger.Warnf("Withdrawal tx %s not found on network, attempting re-broadcast", w.TxId)
			return wp.rebroadcastWithdrawal(w)
		}
		return fmt.Errorf("query tx %s: %w", w.TxId, err)
	}
	wp.logger.Infof("checkAndSubmit: tx %s has %d confirmations (required: %d)", w.TxId, confs, wp.requiredConfs)
	if confs < wp.requiredConfs {
		return nil
	}

	txBytes := w.TxBytes
	if len(txBytes) == 0 && rawHex != "" {
		if decoded, err := hex.DecodeString(rawHex); err == nil {
			txBytes = decoded
		}
	}
	if len(txBytes) == 0 {
		return fmt.Errorf("missing raw tx bytes for %s", w.TxId)
	}

	amountSat, err := parseAmountString(w.DestAmount)
	if err != nil {
		return fmt.Errorf("invalid dest amount for %s: %w", w.ReqTaskId, err)
	}
	taskId, ok := new(big.Int).SetString(w.ReqTaskId, 10)
	if !ok {
		return fmt.Errorf("invalid task id %s", w.ReqTaskId)
	}

	req := &withdrawalRequest{
		TxBytes:     txBytes,
		TxId:        w.TxId,
		TotalAmount: amountSat,
		TaskIds:     []*big.Int{taskId},
	}

	if err := wp.utxoProcessor.SubmitWithdrawalRequest(req); err != nil {
		return fmt.Errorf("submit withdrawal request: %w", err)
	}

	updated := &models.Withdrawal{
		ReqTaskId:      w.ReqTaskId,
		ReqTxHash:      w.ReqTxHash,
		ReqBlock:       w.ReqBlock,
		ReqLogIndex:    w.ReqLogIndex,
		Status:         models.WITHDRAW_STATUS_CONFIRMED,
		DestAddress:    w.DestAddress,
		DestAmount:     w.DestAmount,
		TxId:           w.TxId,
		ExternalId:     w.ExternalId,
		Vout:           w.Vout,
		TxBytes:        w.TxBytes,
		UnsignedTx:     w.UnsignedTx,
		FinishTxHash:   w.FinishTxHash,
		FinishBlock:    w.FinishBlock,
		FinishLogIndex: w.FinishLogIndex,
	}
	if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
		wp.logger.Warnf("Failed to update withdrawal %s status to confirmed: %v", w.ReqTaskId, err)
	} else {
		wp.broadcastWithdrawalStatus(updated)
	}
	return nil
}

// rebroadcastWithdrawal attempts to re-broadcast a withdrawal transaction that is not found on the network.
// It uses the saved TxBytes (signed transaction) if available.
func (wp *WithdrawalProcessor) rebroadcastWithdrawal(w *models.Withdrawal) error {
	// First check if we have the signed transaction bytes
	if len(w.TxBytes) > 0 {
		signedHex := hex.EncodeToString(w.TxBytes)
		txid, err := wp.dogeClient.SendRawTransaction(signedHex)
		if err != nil {
			if errors.Is(err, doge.ErrTxAlreadyInChain) {
				// Transaction is already confirmed, query confirmations
				wp.logger.Infof("Withdrawal tx %s already in chain, checking confirmations", w.TxId)
				return nil // Will be picked up on next poll
			}
			if errors.Is(err, doge.ErrEmptyRPCResponse) {
				// Empty response from RPC - transaction may have been submitted
				wp.logger.Warnf("Empty RPC response for re-broadcast withdrawal %s, assuming submitted", w.ReqTaskId)
				return nil // Will be picked up on next poll
			}
			wp.logger.Warnf("Re-broadcast of signed tx for withdrawal %s failed: %v", w.ReqTaskId, err)
			// Don't return error, try Fireblocks re-signing below
		} else {
			wp.logger.Infof("Re-broadcasted signed tx %s for withdrawal %s", txid, w.ReqTaskId)
			return nil
		}
	}

	// If we have Fireblocks external ID but no valid signed tx, query Fireblocks and re-apply signatures
	if wp.fireblocksClient != nil && w.ExternalId != "" && len(w.UnsignedTx) > 0 {
		wp.logger.Infof("Re-querying Fireblocks for withdrawal %s (externalId=%s)", w.ReqTaskId, w.ExternalId)
		details, err := wp.fireblocksClient.queryTransaction(w.ExternalId)
		if err != nil {
			return fmt.Errorf("re-query fireblocks: %w", err)
		}

		if strings.ToUpper(details.Status) != "COMPLETED" {
			wp.logger.Warnf("Fireblocks tx %s not completed (status=%s), cannot re-broadcast", w.ExternalId, details.Status)
			return nil
		}

		if len(details.SignedMessages) == 0 {
			return fmt.Errorf("fireblocks returned no signatures for withdrawal %s", w.ReqTaskId)
		}

		// Deserialize unsigned tx and re-apply signatures
		tx, err := wallet.DeserializeTransactionFromBytes(w.UnsignedTx)
		if err != nil {
			return fmt.Errorf("deserialize unsigned tx: %w", err)
		}

		dogeNet := types.GetDogeNetwork(global.GetConfig().Doge.NetworkType)

		// Load UTXOs for signature application
		utxoAdapters := make([]*wallet.UTXOAdapter, 0, len(tx.TxIn))
		for _, txIn := range tx.TxIn {
			var utxo models.UTXO
			if err := wp.conn.GetDB().Where("txid = ? AND out_index = ?", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index).First(&utxo).Error; err != nil {
				return fmt.Errorf("load utxo %s:%d: %w", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, err)
			}
			utxoAdapters = append(utxoAdapters, wallet.ToUTXOAdapter(&utxo))
		}

		if err := wallet.ApplyFireblocksSignaturesToTx(tx, utxoAdapters, details.SignedMessages, dogeNet); err != nil {
			return fmt.Errorf("apply fireblocks signatures: %w", err)
		}

		signedHex, signedBytes, err := serializeTx(tx)
		if err != nil {
			return fmt.Errorf("serialize signed tx: %w", err)
		}

		txid, err := wp.dogeClient.SendRawTransaction(signedHex)
		if err != nil {
			if errors.Is(err, doge.ErrTxAlreadyInChain) {
				wp.logger.Infof("Re-signed withdrawal tx already in chain for withdrawal %s", w.ReqTaskId)
				return nil
			}
			if errors.Is(err, doge.ErrEmptyRPCResponse) {
				// Empty response from RPC - compute txid from signed tx and proceed
				wp.logger.Warnf("Empty RPC response for re-signed withdrawal %s, computing txid and proceeding", w.ReqTaskId)
				txid = tx.TxHash().String()
			} else {
				return fmt.Errorf("send re-signed tx: %w", err)
			}
		}

		// Update withdrawal with new txid and signed bytes
		updated := &models.Withdrawal{
			ReqTaskId:   w.ReqTaskId,
			ReqTxHash:   w.ReqTxHash,
			ReqBlock:    w.ReqBlock,
			ReqLogIndex: w.ReqLogIndex,
			Status:      models.WITHDRAW_STATUS_PENDING,
			DestAddress: w.DestAddress,
			DestAmount:  w.DestAmount,
			TxId:        txid,
			Vout:        w.Vout,
			TxBytes:     signedBytes,
			ExternalId:  w.ExternalId,
			UnsignedTx:  w.UnsignedTx,
		}
		if err := wp.eventRepo.CreateOrUpdateWithdrawal(nil, updated); err != nil {
			wp.logger.Warnf("Failed to update withdrawal %s after re-broadcast: %v", w.ReqTaskId, err)
		} else {
			wp.broadcastWithdrawalStatus(updated)
		}
		wp.logger.Infof("Re-broadcasted Fireblocks-signed tx %s for withdrawal %s", txid, w.ReqTaskId)
		return nil
	}

	return fmt.Errorf("cannot re-broadcast withdrawal %s: no signed tx and no fireblocks data", w.ReqTaskId)
}

func isFireblocksFailureStatus(status string) bool {
	switch status {
	case "CANCELLING", "CANCELLED", "BLOCKED", "REJECTED", "FAILED":
		return true
	default:
		return false
	}
}

func serializeTx(tx *wire.MsgTx) (string, []byte, error) {
	var buffer bytes.Buffer
	if err := tx.Serialize(&buffer); err != nil {
		return "", nil, err
	}
	encoded := hex.EncodeToString(buffer.Bytes())
	return encoded, buffer.Bytes(), nil
}

func parseAmountString(val string) (*big.Int, error) {
	amt := new(big.Int)
	if val == "" {
		return amt, fmt.Errorf("empty amount")
	}
	if _, ok := amt.SetString(val, 10); !ok {
		return amt, fmt.Errorf("parse amount string %s failed", val)
	}
	return amt, nil
}
