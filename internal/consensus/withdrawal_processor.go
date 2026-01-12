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
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
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
			if err := wp.broadcastWithdrawal(&list[i]); err != nil {
				wp.logger.Errorf("broadcast withdrawal %s failed: %v", list[i].ReqTaskId, err)
				wp.markWithdrawalRetry(&list[i])
			}
		}
	}
	return nil
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

	for i := range pending {
		w := &pending[i]
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
		if err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId); err != nil {
			return fmt.Errorf("close send orders for %s: %w", w.ReqTaskId, err)
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
		if err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId); err != nil {
			return fmt.Errorf("close send orders for %s: %w", w.ReqTaskId, err)
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
	if err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId); err != nil {
		wp.logger.Warnf("Failed to close send orders for %s: %v", w.ReqTaskId, err)
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
			err := p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeWithdrawalStatus, func(msg *types.P2PBroadcastMessage) error {
				return wp.handleWithdrawalStatusMessage(msg)
			})
			if err != nil {
				wp.logger.Errorf("Failed to register withdrawal status handler: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			return
		}
		wp.logger.Errorf("Failed to register withdrawal status handler after %d retries", maxRetries)
	}()
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

	if err := wallet.CloseSendOrdersForWithdrawal(wp.conn.GetDB(), w.ReqTaskId); err != nil {
		return fmt.Errorf("cleanup send orders for withdrawal %s: %w", w.ReqTaskId, err)
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

	networkFee := int64(global.GetConfig().Withdraw.FeeRate)
	if networkFee <= 0 {
		networkFee = 1
	}

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
		return fmt.Errorf("send raw transaction: %w", err)
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

	// Use SendOrder txid in note for cosigner callback validation
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
			// Reset UTXOs from pending back to processed on broadcast failure
			for _, txIn := range tx.TxIn {
				if resetErr := wp.conn.GetDB().Model(&models.UTXO{}).
					Where("txid = ? AND out_index = ? AND status = ?", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, models.UTXO_STATUS_PENDING).
					Update("status", models.UTXO_STATUS_PROCESSED).Error; resetErr != nil {
					wp.logger.Warnf("Failed to reset UTXO %s:%d to processed: %v", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, resetErr)
				}
			}
			return fmt.Errorf("send raw tx: %w", err)
		}

		// Note: UTXOs remain in PENDING status until blockchain confirms the transaction
		// The doge/module.go handleSpendingTransaction() will mark them as SPENT when confirmed

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
	for _, txIn := range tx.TxIn {
		if err := wp.conn.GetDB().Model(&models.UTXO{}).
			Where("txid = ? AND out_index = ? AND status = ?", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, models.UTXO_STATUS_PENDING).
			Update("status", models.UTXO_STATUS_PROCESSED).Error; err != nil {
			wp.logger.Warnf("Failed to reset UTXO %s:%d to processed: %v", txIn.PreviousOutPoint.Hash.String(), txIn.PreviousOutPoint.Index, err)
		}
	}
	wp.logger.Infof("Reset pending UTXOs for withdrawal %s", w.ReqTaskId)
}

func (wp *WithdrawalProcessor) checkAndSubmit(w *models.Withdrawal) error {
	if w.TxId == "" {
		return fmt.Errorf("withdrawal %s missing txid", w.ReqTaskId)
	}

	rawHex, confs, err := wp.dogeClient.GetRawTransactionHex(w.TxId)
	if err != nil {
		return fmt.Errorf("query tx %s: %w", w.TxId, err)
	}
	if confs < wp.requiredConfs {
		wp.logger.Debugf("Withdrawal tx %s confirmations %d/%d", w.TxId, confs, wp.requiredConfs)
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
