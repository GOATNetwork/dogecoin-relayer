package tss

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/pkg/types"

	tsstypes "github.com/goatnetwork/tss/pkg/types"
	log "github.com/sirupsen/logrus"
)

const (
	PostAddressUrl   = "/api/v1/common/address"
	PostSignStartUrl = "/api/v1/common/sign/start"
	GetSignStatusUrl = "/api/v1/common/sign/status/%s" // %s is the session id

	TssCurve    = "secp256k1"
	TssCoinType = 60 // eth
)

type SignClient struct {
	cfg    config.TssConfig
	logger *log.Entry

	httpClient *http.Client
}

func NewSignClient(cfg config.TssConfig) *SignClient {
	return &SignClient{
		cfg:    cfg,
		logger: types.InitLogEntry("tss-sign-client"),

		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *SignClient) getUrl(uri string) string {
	if c.cfg.Url == "" {
		return ""
	}

	// Ensure URI starts with /
	if !strings.HasPrefix(uri, "/") {
		uri = "/" + uri
	}

	// Remove trailing slash from base URL to avoid double slashes
	baseUrl := strings.TrimSuffix(c.cfg.Url, "/")

	return baseUrl + uri
}

func (c *SignClient) StartSign(ctx context.Context, sessionID string, unsignHash []byte) (*tsstypes.SignStartResponse, error) {
	req := &tsstypes.SignStartRequest{
		Hash:      unsignHash,
		SessionID: sessionID,
		Curve:     TssCurve,
		CoinType:  TssCoinType,
		Account:   0,
		Index:     0,
		SkipPath:  false,
		KDD:       c.cfg.Kdd,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := c.getUrl(PostSignStartUrl)
	if url == "" {
		return nil, fmt.Errorf("tss url is not set")
	}

	c.logger.Infof("TSS request: URL=%s, SessionID=%s, Hash=%x, HashLength=%d", url, sessionID, unsignHash, len(unsignHash))
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	c.logger.Infof("TSS response: Status=%d", resp.StatusCode)

	// Read response body for debugging
	var responseBody bytes.Buffer
	responseBody.ReadFrom(resp.Body)
	responseBodyStr := responseBody.String()
	c.logger.Infof("TSS response body: %s", responseBodyStr)

	var signStartResponse tsstypes.SignStartResponse
	if err := json.Unmarshal(responseBody.Bytes(), &signStartResponse); err != nil {
		return nil, fmt.Errorf("failed to decode sign start response (body: %s): %w", responseBodyStr, err)
	}

	return &signStartResponse, nil
}

// GetSignStatus get sign status from tss server
// SignStatusResponse.Signature is the raw signature for the aim chain, such as evm: [65]byte 0-31 r, 32-63 s, 64 v
func (c *SignClient) GetSignStatus(ctx context.Context, sessionID string) (*tsstypes.SignStatusResponse, error) {
	url := c.getUrl(fmt.Sprintf(GetSignStatusUrl, sessionID))
	if url == "" {
		return nil, fmt.Errorf("tss url is not set")
	}
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	var signStatusResponse tsstypes.SignStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&signStatusResponse); err != nil {
		return nil, fmt.Errorf("failed to decode sign status response: %w", err)
	}

	return &signStatusResponse, nil
}

func (c *SignClient) GetEvmAddress(ctx context.Context, sessionID string) (common.Address, error) {
	req := &tsstypes.AddressRequest{
		Curve:     TssCurve,
		CoinType:  TssCoinType,
		Account:   0,
		Index:     0,
		ChainType: "evm",
		SkipPath:  false,
		KDD:       c.cfg.Kdd,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return common.Address{}, err
	}

	url := c.getUrl(PostAddressUrl)
	if url == "" {
		return common.Address{}, fmt.Errorf("tss url is not set")
	}

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return common.Address{}, fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	var addressResponse tsstypes.AddressResponse
	if err := json.NewDecoder(resp.Body).Decode(&addressResponse); err != nil {
		return common.Address{}, fmt.Errorf("failed to decode address response: %w", err)
	}

	// Log the returned KDD and XY for diagnostics, if provided
	if addressResponse.KDD != nil {
		c.logger.Infof("TSS address (derived) response: kdd=%s addr=%s", addressResponse.KDD.String(), addressResponse.Address)
	} else {
		c.logger.Infof("TSS address (derived) response: addr=%s", addressResponse.Address)
	}

	return common.HexToAddress(addressResponse.Address), nil
}
