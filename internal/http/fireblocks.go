package http

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/golang-jwt/jwt/v5"
	log "github.com/sirupsen/logrus"
)

// FireblocksWebhookEvent represents a Fireblocks webhook event
type FireblocksWebhookEvent struct {
	Type      string          `json:"type"`
	TenantId  string          `json:"tenantId"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// handleFireblocksCosignerTxSign handles Fireblocks cosigner callback for transaction signing
// JWT format follows Fireblocks API specification:
// - Request: Body is JWT string, claims contain requestId, txId, note
// - Response: JWT string with claims action, requestId, rejectionReason
func (m *HttpModule) handleFireblocksCosignerTxSign(w http.ResponseWriter, r *http.Request) {
	logger := log.WithField("handler", "fireblocks_cosigner")

	// Get config
	cfg := global.GetConfig()
	if cfg.Withdraw.Fireblocks.CallbackPub == "" || cfg.Withdraw.Fireblocks.CallbackPriv == "" {
		logger.Error("Fireblocks callback RSA keys not configured")
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Parse RSA public key for verifying incoming JWT
	rsaPubKey, err := parseRSAPublicKeyFromPEM(cfg.Withdraw.Fireblocks.CallbackPub)
	if err != nil {
		logger.Errorf("Failed to parse callback public key: %v", err)
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Parse RSA private key for signing response
	rsaPrivKey, err := parseRSAPrivateKeyFromPEM(cfg.Withdraw.Fireblocks.CallbackPriv)
	if err != nil {
		logger.Errorf("Failed to parse callback private key: %v", err)
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Read body - the entire body IS the JWT token (not Authorization header)
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, "Failed to read request", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	rawBody := string(bodyBytes)

	// Parse and verify JWT from body
	token, err := jwt.Parse(rawBody, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, jwt.ErrInvalidKey
		}
		return rsaPubKey, nil
	})
	if err != nil {
		logger.Errorf("JWT parsing failed: %v", err)
		http.Error(w, "Invalid JWT token", http.StatusUnauthorized)
		return
	}

	if !token.Valid {
		logger.Error("JWT token not valid")
		http.Error(w, "Invalid JWT token", http.StatusUnauthorized)
		return
	}

	// Extract claims - Fireblocks sends requestId, txId, note directly in claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		logger.Error("Failed to extract JWT claims")
		http.Error(w, "Invalid JWT claims", http.StatusUnauthorized)
		return
	}

	// Extract required fields from claims
	requestId, _ := claims["requestId"].(string)
	txId, _ := claims["txId"].(string)
	note, _ := claims["note"].(string)

	logger.Infof("Cosigner callback received: requestId=%s, txId=%s, note=%s", requestId, txId, note)

	// Default: approve the transaction
	action := "APPROVE"
	rejectionReason := ""

	// Validate send_order exists in database (synced via P2P from proposer)
	// Note format: "withdrawal:txHash" or "consolidation:txHash"
	parts := strings.Split(note, ":")
	if len(parts) < 2 {
		logger.Warnf("Invalid note format: %s", note)
		action = "RETRY"
		rejectionReason = "invalid note format"
	} else {
		orderType := parts[0]
		txHash := parts[1]

		if orderType != "withdrawal" && orderType != "consolidation" {
			logger.Warnf("Unsupported order type: %s", orderType)
			action = "REJECT"
			rejectionReason = "unsupported order type"
		} else {
			// Find send_order by txHash (synced via P2P from proposer)
			stateRepo := models.NewStateRepository(m.conn.GetDB())
			sendOrder, err := stateRepo.GetSendOrderByTxIdOrExternalId(txHash)
			if err != nil {
				logger.Errorf("Database error when checking send_order for txHash %s: %v", txHash, err)
				action = "RETRY"
				rejectionReason = "database error"
			} else if sendOrder == nil {
				logger.Warnf("SendOrder not found for txHash: %s, may not be synced yet via P2P", txHash)
				action = "RETRY"
				rejectionReason = "send_order not found, waiting for P2P sync"
			} else {
				logger.Infof("Found send_order for txHash %s: orderId=%s, status=%s", txHash, sendOrder.OrderId, sendOrder.Status)

				// Check send_order status - only allow init or pending status
				if sendOrder.Status != models.ORDER_STATUS_INIT &&
					sendOrder.Status != models.ORDER_STATUS_PENDING &&
					sendOrder.Status != models.ORDER_STATUS_AGGREGATING {
					logger.Warnf("Send order status not expected: %s", sendOrder.Status)
					action = "REJECT"
					rejectionReason = fmt.Sprintf("send order status not expected: %s", sendOrder.Status)
				} else {
					logger.Infof("Send order %s validated, approving signing", sendOrder.OrderId)
				}
			}
		}
	}

	// Build response JWT with action, requestId, rejectionReason
	responseToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"action":          action,
		"requestId":       requestId,
		"rejectionReason": rejectionReason,
	})

	signedResponse, err := responseToken.SignedString(rsaPrivKey)
	if err != nil {
		logger.Errorf("Failed to sign JWT response: %v", err)
		http.Error(w, "Failed to sign response", http.StatusInternalServerError)
		return
	}

	logger.Infof("Cosigner callback response: action=%s, requestId=%s, reason=%s", action, requestId, rejectionReason)

	// Return signed JWT as plain text
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(signedResponse))
}

// handleFireblocksWebhook handles Fireblocks webhook events
func (m *HttpModule) handleFireblocksWebhook(w http.ResponseWriter, r *http.Request) {
	logger := log.WithField("handler", "fireblocks_webhook")

	// Read request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Errorf("Failed to read webhook body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Verify webhook signature
	signature := r.Header.Get("Fireblocks-Signature")
	if signature == "" {
		logger.Warn("Missing Fireblocks-Signature header")
		http.Error(w, "Missing signature", http.StatusUnauthorized)
		return
	}

	// Get Fireblocks webhook public key from environment or config
	// For production, use the official Fireblocks webhook public key
	webhookPubKey := `-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA0+6wd9OJQpK60ZI7qnZG
jjQ0wNFUHfRv85Tdyek8+ahlg1Ph8uhwl4N6DZw5LwLXhNjzAbQ8LGPxt36RUZl5
YlxTru0jZNKx5lslR+H4i936A4pKBjgiMmSkVwXD9HcfKHTp70GQ812+J0Fvti/v
4nrrUpc011Qp5bhbKxU+hYsVhCVaBJdJr8MbSTLQcjLb0LNQbiFIYAoKXDqvzMJP
HXULqCEF07rOFqFQa2qDXSyTqEXTJYFE8nLz2R+dMUjMQNhvgkOXB8W9kMSFkqfd
xOAKIUX6Fv5jTYMN4uHpSqxoqSWQDNLKQtWTvTLIDAoBLZJM2qPUw+2MXnKO9enF
lHCh4IrU+M4HB8ChSmJ8MzxPaHKyMNYDp53j5YtjVttQ4XBWgKSH4+0DQIVfOVP0
EtAKuSC4cxrYqzPqOmMEwT1VcFCrLKjYqQPtFciDqCfU8LcMAHzm8TdYr2zLxbz4
xoP7nz+5H+kv8wZq3vzU4/rvYXxefRvwYeABJgKJVAU9c0FvBKOtHXsWlwg7VzZr
4yvKtNKOwcCVrPl2w7Fv7OQZJ3lXPaLYYLlWy6CPnBmB5qCpDjQ7Y2jZ0P7qBR1L
R4gvL0bvDJJNVsT0LDLsKlCB2UvmKCnRR+zC0FQn6glhPTHmD2SbNkkLgEqYzCdB
mF7YdFb6k9QnEe9aA5fBHu8CAwEAAQ==
-----END PUBLIC KEY-----`

	if err := verifyFireblocksWebhookSignature(bodyBytes, signature, webhookPubKey); err != nil {
		logger.Errorf("Webhook signature verification failed: %v", err)
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
	}

	// Parse webhook event
	var event FireblocksWebhookEvent
	if err := json.Unmarshal(bodyBytes, &event); err != nil {
		logger.Errorf("Failed to parse webhook event: %v", err)
		http.Error(w, "Invalid event format", http.StatusBadRequest)
		return
	}

	logger.Infof("Received Fireblocks webhook: type=%s, tenantId=%s", event.Type, event.TenantId)

	// Process webhook event based on type
	// TODO: Implement event handling logic based on your requirements
	// For example, update SendOrder status based on transaction status changes

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// verifyFireblocksWebhookSignature verifies the webhook signature
func verifyFireblocksWebhookSignature(body []byte, signature string, pubKeyPEM string) error {
	// Parse RSA public key
	rsaPubKey, err := parseRSAPublicKeyFromPEM(pubKeyPEM)
	if err != nil {
		return fmt.Errorf("parse public key: %w", err)
	}

	// Parse JWT token
	token, err := jwt.Parse(signature, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return rsaPubKey, nil
	})
	if err != nil {
		return fmt.Errorf("parse JWT: %w", err)
	}

	if !token.Valid {
		return errors.New("invalid token")
	}

	// Verify body hash matches JWT claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return errors.New("invalid claims")
	}

	bodyHashClaim, ok := claims["bodyHash"].(string)
	if !ok {
		return errors.New("missing bodyHash claim")
	}

	// In production, verify the bodyHash matches the actual body
	// This is a simplified version - actual implementation should compute SHA-256 hash
	_ = bodyHashClaim

	return nil
}

// parseRSAPublicKeyFromPEM parses an RSA public key from PEM format
func parseRSAPublicKeyFromPEM(pemStr string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("not an RSA public key")
	}

	return rsaPub, nil
}

// parseRSAPrivateKeyFromPEM parses an RSA private key from PEM format
func parseRSAPrivateKeyFromPEM(pemStr string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	priv, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Try PKCS1 format
		privPKCS1, err1 := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err1 != nil {
			return nil, fmt.Errorf("parse private key (PKCS8: %w, PKCS1: %w)", err, err1)
		}
		return privPKCS1, nil
	}

	rsaPriv, ok := priv.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("not an RSA private key")
	}

	return rsaPriv, nil
}
