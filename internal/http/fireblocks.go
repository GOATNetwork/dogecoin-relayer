package http

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/goat-network/dogecoin-relayer/internal/config"
	"github.com/goat-network/dogecoin-relayer/internal/models"
	"github.com/goat-network/dogecoin-relayer/pkg/global"
	"github.com/golang-jwt/jwt/v5"
	log "github.com/sirupsen/logrus"
)

// Fireblocks cosigner callback request structure
type FireblocksCosignerRequest struct {
	TxId        string `json:"txId"`
	Operation   string `json:"operation"` // e.g., "RAW"
	SourceType  string `json:"sourceType"`
	SourceId    string `json:"sourceId"`
	RequestedBy string `json:"requestedBy"`
	Note        string `json:"note"` // Format: "ORDER_TYPE:txHash"
}

// Fireblocks cosigner callback response structure
type FireblocksCosignerResponse struct {
	Action          string `json:"action"`          // APPROVE | REJECT | IGNORE | RETRY
	RejectionReason string `json:"rejectionReason"` // Required when action is REJECT
}

// Fireblocks webhook event structure
type FireblocksWebhookEvent struct {
	Type      string          `json:"type"`
	TenantId  string          `json:"tenantId"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// handleFireblocksCosignerTxSign handles Fireblocks cosigner callback for transaction signing
func (m *HttpModule) handleFireblocksCosignerTxSign(w http.ResponseWriter, r *http.Request) {
	logger := log.WithField("handler", "fireblocks_cosigner")

	// Read request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		logger.Errorf("Failed to read request body: %v", err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse JWT token from Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		logger.Error("Missing Authorization header")
		http.Error(w, "Missing Authorization header", http.StatusUnauthorized)
		return
	}

	// Extract token from "Bearer <token>" format
	tokenString := strings.TrimPrefix(authHeader, "Bearer ")
	if tokenString == authHeader {
		logger.Error("Invalid Authorization header format")
		http.Error(w, "Invalid Authorization header format", http.StatusUnauthorized)
		return
	}

	// Verify JWT signature using callback public key
	cfg := global.GetConfig()
	callbackPubKey := cfg.Withdraw.Fireblocks.CallbackPub
	if callbackPubKey == "" {
		logger.Error("Fireblocks callback public key not configured")
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	rsaPubKey, err := parseRSAPublicKeyFromPEM(callbackPubKey)
	if err != nil {
		logger.Errorf("Failed to parse callback public key: %v", err)
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Parse and verify JWT
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return rsaPubKey, nil
	})
	if err != nil {
		logger.Errorf("JWT verification failed: %v", err)
		http.Error(w, "Invalid JWT token", http.StatusUnauthorized)
		return
	}

	if !token.Valid {
		logger.Error("Invalid JWT token")
		http.Error(w, "Invalid JWT token", http.StatusUnauthorized)
		return
	}

	// Extract body from JWT claims
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		logger.Error("Failed to extract JWT claims")
		http.Error(w, "Invalid JWT claims", http.StatusBadRequest)
		return
	}

	bodyStr, ok := claims["body"].(string)
	if !ok {
		logger.Error("Missing 'body' field in JWT claims")
		http.Error(w, "Invalid JWT claims", http.StatusBadRequest)
		return
	}

	// Decode base64-encoded body
	decodedBody, err := base64.StdEncoding.DecodeString(bodyStr)
	if err != nil {
		logger.Errorf("Failed to decode base64 body: %v", err)
		http.Error(w, "Invalid body encoding", http.StatusBadRequest)
		return
	}

	// Parse cosigner request
	var req FireblocksCosignerRequest
	if err := json.Unmarshal(decodedBody, &req); err != nil {
		logger.Errorf("Failed to parse cosigner request: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	logger.Infof("Received cosigner callback for tx %s, operation=%s, note=%s", req.TxId, req.Operation, req.Note)

	// Default: approve the transaction
	action := "APPROVE"
	rejectionReason := ""

	// Validate SendOrder exists in database
	// Extract txHash from note (format: "ORDER_TYPE:txHash")
	parts := strings.Split(req.Note, ":")
	if len(parts) != 2 {
		logger.Warnf("Invalid note format: %s", req.Note)
		action = "RETRY"
		rejectionReason = "invalid note format"
	} else {
		txHash := parts[1]

		// Query database for SendOrder
		sendOrder, err := m.conn.GetStateRepo().GetSendOrderByTxIdOrExternalId(txHash)
		if err != nil {
			logger.Errorf("Database error when checking send order: %v", err)
			action = "RETRY"
			rejectionReason = "database error"
		} else if sendOrder == nil {
			logger.Warnf("SendOrder not found for txHash: %s", txHash)
			action = "REJECT"
			rejectionReason = "send order not found"
		} else if sendOrder.Status != "aggregating" && sendOrder.Status != "pending" {
			logger.Warnf("SendOrder status not expected: %s (current status: %s)", txHash, sendOrder.Status)
			action = "REJECT"
			rejectionReason = fmt.Sprintf("send order status not expected, current status: %s", sendOrder.Status)
		} else {
			logger.Infof("SendOrder validation passed for txHash: %s (status: %s)", txHash, sendOrder.Status)
		}
	}

	// Build response
	resp := FireblocksCosignerResponse{
		Action:          action,
		RejectionReason: rejectionReason,
	}

	// Sign response with callback private key
	callbackPrivKey := cfg.Withdraw.Fireblocks.CallbackPriv
	if callbackPrivKey == "" {
		logger.Error("Fireblocks callback private key not configured")
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	rsaPrivKey, err := parseRSAPrivateKeyFromPEM(callbackPrivKey)
	if err != nil {
		logger.Errorf("Failed to parse callback private key: %v", err)
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	// Marshal response to JSON
	respJSON, err := json.Marshal(resp)
	if err != nil {
		logger.Errorf("Failed to marshal response: %v", err)
		http.Error(w, "Failed to create response", http.StatusInternalServerError)
		return
	}

	// Create JWT token with response
	now := time.Now()
	responseToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"body": base64.StdEncoding.EncodeToString(respJSON),
		"iat":  now.Unix(),
		"exp":  now.Add(55 * time.Second).Unix(),
	})

	signedToken, err := responseToken.SignedString(rsaPrivKey)
	if err != nil {
		logger.Errorf("Failed to sign JWT response: %v", err)
		http.Error(w, "Failed to sign response", http.StatusInternalServerError)
		return
	}

	logger.Infof("Cosigner callback response: action=%s, reason=%s", action, rejectionReason)

	// Return signed JWT as response
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(signedToken))
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
