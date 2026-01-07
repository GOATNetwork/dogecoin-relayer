package grpc

import (
	"context"
	"strings"
	"testing"

	"github.com/goat-network/dogecoin-relayer/proto"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTransaction_ValidationErrors(t *testing.T) {
	srv := &grpcServer{
		logger: log.WithField("component", "test"),
	}

	tests := []struct {
		name    string
		req     *proto.NewTransactionRequest
		wantErr string
	}{
		{
			name:    "empty transaction_id",
			req:     &proto.NewTransactionRequest{TransactionId: "", RawTransaction: "abc", EvmAddress: "0x123"},
			wantErr: "transaction_id is required",
		},
		{
			name:    "empty evm_address",
			req:     &proto.NewTransactionRequest{TransactionId: strings.Repeat("a", 64), RawTransaction: "abc", EvmAddress: ""},
			wantErr: "evm_address is required",
		},
		{
			name:    "invalid txid length",
			req:     &proto.NewTransactionRequest{TransactionId: "abc", RawTransaction: "abc", EvmAddress: "0x123"},
			wantErr: "transaction_id must be 64 characters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := srv.NewTransaction(context.Background(), tt.req)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestSplitEvmAddresses(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
		wantErr  bool
	}{
		{
			name:     "single address",
			input:    "0xabc123",
			expected: []string{"abc123"},
		},
		{
			name:     "multiple addresses",
			input:    "0xabc,0xdef,0x123",
			expected: []string{"123", "abc", "def"},
		},
		{
			name:     "deduplicate addresses",
			input:    "0xabc,0xABC,0xdef",
			expected: []string{"abc", "def"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := splitEvmAddresses(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueryDepositAddress_NoConfig(t *testing.T) {
	srv := &grpcServer{
		logger: log.WithField("component", "test"),
	}

	_, err := srv.QueryDepositAddress(context.Background(), &proto.QueryDepositAddressRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "configuration not initialized")
}
