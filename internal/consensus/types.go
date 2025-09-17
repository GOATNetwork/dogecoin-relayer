package consensus

import (
	"encoding/json"
	"math/big"

	"github.com/goat-network/dogecoin-relayer/internal/models"
)

// LightweightUTXO contains only essential UTXO data for P2P messages
type LightweightUTXO struct {
	Uid      string `json:"uid"`
	Txid     string `json:"txid"`
	OutIndex int    `json:"out_index"`
	Amount   int64  `json:"amount"`
	Receiver string `json:"receiver"`
	EvmAddr  string `json:"evm_addr"`
}

// ToLightweight converts a full UTXO to a lightweight version for P2P
func ToLightweightUTXO(utxo *models.UTXO) *LightweightUTXO {
	return &LightweightUTXO{
		Uid:      utxo.Uid,
		Txid:     utxo.Txid,
		OutIndex: utxo.OutIndex,
		Amount:   utxo.Amount,
		Receiver: utxo.Receiver,
		EvmAddr:  utxo.EvmAddr,
	}
}

// DepositProposal represents a batch proposal for processing multiple UTXOs
type DepositProposal struct {
	BatchID          string             `json:"batch_id"`          // Unique identifier for this batch
	LightweightUTXOs []*LightweightUTXO `json:"lightweight_utxos"` // Lightweight UTXO data for P2P
	TotalAmountStr   string             `json:"total_amount"`      // Total amount as string to avoid big.Int issues
	Proposer         string             `json:"proposer"`          // Address of the proposer node
	SessionID        string             `json:"session_id"`        // TSS session ID for signing

	// Keep original data for internal use (not serialized in P2P)
	UTXOs       []*models.UTXO `json:"-"` // Original UTXOs (excluded from JSON)
	Calldata    []byte         `json:"-"` // Calldata (excluded from JSON)
	TotalAmount *big.Int       `json:"-"` // Original big.Int (excluded from JSON)
}

func NewDepositProposal(batchID string, utxos []*models.UTXO, totalAmount *big.Int, calldata []byte, proposer, sessionID string) *DepositProposal {
	// Convert full UTXOs to lightweight versions for P2P
	lightweightUTXOs := make([]*LightweightUTXO, len(utxos))
	for i, utxo := range utxos {
		lightweightUTXOs[i] = ToLightweightUTXO(utxo)
	}

	return &DepositProposal{
		BatchID:          batchID,
		LightweightUTXOs: lightweightUTXOs,
		TotalAmountStr:   totalAmount.String(), // Convert to string for safe JSON
		Proposer:         proposer,
		SessionID:        sessionID,
		UTXOs:            utxos,       // Keep original for internal use
		Calldata:         calldata,    // Keep original for internal use
		TotalAmount:      totalAmount, // Keep original for internal use
	}
}

// P2PDepositProposal is a lightweight version for P2P transmission only
type P2PDepositProposal struct {
	BatchID          string             `json:"batch_id"`
	LightweightUTXOs []*LightweightUTXO `json:"lightweight_utxos"`
	TotalAmountStr   string             `json:"total_amount"`
	Proposer         string             `json:"proposer"`
	SessionID        string             `json:"session_id"`
}

func (d *DepositProposal) MarshalJSON() ([]byte, error) {
	// Create a lightweight version for P2P transmission
	p2pProposal := &P2PDepositProposal{
		BatchID:          d.BatchID,
		LightweightUTXOs: d.LightweightUTXOs,
		TotalAmountStr:   d.TotalAmountStr,
		Proposer:         d.Proposer,
		SessionID:        d.SessionID,
	}
	return json.Marshal(p2pProposal)
}

func (d *DepositProposal) UnmarshalJSON(data []byte) error {
	// Unmarshal to lightweight P2P structure first
	p2pProposal := &P2PDepositProposal{}
	err := json.Unmarshal(data, p2pProposal)
	if err != nil {
		return err
	}

	// Copy data from P2P structure to full structure
	d.BatchID = p2pProposal.BatchID
	d.LightweightUTXOs = p2pProposal.LightweightUTXOs
	d.TotalAmountStr = p2pProposal.TotalAmountStr
	d.Proposer = p2pProposal.Proposer
	d.SessionID = p2pProposal.SessionID

	// Don't set UTXOs, Calldata, TotalAmount - these will be set separately
	return nil
}

// WithdrawalProposal represents a batch proposal for processing withdrawal UTXOs
type WithdrawalProposal struct {
	BatchID     string         `json:"batch_id"`     // Unique identifier for this batch
	UTXOs       []*models.UTXO `json:"utxos"`        // List of withdrawal UTXOs in this batch
	TotalAmount *big.Int       `json:"total_amount"` // Total amount across all UTXOs
	Calldata    []byte         `json:"calldata"`     // Generated bridgeOutFinish calldata for the batch
	TaskIds     []*big.Int     `json:"task_ids"`     // Task IDs associated with this withdrawal batch
	Proposer    string         `json:"proposer"`     // Address of the proposer node
	SessionID   string         `json:"session_id"`   // TSS session ID for signing
}

func NewWithdrawalProposal(batchID string, utxos []*models.UTXO, totalAmount *big.Int, calldata []byte, taskIds []*big.Int, proposer, sessionID string) *WithdrawalProposal {
	return &WithdrawalProposal{
		BatchID:     batchID,
		UTXOs:       utxos,
		TotalAmount: totalAmount,
		Calldata:    calldata,
		TaskIds:     taskIds,
		Proposer:    proposer,
		SessionID:   sessionID,
	}
}

func (w *WithdrawalProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(w)
}

func (w *WithdrawalProposal) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, w)
}
