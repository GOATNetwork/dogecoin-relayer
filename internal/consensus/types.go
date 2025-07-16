package consensus

import (
	"encoding/json"
	"math/big"

	"github.com/goat-network/dogecoin-relayer/internal/models"
)

// DepositProposal represents a batch proposal for processing multiple UTXOs
type DepositProposal struct {
	BatchID     string         `json:"batch_id"`     // Unique identifier for this batch
	UTXOs       []*models.UTXO `json:"utxos"`        // List of UTXOs in this batch
	TotalAmount *big.Int       `json:"total_amount"` // Total amount across all UTXOs
	Calldata    []byte         `json:"calldata"`     // Generated calldata for the batch
	Proposer    string         `json:"proposer"`     // Address of the proposer node
	SessionID   string         `json:"session_id"`   // TSS session ID for signing
}

func NewDepositProposal(batchID string, utxos []*models.UTXO, totalAmount *big.Int, calldata []byte, proposer, sessionID string) *DepositProposal {
	return &DepositProposal{
		BatchID:     batchID,
		UTXOs:       utxos,
		TotalAmount: totalAmount,
		Calldata:    calldata,
		Proposer:    proposer,
		SessionID:   sessionID,
	}
}

func (d *DepositProposal) MarshalJSON() ([]byte, error) {
	return json.Marshal(d)
}

func (d *DepositProposal) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, d)
}
