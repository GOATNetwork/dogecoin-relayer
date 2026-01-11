package types

// P2PMessageType defines the type of messages
type P2PMessageType string

const (
	// P2PMessageTypeHeartbeat is for heartbeat messages
	P2PMessageTypeHeartbeat P2PMessageType = "heartbeat"
	// P2PMessageTypeBridgeIn is for bridge in contract call messages
	P2PMessageTypeBridgeIn P2PMessageType = "bridge-in"
	// P2PMessageTypeBridgeOut is for bridge out contract call messages
	P2PMessageTypeBridgeOut P2PMessageType = "bridge-out"
	// P2PMessageTypeDepositProposal is for deposit proposal messages
	P2PMessageTypeDepositProposal P2PMessageType = "deposit-proposal"
	// P2PMessageTypeDepositNotification is for new deposit notification from gRPC
	P2PMessageTypeDepositNotification P2PMessageType = "deposit-notification"
	P2PMessageTypeWithdrawalStatus    P2PMessageType = "withdrawal-status"
)

type WithdrawalStatusPayload struct {
	ReqTaskId      string `json:"req_task_id"`
	Status         string `json:"status"`
	ReqTxHash      string `json:"req_tx_hash,omitempty"`
	ReqBlock       uint64 `json:"req_block,omitempty"`
	ReqLogIndex    uint   `json:"req_log_index,omitempty"`
	DestAddress    string `json:"dest_address,omitempty"`
	DestAmount     string `json:"dest_amount,omitempty"`
	TxId           string `json:"tx_id,omitempty"`
	ExternalId     string `json:"external_id,omitempty"`
	Vout           int    `json:"vout,omitempty"`
	TxBytes        []byte `json:"tx_bytes,omitempty"`
	UnsignedTx     []byte `json:"unsigned_tx,omitempty"`
	FinishTxHash   string `json:"finish_tx_hash,omitempty"`
	FinishBlock    uint64 `json:"finish_block,omitempty"`
	FinishLogIndex uint   `json:"finish_log_index,omitempty"`
	UpdatedAt      int64  `json:"updated_at"`
}

// P2PBroadcastMessage is the message for broadcast to all peers
type P2PBroadcastMessage struct {
	Type      P2PMessageType
	SessionID string
	Payload   []byte
}
