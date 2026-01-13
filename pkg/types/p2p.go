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
	// P2PMessageTypeSendOrderBroadcasted is for send_order broadcast to cosigner nodes
	P2PMessageTypeSendOrderBroadcasted P2PMessageType = "send-order-broadcasted"
	// P2PMessageTypeSendOrderTxidUpdate is for updating send_order txid after Fireblocks signing
	P2PMessageTypeSendOrderTxidUpdate P2PMessageType = "send-order-txid-update"
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

// SendOrderBroadcastPayload is the payload for send_order broadcast to cosigner nodes
type SendOrderBroadcastPayload struct {
	TxId       string            `json:"tx_id"`
	ExternalId string            `json:"external_id"`
	OrderId    string            `json:"order_id"`
	OrderType  string            `json:"order_type"`
	Status     string            `json:"status"`
	VINs       []SendOrderVIN    `json:"vins"`
	VOUTs      []SendOrderVOUT   `json:"vouts"`
	UpdatedAt  int64             `json:"updated_at"`
}

// SendOrderVIN represents VIN info for P2P broadcast
type SendOrderVIN struct {
	Txid     string `json:"txid"`
	OutIndex int    `json:"out_index"`
	Source   string `json:"source"`
}

// SendOrderVOUT represents VOUT info for P2P broadcast
type SendOrderVOUT struct {
	Txid       string `json:"txid"`
	OutIndex   int    `json:"out_index"`
	WithdrawId string `json:"withdraw_id"`
	Amount     int64  `json:"amount"`
	Receiver   string `json:"receiver"`
	Source     string `json:"source"`
}

// SendOrderTxidUpdatePayload is the payload for updating send_order txid after signing
type SendOrderTxidUpdatePayload struct {
	ExternalId    string `json:"external_id"`     // Fireblocks transaction ID
	OldTxid       string `json:"old_txid"`        // Unsigned transaction hash
	NewTxid       string `json:"new_txid"`        // Signed transaction hash (on-chain)
	UpdatedAt     int64  `json:"updated_at"`
}

// P2PBroadcastMessage is the message for broadcast to all peers
type P2PBroadcastMessage struct {
	Type      P2PMessageType
	SessionID string
	Payload   []byte
}
