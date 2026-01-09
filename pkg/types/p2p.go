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
)

// P2PBroadcastMessage is the message for broadcast to all peers
type P2PBroadcastMessage struct {
	Type      P2PMessageType
	SessionID string
	Payload   []byte
}
