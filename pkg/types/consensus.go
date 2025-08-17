package types

// Blockchain event names - these are the raw event names detected from smart contracts
const (
	// Bridge-related events
	EventNameBridgeIn          = "BridgeIn"
	EventNameBridgeOutProposed = "BridgeOutProposed"
	EventNameBridgeOutFinished = "BridgeOutFinished"

	// Consensus-related events
    EventNameSubmitterChosen    = "SubmitterChosen"
    EventNameAddProposerReq     = "AddProposerRequested"
    EventNameRemoveProposerReq  = "RemoveProposerRequested"
    EventNameProposerConfirmed  = "ProposerConfirmed"
)
