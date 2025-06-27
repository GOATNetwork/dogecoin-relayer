package p2p

import (
	"fmt"

	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

type P2PSender interface {
	RegisterP2PHandler(msgType types.P2PMessageType, handler func(*types.P2PBroadcastMessage) error) error
	BroadcastP2PMessage(msg types.P2PBroadcastMessage) error
}

var _ P2PSender = (*P2PModule)(nil)

func (m *P2PModule) RegisterP2PHandler(msgType types.P2PMessageType, handler func(*types.P2PBroadcastMessage) error) error {
	if m.network == nil {
		return fmt.Errorf("network not initialized")
	}
	return m.network.RegisterHandler(msgType, handler)
}

func (m *P2PModule) BroadcastP2PMessage(msg types.P2PBroadcastMessage) error {
	if m.network == nil {
		return fmt.Errorf("network not initialized")
	}
	return m.network.BroadcastMessage(nil, msg.Type, msg.SessionID, msg.Payload)
}

/**
The demo code to register P2P handler for bridge in message,
it should be called after the network is initialized, may be in the Run method of each registered module,
and the handler should be called when the bridge in message is received.

Example:
	go func() {
		p2pConfig := global.GetConfig().P2P
		time.Sleep(time.Duration(p2pConfig.ListenWaitSeconds+p2pConfig.ConnectionWaitSeconds+5) * time.Second)
		p2pModule, ok := module.GetModule((&p2p.P2PModule{}).Name())
		if ok {
			// register a handler for bridge in message
			err := p2pModule.(p2p.P2PSender).RegisterP2PHandler(types.P2PMessageTypeBridgeIn, func(msg *types.P2PBroadcastMessage) error {
				m.logger.Infof("Received bridge in message: %s", msg.SessionID)
				return nil
			})
			if err != nil {
				m.logger.Errorf("Failed to register p2p bridge in handler: %v", err)
			} else {
				m.logger.Info("P2P bridge in handler registered")
			}

			// send a test message to the p2p network
			p2pModule.(p2p.P2PSender).BroadcastP2PMessage(types.P2PBroadcastMessage{
				Type:      types.P2PMessageTypeBridgeIn,
				SessionID: "123",
				Payload:   []byte("test"),
			})
		}
	}()
*/
