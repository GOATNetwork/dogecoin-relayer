package tss

import (
	"context"

	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

func (m *TssModule) subscribeEvent() {
	m.eventBus.Subscribe(eventbus.EventTssSigRequest, m.handleSignStart)
}

func (m *TssModule) unSubscribeEvent() {
	m.eventBus.Unsubscribe(eventbus.EventTssSigRequest, m.handleSignStart)
}

func (m *TssModule) handleSignStart(data any) {
	req, ok := data.(types.TssSigRequest)
	if !ok {
		m.logger.Errorf("invalid data type: %T", data)
		return
	}

	_, err := m.signClient.StartSign(context.Background(), req.SessionID, req.UnsignHash)
	if err != nil {
		m.logger.Errorf("failed to sign start: %v", err)
		return
	}

	// TODO: manange self module sessions state, publish event to other modules
	m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
		SessionID: req.SessionID,
		RawSig:    nil,
	})
}
