package tss

import (
	"context"
	"time"

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
	ssResp, err := m.signClient.StartSign(context.Background(), req.SessionID, req.UnsignHash)
	if err != nil {
		m.logger.Errorf("failed to sign start: %v", err)
		m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
			SessionID: req.SessionID,
			Success:   false,
			Message:   ssResp.Message,
			RawSig:    nil,
		})
		return
	}
	if !ssResp.Success {
		m.logger.Errorf("sign start response failed: %s, session id: %s", ssResp.Message, req.SessionID)
		m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
			SessionID: req.SessionID,
			Success:   false,
			Message:   ssResp.Message,
			RawSig:    nil,
		})
		return
	}

	m.activeSessions.Store(req.SessionID, time.Now())
	m.logger.Infof("Added session %s to active sessions", req.SessionID)
}
