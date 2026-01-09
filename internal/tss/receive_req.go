package tss

import (
	"strings"
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
	ssResp, err := m.signClient.StartSign(m.ctx, req.SessionID, req.UnsignHash)
	if err != nil {
		m.logger.Errorf("failed to sign start: %v", err)

		// Handle nil response safely
		errorMessage := err.Error()
		if ssResp != nil && ssResp.Message != "" {
			errorMessage = ssResp.Message
		}

		// Check if session already exists - this is normal for coordinated signing
		if ssResp != nil && ssResp.Message != "" &&
			(ssResp.Message == "session already exists" ||
				strings.Contains(ssResp.Message, "already exists")) {
			m.logger.Infof("Session %s already exists, joining existing session", req.SessionID)
			m.activeSessions.Store(req.SessionID, time.Now())
			return
		}

		m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
			SessionID: req.SessionID,
			Success:   false,
			Message:   errorMessage,
			RawSig:    nil,
		})
		return
	}
	if !ssResp.Success {
		// Check if session already exists - this is normal for coordinated signing
		if ssResp.Message != "" &&
			(ssResp.Message == "session already exists" ||
				strings.Contains(ssResp.Message, "already exists")) {
			m.logger.Infof("Session %s already exists, joining existing session", req.SessionID)
			m.activeSessions.Store(req.SessionID, time.Now())
			return
		}

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
