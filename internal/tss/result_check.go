package tss

import (
	"context"
	"time"

	"github.com/goat-network/dogecoin-relayer/pkg/eventbus"
	"github.com/goat-network/dogecoin-relayer/pkg/types"
)

func (m *TssModule) checkSignHandler(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var sessionCount int
			m.activeSessions.Range(func(key, value any) bool {
				sessionID := key.(string)
				startTime := value.(time.Time)
				sessionCount++

				// if timeout, remove from active sessions
				if time.Since(startTime) > time.Duration(m.cfg.Timeout)*time.Second {
					m.logger.Warnf("Session %s timed out", sessionID)
					m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
						SessionID: sessionID,
						Success:   false,
						Message:   "session timed out",
						RawSig:    nil,
					})
					m.activeSessions.Delete(sessionID)
					m.logger.Infof("Removed session %s from active sessions", sessionID)
					return true
				}

				// check session status
				signStatus, err := m.signClient.GetSignStatus(ctx, sessionID)
				if err != nil {
					m.logger.Errorf("Failed to get status for session %s: %v", sessionID, err)
					return true
				}

				// session failed, remove from active sessions
				if !signStatus.Success || signStatus.Status == "failed" {
					msg := signStatus.Message
					if msg == "" {
						msg = "session failed"
					}
					m.logger.Warnf("Session %s failed", sessionID)
					m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
						SessionID: sessionID,
						Success:   false,
						Message:   msg,
						RawSig:    nil,
					})
					m.activeSessions.Delete(sessionID)
					m.logger.Infof("Removed session %s from active sessions", sessionID)
					return true
				}

				// session signed, remove from active sessions
				if signStatus.Signature != nil {
					m.logger.Infof("Session %s is signed", sessionID)
					m.eventBus.Publish(eventbus.EventTssSigResponse, types.TssSigResponse{
						SessionID: sessionID,
						Success:   true,
						Message:   "",
						RawSig:    signStatus.Signature,
					})
					m.activeSessions.Delete(sessionID)
					m.logger.Infof("Removed session %s from active sessions", sessionID)
					return true
				}

				// other status, continue to check. It is not a final status, e.g. "running", "pending"
				return true
			})

			// memory protection: if session count is too many, record warning
			if sessionCount > 500 {
				m.logger.Warnf("Too many active sessions: %d", sessionCount)
			}
		}
	}
}
