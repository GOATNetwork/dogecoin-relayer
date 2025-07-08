package models

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"
)

type EventRepository struct {
	db *gorm.DB
}

func NewEventRepository(db *gorm.DB) *EventRepository {
	return &EventRepository{db: db}
}

// DetectedEvent operations
func (r *EventRepository) CreateDetectedEvent(event *DetectedEvent, eventData map[string]interface{}) error {
	// Convert event data to JSON string
	if eventData != nil {
		jsonData, err := json.Marshal(eventData)
		if err != nil {
			return fmt.Errorf("failed to marshal event data: %w", err)
		}
		event.EventData = string(jsonData)
	}

	// Set unique index for preventing duplicate processing
	event.UniqueIndex = fmt.Sprintf("%s_%d_%d", event.TxHash, event.BlockNumber, event.LogIndex)
	event.ProcessedAt = time.Now()

	return r.db.Create(event).Error
}

func (r *EventRepository) GetDetectedEventsByStatus(status string, limit int) ([]DetectedEvent, error) {
	var events []DetectedEvent
	query := r.db.Where("status = ?", status).Order("created_at ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	return events, query.Find(&events).Error
}

func (r *EventRepository) GetDetectedEventsByBlock(blockNumber uint64) ([]DetectedEvent, error) {
	var events []DetectedEvent
	return events, r.db.Where("block_number = ?", blockNumber).Find(&events).Error
}

func (r *EventRepository) UpdateDetectedEventStatus(id uint, status string) error {
	return r.db.Model(&DetectedEvent{}).Where("id = ?", id).Update("status", status).Error
}

func (r *EventRepository) GetDetectedEventData(event *DetectedEvent) (map[string]interface{}, error) {
	if event.EventData == "" {
		return nil, nil
	}

	var data map[string]interface{}
	err := json.Unmarshal([]byte(event.EventData), &data)
	return data, err
}

// EventScanState operations
func (r *EventRepository) GetScanState(contractAddress string) (*EventScanState, error) {
	var state EventScanState
	err := r.db.Where("contract_address = ?", contractAddress).First(&state).Error
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *EventRepository) UpdateScanState(contractAddress string, lastScannedBlock uint64) error {
	state := &EventScanState{
		ContractAddress:  contractAddress,
		LastScannedBlock: lastScannedBlock,
		LastScannedAt:    time.Now(),
		IsActive:         true,
	}

	// Use Upsert (create or update)
	return r.db.Save(state).Error
}

func (r *EventRepository) CreateOrUpdateScanState(state *EventScanState) error {
	state.LastScannedAt = time.Now()
	return r.db.Save(state).Error
}

// EventProcessingLog operations
func (r *EventRepository) CreateProcessingLog(log *EventProcessingLog) error {
	log.ProcessedAt = time.Now()
	return r.db.Create(log).Error
}

func (r *EventRepository) GetProcessingLogs(detectedEventID uint) ([]EventProcessingLog, error) {
	var logs []EventProcessingLog
	return logs, r.db.Where("detected_event_id = ?", detectedEventID).Order("created_at DESC").Find(&logs).Error
}

func (r *EventRepository) GetFailedProcessingLogs(maxRetries int) ([]EventProcessingLog, error) {
	var logs []EventProcessingLog
	return logs, r.db.Where("status = ? AND retry_count < ?", "failed", maxRetries).Find(&logs).Error
}

// Utility methods
func (r *EventRepository) GetEventProcessingStatistics() (map[string]int64, error) {
	stats := make(map[string]int64)

	// Count by status
	statuses := []string{"pending", "processed", "failed"}
	for _, status := range statuses {
		var count int64
		if err := r.db.Model(&DetectedEvent{}).Where("status = ?", status).Count(&count).Error; err != nil {
			return nil, err
		}
		stats[status] = count
	}

	// Total events
	var total int64
	if err := r.db.Model(&DetectedEvent{}).Count(&total).Error; err != nil {
		return nil, err
	}
	stats["total"] = total

	return stats, nil
}

func (r *EventRepository) CleanupOldEvents(olderThanDays int, status string) error {
	cutoffDate := time.Now().AddDate(0, 0, -olderThanDays)
	return r.db.Where("created_at < ? AND status = ?", cutoffDate, status).Delete(&DetectedEvent{}).Error
}
