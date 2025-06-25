package eventbus

import (
	"fmt"
	"sync"
)

// EventType represents the type of an event
type EventType string

// Known event types
const (
	// Network events
	EventNetworkInitialized      EventType = "network:initialized"
	EventNetworkHeartbeat        EventType = "network:heartbeat"
	EventNetworkPeerConnected    EventType = "network:peer:connected"
	EventNetworkPeerDisconnected EventType = "network:peer:disconnected"
)

// Event represents an event with a payload
type Event struct {
	Type    EventType
	Payload any
}

// Handler is a function that handles an event
type Handler func(data any)

// Bus is an event bus that allows publishing and subscribing to events
type Bus struct {
	handlers     map[EventType][]Handler
	handlersLock sync.RWMutex
}

// NewEventBus creates a new event bus
func NewEventBus() *Bus {
	return &Bus{
		handlers: make(map[EventType][]Handler),
	}
}

// Subscribe registers a handler for an event type
func (b *Bus) Subscribe(eventType EventType, handler Handler) {
	b.handlersLock.Lock()
	defer b.handlersLock.Unlock()

	handlers, ok := b.handlers[eventType]
	if !ok {
		handlers = []Handler{}
	}

	b.handlers[eventType] = append(handlers, handler)
	fmt.Printf("DEBUG: Subscribed to event type: %s, total handlers: %d\n", eventType, len(b.handlers[eventType]))
}

// Unsubscribe removes a handler for an event type
func (b *Bus) Unsubscribe(eventType EventType, handler Handler) {
	b.handlersLock.Lock()
	defer b.handlersLock.Unlock()

	handlers, ok := b.handlers[eventType]
	if !ok {
		return
	}

	// Find and remove the handler
	for i, h := range handlers {
		if &h == &handler {
			b.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}

// Publish publishes an event with data
func (b *Bus) Publish(eventType EventType, data any) {
	b.handlersLock.RLock()
	handlers, ok := b.handlers[eventType]
	b.handlersLock.RUnlock()

	fmt.Printf("DEBUG: Publishing event type: %s, handlers found: %v\n", eventType, ok)

	if !ok {
		return
	}

	// Call each handler with the data
	fmt.Printf("DEBUG: Calling %d handlers for event type: %s\n", len(handlers), eventType)
	for _, handler := range handlers {
		handler(data)
	}
}
