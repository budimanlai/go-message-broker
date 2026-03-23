package gomessagebroker

import (
	"fmt"
	"time"
)

type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Headers   map[string]string
	Timestamp time.Time
}

// NewMessage creates a new message with basic metadata.
func NewMessage(topic string, payload []byte) Message {
	return Message{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Topic:     topic,
		Payload:   payload,
		Headers:   make(map[string]string),
		Timestamp: time.Now(),
	}
}
