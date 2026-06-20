package eventbus

import (
	"context"
	"time"
)

// FailedPublish holds the context of a single failed broker publish attempt.
type FailedPublish struct {
	Topic   string
	Broker  string
	Message Message
	Error   error
	Time    time.Time
}

// FallbackAdapter is the contract for storing failed publish attempts.
type FallbackAdapter interface {
	Store(ctx context.Context, failed FailedPublish) error
}

// FallbackConfig configures the adapter used to store failed messages.
// Subscribe-side retry is configured separately via TopicConfig.Retry.
type FallbackConfig struct {
	Adapter FallbackAdapter
}
