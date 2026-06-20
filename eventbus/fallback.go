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

// FallbackConfig configures what happens when a broker publish fails.
// MaxRetry is reserved for future retry support and has no effect yet.
type FallbackConfig struct {
	MaxRetry int
	Adapter  FallbackAdapter
}
