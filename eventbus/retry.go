package eventbus

import (
	"context"
	"time"
)

// RetryPolicy defines how many times a handler is retried on failure.
// Delay is the wait between attempts; zero means retry immediately.
type RetryPolicy struct {
	MaxRetry int
	Delay    time.Duration
}

// applyRetry wraps handler with a retry loop governed by policy.
// Returns the original handler unchanged when policy is nil or MaxRetry <= 0.
//
// Execution per message:
//
//	attempt 1 … MaxRetry+1 → on success: return nil
//	all attempts fail      → call FallbackAdapter.Store, return nil
//
// Returning nil after exhaustion acknowledges the message at the broker level,
// delegating failure ownership to the FallbackAdapter.
func applyRetry(handler Handler, policy *RetryPolicy, fallback *FallbackConfig, topic string) Handler {
	if policy == nil || policy.MaxRetry <= 0 {
		return handler
	}
	return func(ctx context.Context, msg Message) error {
		var lastErr error
		for attempt := 0; attempt <= policy.MaxRetry; attempt++ {
			if lastErr = handler(ctx, msg); lastErr == nil {
				return nil
			}
			if policy.Delay > 0 && attempt < policy.MaxRetry {
				time.Sleep(policy.Delay)
			}
		}

		// All attempts exhausted: delegate to fallback storage.
		if fallback != nil && fallback.Adapter != nil {
			_ = fallback.Adapter.Store(ctx, FailedPublish{
				Topic:   topic,
				Message: msg,
				Error:   lastErr,
				Time:    time.Now(),
			})
		}

		// Return nil so the broker adapter ACKs/completes the message.
		// Retry is owned by EventBus; broker-level DLQ must not be triggered.
		return nil
	}
}
