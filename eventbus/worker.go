package eventbus

import (
	"context"
	"time"

	broker "github.com/budimanlai/go-message-broker"
)

// broadcastJob is a single unit of work for the broker worker pool.
type broadcastJob struct {
	adapter broker.Adapter
	name    string
	topic   string
	msg     Message
}

// runBroadcastWorker processes jobs from the broadcast queue until the context is cancelled.
func (e *eventBus) runBroadcastWorker() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case job := <-e.broadcastQueue:
			e.publishSync(job)
		}
	}
}

// publishSync executes a single publish and forwards any error to the fallback handler.
// It is the shared execution path for both synchronous mode and worker pool mode.
func (e *eventBus) publishSync(job broadcastJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := job.adapter.Publish(ctx, job.topic, job.msg); err != nil {
		e.handlePublishError(ctx, FailedPublish{
			Topic:   job.topic,
			Broker:  job.name,
			Message: job.msg,
			Error:   err,
			Time:    time.Now(),
		})
	}
}
