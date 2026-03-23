package eventbus

import (
	"context"
	"fmt"
	"sync"
)

type LocalBus struct {
	subscribers map[string][]Handler
	channels    map[string]chan Message
	mu          sync.RWMutex

	workerPool int
	bufferSize int
}

func NewLocalBus(workerPool int, bufferSize int) *LocalBus {
	if workerPool <= 0 {
		workerPool = 4
	}
	if bufferSize <= 0 {
		bufferSize = 100
	}

	return &LocalBus{
		subscribers: make(map[string][]Handler),
		channels:    make(map[string]chan Message),
		workerPool:  workerPool,
		bufferSize:  bufferSize,
	}
}

func (b *LocalBus) Subscribe(topic string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], handler)

	if _, ok := b.channels[topic]; !ok {
		ch := make(chan Message, b.bufferSize)
		b.channels[topic] = ch

		for i := 0; i < b.workerPool; i++ {
			go b.startWorker(topic, ch)
		}
	}
}

func (b *LocalBus) Emit(ctx context.Context, topic string, msg Message) error {
	b.mu.RLock()
	ch, ok := b.channels[topic]
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	select {
	case ch <- msg:
		return nil
	default:
		return fmt.Errorf("localbus buffer full for topic: %s", topic)
	}
}

func (b *LocalBus) startWorker(topic string, ch chan Message) {
	for msg := range ch {
		b.mu.RLock()
		handlers := b.subscribers[topic]
		b.mu.RUnlock()

		for _, h := range handlers {
			func(handler Handler) {
				defer func() {
					if r := recover(); r != nil {
						// optional: log panic
					}
				}()
				_ = handler(context.Background(), msg)
			}(h)
		}
	}
}
