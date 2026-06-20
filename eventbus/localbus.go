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
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	workerPool  int
	bufferSize  int
}

func NewLocalBus(workerPool int, bufferSize int) *LocalBus {
	if workerPool <= 0 {
		workerPool = 4
	}
	if bufferSize <= 0 {
		bufferSize = 100
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &LocalBus{
		subscribers: make(map[string][]Handler),
		channels:    make(map[string]chan Message),
		ctx:         ctx,
		cancel:      cancel,
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
			b.wg.Add(1)
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

// Close signals all workers to stop and waits until they exit.
func (b *LocalBus) Close() {
	b.cancel()
	b.wg.Wait()
}

func (b *LocalBus) startWorker(topic string, ch chan Message) {
	defer b.wg.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case msg := <-ch:
			b.mu.RLock()
			handlers := b.subscribers[topic]
			b.mu.RUnlock()

			for _, h := range handlers {
				func(handler Handler) {
					defer func() { recover() }() //nolint:errcheck
					_ = handler(context.Background(), msg)
				}(h)
			}
		}
	}
}
