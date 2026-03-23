package eventbus

import (
	"context"
	"fmt"
	"sync"
	"time"

	broker "github.com/budimanlai/go-message-broker"
)

type Handler = broker.Handler
type Message = broker.Message

type TopicConfig struct {
	EmitToLocal  bool
	EmitToBroker bool

	ConsumeFromLocal  bool
	ConsumeFromBroker bool
}

type EventBus interface {
	Start(ctx context.Context) error
	Emit(ctx context.Context, topic string, msg Message) error
	Subscribe(topic string, handler Handler)
	Close() error
}

type eventBus struct {
	local        *LocalBus
	broker       broker.Broker
	topicConfigs map[string]TopicConfig
	defaultCfg   TopicConfig

	ctx    context.Context
	cancel context.CancelFunc

	started bool
	mu      sync.Mutex
}

type Config struct {
	Broker     broker.Broker
	Topics     map[string]TopicConfig
	Default    TopicConfig
	WorkerPool int
	BufferSize int
}

func NewEventBus(cfg Config) EventBus {
	var local *LocalBus

	if cfg.WorkerPool > 0 {
		local = NewLocalBus(cfg.WorkerPool, cfg.BufferSize)
	}

	defaultCfg := cfg.Default
	if !defaultCfg.EmitToLocal && !defaultCfg.EmitToBroker {
		defaultCfg.EmitToLocal = true
		defaultCfg.ConsumeFromLocal = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &eventBus{
		local:        local,
		broker:       cfg.Broker,
		topicConfigs: cfg.Topics,
		defaultCfg:   defaultCfg,
		ctx:          ctx,
		cancel:       cancel,
	}

}

func (e *eventBus) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	if e.broker != nil {
		if err := e.broker.Connect(); err != nil {
			return err
		}
	}

	e.started = true
	return nil
}

func (e *eventBus) Emit(ctx context.Context, topic string, msg Message) error {
	cfg := e.getTopicConfig(topic)

	// Local
	if cfg.EmitToLocal && e.local != nil {
		if err := e.local.Emit(ctx, topic, msg); err != nil {
			return fmt.Errorf("local emit failed: %w", err)
		}
	}

	// Broker (async)
	if cfg.EmitToBroker && e.broker != nil {
		go func(m Message) {
			ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_ = e.broker.Publish(ctx2, topic, m)
		}(msg)
	}

	return nil
}

func (e *eventBus) Subscribe(topic string, handler Handler) {
	cfg := e.getTopicConfig(topic)

	// Local
	if cfg.ConsumeFromLocal && e.local != nil {
		e.local.Subscribe(topic, handler)
	}

	// Broker
	if cfg.ConsumeFromBroker && e.broker != nil {
		go func() {
			_ = e.broker.Subscribe(e.ctx, topic, handler)
		}()
	}
}

func (e *eventBus) Close() error {
	if e.cancel != nil {
		e.cancel()
	}

	if e.broker != nil {
		return e.broker.Disconnect()
	}
	return nil
}

func (e *eventBus) getTopicConfig(topic string) TopicConfig {
	if cfg, ok := e.topicConfigs[topic]; ok {
		return cfg
	}
	return e.defaultCfg
}
