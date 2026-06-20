package eventbus

import (
	"context"
	"fmt"
	"sync"

	broker "github.com/budimanlai/go-message-broker"
)

type Handler = broker.Handler
type Message = broker.Message

type TopicConfig struct {
	EmitToLocal  bool
	EmitToBroker bool

	ConsumeFromLocal  bool
	ConsumeFromBroker bool

	// Brokers lists named broker keys (from Config.Brokers) this topic targets.
	// Empty means existing behavior applies (uses Config.Broker).
	Brokers []string

	// Middleware is applied in order to every handler subscribed to this topic.
	// Empty means no middleware (existing behavior).
	Middleware []Middleware

	// Retry configures how many times the handler is retried on failure.
	// Nil means no retry (existing behavior).
	Retry *RetryPolicy
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
	brokers      map[string]broker.Adapter
	fallback     *FallbackConfig
	topicConfigs map[string]TopicConfig
	defaultCfg   TopicConfig

	broadcastQueue       chan broadcastJob
	broadcastWorkerCount int

	ctx    context.Context
	cancel context.CancelFunc

	started bool
	mu      sync.Mutex
}

type Config struct {
	// Broker is the single default broker (backward compatible).
	Broker broker.Broker

	// Brokers is a named set of adapters for multi-broker routing.
	// Keys are arbitrary names referenced from TopicConfig.Brokers.
	Brokers map[string]broker.Adapter

	// Fallback configures what happens when a broker publish fails.
	// Nil means publish errors are silently dropped (async publish cannot surface them).
	Fallback *FallbackConfig

	// BroadcastWorkerCount is the number of broker publish workers.
	// 0 (default) uses synchronous mode: publish runs in the Emit goroutine.
	// Any positive value enables the worker pool: Emit enqueues jobs and workers publish.
	BroadcastWorkerCount int

	// BroadcastQueueSize is the capacity of the broadcast job queue.
	// Only used when BroadcastWorkerCount > 0. Defaults to 100 when unset.
	BroadcastQueueSize int

	Topics     map[string]TopicConfig
	Default    TopicConfig
	WorkerPool int
	BufferSize int
}

// resolvedBroker pairs a broker name with its adapter so the name can be
// included in FailedPublish when a publish error occurs.
type resolvedBroker struct {
	name    string
	adapter broker.Adapter
}

func NewEventBus(cfg Config) EventBus {
	var local *LocalBus

	if cfg.WorkerPool > 0 {
		local = NewLocalBus(cfg.WorkerPool, cfg.BufferSize)
	}

	var broadcastQueue chan broadcastJob
	if cfg.BroadcastWorkerCount > 0 {
		queueSize := cfg.BroadcastQueueSize
		if queueSize <= 0 {
			queueSize = 100
		}
		broadcastQueue = make(chan broadcastJob, queueSize)
	}

	defaultCfg := cfg.Default
	if !defaultCfg.EmitToLocal && !defaultCfg.EmitToBroker {
		defaultCfg.EmitToLocal = true
		defaultCfg.ConsumeFromLocal = true
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &eventBus{
		local:                local,
		broker:               cfg.Broker,
		brokers:              cfg.Brokers,
		fallback:             cfg.Fallback,
		broadcastQueue:       broadcastQueue,
		broadcastWorkerCount: cfg.BroadcastWorkerCount,
		topicConfigs:         cfg.Topics,
		defaultCfg:           defaultCfg,
		ctx:                  ctx,
		cancel:               cancel,
	}
}

func (e *eventBus) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		return nil
	}

	connected := map[broker.Adapter]bool{}

	if e.broker != nil {
		if err := e.broker.Connect(); err != nil {
			return err
		}
		connected[e.broker] = true
	}

	for name, b := range e.brokers {
		if connected[b] {
			continue
		}
		if err := b.Connect(); err != nil {
			return fmt.Errorf("eventbus: connect broker %q: %w", name, err)
		}
		connected[b] = true
	}

	for i := 0; i < e.broadcastWorkerCount; i++ {
		go e.runBroadcastWorker()
	}

	e.started = true
	return nil
}

// Emit delivers msg to the topic, running local delivery then broker dispatch.
func (e *eventBus) Emit(ctx context.Context, topic string, msg Message) error {
	cfg := e.getTopicConfig(topic)

	if err := e.runBeforePublish(ctx, topic, msg, cfg); err != nil {
		return err
	}

	if err := e.broadcast(topic, msg, cfg); err != nil {
		return err
	}

	e.runAfterPublish(ctx, topic, msg, cfg)
	return nil
}

func (e *eventBus) Subscribe(topic string, handler Handler) {
	cfg := e.getTopicConfig(topic)

	// applyRetry wraps only the handler; applyMiddleware wraps the retry loop.
	// Order: BeforeHandle → [retry(handler)] → AfterHandle
	withRetry := applyRetry(handler, cfg.Retry, e.fallback, topic)
	wrapped := applyMiddleware(withRetry, cfg.Middleware)

	if cfg.ConsumeFromLocal && e.local != nil {
		e.local.Subscribe(topic, wrapped)
	}

	if cfg.ConsumeFromBroker && e.broker != nil {
		go func() {
			_ = e.broker.Subscribe(e.ctx, topic, wrapped)
		}()
	}
}

func (e *eventBus) Close() error {
	if e.cancel != nil {
		e.cancel()
	}

	disconnected := map[broker.Adapter]bool{}

	for _, b := range e.brokers {
		if disconnected[b] {
			continue
		}
		_ = b.Disconnect()
		disconnected[b] = true
	}

	if e.broker != nil && !disconnected[e.broker] {
		return e.broker.Disconnect()
	}
	return nil
}

// getTopicConfig returns the per-topic config, falling back to the default.
func (e *eventBus) getTopicConfig(topic string) TopicConfig {
	if cfg, ok := e.topicConfigs[topic]; ok {
		return cfg
	}
	return e.defaultCfg
}

// runBeforePublish handles in-process (local) delivery before broker dispatch.
func (e *eventBus) runBeforePublish(ctx context.Context, topic string, msg Message, cfg TopicConfig) error {
	if !cfg.EmitToLocal || e.local == nil {
		return nil
	}
	if err := e.local.Emit(ctx, topic, msg); err != nil {
		return fmt.Errorf("local emit failed: %w", err)
	}
	return nil
}

// broadcast routes the message to the appropriate broker(s) based on TopicConfig.
// Named brokers are resolved synchronously so routing errors surface before any goroutine is spawned.
func (e *eventBus) broadcast(topic string, msg Message, cfg TopicConfig) error {
	if !cfg.EmitToBroker {
		return nil
	}

	if len(cfg.Brokers) == 0 {
		e.publishToDefaultBroker(topic, msg)
		return nil
	}

	targets, err := e.resolveBrokers(cfg.Brokers)
	if err != nil {
		return err
	}

	for _, rb := range targets {
		e.publishToBroker(rb.adapter, rb.name, topic, msg)
	}
	return nil
}

// runAfterPublish is the extension point for post-publish hooks (metrics, logging, middleware).
func (e *eventBus) runAfterPublish(_ context.Context, _ string, _ Message, _ TopicConfig) {}

// publishToDefaultBroker sends asynchronously via Config.Broker (backward-compat path).
func (e *eventBus) publishToDefaultBroker(topic string, msg Message) {
	if e.broker == nil {
		return
	}
	e.publishToBroker(e.broker, "default", topic, msg)
}

// publishToBroker routes a single publish to the worker pool or runs it synchronously.
// Worker pool mode: job is enqueued; a bounded set of workers performs the actual publish.
// Synchronous mode (BroadcastWorkerCount == 0): publish runs in the caller's goroutine.
func (e *eventBus) publishToBroker(b broker.Adapter, name string, topic string, msg Message) {
	job := broadcastJob{adapter: b, name: name, topic: topic, msg: msg}

	if e.broadcastQueue != nil {
		select {
		case e.broadcastQueue <- job:
		case <-e.ctx.Done():
		}
		return
	}

	e.publishSync(job)
}

// handlePublishError forwards a failed publish to the FallbackAdapter when configured.
// If no fallback is set, the error is silently dropped — async publish cannot surface it to the caller.
func (e *eventBus) handlePublishError(ctx context.Context, f FailedPublish) {
	if e.fallback == nil || e.fallback.Adapter == nil {
		return
	}
	_ = e.fallback.Adapter.Store(ctx, f)
}

// resolveBrokers looks up each named broker from e.brokers.
// Returns a descriptive error if any name is not found.
func (e *eventBus) resolveBrokers(names []string) ([]resolvedBroker, error) {
	out := make([]resolvedBroker, 0, len(names))
	for _, name := range names {
		b, ok := e.brokers[name]
		if !ok {
			return nil, fmt.Errorf("eventbus: broker %q not found in Config.Brokers", name)
		}
		out = append(out, resolvedBroker{name: name, adapter: b})
	}
	return out, nil
}
