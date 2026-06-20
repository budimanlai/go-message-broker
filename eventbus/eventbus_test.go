package eventbus

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	broker "github.com/budimanlai/go-message-broker"
)

// ---------------------------------------------------------------------------
// Shared mocks
// ---------------------------------------------------------------------------

// mockBroker records every Publish call. Set publishErr to simulate failures.
type mockBroker struct {
	mu         sync.Mutex
	published  []broker.Message
	publishErr error

	// Add wg.Add(n) before calling Emit so the test can wait on wg.Wait().
	wg sync.WaitGroup
}

func (m *mockBroker) Connect() error    { return nil }
func (m *mockBroker) Disconnect() error { return nil }
func (m *mockBroker) Subscribe(_ context.Context, _ string, _ broker.Handler) error {
	return nil
}
func (m *mockBroker) Publish(_ context.Context, _ string, msg broker.Message) error {
	defer m.wg.Done()
	if m.publishErr != nil {
		return m.publishErr
	}
	m.mu.Lock()
	m.published = append(m.published, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockBroker) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.published)
}

// mockFallbackAdapter records every Store call.
type mockFallbackAdapter struct {
	mu     sync.Mutex
	stored []FailedPublish
}

func (m *mockFallbackAdapter) Store(_ context.Context, f FailedPublish) error {
	m.mu.Lock()
	m.stored = append(m.stored, f)
	m.mu.Unlock()
	return nil
}

func (m *mockFallbackAdapter) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.stored)
}

func (m *mockFallbackAdapter) first() FailedPublish {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stored[0]
}

// waitWithTimeout blocks until the WaitGroup is done or the deadline passes.
func waitWithTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	ch := make(chan struct{})
	go func() { wg.Wait(); close(ch) }()
	select {
	case <-ch:
		return true
	case <-time.After(d):
		return false
	}
}

// ---------------------------------------------------------------------------
// Emit routing tests
// ---------------------------------------------------------------------------

// TestEmit_DefaultBroker: TopicConfig.Brokers kosong → menggunakan Config.Broker.
func TestEmit_DefaultBroker(t *testing.T) {
	mock := &mockBroker{}
	mock.wg.Add(1)

	eb := NewEventBus(Config{
		Broker: mock,
		Topics: map[string]TopicConfig{
			"order.created": {EmitToBroker: true},
		},
	})

	msg := broker.NewMessage("order.created", []byte(`{"id":1}`))
	if err := eb.Emit(context.Background(), "order.created", msg); err != nil {
		t.Fatalf("Emit gagal: %v", err)
	}

	if !waitWithTimeout(&mock.wg, 2*time.Second) {
		t.Fatal("timeout: Publish tidak dipanggil dalam 2 detik")
	}
	if mock.count() != 1 {
		t.Errorf("Publish count: got %d, want 1", mock.count())
	}
}

// TestEmit_OneBroker: TopicConfig.Brokers = ["redis"] → hanya redis yang menerima.
func TestEmit_OneBroker(t *testing.T) {
	redisMock := &mockBroker{}
	redisMock.wg.Add(1)

	eb := NewEventBus(Config{
		Brokers: map[string]broker.Adapter{"redis": redisMock},
		Topics: map[string]TopicConfig{
			"cache.clear": {EmitToBroker: true, Brokers: []string{"redis"}},
		},
	})

	msg := broker.NewMessage("cache.clear", []byte(`{"key":"all"}`))
	if err := eb.Emit(context.Background(), "cache.clear", msg); err != nil {
		t.Fatalf("Emit gagal: %v", err)
	}

	if !waitWithTimeout(&redisMock.wg, 2*time.Second) {
		t.Fatal("timeout: Publish tidak dipanggil dalam 2 detik")
	}
	if redisMock.count() != 1 {
		t.Errorf("redis Publish count: got %d, want 1", redisMock.count())
	}
}

// TestEmit_MultipleBrokers: Brokers = ["rabbit","redis"] → keduanya menerima.
func TestEmit_MultipleBrokers(t *testing.T) {
	rabbitMock := &mockBroker{}
	redisMock := &mockBroker{}
	rabbitMock.wg.Add(1)
	redisMock.wg.Add(1)

	eb := NewEventBus(Config{
		Brokers: map[string]broker.Adapter{
			"rabbit": rabbitMock,
			"redis":  redisMock,
		},
		Topics: map[string]TopicConfig{
			"telemetry.data": {EmitToBroker: true, Brokers: []string{"rabbit", "redis"}},
		},
	})

	msg := broker.NewMessage("telemetry.data", []byte(`{"speed":80}`))
	if err := eb.Emit(context.Background(), "telemetry.data", msg); err != nil {
		t.Fatalf("Emit gagal: %v", err)
	}

	if !waitWithTimeout(&rabbitMock.wg, 2*time.Second) {
		t.Fatal("timeout: rabbit Publish tidak dipanggil")
	}
	if !waitWithTimeout(&redisMock.wg, 2*time.Second) {
		t.Fatal("timeout: redis Publish tidak dipanggil")
	}
	if rabbitMock.count() != 1 {
		t.Errorf("rabbit count: got %d, want 1", rabbitMock.count())
	}
	if redisMock.count() != 1 {
		t.Errorf("redis count: got %d, want 1", redisMock.count())
	}
}

// TestEmit_UnknownBroker: nama broker tidak ada → harus return error.
func TestEmit_UnknownBroker(t *testing.T) {
	eb := NewEventBus(Config{
		Brokers: map[string]broker.Adapter{"redis": &mockBroker{}},
		Topics: map[string]TopicConfig{
			"some.event": {EmitToBroker: true, Brokers: []string{"elastic"}},
		},
	})

	msg := broker.NewMessage("some.event", []byte(`{}`))
	err := eb.Emit(context.Background(), "some.event", msg)
	if err == nil {
		t.Fatal("harus return error untuk broker yang tidak dikenal")
	}
	if !strings.Contains(err.Error(), "elastic") {
		t.Errorf("error harus menyebut nama broker, got: %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Fallback publish tests
// ---------------------------------------------------------------------------

// TestFallback_StoreCalledOnPublishError: broker gagal → FallbackAdapter.Store dipanggil.
func TestFallback_StoreCalledOnPublishError(t *testing.T) {
	failingBroker := &mockBroker{publishErr: errors.New("connection refused")}
	failingBroker.wg.Add(1)
	fallback := &mockFallbackAdapter{}

	eb := NewEventBus(Config{
		Broker:   failingBroker,
		Fallback: &FallbackConfig{Adapter: fallback},
		Topics:   map[string]TopicConfig{"order.fail": {EmitToBroker: true}},
	})

	msg := broker.NewMessage("order.fail", []byte(`{}`))
	if err := eb.Emit(context.Background(), "order.fail", msg); err != nil {
		t.Fatalf("Emit seharusnya tidak error: %v", err)
	}

	waitWithTimeout(&failingBroker.wg, 2*time.Second)

	if fallback.count() != 1 {
		t.Errorf("fallback.Store count: got %d, want 1", fallback.count())
	}
}

// TestFallback_NilFallback_NoError: broker gagal, tidak ada fallback → tidak panic.
func TestFallback_NilFallback_NoError(t *testing.T) {
	failingBroker := &mockBroker{publishErr: errors.New("down")}
	failingBroker.wg.Add(1)

	eb := NewEventBus(Config{
		Broker:  failingBroker,
		Topics:  map[string]TopicConfig{"test": {EmitToBroker: true}},
		// Fallback: nil (default)
	})

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	msg := broker.NewMessage("test", []byte(`{}`))
	eb.Emit(context.Background(), "test", msg)
	waitWithTimeout(&failingBroker.wg, 2*time.Second)
}

// TestFallback_FailedPublishFields: verifikasi field Topic, Broker, Error, Time terisi benar.
func TestFallback_FailedPublishFields(t *testing.T) {
	const wantTopic = "invoice.sent"
	const wantBroker = "default"
	wantErr := errors.New("timeout")

	failingBroker := &mockBroker{publishErr: wantErr}
	failingBroker.wg.Add(1)
	fallback := &mockFallbackAdapter{}

	eb := NewEventBus(Config{
		Broker:   failingBroker,
		Fallback: &FallbackConfig{Adapter: fallback},
		Topics:   map[string]TopicConfig{wantTopic: {EmitToBroker: true}},
	})

	msg := broker.NewMessage(wantTopic, []byte(`{"amount":100}`))
	eb.Emit(context.Background(), wantTopic, msg)
	waitWithTimeout(&failingBroker.wg, 2*time.Second)

	if fallback.count() == 0 {
		t.Fatal("fallback.Store tidak dipanggil")
	}
	f := fallback.first()
	if f.Topic != wantTopic {
		t.Errorf("Topic: got %q, want %q", f.Topic, wantTopic)
	}
	if f.Broker != wantBroker {
		t.Errorf("Broker: got %q, want %q", f.Broker, wantBroker)
	}
	if f.Error != wantErr {
		t.Errorf("Error: got %v, want %v", f.Error, wantErr)
	}
	if f.Time.IsZero() {
		t.Error("Time seharusnya tidak zero")
	}
}

// ---------------------------------------------------------------------------
// Worker pool test
// ---------------------------------------------------------------------------

// TestWorkerPool_ProcessesJobs: BroadcastWorkerCount > 0 → worker memproses job dari queue.
func TestWorkerPool_ProcessesJobs(t *testing.T) {
	mock := &mockBroker{}
	mock.wg.Add(1)

	eb := NewEventBus(Config{
		Broker:               mock,
		BroadcastWorkerCount: 2,
		BroadcastQueueSize:   10,
		Topics: map[string]TopicConfig{
			"order.placed": {EmitToBroker: true},
		},
	})

	if err := eb.Start(context.Background()); err != nil {
		t.Fatalf("Start() gagal: %v", err)
	}
	defer eb.Close()

	msg := broker.NewMessage("order.placed", []byte(`{"id":99}`))
	if err := eb.Emit(context.Background(), "order.placed", msg); err != nil {
		t.Fatalf("Emit() gagal: %v", err)
	}

	if !waitWithTimeout(&mock.wg, 2*time.Second) {
		t.Fatal("timeout: worker tidak memproses job dalam 2 detik")
	}
	if mock.count() != 1 {
		t.Errorf("Publish count: got %d, want 1", mock.count())
	}
}
