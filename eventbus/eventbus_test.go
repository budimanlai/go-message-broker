package eventbus

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	broker "github.com/budimanlai/go-message-broker"
)

// mockBroker records every Publish call and signals via WaitGroup so tests can
// synchronize with the async goroutines spawned by Emit.
type mockBroker struct {
	mu        sync.Mutex
	published []broker.Message

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

// TestEmit_DefaultBroker: TopicConfig.Brokers adalah kosong → menggunakan Config.Broker.
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
		t.Errorf("jumlah Publish: got %d, want 1", mock.count())
	}
}

// TestEmit_OneBroker: TopicConfig.Brokers = ["redis"] → hanya redis yang menerima pesan.
func TestEmit_OneBroker(t *testing.T) {
	redisMock := &mockBroker{}
	redisMock.wg.Add(1)

	eb := NewEventBus(Config{
		Brokers: map[string]broker.Adapter{
			"redis": redisMock,
		},
		Topics: map[string]TopicConfig{
			"cache.clear": {
				EmitToBroker: true,
				Brokers:      []string{"redis"},
			},
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

// TestEmit_MultipleBrokers: TopicConfig.Brokers = ["rabbit","redis"] → keduanya menerima pesan.
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
			"telemetry.data": {
				EmitToBroker: true,
				Brokers:      []string{"rabbit", "redis"},
			},
		},
	})

	msg := broker.NewMessage("telemetry.data", []byte(`{"speed":80}`))
	if err := eb.Emit(context.Background(), "telemetry.data", msg); err != nil {
		t.Fatalf("Emit gagal: %v", err)
	}

	if !waitWithTimeout(&rabbitMock.wg, 2*time.Second) {
		t.Fatal("timeout: rabbit Publish tidak dipanggil dalam 2 detik")
	}
	if !waitWithTimeout(&redisMock.wg, 2*time.Second) {
		t.Fatal("timeout: redis Publish tidak dipanggil dalam 2 detik")
	}

	if rabbitMock.count() != 1 {
		t.Errorf("rabbit Publish count: got %d, want 1", rabbitMock.count())
	}
	if redisMock.count() != 1 {
		t.Errorf("redis Publish count: got %d, want 1", redisMock.count())
	}
}

// TestEmit_UnknownBroker: TopicConfig.Brokers berisi nama yang tidak ada → harus return error.
func TestEmit_UnknownBroker(t *testing.T) {
	eb := NewEventBus(Config{
		Brokers: map[string]broker.Adapter{
			"redis": &mockBroker{},
		},
		Topics: map[string]TopicConfig{
			"some.event": {
				EmitToBroker: true,
				Brokers:      []string{"elastic"}, // tidak terdaftar
			},
		},
	})

	msg := broker.NewMessage("some.event", []byte(`{}`))
	err := eb.Emit(context.Background(), "some.event", msg)
	if err == nil {
		t.Fatal("Emit harus return error untuk broker yang tidak dikenal, tapi tidak ada error")
	}
	if !strings.Contains(err.Error(), "elastic") {
		t.Errorf("pesan error harus menyebut nama broker yang tidak ditemukan, got: %q", err.Error())
	}
}
