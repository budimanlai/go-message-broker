package eventbus

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	broker "github.com/budimanlai/go-message-broker"
)

// TestLocalBus_Delivery: pesan yang di-Emit diterima oleh handler subscriber.
func TestLocalBus_Delivery(t *testing.T) {
	lb := NewLocalBus(2, 10)
	defer lb.Close()

	var wg sync.WaitGroup
	wg.Add(1)

	lb.Subscribe("test.event", func(_ context.Context, msg broker.Message) error {
		wg.Done()
		return nil
	})

	msg := broker.NewMessage("test.event", []byte(`{"data":"hello"}`))
	if err := lb.Emit(context.Background(), "test.event", msg); err != nil {
		t.Fatalf("Emit gagal: %v", err)
	}

	if !waitWithTimeout(&wg, 2*time.Second) {
		t.Fatal("timeout: handler tidak dipanggil dalam 2 detik")
	}
}

// TestLocalBus_Close_StopsWorkers: Close() menghentikan semua goroutine worker.
// Jika goroutine bocor, Close() tidak akan pernah return dan test akan timeout.
func TestLocalBus_Close_StopsWorkers(t *testing.T) {
	lb := NewLocalBus(4, 10)

	// Subscribe di beberapa topic untuk spawn banyak goroutine
	for _, topic := range []string{"topic.a", "topic.b", "topic.c"} {
		lb.Subscribe(topic, func(_ context.Context, _ broker.Message) error {
			return nil
		})
	}

	done := make(chan struct{})
	go func() {
		lb.Close()
		close(done)
	}()

	select {
	case <-done:
		// Close() return → semua worker berhenti dengan benar
	case <-time.After(3 * time.Second):
		t.Fatal("Close() tidak return dalam 3 detik — kemungkinan goroutine leak")
	}
}

// TestLocalBus_BufferFull_ReturnsError: ketika buffer penuh, Emit mengembalikan error.
func TestLocalBus_BufferFull_ReturnsError(t *testing.T) {
	// workerPool=1, bufferSize=1 → channel kapasitas 1, satu goroutine worker
	lb := NewLocalBus(1, 1)

	ready := make(chan struct{})
	block := make(chan struct{})

	lb.Subscribe("slow.topic", func(_ context.Context, _ broker.Message) error {
		// Sinyal bahwa worker sedang memproses
		select {
		case ready <- struct{}{}:
		default:
		}
		// Blokir sampai test memberi sinyal lanjut
		<-block
		return nil
	})

	msg := broker.NewMessage("slow.topic", []byte(`{}`))

	// Pesan 1: worker langsung mengambilnya dan masuk ke handler (blocking)
	if err := lb.Emit(context.Background(), "slow.topic", msg); err != nil {
		t.Fatalf("Emit 1 gagal: %v", err)
	}

	// Tunggu worker masuk ke handler
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: worker tidak mulai dalam 2 detik")
	}

	// Pesan 2: masuk ke buffer (worker masih blocking, kapasitas 1)
	if err := lb.Emit(context.Background(), "slow.topic", msg); err != nil {
		t.Fatalf("Emit 2 gagal: %v", err)
	}

	// Pesan 3: buffer penuh → harus error
	err := lb.Emit(context.Background(), "slow.topic", msg)
	if err == nil {
		t.Error("expected error saat buffer penuh, got nil")
	}
	if !strings.Contains(err.Error(), "buffer full") {
		t.Errorf("error harus menyebut 'buffer full', got: %q", err.Error())
	}

	// Buka blokir worker dan cleanup
	close(block)
	lb.Close()
}
