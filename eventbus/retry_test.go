package eventbus

import (
	"context"
	"errors"
	"testing"

	broker "github.com/budimanlai/go-message-broker"
)

// TestRetry_NilPolicy: policy nil → handler dikembalikan as-is, error handler diteruskan.
func TestRetry_NilPolicy(t *testing.T) {
	callCount := 0
	wantErr := errors.New("fail")
	handler := func(_ context.Context, _ Message) error {
		callCount++
		return wantErr
	}

	wrapped := applyRetry(handler, nil, nil, "test")

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != wantErr {
		t.Errorf("error: got %v, want %v", err, wantErr)
	}
	if callCount != 1 {
		t.Errorf("handler dipanggil %d kali, want 1", callCount)
	}
}

// TestRetry_SuccessFirstAttempt: sukses di attempt pertama → tidak ada retry.
func TestRetry_SuccessFirstAttempt(t *testing.T) {
	callCount := 0
	handler := func(_ context.Context, _ Message) error {
		callCount++
		return nil
	}

	policy := &RetryPolicy{MaxRetry: 3}
	wrapped := applyRetry(handler, policy, nil, "test")

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("handler dipanggil %d kali, want 1", callCount)
	}
}

// TestRetry_SuccessOnRetry: gagal di attempt 1 dan 2, sukses di attempt 3.
func TestRetry_SuccessOnRetry(t *testing.T) {
	callCount := 0
	handler := func(_ context.Context, _ Message) error {
		callCount++
		if callCount < 3 {
			return errors.New("temporary")
		}
		return nil
	}

	policy := &RetryPolicy{MaxRetry: 3}
	wrapped := applyRetry(handler, policy, nil, "test")

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
	if callCount != 3 {
		t.Errorf("handler dipanggil %d kali, want 3", callCount)
	}
}

// TestRetry_Exhausted_CallsFallback: semua attempt gagal → FallbackAdapter.Store dipanggil, return nil.
func TestRetry_Exhausted_CallsFallback(t *testing.T) {
	wantErr := errors.New("always fails")
	callCount := 0
	handler := func(_ context.Context, _ Message) error {
		callCount++
		return wantErr
	}

	fallback := &mockFallbackAdapter{}
	// MaxRetry=2 → total 3 attempts (attempt 0, 1, 2)
	policy := &RetryPolicy{MaxRetry: 2}

	wrapped := applyRetry(handler, policy, &FallbackConfig{Adapter: fallback}, "order.created")

	msg := broker.NewMessage("order.created", []byte(`{"id":1}`))
	err := wrapped(context.Background(), msg)

	// Setelah exhaustion, return nil sehingga broker mengACK pesan
	if err != nil {
		t.Errorf("expected nil setelah exhaustion, got %v", err)
	}
	if callCount != 3 {
		t.Errorf("handler dipanggil %d kali, want 3", callCount)
	}
	if fallback.count() != 1 {
		t.Errorf("fallback.Store dipanggil %d kali, want 1", fallback.count())
	}

	f := fallback.first()
	if f.Error != wantErr {
		t.Errorf("FailedPublish.Error: got %v, want %v", f.Error, wantErr)
	}
	if f.Topic != "order.created" {
		t.Errorf("FailedPublish.Topic: got %q, want %q", f.Topic, "order.created")
	}
	if f.Time.IsZero() {
		t.Error("FailedPublish.Time seharusnya tidak zero")
	}
}

// TestRetry_Exhausted_NoFallback: semua attempt gagal, tidak ada fallback → tidak panic, return nil.
func TestRetry_Exhausted_NoFallback(t *testing.T) {
	handler := func(_ context.Context, _ Message) error {
		return errors.New("fail")
	}

	policy := &RetryPolicy{MaxRetry: 1}
	wrapped := applyRetry(handler, policy, nil, "test")

	msg := broker.NewMessage("test", []byte(`{}`))

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("unexpected panic: %v", r)
		}
	}()

	err := wrapped(context.Background(), msg)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

// TestRetry_MaxRetryZero: MaxRetry=0 diperlakukan sebagai "tidak ada retry" → handler dikembalikan as-is.
func TestRetry_MaxRetryZero(t *testing.T) {
	callCount := 0
	wantErr := errors.New("fail")
	handler := func(_ context.Context, _ Message) error {
		callCount++
		return wantErr
	}

	// MaxRetry <= 0 → applyRetry mengembalikan handler original tanpa wrapping
	policy := &RetryPolicy{MaxRetry: 0}
	wrapped := applyRetry(handler, policy, nil, "test")

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != wantErr {
		t.Errorf("error: got %v, want %v", err, wantErr)
	}
	if callCount != 1 {
		t.Errorf("handler dipanggil %d kali, want 1", callCount)
	}
}
