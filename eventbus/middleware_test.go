package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"

	broker "github.com/budimanlai/go-message-broker"
)

// ---------------------------------------------------------------------------
// mockMiddleware
// ---------------------------------------------------------------------------

type mockMiddleware struct {
	mu          sync.Mutex
	beforeCalls int
	afterCalls  int
	afterErrors []error
	beforeErr   error
}

func (m *mockMiddleware) BeforeHandle(_ context.Context, _ Message) error {
	m.mu.Lock()
	m.beforeCalls++
	m.mu.Unlock()
	return m.beforeErr
}

func (m *mockMiddleware) AfterHandle(_ context.Context, _ Message, err error) {
	m.mu.Lock()
	m.afterCalls++
	m.afterErrors = append(m.afterErrors, err)
	m.mu.Unlock()
}

func (m *mockMiddleware) beforeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.beforeCalls
}

func (m *mockMiddleware) afterCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.afterCalls
}

// orderMiddleware tracks execution sequence by appending named entries to a shared slice.
type orderMiddleware struct {
	name  string
	order *[]string
	mu    *sync.Mutex
}

func (m *orderMiddleware) BeforeHandle(_ context.Context, _ Message) error {
	m.mu.Lock()
	*m.order = append(*m.order, m.name+".before")
	m.mu.Unlock()
	return nil
}

func (m *orderMiddleware) AfterHandle(_ context.Context, _ Message, _ error) {
	m.mu.Lock()
	*m.order = append(*m.order, m.name+".after")
	m.mu.Unlock()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestMiddleware_NoMiddleware: applyMiddleware dengan slice kosong mengembalikan handler asli.
func TestMiddleware_NoMiddleware(t *testing.T) {
	called := false
	handler := func(_ context.Context, _ Message) error {
		called = true
		return nil
	}

	wrapped := applyMiddleware(handler, nil)

	msg := broker.NewMessage("test", []byte(`{}`))
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler seharusnya dipanggil")
	}
}

// TestMiddleware_BeforeAndAfterCalled: BeforeHandle dan AfterHandle dipanggil masing-masing sekali.
func TestMiddleware_BeforeAndAfterCalled(t *testing.T) {
	handler := func(_ context.Context, _ Message) error { return nil }
	mw := &mockMiddleware{}

	wrapped := applyMiddleware(handler, []Middleware{mw})

	msg := broker.NewMessage("test", []byte(`{}`))
	wrapped(context.Background(), msg)

	if mw.beforeCount() != 1 {
		t.Errorf("BeforeHandle: got %d, want 1", mw.beforeCount())
	}
	if mw.afterCount() != 1 {
		t.Errorf("AfterHandle: got %d, want 1", mw.afterCount())
	}
}

// TestMiddleware_AfterHandleReceivesHandlerError: AfterHandle menerima error dari handler.
func TestMiddleware_AfterHandleReceivesHandlerError(t *testing.T) {
	handlerErr := errors.New("handler failed")
	handler := func(_ context.Context, _ Message) error { return handlerErr }
	mw := &mockMiddleware{}

	wrapped := applyMiddleware(handler, []Middleware{mw})

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != handlerErr {
		t.Errorf("return error: got %v, want %v", err, handlerErr)
	}

	mw.mu.Lock()
	afterErr := mw.afterErrors[0]
	mw.mu.Unlock()

	if afterErr != handlerErr {
		t.Errorf("AfterHandle error: got %v, want %v", afterErr, handlerErr)
	}
}

// TestMiddleware_BeforeHandleError_StopsProcessing: BeforeHandle error → handler dan AfterHandle tidak dipanggil.
func TestMiddleware_BeforeHandleError_StopsProcessing(t *testing.T) {
	handlerCalled := false
	handler := func(_ context.Context, _ Message) error {
		handlerCalled = true
		return nil
	}

	beforeErr := errors.New("unauthorized")
	mw := &mockMiddleware{beforeErr: beforeErr}

	wrapped := applyMiddleware(handler, []Middleware{mw})

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err != beforeErr {
		t.Errorf("return error: got %v, want %v", err, beforeErr)
	}
	if handlerCalled {
		t.Error("handler tidak seharusnya dipanggil saat BeforeHandle error")
	}
	if mw.afterCount() > 0 {
		t.Error("AfterHandle tidak seharusnya dipanggil saat BeforeHandle error")
	}
}

// TestMiddleware_MultipleMiddleware_Order: urutan eksekusi BeforeHandle → handler → AfterHandle.
func TestMiddleware_MultipleMiddleware_Order(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handler := func(_ context.Context, _ Message) error {
		mu.Lock()
		order = append(order, "handler")
		mu.Unlock()
		return nil
	}

	mw1 := &orderMiddleware{name: "mw1", order: &order, mu: &mu}
	mw2 := &orderMiddleware{name: "mw2", order: &order, mu: &mu}

	wrapped := applyMiddleware(handler, []Middleware{mw1, mw2})

	msg := broker.NewMessage("test", []byte(`{}`))
	wrapped(context.Background(), msg)

	want := []string{"mw1.before", "mw2.before", "handler", "mw1.after", "mw2.after"}
	if len(order) != len(want) {
		t.Fatalf("execution order: got %v, want %v", order, want)
	}
	for i, v := range want {
		if order[i] != v {
			t.Errorf("step %d: got %q, want %q", i, order[i], v)
		}
	}
}

// TestMiddleware_SecondBeforeHandleError_HandlerNotCalled: middleware kedua gagal → handler tidak dipanggil,
// AfterHandle tidak dipanggil untuk keduanya.
func TestMiddleware_SecondBeforeHandleError_HandlerNotCalled(t *testing.T) {
	handlerCalled := false
	handler := func(_ context.Context, _ Message) error {
		handlerCalled = true
		return nil
	}

	mw1 := &mockMiddleware{}                                         // sukses
	mw2 := &mockMiddleware{beforeErr: errors.New("rate limited")} // gagal

	wrapped := applyMiddleware(handler, []Middleware{mw1, mw2})

	msg := broker.NewMessage("test", []byte(`{}`))
	err := wrapped(context.Background(), msg)

	if err == nil {
		t.Error("expected error, got nil")
	}
	if handlerCalled {
		t.Error("handler tidak seharusnya dipanggil")
	}
	if mw1.beforeCount() != 1 {
		t.Errorf("mw1 BeforeHandle: got %d, want 1", mw1.beforeCount())
	}
	if mw2.beforeCount() != 1 {
		t.Errorf("mw2 BeforeHandle: got %d, want 1", mw2.beforeCount())
	}
	if mw1.afterCount() > 0 {
		t.Errorf("mw1 AfterHandle tidak seharusnya dipanggil, got %d", mw1.afterCount())
	}
	if mw2.afterCount() > 0 {
		t.Errorf("mw2 AfterHandle tidak seharusnya dipanggil, got %d", mw2.afterCount())
	}
}
