package eventbus

import "context"

// Middleware wraps a handler with BeforeHandle and AfterHandle hooks.
//
// Execution order:
//
//	BeforeHandle → Handler → AfterHandle
//
// If BeforeHandle returns an error, processing stops immediately:
// the handler and AfterHandle are not called.
type Middleware interface {
	BeforeHandle(ctx context.Context, msg Message) error
	AfterHandle(ctx context.Context, msg Message, err error)
}

// applyMiddleware wraps handler with the given middleware slice.
// Middleware runs in slice order: first in, first executed.
// Returns the original handler unchanged when middlewares is empty.
func applyMiddleware(handler Handler, middlewares []Middleware) Handler {
	if len(middlewares) == 0 {
		return handler
	}
	return func(ctx context.Context, msg Message) error {
		for _, mw := range middlewares {
			if err := mw.BeforeHandle(ctx, msg); err != nil {
				return err
			}
		}

		handlerErr := handler(ctx, msg)

		for _, mw := range middlewares {
			mw.AfterHandle(ctx, msg, handlerErr)
		}
		return handlerErr
	}
}
