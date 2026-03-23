package gomessagebroker

import "context"

type Queue interface {
	Push(ctx context.Context, queue string, msg []byte) error
	Consume(ctx context.Context, queue string, handler Handler) error
}
