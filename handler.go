package gomessagebroker

import "context"

type Handler func(ctx context.Context, msg Message) error
