package gomessagebroker

import "context"

type Broker interface {
	Connect() error
	Disconnect() error
	Publish(ctx context.Context, topic string, msg Message) error
	Subscribe(ctx context.Context, topic string, handler Handler) error
}

// Adapter is an alias for Broker, used in multi-broker configurations.
type Adapter = Broker
