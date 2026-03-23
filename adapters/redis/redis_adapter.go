package redis

import (
	"context"
	"encoding/json"
	"fmt"

	broker "github.com/budimanlai/go-message-broker"
	"github.com/go-redis/redis/v8"
)

type RedisAdapter struct {
	client   *redis.Client
	opts     *redis.Options
	isShared bool
}

// Ensure RedisAdapter implements broker.Broker
var _ broker.Broker = (*RedisAdapter)(nil)

func NewRedisAdapter(config map[string]interface{}) (broker.Broker, error) {
	if client, ok := config["redis_client"].(*redis.Client); ok {
		return &RedisAdapter{
			client:   client,
			isShared: true,
		}, nil
	}

	addr, _ := config["addr"].(string)
	password, _ := config["password"].(string)
	db, _ := config["db"].(int)

	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	return &RedisAdapter{
		opts: opts,
	}, nil
}

func (r *RedisAdapter) Connect() error {
	if r.isShared {
		return r.client.Ping(context.Background()).Err()
	}
	r.client = redis.NewClient(r.opts)
	return r.client.Ping(context.Background()).Err()
}

func (r *RedisAdapter) Disconnect() error {
	if r.isShared {
		return nil
	}
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

func (r *RedisAdapter) Publish(ctx context.Context, topic string, msg broker.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return r.client.Publish(ctx, topic, data).Err()
}

func (r *RedisAdapter) Subscribe(ctx context.Context, topic string, handler broker.Handler) error {
	pubsub := r.client.Subscribe(ctx, topic)

	// Wait for confirmation that subscription is created
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return err
	}

	// Process messages in a background goroutine
	go func() {
		ch := pubsub.Channel()
		for msg := range ch {
			var m broker.Message
			if err := json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				// In a real app, perhaps log this error or send to a DLQ
				fmt.Printf("Error unmarshaling message: %v\n", err)
				continue
			}

			// Call the handler
			if err := handler(ctx, m); err != nil {
				fmt.Printf("Handler error: %v\n", err)
			}
		}
	}()

	return nil
}

func init() {
	broker.Register("redis", NewRedisAdapter)
}
