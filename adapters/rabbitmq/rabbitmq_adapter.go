package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	broker "github.com/budimanlai/go-message-broker"
	"github.com/streadway/amqp"
)

type RabbitMQAdapter struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	url      string
	isShared bool
}

// Ensure RabbitMQAdapter implements broker.Broker
var _ broker.Broker = (*RabbitMQAdapter)(nil)

func NewRabbitMQAdapter(config map[string]interface{}) (broker.Broker, error) {
	if conn, ok := config["amqp_connection"].(*amqp.Connection); ok {
		return &RabbitMQAdapter{
			conn:     conn,
			isShared: true,
		}, nil
	}

	url, ok := config["url"].(string)
	if !ok {
		return nil, fmt.Errorf("rabbitmq url required in config")
	}
	return &RabbitMQAdapter{url: url}, nil
}

func (r *RabbitMQAdapter) Connect() error {
	var err error
	if !r.isShared {
		r.conn, err = amqp.Dial(r.url)
		if err != nil {
			return err
		}
	} else if r.conn == nil || r.conn.IsClosed() {
		return fmt.Errorf("shared connection is closed or nil")
	}

	r.channel, err = r.conn.Channel()
	return err
}

func (r *RabbitMQAdapter) Disconnect() error {
	if r.channel != nil {
		r.channel.Close()
	}
	if !r.isShared && r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *RabbitMQAdapter) Publish(ctx context.Context, topic string, msg broker.Message) error {
	if r.channel == nil {
		return fmt.Errorf("rabbitmq channel is not open")
	}

	// Declare exchange to ensure it exists
	err := r.channel.ExchangeDeclare(
		topic,    // name (using topic as exchange name for simplicity)
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return err
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return r.channel.Publish(
		topic, // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		})
}

func (r *RabbitMQAdapter) Subscribe(ctx context.Context, topic string, handler broker.Handler) error {
	if r.channel == nil {
		return fmt.Errorf("rabbitmq channel is not open")
	}

	// Declare exchange
	err := r.channel.ExchangeDeclare(
		topic, "fanout", true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	// Declare a temporary queue
	q, err := r.channel.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	// Bind queue to exchange
	err = r.channel.QueueBind(
		q.Name, // queue name
		"",     // routing key
		topic,  // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := r.channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			var m broker.Message
			if err := json.Unmarshal(d.Body, &m); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				continue
			}
			if err := handler(ctx, m); err != nil {
				log.Printf("Handler error: %v", err)
			}
		}
	}()

	return nil
}

func init() {
	broker.Register("rabbitmq", NewRabbitMQAdapter)
}
