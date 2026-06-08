package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	broker "github.com/budimanlai/go-message-broker"
	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultExchange = "eventbus"

type RabbitMQAdapter struct {
	conn       *amqp.Connection
	pubChannel *amqp.Channel

	url        string
	isShared   bool
	poolLimit  int
	workerName string
}

var _ broker.Broker = (*RabbitMQAdapter)(nil)

func NewRabbitMQAdapter(config map[string]interface{}) (broker.Broker, error) {
	poolLimit := 10
	if v, ok := config["pool_limit"].(int); ok && v > 0 {
		poolLimit = v
	}

	workerName := "default-worker"
	if v, ok := config["worker_name"].(string); ok && v != "" {
		workerName = v
	}

	if conn, ok := config["amqp_connection"].(*amqp.Connection); ok {
		return &RabbitMQAdapter{
			conn:       conn,
			isShared:   true,
			poolLimit:  poolLimit,
			workerName: workerName,
		}, nil
	}

	url, ok := config["url"].(string)
	if !ok {
		return nil, fmt.Errorf("rabbitmq url required")
	}

	return &RabbitMQAdapter{
		url:        url,
		poolLimit:  poolLimit,
		workerName: workerName,
	}, nil
}

func (r *RabbitMQAdapter) Connect() error {
	var err error

	if r.isShared {
		if r.conn == nil || r.conn.IsClosed() {
			return fmt.Errorf("shared connection invalid")
		}
	} else {
		r.conn, err = amqp.Dial(r.url)
		if err != nil {
			return err
		}
	}

	r.pubChannel, err = r.conn.Channel()
	if err != nil {
		return err
	}

	// Declare exchange once
	return r.pubChannel.ExchangeDeclare(
		defaultExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
}

func (r *RabbitMQAdapter) Disconnect() error {
	if r.pubChannel != nil {
		_ = r.pubChannel.Close()
	}
	if !r.isShared && r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

func (r *RabbitMQAdapter) Publish(ctx context.Context, topic string, msg broker.Message) error {
	if r.pubChannel == nil {
		return fmt.Errorf("channel not initialized")
	}

	return r.pubChannel.PublishWithContext(
		ctx,
		defaultExchange,
		topic,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			MessageId:   msg.ID,
			Timestamp:   msg.Timestamp,
			Body:        msg.Payload,
		},
	)
}

func (r *RabbitMQAdapter) Subscribe(ctx context.Context, topic string, handler broker.Handler) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}

	// QoS
	if err := ch.Qos(r.poolLimit, 0, false); err != nil {
		return err
	}

	queueName := fmt.Sprintf("%s.%s", r.workerName, topic)

	q, err := ch.QueueDeclare(
		queueName,
		true, // durable
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,
		topic,
		defaultExchange,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	consumerTag := fmt.Sprintf("%s-%d", queueName, time.Now().UnixNano())

	msgs, err := ch.Consume(
		q.Name,
		consumerTag,
		false, // manual ack
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			_ = ch.Cancel(consumerTag, false)
			_ = ch.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				log.Printf("[RabbitMQ] stop consumer: %s", queueName)
				return

			case d, ok := <-msgs:
				if !ok {
					return
				}

				m := broker.Message{
					ID:        d.MessageId,
					Topic:     topic,
					Payload:   d.Body,
					Headers:   make(map[string]string),
					Timestamp: d.Timestamp,
				}

				if err := handler(ctx, m); err != nil {
					log.Printf("handler error: %v", err)

					// retry simple (requeue)
					_ = d.Nack(false, true)
				} else {
					_ = d.Ack(false)
				}
			}
		}
	}()

	return nil
}

func init() {
	broker.Register("rabbitmq", NewRabbitMQAdapter)
}
