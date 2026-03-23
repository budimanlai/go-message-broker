package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	broker "github.com/budimanlai/go-message-broker"
	_ "github.com/budimanlai/go-message-broker/adapters/db"       // Register DB adapter
	_ "github.com/budimanlai/go-message-broker/adapters/rabbitmq" // Register RabbitMQ adapter
	_ "github.com/budimanlai/go-message-broker/adapters/redis"    // Register Redis adapter
	_ "github.com/go-sql-driver/mysql"                            // Register MySQL driver for DB adapter
)

func main() {
	// By default run Redis example.
	// You can switch this to runRabbitMQExample() or runDBExample()
	// runRedisExample()

	// runRabbitMQExample()
	runDBExample()
}

func runRedisExample() {
	fmt.Println("=== Running Redis Example ===")
	config := map[string]interface{}{
		"addr":     "localhost:6379",
		"password": "",
		"db":       0,
	}
	runBroker("redis", config)
}

func runRabbitMQExample() {
	fmt.Println("=== Running RabbitMQ Example ===")
	config := map[string]interface{}{
		"url": "amqp://guest:guest@localhost:5672/",
	}
	runBroker("rabbitmq", config)
}

func runDBExample() {
	fmt.Println("=== Running DB Example ===")
	// Note: You need a real DB running and the 'broker_messages' table created.
	// Table Schema:
	// CREATE TABLE broker_messages (
	//   id INT AUTO_INCREMENT PRIMARY KEY,
	//   topic VARCHAR(255) NOT NULL,
	//   payload TEXT,
	//   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	// );
	config := map[string]interface{}{
		"driver": "mysql", // or "postgres" with appropriate driver imported
		"dsn":    "root:@tcp(127.0.0.1:3306)/gocore?parseTime=true",
	}
	runBroker("db", config)
}

func runBroker(adapterName string, config map[string]interface{}) {
	// 1. Initialize Broker
	b, err := broker.New(adapterName, config)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// 2. Connect
	if err := b.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s: %v", adapterName, err)
	}
	defer b.Disconnect()

	// 3. Subscribe
	topic := "example-topic"
	err = b.Subscribe(context.Background(), topic, func(ctx context.Context, msg broker.Message) error {
		fmt.Printf("%v\n", string(msg.Payload))
		fmt.Printf("[%s] Received: %s (ID: %s)\n", adapterName, string(msg.Payload), msg.ID)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// 4. Publish messages
	go func() {
		for i := 1; i <= 5; i++ {
			payload := map[string]interface{}{
				"message_number": i,
				"params1":        fmt.Sprintf("Hello from %s #%d", adapterName, i),
				"params2":        true,
				"params3":        3.14,
				"timestamp":      time.Now().Format(time.RFC3339),
			}
			data, _ := json.Marshal(payload)

			msg := broker.NewMessage(topic, data)
			if err := b.Publish(context.Background(), topic, msg); err != nil {
				log.Printf("Failed to publish: %v", err)
			} else {
				fmt.Printf("[%s] Published message #%d\n", adapterName, i)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// Keep running until interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Press Ctrl+C to stop...")
	select {
	case <-sigChan:
		fmt.Println("\nShutting down...")
	case <-time.After(20 * time.Second):
		fmt.Println("\nExample finished automatically.")
	}
}
