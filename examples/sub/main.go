package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	broker "github.com/budimanlai/go-message-broker"
	_ "github.com/budimanlai/go-message-broker/adapters/db" // Register DB adapter
	"github.com/budimanlai/go-pkg/databases"
)

func main() {
	runDbExample()
}

func runDbExample() {
	fmt.Println("=== Running DB Example Subscribe ===")
	config := databases.DbConfig{
		Driver:   databases.MySQL, // or databases.Postgres
		Host:     "localhost",
		Port:     "3306",
		Username: "root",
		Password: "",
		Name:     "motrixs",
	}

	dbManager := databases.NewDbManager(config)
	// Open with default configuration
	err := dbManager.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer dbManager.Close()

	configMap := map[string]interface{}{
		"db_instance":   dbManager.GetDb(),
		"worker_name":   "ocpp",
		"poll_interval": 1 * time.Second, // 1 second
		"pool_limit":    1000,            // Limit to 1000 messages at a time
	}

	runBroker("db", configMap)
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

	// 3. Publish some messages
	topic := "ocpp.start_transaction"
	err = b.Subscribe(context.Background(), topic, func(ctx context.Context, msg broker.Message) error {
		fmt.Printf("[%s] Received: %s (ID: %s)\n", adapterName, string(msg.Payload), msg.ID)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Keep running until interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Press Ctrl+C to stop...")

	// Block until a signal is received.
	// This will keep the main goroutine (and thus the application) running indefinitely.
	<-sigChan
	fmt.Println("\nShutting down...")
}
