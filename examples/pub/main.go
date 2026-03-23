package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	broker "github.com/budimanlai/go-message-broker"
	_ "github.com/budimanlai/go-message-broker/adapters/db"       // Register DB adapter
	_ "github.com/budimanlai/go-message-broker/adapters/rabbitmq" // Register RabbitMQ adapter
	_ "github.com/budimanlai/go-message-broker/adapters/redis"    // Register Redis adapter
	"github.com/budimanlai/go-pkg/databases"
	_ "github.com/go-sql-driver/mysql" // Register MySQL driver for DB adapter
)

func main() {
	runDbExample()
}

func runDbExample() {
	fmt.Println("=== Running DB Example Publisher ===")
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
		"db_instance": dbManager.GetDb(),
		"worker_name": "example-worker",
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
	for i := 1; i <= 100; i++ {
		payload := map[string]interface{}{
			"message_number": i,
			"params1":        fmt.Sprintf("Hello from %s #%d", adapterName, i),
			"params2":        true,
			"params3":        3.14,
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
}
