package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	broker "github.com/budimanlai/go-message-broker"
	_ "github.com/budimanlai/go-message-broker/adapters/db"       // Register DB adapter
	_ "github.com/budimanlai/go-message-broker/adapters/rabbitmq" // Register RabbitMQ adapter
	"github.com/budimanlai/go-message-broker/eventbus"
	"github.com/budimanlai/go-pkg/databases"
)

func main() {
	// runDbExample("ai_ml")
	runRabbitMqExample("ocpp")
}

func runRabbitMqExample(workerName string) {
	fmt.Printf("=== Running RabbitMQ Example Subscribe - Worker: %s ===\n", workerName)

	// tambahkan contoh setup RabbitMQAdapter jika ingin menggunakan RabbitMQ sebagai broker
	configMap := map[string]interface{}{
		"url":         "amqp://admin:admin123@localhost:5672/",
		"worker_name": workerName,
	}

	rabbitAdapter, err := broker.New("rabbitmq", configMap)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// 2. Connect
	if err := rabbitAdapter.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s: %v", "rabbitmq", err)
	}
	defer rabbitAdapter.Disconnect()

	runEventBus(rabbitAdapter)
}

func runDbExample(workerName string) {
	fmt.Printf("=== Running DB Example Subscribe - Worker: %s ===\n", workerName)

	// tambahkan contoh setup DbAdapter jika ingin menggunakan DB sebagai broker
	configDb := databases.DbConfig{
		Driver:   databases.MySQL, // or databases.Postgres
		Host:     "localhost",
		Port:     "3306",
		Username: "root",
		Password: "",
		Name:     "motrixs",
	}

	dbManager := databases.NewDbManager(configDb)
	// Open with default configuration
	err := dbManager.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer dbManager.Close()

	configMap := map[string]interface{}{
		"db_instance": dbManager.GetDb(),
		"worker_name": workerName,
	}

	dbAdapter, err := broker.New("db", configMap)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	// 2. Connect
	if err := dbAdapter.Connect(); err != nil {
		log.Fatalf("Failed to connect to %s: %v", "db", err)
	}
	defer dbAdapter.Disconnect()

	runEventBus(dbAdapter)
}

func runEventBus(adapter broker.Broker) {
	config := eventbus.Config{
		Broker: adapter, // atau kafkaAdapter

		Topics: map[string]eventbus.TopicConfig{
			"telemetry.incoming.persistent": {
				ConsumeFromBroker: true,
			},
		},
	}

	// 2. Inisialisasi EventBus
	eb := eventbus.NewEventBus(config)
	defer eb.Close()

	topicPersist := "telemetry.incoming.persistent"

	// 3. Define Handler untuk memproses event.
	handlerDbUpdate := func(ctx context.Context, msg broker.Message) error {
		fmt.Printf("[Broker Consumer] DBUPDATE ==> Diterima event di topic '%s': %s (ID: %s)\n", msg.Topic, string(msg.Payload), msg.ID)
		return nil
	}

	// 4. Subscribe ke topic telemetry.incoming.persisten untuk memproses data yang akan disimpan ke database
	fmt.Printf("Subscribing ke topic: %s\n", topicPersist)
	eb.Subscribe(topicPersist, handlerDbUpdate)

	// 5. Emit event
	// Disimulasikan mengirim event secara periodik

	// 6. Tunggu sinyal termination agar program tidak langsung exit
	fmt.Println("EventBus berjalan. Tekan Ctrl+C untuk berhenti.")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down...")
}
