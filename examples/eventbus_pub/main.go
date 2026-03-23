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
	"github.com/budimanlai/go-message-broker/eventbus"
	"github.com/budimanlai/go-pkg/databases"
)

func main() {
	runDbExample()
}

func runDbExample() {
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
			// process telemetry.incoming di local worker, lalu emit ke topic lain untuk diproses lebih lanjut (misal disimpan ke database)
			"telemetry.incoming": {
				EmitToLocal:      true,
				ConsumeFromLocal: true,
			},

			// process telemetry.incoming.persistent di broker worker service misalnya untuk Alert atau update status di database
			"telemetry.incoming.persistent": {
				EmitToBroker: true,
			},
		},

		WorkerPool: 10,
		BufferSize: 1000,
	}

	// 2. Inisialisasi EventBus
	eb := eventbus.NewEventBus(config)
	defer eb.Close()

	topic := "telemetry.incoming"
	topicPersist := "telemetry.incoming.persistent"

	handlerDbWriter := func(ctx context.Context, msg broker.Message) error {
		fmt.Printf("[Local Consumer] DBWRITER ==> Diterima event di topic '%s': %s (ID: %s)\n", msg.Topic, string(msg.Payload), msg.ID)

		// setelah memproses data, kita emit ke topic lain untuk diproses lebih lanjut (misal disimpan ke database)
		newMsg := msg
		newMsg.Topic = topicPersist

		eb.Emit(ctx, topicPersist, newMsg)
		return nil
	}

	// 4. Subscribe ke topic tertentu
	fmt.Printf("Subscribing ke topic: %s\n", topic)
	eb.Subscribe(topic, handlerDbWriter)

	// 5. Emit event
	// Disimulasikan mengirim event secara periodik
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		counter := 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				payloadText := fmt.Sprintf("Transaction #%d started at %s", counter, time.Now().Format(time.RFC3339))

				// Buat message baru
				msg := broker.NewMessage(topic, []byte(payloadText))

				fmt.Printf("\n[Publisher] Sending event: %s\n", payloadText)

				// Context bisa digunakan untuk tracing atau timeout
				if err := eb.Emit(ctx, topic, msg); err != nil {
					log.Printf("Gagal emit event: %v", err)
				}

				counter++
			}
		}
	}()

	// 6. Tunggu sinyal termination agar program tidak langsung exit
	fmt.Println("EventBus berjalan. Tekan Ctrl+C untuk berhenti.")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	cancel()

	fmt.Println("Shutting down...")
}
