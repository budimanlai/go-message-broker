# go-message-broker

Library Go untuk message broker dengan interface yang seragam. Mendukung multiple backend (RabbitMQ, Redis, Database) sehingga Anda dapat mengganti infrastruktur tanpa mengubah kode aplikasi.

## Instalasi

```bash
go get github.com/budimanlai/go-message-broker
```

## Konsep Dasar

Library ini terdiri dari dua lapisan:

- **Broker** — interface tingkat rendah untuk publish dan subscribe pesan ke backend tertentu (RabbitMQ, Redis, DB).
- **EventBus** — layer tingkat tinggi yang bisa menggabungkan pemrosesan lokal (in-process) dan broker eksternal dalam satu API.

## Adapter yang Tersedia

| Adapter | Import Path | Keterangan |
|---|---|---|
| RabbitMQ | `adapters/rabbitmq` | Menggunakan AMQP topic exchange |
| Redis | `adapters/redis` | Menggunakan Redis Pub/Sub |
| Database | `adapters/db` | Polling tabel SQL (MySQL / PostgreSQL) |

---

## Penggunaan Broker Langsung

### 1. RabbitMQ

```go
import (
    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq" // register adapter
)

b, err := broker.New("rabbitmq", map[string]interface{}{
    "url":         "amqp://user:password@localhost:5672/",
    "worker_name": "my-service",   // nama worker, digunakan sebagai prefix nama queue
    "pool_limit":  10,             // jumlah pesan yang diproses bersamaan (QoS)
})
if err != nil {
    log.Fatal(err)
}

if err := b.Connect(); err != nil {
    log.Fatal(err)
}
defer b.Disconnect()

// Publish
msg := broker.NewMessage("order.created", []byte(`{"order_id": 123}`))
b.Publish(ctx, "order.created", msg)

// Subscribe
b.Subscribe(ctx, "order.created", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("Diterima:", string(msg.Payload))
    return nil
})
```

> Nama queue yang terbentuk di RabbitMQ: `{worker_name}.{topic}`, contoh: `my-service.order.created`

---

### 2. Redis

```go
import (
    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/redis" // register adapter
)

b, err := broker.New("redis", map[string]interface{}{
    "addr":     "localhost:6379",
    "password": "",
    "db":       0,
})
if err != nil {
    log.Fatal(err)
}

if err := b.Connect(); err != nil {
    log.Fatal(err)
}
defer b.Disconnect()

// Publish
msg := broker.NewMessage("user.registered", []byte(`{"user_id": 42}`))
b.Publish(ctx, "user.registered", msg)

// Subscribe
b.Subscribe(ctx, "user.registered", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("Diterima:", string(msg.Payload))
    return nil
})
```

> Adapter Redis menggunakan existing `*redis.Client` dengan mengisi key `redis_client` di config map.

---

### 3. Database (MySQL / PostgreSQL)

Adapter ini menggunakan tabel SQL sebagai message queue dengan mekanisme polling. Cocok jika Anda tidak memiliki infrastruktur RabbitMQ atau Redis.

#### Buat tabel berikut di database Anda:

```sql
CREATE TABLE broker (
    worker_name VARCHAR(25)  NOT NULL,
    topic       VARCHAR(255) NOT NULL,
    description TEXT,
    status      VARCHAR(15)  NOT NULL DEFAULT 'active',
    INDEX idx_broker_topic (topic)
);

CREATE TABLE broker_messages (
    id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    worker_name   VARCHAR(255)    NOT NULL,
    topic         VARCHAR(255)    NOT NULL,
    payload       TEXT,
    status        VARCHAR(15)     NOT NULL DEFAULT 'pending',
    retry_count   INT             NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at    DATETIME        NOT NULL,
    INDEX idx_bm_topic_worker_status (topic, worker_name, status)
);
```

#### Contoh penggunaan:

```go
import (
    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/db" // register adapter
)

b, err := broker.New("db", map[string]interface{}{
    "driver":      "mysql",                                          // atau "postgres"
    "dsn":         "root:password@tcp(localhost:3306)/mydb?parseTime=true",
    "worker_name": "my-service",
    "pool_limit":  10,                    // jumlah pesan yang diproses bersamaan
    "poll_interval": 2 * time.Second,    // interval polling tabel (default: 2 detik)
    "max_retry":   3,                    // maksimal retry jika handler gagal
})
if err != nil {
    log.Fatal(err)
}

if err := b.Connect(); err != nil {
    log.Fatal(err)
}
defer b.Disconnect()

// Publish
msg := broker.NewMessage("invoice.generated", []byte(`{"invoice_id": 99}`))
b.Publish(ctx, "invoice.generated", msg)

// Subscribe
b.Subscribe(ctx, "invoice.generated", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("Diterima:", string(msg.Payload))
    return nil
})
```

> Adapter DB mendukung pengiriman existing `*gorm.DB` dengan mengisi key `db_instance` di config map.

---

## Penggunaan EventBus

EventBus memungkinkan satu topic diproses secara lokal (in-process, cepat) dan sekaligus dikirim ke broker eksternal. Cocok untuk pipeline event yang kompleks.

### Konsep TopicConfig

Setiap topic dapat dikonfigurasi secara independen:

| Field | Keterangan |
|---|---|
| `EmitToLocal` | Event dikirim ke worker lokal (in-process) saat Emit dipanggil |
| `EmitToBroker` | Event dikirim ke broker eksternal (RabbitMQ, Redis, DB) saat Emit dipanggil |
| `ConsumeFromLocal` | Handler subscribe dari channel lokal |
| `ConsumeFromBroker` | Handler subscribe dari broker eksternal |

### Contoh: Publisher

```go
import (
    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq"
    "github.com/budimanlai/go-message-broker/eventbus"
)

// 1. Buat adapter
rabbitAdapter, _ := broker.New("rabbitmq", map[string]interface{}{
    "url":         "amqp://admin:admin123@localhost:5672/",
    "worker_name": "fleet",
})
rabbitAdapter.Connect()
defer rabbitAdapter.Disconnect()

// 2. Konfigurasi EventBus
eb := eventbus.NewEventBus(eventbus.Config{
    Broker: rabbitAdapter,
    Topics: map[string]eventbus.TopicConfig{
        // Diproses lokal dulu (cepat, no network)
        "telemetry.incoming": {
            EmitToLocal:      true,
            ConsumeFromLocal: true,
        },
        // Lalu dikirim ke broker untuk diproses service lain
        "telemetry.incoming.persistent": {
            EmitToBroker: true,
        },
    },
    WorkerPool: 10,   // jumlah goroutine worker lokal
    BufferSize: 1000, // ukuran buffer channel lokal
})
defer eb.Close()

// 3. Subscribe ke topic lokal
eb.Subscribe("telemetry.incoming", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("Proses lokal:", string(msg.Payload))

    // Forward ke topic lain (ke broker)
    newMsg := msg
    newMsg.Topic = "telemetry.incoming.persistent"
    eb.Emit(ctx, "telemetry.incoming.persistent", newMsg)
    return nil
})

// 4. Emit event
msg := broker.NewMessage("telemetry.incoming", []byte(`{"device_id":"abc","speed":80}`))
eb.Emit(ctx, "telemetry.incoming", msg)
```

### Contoh: Subscriber (Service Terpisah)

```go
rabbitAdapter, _ := broker.New("rabbitmq", map[string]interface{}{
    "url":         "amqp://admin:admin123@localhost:5672/",
    "worker_name": "ocpp", // nama worker berbeda = queue berbeda
})
rabbitAdapter.Connect()
defer rabbitAdapter.Disconnect()

eb := eventbus.NewEventBus(eventbus.Config{
    Broker: rabbitAdapter,
    Topics: map[string]eventbus.TopicConfig{
        "telemetry.incoming.persistent": {
            ConsumeFromBroker: true,
        },
    },
})
defer eb.Close()

eb.Subscribe("telemetry.incoming.persistent", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("Simpan ke DB:", string(msg.Payload))
    return nil
})

// Tunggu sinyal shutdown
sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
```

---

## Struktur Pesan

```go
type Message struct {
    ID        string            // ID unik (auto-generate dari timestamp nanosecond)
    Topic     string            // Nama topic
    Payload   []byte            // Isi pesan (bebas format, umumnya JSON)
    Headers   map[string]string // Metadata tambahan
    Timestamp time.Time         // Waktu pembuatan pesan
}

// Buat pesan baru
msg := broker.NewMessage("order.created", []byte(`{"order_id": 1}`))
```

---

## Menggunakan Koneksi yang Sudah Ada (Shared Connection)

Semua adapter mendukung penggunaan koneksi yang sudah ada agar tidak membuat koneksi baru:

```go
// RabbitMQ — gunakan *amqp.Connection yang sudah ada
broker.New("rabbitmq", map[string]interface{}{
    "amqp_connection": existingAmqpConn,
})

// Redis — gunakan *redis.Client yang sudah ada
broker.New("redis", map[string]interface{}{
    "redis_client": existingRedisClient,
})

// DB — gunakan *gorm.DB yang sudah ada
broker.New("db", map[string]interface{}{
    "db_instance": existingGormDB,
})
```

---

## Struktur Direktori

```
go-message-broker/
├── broker.go          # Interface Broker utama
├── message.go         # Struct Message dan konstruktor
├── handler.go         # Type Handler (func)
├── queue.go           # Interface Queue (opsional)
├── factory.go         # Registry dan factory untuk adapter
├── adapters/
│   ├── rabbitmq/      # Adapter RabbitMQ (AMQP)
│   ├── redis/         # Adapter Redis Pub/Sub
│   └── db/            # Adapter Database (MySQL/PostgreSQL)
├── eventbus/
│   ├── eventbus.go    # EventBus — routing lokal + broker
│   └── localbus.go    # LocalBus — worker pool in-process
└── examples/
    ├── eventbus_pub/  # Contoh publisher
    └── eventbus_sub/  # Contoh subscriber (service terpisah)
```

---

## License

MIT
