# go-message-broker

Library Go untuk message broker dengan interface yang seragam. Mendukung multiple backend (RabbitMQ, Redis, Database) sehingga Anda dapat mengganti infrastruktur tanpa mengubah kode aplikasi.

## Instalasi

```bash
go get github.com/budimanlai/go-message-broker
```

## Konsep Dasar

Library ini terdiri dari dua lapisan:

- **Broker** — interface tingkat rendah untuk publish dan subscribe pesan ke backend tertentu.
- **EventBus** — layer tingkat tinggi di atas Broker yang mendukung multi-broker routing, middleware, retry otomatis, fallback storage, dan asynchronous worker pool.

## Adapter yang Tersedia

| Adapter | Import Path | Keterangan |
|---|---|---|
| RabbitMQ | `adapters/rabbitmq` | AMQP topic exchange |
| Redis | `adapters/redis` | Redis Pub/Sub |
| Database | `adapters/db` | Polling tabel SQL (MySQL / PostgreSQL) |

## Fitur EventBus

| Fitur | Keterangan |
|---|---|
| Multi-broker routing | Satu topic dapat dikirim ke beberapa broker sekaligus |
| Middleware | `BeforeHandle` / `AfterHandle` per topic |
| Retry otomatis | `RetryPolicy` dengan `MaxRetry` dan `Delay` per topic |
| Fallback storage | Simpan pesan yang gagal ke storage custom |
| Worker pool | Publish asynchronous dengan jumlah worker yang dapat dikonfigurasi |
| Local bus | Pemrosesan in-process tanpa broker eksternal |

## Struktur Direktori

```
go-message-broker/
├── broker.go          # Interface Broker dan type alias Adapter
├── message.go         # Struct Message dan konstruktor
├── handler.go         # Type Handler (func)
├── factory.go         # Registry dan factory untuk adapter
├── adapters/
│   ├── rabbitmq/      # Adapter RabbitMQ (AMQP)
│   ├── redis/         # Adapter Redis Pub/Sub
│   └── db/            # Adapter Database (MySQL/PostgreSQL)
├── eventbus/
│   ├── eventbus.go    # EventBus — routing lokal + broker
│   ├── localbus.go    # LocalBus — worker pool in-process
│   ├── middleware.go  # Interface Middleware
│   ├── retry.go       # RetryPolicy
│   ├── fallback.go    # FallbackAdapter, FallbackConfig, FailedPublish
│   └── worker.go      # Broadcast worker pool
└── examples/
    ├── eventbus_pub/  # Contoh publisher
    └── eventbus_sub/  # Contoh subscriber
```

## License

MIT
