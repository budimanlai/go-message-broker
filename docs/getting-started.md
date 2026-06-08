# Getting Started

## Apa itu go-message-broker?

`go-message-broker` adalah library Go untuk messaging dengan interface yang seragam. Anda bisa menggunakan RabbitMQ, Redis, atau Database sebagai backend tanpa mengubah kode aplikasi — cukup ganti adapter-nya.

## Instalasi

```bash
go get github.com/budimanlai/go-message-broker
```

## Dua Layer Utama

```
┌─────────────────────────────────────────────┐
│                  EventBus                   │  ← layer tinggi (opsional)
│        (local in-process + broker)          │
└──────────────────┬──────────────────────────┘
                   │
┌──────────────────▼──────────────────────────┐
│           Broker Interface                  │  ← abstraksi utama
│   Connect / Disconnect / Publish / Subscribe│
└────────┬───────────────┬──────────┬─────────┘
         │               │          │
   ┌─────▼──┐     ┌──────▼──┐ ┌────▼─────┐
   │RabbitMQ│     │  Redis  │ │    DB    │
   │Adapter │     │ Adapter │ │ Adapter  │
   └────────┘     └─────────┘ └──────────┘
```

**Broker** — interface tingkat rendah. Gunakan ini jika Anda hanya butuh publish dan subscribe ke satu backend.

**EventBus** — layer di atas Broker. Gunakan ini jika Anda butuh:
- Memproses event secara lokal (in-process, tanpa network) sekaligus mengirim ke broker eksternal
- Routing event ke beberapa topic
- Pipeline event yang kompleks

## Adapter yang Tersedia

| Adapter | Import Path | Kapan Dipakai |
|---|---|---|
| RabbitMQ | `adapters/rabbitmq` | Production, butuh durability dan retry |
| Redis | `adapters/redis` | Real-time, fire-and-forget |
| Database | `adapters/db` | Tanpa infrastruktur tambahan, butuh audit trail |

## Cara Kerja Plugin Registry

Adapter didaftarkan secara otomatis saat diimport menggunakan blank import (`_`). Pola ini sama seperti `database/sql` di Go standard library.

```go
import (
    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq" // mendaftarkan adapter "rabbitmq"
)

b, err := broker.New("rabbitmq", config)
```

Tanpa blank import, pemanggilan `broker.New("rabbitmq", ...)` akan mengembalikan error `unknown adapter "rabbitmq"`.

## Struktur Direktori

```
go-message-broker/
├── broker.go          # Interface Broker utama
├── message.go         # Struct Message dan konstruktor
├── handler.go         # Type Handler (func)
├── queue.go           # Interface Queue
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

## Langkah Selanjutnya

- [Struktur Pesan](./message.md) — memahami format payload dan cara membuat pesan
- [Adapter RabbitMQ](./adapter-rabbitmq.md)
- [Adapter Redis](./adapter-redis.md)
- [Adapter Database](./adapter-db.md)
- [EventBus](./eventbus.md) — hybrid local + broker messaging
- [Interoperabilitas](./interop.md) — integrasi dengan sistem eksternal
