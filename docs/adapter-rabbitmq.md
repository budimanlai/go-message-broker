# Adapter RabbitMQ

## Cara Kerja

Adapter ini menggunakan AMQP topic exchange bernama `eventbus`. Setiap subscriber membuat queue sendiri dengan format nama `{worker_name}.{topic}`, lalu di-bind ke exchange tersebut.

```
Publisher                     RabbitMQ
    │                             │
    ├─ Publish("order.created") ──► exchange: eventbus (type: topic)
    │                             │
    │                             ├──► queue: service-a.order.created ──► subscriber A
    │                             └──► queue: service-b.order.created ──► subscriber B
```

Setiap service dengan `worker_name` berbeda menerima salinan pesan sendiri (fan-out).

## Instalasi Dependency

```bash
go get github.com/rabbitmq/amqp091-go
```

## Konfigurasi

| Key | Type | Default | Keterangan |
|---|---|---|---|
| `url` | `string` | — | AMQP connection URL (wajib jika tidak pakai shared connection) |
| `worker_name` | `string` | `"default-worker"` | Prefix nama queue, gunakan nama service Anda |
| `pool_limit` | `int` | `10` | Jumlah pesan yang diproses bersamaan (QoS prefetch) |
| `amqp_connection` | `*amqp.Connection` | — | Gunakan koneksi AMQP yang sudah ada (shared connection) |

## Penggunaan Dasar

```go
package main

import (
    "context"
    "fmt"
    "log"

    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq"
)

func main() {
    b, err := broker.New("rabbitmq", map[string]interface{}{
        "url":         "amqp://user:password@localhost:5672/",
        "worker_name": "order-service",
        "pool_limit":  10,
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := b.Connect(); err != nil {
        log.Fatal(err)
    }
    defer b.Disconnect()

    ctx := context.Background()

    // Publish
    msg := broker.NewMessage("order.created", []byte(`{"order_id": 123}`))
    if err := b.Publish(ctx, "order.created", msg); err != nil {
        log.Printf("publish error: %v", err)
    }

    // Subscribe
    b.Subscribe(ctx, "order.created", func(ctx context.Context, msg broker.Message) error {
        fmt.Println("Diterima:", string(msg.Payload))
        return nil
    })
}
```

## Menggunakan Shared Connection

Jika aplikasi Anda sudah memiliki koneksi AMQP, berikan langsung agar tidak membuat koneksi baru:

```go
import amqp "github.com/rabbitmq/amqp091-go"

conn, err := amqp.Dial("amqp://user:password@localhost:5672/")
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

b, err := broker.New("rabbitmq", map[string]interface{}{
    "amqp_connection": conn,
    "worker_name":     "order-service",
})
```

Saat menggunakan shared connection, pemanggilan `b.Disconnect()` tidak akan menutup koneksi AMQP — tanggung jawab penutupan ada di kode yang membuat koneksi.

## Perilaku Subscribe

- Queue bersifat **durable** — pesan tidak hilang saat RabbitMQ restart.
- Menggunakan **manual acknowledgement** — pesan di-ack setelah handler sukses, di-nack (requeue) jika handler error.
- Jika handler error, pesan akan **direqueue** dan dicoba lagi.
- Consumer berjalan di goroutine terpisah dan berhenti otomatis saat `ctx` di-cancel.

## Wire Format

Body pesan yang dikirim ke RabbitMQ adalah **raw `msg.Payload`** (bukan wrapper JSON). Metadata seperti ID dan timestamp disimpan di AMQP message properties:

```
Body            : <isi msg.Payload — format bebas>
MessageId       : msg.ID  (opsional, bisa kosong)
Timestamp       : msg.Timestamp
ContentType     : application/json
```

Ini memungkinkan sistem lain (non-Go, non-library-ini) untuk publish ke exchange `eventbus` dan di-consume oleh library ini, maupun sebaliknya — selama format payload disepakati di level aplikasi.

Lihat [Interoperabilitas](./interop.md) untuk detail lebih lanjut.
