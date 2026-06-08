# Adapter Redis

## Cara Kerja

Adapter ini menggunakan Redis Pub/Sub. Saat `Publish` dipanggil, pesan dikirim ke channel Redis. Semua subscriber yang sedang aktif pada channel yang sama akan menerima pesan secara real-time.

```
Publisher                        Redis
    │                              │
    ├─ Publish("order.created") ──► PUBLISH order.created <payload>
    │                              │
    │                              ├──► subscriber A (sedang SUBSCRIBE)
    │                              └──► subscriber B (sedang SUBSCRIBE)
```

**Catatan penting:** Redis Pub/Sub bersifat fire-and-forget. Pesan yang dikirim saat tidak ada subscriber aktif akan hilang. Gunakan RabbitMQ atau DB adapter jika Anda butuh durability.

## Instalasi Dependency

```bash
go get github.com/go-redis/redis/v8
```

## Konfigurasi

| Key | Type | Default | Keterangan |
|---|---|---|---|
| `addr` | `string` | — | Alamat Redis, contoh: `localhost:6379` (wajib jika tidak pakai shared client) |
| `password` | `string` | `""` | Password Redis |
| `db` | `int` | `0` | Nomor database Redis |
| `redis_client` | `*redis.Client` | — | Gunakan Redis client yang sudah ada (shared connection) |

## Penggunaan Dasar

```go
package main

import (
    "context"
    "fmt"
    "log"

    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/redis"
)

func main() {
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

    ctx := context.Background()

    // Subscribe (harus dipanggil sebelum Publish agar tidak miss pesan)
    b.Subscribe(ctx, "notification.push", func(ctx context.Context, msg broker.Message) error {
        fmt.Println("Diterima:", string(msg.Payload))
        return nil
    })

    // Publish
    msg := broker.NewMessage("notification.push", []byte(`{"user_id": 42, "text": "Pesanan Anda diproses"}`))
    if err := b.Publish(ctx, "notification.push", msg); err != nil {
        log.Printf("publish error: %v", err)
    }
}
```

## Menggunakan Shared Client

Jika aplikasi Anda sudah memiliki `*redis.Client`, berikan langsung:

```go
import "github.com/go-redis/redis/v8"

redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

b, err := broker.New("redis", map[string]interface{}{
    "redis_client": redisClient,
})
```

Saat menggunakan shared client, pemanggilan `b.Disconnect()` tidak akan menutup koneksi Redis.

## Perilaku Subscribe

- Subscribe berjalan di goroutine terpisah.
- Tidak ada retry — jika handler error, pesan dilewati dan error di-log.
- Tidak ada durability — pesan hanya diterima oleh subscriber yang sedang aktif saat pesan dikirim.

## Wire Format

Body yang dikirim ke Redis adalah **raw `msg.Payload`** langsung. Redis Pub/Sub tidak memiliki fasilitas message properties, sehingga hanya `Payload` dan `Topic` yang tersedia di sisi subscriber:

```go
// Yang tersedia di handler sisi subscriber:
msg.Topic    // nama channel Redis
msg.Payload  // raw bytes yang dikirim publisher
msg.ID       // selalu kosong (Redis Pub/Sub tidak support metadata)
msg.Timestamp // selalu zero value
```

Jika Anda butuh ID atau timestamp, sertakan di dalam payload Anda sendiri. Lihat [Struktur Pesan](./message.md).
