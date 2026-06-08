# EventBus

## Apa itu EventBus?

EventBus adalah layer opsional di atas `Broker` yang memungkinkan setiap topic dikonfigurasi secara independen untuk diproses **secara lokal (in-process)**, **dikirim ke broker eksternal**, atau keduanya sekaligus.

Gunakan EventBus jika Anda butuh pipeline seperti ini:

```
Emit("telemetry.incoming")
        │
        ├──► [Local Worker] proses cepat di memory (validasi, transformasi)
        │           │
        │           └──► Emit("telemetry.persistent")
        │                       │
        │                       └──► [RabbitMQ] dikirim ke service lain
        │
        └──► (tidak ke broker, cukup lokal)
```

## TopicConfig

Setiap topic dikonfigurasi dengan empat flag:

| Field | Keterangan |
|---|---|
| `EmitToLocal` | Saat `Emit` dipanggil, kirim ke worker lokal (in-process) |
| `EmitToBroker` | Saat `Emit` dipanggil, kirim ke broker eksternal (async) |
| `ConsumeFromLocal` | Handler subscribe dari channel lokal |
| `ConsumeFromBroker` | Handler subscribe dari broker eksternal |

Flag-flag ini bisa dikombinasikan sesuai kebutuhan.

## Konfigurasi EventBus

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker:     rabbitAdapter, // opsional, bisa nil jika hanya pakai lokal
    WorkerPool: 10,            // jumlah goroutine worker lokal
    BufferSize: 1000,          // ukuran buffer channel lokal per topic

    Topics: map[string]eventbus.TopicConfig{
        "telemetry.incoming": {
            EmitToLocal:      true, // diproses lokal
            ConsumeFromLocal: true,
        },
        "telemetry.persistent": {
            EmitToBroker: true, // dikirim ke broker
        },
        "alert.triggered": {
            ConsumeFromBroker: true, // consume dari broker
        },
    },

    // Default config untuk topic yang tidak ada di map Topics
    Default: eventbus.TopicConfig{
        EmitToLocal:      true,
        ConsumeFromLocal: true,
    },
})
```

Jika `Default` tidak diset dan topic tidak terdaftar di `Topics`, perilaku default adalah `EmitToLocal: true, ConsumeFromLocal: true`.

## Contoh: Publisher (satu service)

Service yang menerima data masuk, memproses secara lokal, lalu meneruskan ke broker:

```go
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
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq"
    "github.com/budimanlai/go-message-broker/eventbus"
)

func main() {
    rabbitAdapter, err := broker.New("rabbitmq", map[string]interface{}{
        "url":         "amqp://admin:admin123@localhost:5672/",
        "worker_name": "gateway",
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := rabbitAdapter.Connect(); err != nil {
        log.Fatal(err)
    }
    defer rabbitAdapter.Disconnect()

    eb := eventbus.NewEventBus(eventbus.Config{
        Broker:     rabbitAdapter,
        WorkerPool: 10,
        BufferSize: 1000,
        Topics: map[string]eventbus.TopicConfig{
            // Proses lokal dulu (validasi, transformasi)
            "telemetry.incoming": {
                EmitToLocal:      true,
                ConsumeFromLocal: true,
            },
            // Teruskan ke broker untuk diproses service lain
            "telemetry.persistent": {
                EmitToBroker: true,
            },
        },
    })
    defer eb.Close()

    // Handler lokal: validasi dan teruskan
    eb.Subscribe("telemetry.incoming", func(ctx context.Context, msg broker.Message) error {
        fmt.Println("[Local] proses:", string(msg.Payload))

        // Teruskan ke topic lain via broker
        fwdMsg := broker.NewMessage("telemetry.persistent", msg.Payload)
        return eb.Emit(ctx, "telemetry.persistent", fwdMsg)
    })

    // Simulasi data masuk tiap 10 detik
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        ticker := time.NewTicker(10 * time.Second)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                payload := []byte(`{"device_id":"abc","lat":-6.2,"lng":106.8,"speed":60}`)
                eb.Emit(ctx, "telemetry.incoming", broker.NewMessage("telemetry.incoming", payload))
            }
        }
    }()

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    cancel()
}
```

## Contoh: Subscriber (service terpisah)

Service yang hanya consume dari broker:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/rabbitmq"
    "github.com/budimanlai/go-message-broker/eventbus"
)

func main() {
    rabbitAdapter, err := broker.New("rabbitmq", map[string]interface{}{
        "url":         "amqp://admin:admin123@localhost:5672/",
        "worker_name": "db-writer", // nama berbeda = queue berbeda = terima salinan sendiri
    })
    if err != nil {
        log.Fatal(err)
    }
    if err := rabbitAdapter.Connect(); err != nil {
        log.Fatal(err)
    }
    defer rabbitAdapter.Disconnect()

    eb := eventbus.NewEventBus(eventbus.Config{
        Broker: rabbitAdapter,
        Topics: map[string]eventbus.TopicConfig{
            "telemetry.persistent": {
                ConsumeFromBroker: true,
            },
        },
    })
    defer eb.Close()

    eb.Subscribe("telemetry.persistent", func(ctx context.Context, msg broker.Message) error {
        fmt.Println("[DB Writer] simpan ke DB:", string(msg.Payload))
        // simpan msg.Payload ke database...
        return nil
    })

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
}
```

## LocalBus — Worker Pool

Ketika `WorkerPool > 0`, EventBus membuat `LocalBus` dengan worker pool dan buffered channel per topic.

- Setiap topic punya channel buffer sendiri (ukuran = `BufferSize`).
- `WorkerPool` goroutine membaca dari channel dan memanggil semua handler yang subscribe.
- Jika buffer penuh saat `Emit`, akan mengembalikan error `"localbus buffer full for topic: ..."`.
- Jika handler panic, panic di-recover dan pesan berikutnya tetap diproses.

## Menutup EventBus

```go
eb.Close()
```

`Close` membatalkan context internal (menghentikan semua consumer broker) dan memanggil `Disconnect` pada broker. Panggil ini saat aplikasi shutdown, idealnya dengan `defer`.
