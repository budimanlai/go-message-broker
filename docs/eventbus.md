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

---

## TopicConfig

Setiap topic dikonfigurasi dengan field berikut:

| Field | Type | Keterangan |
|---|---|---|
| `EmitToLocal` | `bool` | Saat `Emit` dipanggil, kirim ke worker lokal (in-process) |
| `EmitToBroker` | `bool` | Saat `Emit` dipanggil, kirim ke broker eksternal |
| `ConsumeFromLocal` | `bool` | Handler subscribe dari channel lokal |
| `ConsumeFromBroker` | `bool` | Handler subscribe dari broker eksternal |
| `Brokers` | `[]string` | Nama broker target dari `Config.Brokers`. Kosong = gunakan `Config.Broker` |
| `Middleware` | `[]Middleware` | Chain middleware per topic |
| `Retry` | `*RetryPolicy` | Konfigurasi retry otomatis per topic |

---

## Konfigurasi EventBus

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker:     rabbitAdapter, // single broker (backward compatible)
    WorkerPool: 10,            // jumlah goroutine worker lokal
    BufferSize: 1000,          // ukuran buffer channel lokal per topic

    Topics: map[string]eventbus.TopicConfig{
        "telemetry.incoming": {
            EmitToLocal:      true,
            ConsumeFromLocal: true,
        },
        "telemetry.persistent": {
            EmitToBroker: true,
        },
        "alert.triggered": {
            ConsumeFromBroker: true,
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

---

## Multi-Broker Routing

Daftarkan beberapa adapter di `Config.Brokers` lalu tentukan target per topic menggunakan `TopicConfig.Brokers`.

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Brokers: map[string]broker.Adapter{
        "rabbit": rabbitAdapter,
        "redis":  redisAdapter,
    },
    Topics: map[string]eventbus.TopicConfig{
        // Dikirim ke rabbit saja
        "order.created": {
            EmitToBroker: true,
            Brokers:      []string{"rabbit"},
        },
        // Dikirim ke rabbit DAN redis sekaligus
        "telemetry.data": {
            EmitToBroker: true,
            Brokers:      []string{"rabbit", "redis"},
        },
    },
})
```

Jika `TopicConfig.Brokers` kosong, EventBus menggunakan `Config.Broker` (backward compatible).

Nama broker yang tidak terdaftar di `Config.Brokers` menghasilkan error saat `Emit` dipanggil.

---

## Middleware

Middleware dieksekusi untuk setiap pesan yang diterima handler subscriber. Urutan eksekusi:

```
BeforeHandle → handler → AfterHandle
```

Implementasikan interface `Middleware`:

```go
type Middleware interface {
    BeforeHandle(ctx context.Context, msg Message) error
    AfterHandle(ctx context.Context, msg Message, err error)
}
```

Daftarkan di `TopicConfig.Middleware`:

```go
Topics: map[string]eventbus.TopicConfig{
    "order.created": {
        ConsumeFromBroker: true,
        Middleware:        []eventbus.Middleware{&LogMiddleware{}, &AuthMiddleware{}},
    },
},
```

Beberapa aturan penting:
- Middleware dieksekusi berurutan sesuai urutan di slice.
- Jika `BeforeHandle` mengembalikan error, handler **tidak** dipanggil dan middleware berikutnya tidak dijalankan.
- `AfterHandle` menerima error dari handler (nil jika sukses). `AfterHandle` **tidak** dipanggil jika `BeforeHandle` gagal.

---

## Retry Otomatis

Konfigurasi retry per topic menggunakan `RetryPolicy`:

```go
Topics: map[string]eventbus.TopicConfig{
    "payment.process": {
        ConsumeFromBroker: true,
        Retry: &eventbus.RetryPolicy{
            MaxRetry: 3,
            Delay:    500 * time.Millisecond,
        },
    },
},
```

| Field | Keterangan |
|---|---|
| `MaxRetry` | Jumlah retry setelah attempt pertama. Total attempt = `MaxRetry + 1` |
| `Delay` | Jeda antar attempt. `0` berarti retry langsung tanpa jeda |

Perilaku saat semua attempt habis:
- Jika `FallbackConfig` dikonfigurasi → pesan diteruskan ke `FallbackAdapter.Store`.
- Broker menerima ACK — EventBus yang memiliki retry, bukan broker (tidak ada DLQ di sisi broker).

---

## Fallback Storage

Fallback digunakan di dua kondisi:
1. Publish ke broker gagal (sisi Emit).
2. Semua attempt retry subscriber habis (sisi Subscribe).

Implementasikan interface `FallbackAdapter`:

```go
type FallbackAdapter interface {
    Store(ctx context.Context, failed FailedPublish) error
}
```

`FailedPublish` berisi:

| Field | Keterangan |
|---|---|
| `Topic` | Nama topic |
| `Broker` | Nama adapter yang gagal |
| `Message` | Pesan yang gagal |
| `Error` | Error terakhir |
| `Time` | Waktu kegagalan |

Daftarkan di `Config.Fallback`:

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker: rabbitAdapter,
    Fallback: &eventbus.FallbackConfig{
        Adapter: &DBFallback{db: gormDB},
    },
    Topics: map[string]eventbus.TopicConfig{
        "invoice.sent": {EmitToBroker: true},
    },
})
```

Jika `Fallback` nil, kegagalan publish dan retry yang habis akan diabaikan tanpa panic.

---

## Asynchronous Broadcast Worker Pool

Secara default, publish ke broker berjalan **synchronous** di goroutine yang memanggil `Emit`. Aktifkan worker pool untuk publish asynchronous:

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker:               rabbitAdapter,
    BroadcastWorkerCount: 4,   // jumlah goroutine worker publish
    BroadcastQueueSize:   200, // kapasitas antrian job (default: 100)
    Topics: map[string]eventbus.TopicConfig{
        "telemetry.data": {EmitToBroker: true},
    },
})
```

| Mode | `BroadcastWorkerCount` | Keterangan |
|---|---|---|
| Synchronous (default) | `0` | Publish berjalan di goroutine `Emit`. `Start()` opsional |
| Worker pool | `> 0` | Publish diqueue, diproses worker. `Start()` wajib dipanggil |

---

## LocalBus — Worker Pool In-Process

Ketika `WorkerPool > 0`, EventBus membuat `LocalBus` dengan worker pool dan buffered channel per topic.

- Setiap topic punya channel buffer sendiri (ukuran = `BufferSize`).
- `WorkerPool` goroutine membaca dari channel dan memanggil semua handler yang subscribe.
- Jika buffer penuh saat `Emit`, akan mengembalikan error `"localbus buffer full for topic: ..."`.
- Jika handler panic, panic di-recover dan pesan berikutnya tetap diproses.
- Saat `Close()`, pesan yang masih ada di buffer diselesaikan sebelum goroutine berhenti.

---

## Memulai EventBus dengan `Start`

`Start(ctx context.Context) error` memanggil `Connect` pada semua broker secara otomatis dan (jika dikonfigurasi) meluncurkan broadcast worker pool:

```go
if err := eb.Start(ctx); err != nil {
    log.Fatal(err)
}
defer eb.Close()
```

Alternatifnya, panggil `Connect` pada adapter secara manual sebelum membuat EventBus. Hindari memanggil keduanya agar tidak terjadi koneksi ganda.

`Start` wajib dipanggil jika menggunakan `BroadcastWorkerCount > 0`.

---

## Menutup EventBus

```go
eb.Close()
```

Urutan shutdown:
1. Batalkan context internal → semua consumer broker berhenti.
2. Tunggu broadcast worker selesai (`broadcastWg.Wait()`).
3. Drain sisa antrian broadcast secara synchronous.
4. Tutup LocalBus → drain buffer lokal → goroutine worker berhenti.
5. Panggil `Disconnect` pada semua broker (dengan dedup untuk adapter yang di-share).

---

## Contoh: Publisher

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker:     rabbitAdapter,
    WorkerPool: 10,
    BufferSize: 1000,
    Topics: map[string]eventbus.TopicConfig{
        "telemetry.incoming": {
            EmitToLocal:      true,
            ConsumeFromLocal: true,
        },
        "telemetry.persistent": {
            EmitToBroker: true,
        },
    },
})
defer eb.Close()

eb.Subscribe("telemetry.incoming", func(ctx context.Context, msg broker.Message) error {
    fmt.Println("[Local] proses:", string(msg.Payload))

    fwdMsg := broker.NewMessage("telemetry.persistent", msg.Payload)
    return eb.Emit(ctx, "telemetry.persistent", fwdMsg)
})

payload := []byte(`{"device_id":"abc","speed":60}`)
eb.Emit(ctx, "telemetry.incoming", broker.NewMessage("telemetry.incoming", payload))
```

---

## Contoh: Subscriber dengan Middleware dan Retry

```go
eb := eventbus.NewEventBus(eventbus.Config{
    Broker: rabbitAdapter,
    Fallback: &eventbus.FallbackConfig{
        Adapter: &DBFallback{db: gormDB},
    },
    Topics: map[string]eventbus.TopicConfig{
        "telemetry.persistent": {
            ConsumeFromBroker: true,
            Middleware:        []eventbus.Middleware{&LogMiddleware{}},
            Retry: &eventbus.RetryPolicy{
                MaxRetry: 3,
                Delay:    1 * time.Second,
            },
        },
    },
})
defer eb.Close()

eb.Subscribe("telemetry.persistent", func(ctx context.Context, msg broker.Message) error {
    return saveToDatabase(msg)
})

sigChan := make(chan os.Signal, 1)
signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
<-sigChan
```
