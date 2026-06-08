# Struktur Pesan

## Struct Message

```go
type Message struct {
    ID        string            // ID pesan (opsional, lihat penjelasan di bawah)
    Topic     string            // Nama topic
    Payload   []byte            // Isi pesan — format bebas (JSON, plain text, binary, dll)
    Headers   map[string]string // Metadata tambahan
    Timestamp time.Time         // Waktu pembuatan pesan
}
```

## Membuat Pesan Baru

```go
msg := broker.NewMessage("order.created", []byte(`{"order_id": 123}`))
```

`NewMessage` mengisi `ID` (dari `time.Now().UnixNano()`), `Topic`, `Payload`, dan `Timestamp` secara otomatis.

## Format Payload

`Payload` adalah `[]byte` — library ini tidak memaksa format tertentu. Anda bebas menggunakan:

```go
// JSON (paling umum)
payload, _ := json.Marshal(MyStruct{...})
msg := broker.NewMessage("topic", payload)

// Plain text
msg := broker.NewMessage("topic", []byte("hello world"))

// Binary (Protobuf, MessagePack, dll)
payload, _ := proto.Marshal(&MyProto{...})
msg := broker.NewMessage("topic", payload)
```

Di sisi subscriber, Anda yang menentukan cara parse-nya:

```go
broker.Subscribe(ctx, "order.created", func(ctx context.Context, msg broker.Message) error {
    var order Order
    json.Unmarshal(msg.Payload, &order)
    // proses order...
    return nil
})
```

## Message ID — Opsional

`msg.ID` bersifat **opsional** dan tergantung kebutuhan aplikasi Anda:

- Jika Anda butuh **idempotency** (deduplikasi pesan) atau **tracing**, sertakan ID di dalam payload struct Anda sendiri.
- Jika tidak butuh, abaikan saja — library tidak memvalidasi atau memaksa keunikan ID.

**Cara yang disarankan** — ID sebagai bagian dari payload:

```go
type OrderEvent struct {
    EventID  string `json:"event_id"`   // ID ada di sini, bukan di broker.Message.ID
    OrderID  int    `json:"order_id"`
    Status   string `json:"status"`
}

event := OrderEvent{
    EventID: uuid.New().String(),
    OrderID: 123,
    Status:  "created",
}

payload, _ := json.Marshal(event)
msg := broker.NewMessage("order.created", payload)
```

Dengan cara ini, ID tetap ada di payload meskipun publisher-nya bukan dari library ini.

## Metadata di Header (RabbitMQ)

Untuk adapter RabbitMQ, `msg.ID` dan `msg.Timestamp` dikirim sebagai AMQP message properties (bukan di dalam body), sehingga body yang diterima subscriber adalah murni `Payload`:

```
Wire format (body): {"order_id":123,"status":"created"}
AMQP properties  : MessageId = "...", Timestamp = "2024-..."
```

Ini memungkinkan sistem lain (yang tidak menggunakan library ini) tetap bisa membaca body tanpa perlu mem-parse wrapper tambahan.

## Kontrak Antara Publisher dan Subscriber

Karena format payload bebas, publisher dan subscriber harus **sepakat** pada struktur data yang digunakan. Dokumentasikan kontrak ini di level aplikasi (misalnya dalam bentuk struct Go atau JSON schema).

Contoh kontrak untuk topic `telemetry.position`:

```go
// Kontrak yang disepakati — digunakan oleh publisher maupun subscriber
type PositionEvent struct {
    DeviceID  string  `json:"device_id"`
    Latitude  float64 `json:"lat"`
    Longitude float64 `json:"lng"`
    Speed     float64 `json:"speed"`
    Timestamp int64   `json:"ts"` // Unix timestamp
}
```
