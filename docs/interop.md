# Interoperabilitas dengan Sistem Eksternal

## Konsep Dasar

Library ini menggunakan protokol standar (AMQP untuk RabbitMQ, Redis Pub/Sub untuk Redis). Di level protokol, pesan hanyalah `[]byte` — tidak ada format yang dipaksakan.

```
Wire format = raw bytes (msg.Payload)
```

Artinya:
- Publisher dari **aplikasi manapun** (Go, Python, Node.js, Java, Traccar, dsb) bisa diterima oleh subscriber library ini
- Subscriber dari **aplikasi manapun** bisa menerima pesan yang dikirim library ini

Satu-satunya yang perlu disepakati adalah **format payload** — JSON, Protobuf, plain text, atau format lainnya — di level aplikasi, bukan di level library.

## Skenario: Consume dari Publisher Eksternal

Publisher eksternal (misalnya Traccar, sistem IoT, atau service lain) mengirim pesan ke RabbitMQ atau Redis. Library ini subscribe dan memproses payload sesuai format yang dikirim publisher.

### Syarat untuk RabbitMQ

Publisher eksternal harus publish ke:
- **Exchange**: `eventbus`
- **Exchange type**: `topic`
- **Routing key**: nama topic yang ingin di-consume (contoh: `device.position`)

```python
# Contoh publisher Python
import pika, json

connection = pika.BlockingConnection(pika.URLParameters("amqp://admin:admin123@localhost:5672/"))
channel = connection.channel()
channel.exchange_declare(exchange="eventbus", exchange_type="topic", durable=True)

payload = json.dumps({"device_id": "abc", "lat": -6.2, "lng": 106.8, "speed": 60})
channel.basic_publish(
    exchange="eventbus",
    routing_key="device.position",
    body=payload.encode()
)
```

```go
// Subscriber Go menggunakan library ini
b.Subscribe(ctx, "device.position", func(ctx context.Context, msg broker.Message) error {
    // msg.Payload berisi: {"device_id":"abc","lat":-6.2,"lng":106.8,"speed":60}
    var pos PositionData
    json.Unmarshal(msg.Payload, &pos)
    fmt.Printf("Device %s di lat:%.4f lng:%.4f\n", pos.DeviceID, pos.Lat, pos.Lng)
    return nil
})
```

### Syarat untuk Redis

Publisher eksternal cukup publish ke channel Redis yang sama:

```bash
# Dari redis-cli
PUBLISH device.position '{"device_id":"abc","lat":-6.2,"lng":106.8}'
```

```go
// Subscriber Go menggunakan library ini — tidak perlu perubahan
b.Subscribe(ctx, "device.position", func(ctx context.Context, msg broker.Message) error {
    fmt.Println(string(msg.Payload))
    return nil
})
```

## Skenario: Publisher Library ini, Subscriber Eksternal

Library ini publish raw `msg.Payload` ke wire, sehingga subscriber eksternal langsung menerima payload tanpa wrapper tambahan.

```go
// Go publisher menggunakan library ini
payload := []byte(`{"order_id":123,"status":"created","amount":150000}`)
msg := broker.NewMessage("order.created", payload)
b.Publish(ctx, "order.created", msg)
```

```javascript
// Subscriber Node.js — menerima payload langsung
const amqp = require('amqplib');
// ... setup channel ...
channel.consume(queueName, (msg) => {
    const data = JSON.parse(msg.content.toString());
    // data = { order_id: 123, status: "created", amount: 150000 }
    console.log('Order:', data.order_id);
});
```

## Menyepakati Kontrak Payload

Karena format payload bebas, publisher dan subscriber harus menyepakati struktur data. Cara yang disarankan adalah mendokumentasikan kontrak dalam bentuk struct atau JSON schema yang dibagikan ke semua pihak.

```go
// Definisikan di package bersama, atau dokumentasikan sebagai JSON schema

// Topic: device.position
type PositionEvent struct {
    DeviceID  string  `json:"device_id"`
    Latitude  float64 `json:"lat"`
    Longitude float64 `json:"lng"`
    Speed     float64 `json:"speed"`     // km/h
    Timestamp int64   `json:"ts"`        // Unix timestamp (detik)
}

// Topic: order.created
type OrderCreatedEvent struct {
    EventID   string  `json:"event_id"`  // opsional, untuk idempotency
    OrderID   int     `json:"order_id"`
    UserID    int     `json:"user_id"`
    Amount    float64 `json:"amount"`
    CreatedAt string  `json:"created_at"` // RFC3339
}
```

## Field yang Tersedia di Sisi Subscriber

Bergantung pada adapter yang digunakan, berikut field yang tersedia di `broker.Message` saat menerima pesan dari publisher eksternal:

| Field | RabbitMQ | Redis | DB |
|---|---|---|---|
| `Payload` | ✅ raw bytes dari body | ✅ raw bytes dari channel | ✅ raw bytes dari kolom payload |
| `Topic` | ✅ dari routing key | ✅ dari nama channel | ✅ dari kolom topic |
| `ID` | ✅ dari AMQP `MessageId` property (jika diset publisher) | — kosong | — kosong |
| `Timestamp` | ✅ dari AMQP `Timestamp` property (jika diset publisher) | — zero value | — zero value |
| `Headers` | — kosong (tidak di-map dari AMQP headers) | — kosong | — kosong |

Jika Anda butuh `ID` atau `Timestamp` dari publisher eksternal yang tidak support AMQP properties, sertakan data tersebut di dalam payload.
