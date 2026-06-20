# Adapter Database

## Cara Kerja

Adapter ini menggunakan tabel SQL sebagai message queue dengan mekanisme polling. Cocok jika Anda tidak memiliki infrastruktur RabbitMQ atau Redis, namun tetap butuh durability dan audit trail.

```
Publisher                         Database
    │                                │
    ├─ Publish("invoice.generated") ──► INSERT INTO broker_messages
    │                                │   (satu row per worker aktif)
    │                                │
Subscriber (polling tiap N detik) ──► SELECT pending WHERE worker_name = 'my-service'
    │                                │
    ├─ handler dipanggil             │
    └─ UPDATE status = 'done'       ──► UPDATE broker_messages
```

Setiap service dengan `worker_name` berbeda menerima salinan pesan sendiri (fan-out via INSERT...SELECT).

## Setup Tabel Database

Buat dua tabel berikut sebelum menggunakan adapter ini:

```sql
-- Tabel registry worker aktif per topic
CREATE TABLE broker (
    worker_name VARCHAR(25)  NOT NULL,
    topic       VARCHAR(255) NOT NULL,
    description TEXT,
    status      VARCHAR(15)  NOT NULL DEFAULT 'active',
    INDEX idx_broker_topic (topic)
);

-- Tabel antrian pesan
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

Untuk PostgreSQL, ganti `BIGINT UNSIGNED AUTO_INCREMENT` dengan `BIGSERIAL` dan `DATETIME` dengan `TIMESTAMP`.

## Instalasi Dependency

```bash
go get gorm.io/gorm
go get gorm.io/driver/mysql    # untuk MySQL
go get gorm.io/driver/postgres # untuk PostgreSQL
```

## Konfigurasi

| Key | Type | Default | Keterangan |
|---|---|---|---|
| `driver` | `string` | — | `"mysql"` atau `"postgres"` (wajib jika tidak pakai shared db) |
| `dsn` | `string` | — | Data Source Name koneksi database (wajib jika tidak pakai shared db) |
| `worker_name` | `string` | `"default-worker"` | Nama unik service ini, digunakan untuk fan-out |
| `pool_limit` | `int` | `10` | Jumlah pesan yang diambil dan diproses per polling |
| `poll_interval` | `time.Duration` | `2s` | Interval antar polling |
| `poll_interval_ms` | `int` | — | Alternatif poll_interval dalam milidetik |
| `max_retry` | `int` | `3` | Maksimal retry sebelum pesan dianggap gagal |
| `db_instance` | `*gorm.DB` | — | Gunakan koneksi GORM yang sudah ada (shared connection) |

## Penggunaan Dasar

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    broker "github.com/budimanlai/go-message-broker"
    _ "github.com/budimanlai/go-message-broker/adapters/db"
)

func main() {
    b, err := broker.New("db", map[string]interface{}{
        "driver":       "mysql",
        "dsn":          "root:password@tcp(localhost:3306)/mydb?parseTime=true",
        "worker_name":  "invoice-service",
        "pool_limit":   10,
        "poll_interval": 2 * time.Second,
        "max_retry":    3,
    })
    if err != nil {
        log.Fatal(err)
    }

    if err := b.Connect(); err != nil {
        log.Fatal(err)
    }
    defer b.Disconnect()

    ctx := context.Background()

    // Subscribe — worker otomatis terdaftar di tabel broker
    b.Subscribe(ctx, "invoice.generated", func(ctx context.Context, msg broker.Message) error {
        fmt.Println("Proses invoice:", string(msg.Payload))
        return nil
    })

    // Publish
    msg := broker.NewMessage("invoice.generated", []byte(`{"invoice_id": 99}`))
    if err := b.Publish(ctx, "invoice.generated", msg); err != nil {
        log.Printf("publish error: %v", err)
    }

    // Tunggu (polling berjalan di background)
    select {}
}
```

## Menggunakan Shared Connection (GORM)

```go
import "gorm.io/gorm"

// db adalah *gorm.DB yang sudah ada di aplikasi Anda
b, err := broker.New("db", map[string]interface{}{
    "db_instance": db,
    "worker_name": "invoice-service",
})
```

## Alur Status Pesan

```
pending ──► processing ──► done
                │
                └──► pending (retry, jika retry_count < max_retry)
```

Pesan yang telah mencapai `retry_count >= max_retry` tidak lagi diambil oleh poller (dikecualikan oleh WHERE clause), namun statusnya tetap `pending` di database — tidak berubah menjadi `failed`. Ini memudahkan debugging karena baris masih terlihat, tapi perlu diperhatikan agar tabel tidak menumpuk baris yang tidak terproses.

## Fan-out ke Beberapa Service

Ketika dua service berbeda subscribe ke topic yang sama, setiap service menerima salinan pesannya sendiri:

```
Service A: worker_name = "invoice-service"  → subscribe "payment.completed"
Service B: worker_name = "email-service"    → subscribe "payment.completed"

Publish "payment.completed" → INSERT 2 rows:
  - worker_name="invoice-service", topic="payment.completed", status="pending"
  - worker_name="email-service",   topic="payment.completed", status="pending"
```

Ini terjadi otomatis karena adapter menggunakan INSERT...SELECT dari tabel `broker` yang berisi daftar worker aktif.

## Wire Format

Payload yang disimpan di kolom `payload` pada tabel `broker_messages` adalah **raw `msg.Payload`** — bukan wrapper JSON. Ini memudahkan debugging langsung dari database dan memungkinkan sistem lain membaca atau menulis ke tabel tersebut.
