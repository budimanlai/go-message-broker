# Release Notes

## v1.1.0 — 2026-06-20

### Added
- Kompatibilitas publisher dari library lain: adapter kini menerima payload dari publisher eksternal tanpa perlu wrapper khusus.
- Shared connection support untuk semua adapter — `amqp_connection`, `redis_client`, dan `db_instance` dapat dioper langsung ke config map sehingga tidak membuat koneksi baru.
- EventBus layer: routing event lokal (in-process) dan eksternal (broker) dalam satu API dengan konfigurasi per-topic (`EmitToLocal`, `EmitToBroker`, `ConsumeFromLocal`, `ConsumeFromBroker`).
- Worker pool lokal dengan buffer channel yang dapat dikonfigurasi (`WorkerPool`, `BufferSize`).

### Changed
- Dependency `streadway/amqp` diganti dengan `rabbitmq/amqp091-go` (official RabbitMQ Go client).
- Adapter RabbitMQ menggunakan connection pool; jumlah pesan bersamaan dikontrol via `pool_limit` (QoS prefetch).
- Nama queue RabbitMQ kini menggunakan format `{worker_name}.{topic}` sehingga beberapa service dapat subscribe ke topic yang sama dengan queue masing-masing.

### Fixed
- Konfigurasi `worker_name` pada adapter RabbitMQ sebelumnya tidak diterapkan ke nama queue.

---

## v1.0.0 — 2026-05-01

### Added
- Interface `Broker` dengan metode `Connect`, `Disconnect`, `Publish`, dan `Subscribe`.
- Adapter **RabbitMQ** — AMQP topic exchange.
- Adapter **Redis** — Redis Pub/Sub.
- Adapter **Database** — polling tabel SQL (MySQL & PostgreSQL via GORM) dengan dukungan retry (`max_retry`) dan interval polling (`poll_interval`).
- Factory pattern (`broker.New`) untuk registrasi dan inisialisasi adapter.
- Struct `Message` dengan field `ID`, `Topic`, `Payload`, `Headers`, dan `Timestamp`.
- Helper `broker.NewMessage` untuk membuat pesan dengan ID otomatis.
