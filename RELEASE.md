# Release Notes

## v1.2.0 — 2026-06-20

### Added
- **Multi-broker routing**: satu topic dapat dikirim ke beberapa broker sekaligus menggunakan `Config.Brokers` (named adapters) dan `TopicConfig.Brokers []string`. Broker yang tidak terdaftar menghasilkan error eksplisit saat `Emit`.
- **Middleware per topic**: implementasikan interface `Middleware` (`BeforeHandle` / `AfterHandle`) dan daftarkan di `TopicConfig.Middleware`. Jika `BeforeHandle` mengembalikan error, handler tidak dipanggil.
- **Retry otomatis**: `TopicConfig.Retry` menerima `RetryPolicy{MaxRetry, Delay}`. Setelah semua attempt habis, pesan diteruskan ke `FallbackAdapter` dan broker menerima ACK.
- **Fallback storage**: `Config.Fallback` menerima implementasi `FallbackAdapter` (interface `Store(ctx, FailedPublish) error`). Dipanggil saat publish ke broker gagal maupun saat retry subscribe habis.
- **Asynchronous broadcast worker pool**: `Config.BroadcastWorkerCount` dan `Config.BroadcastQueueSize` mengaktifkan worker pool publish. Default (`0`) tetap synchronous untuk backward compatibility.
- `type Adapter = Broker` — type alias sehingga adapter dapat dioper ke `Config.Brokers` tanpa cast.

### Fixed
- **LocalBus goroutine leak**: goroutine worker sebelumnya hanya berhenti saat channel ditutup. Sekarang menggunakan `context` dan `sync.WaitGroup`; `Close()` menjamin semua goroutine berhenti.
- **LocalBus drain on Close**: pesan yang sudah masuk channel buffer tapi belum diproses kini diselesaikan sebelum `Close()` return sehingga tidak ada pesan yang hilang saat service restart.
- **Broadcast queue drain**: `eventBus.Close()` menunggu worker selesai (`broadcastWg.Wait()`) lalu menguras sisa antrian secara synchronous sebelum memutus koneksi broker.
- Kode mati `runAfterPublish` dan field `FallbackConfig.MaxRetry` yang tidak digunakan telah dihapus.

### Changed
- `RetryPolicy.Delay` sekarang aktif: jeda antar attempt dijalankan via `time.Sleep`. Sebelumnya field ini ada tapi tidak berdampak.

---

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
