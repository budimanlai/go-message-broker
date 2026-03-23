package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	broker "github.com/budimanlai/go-message-broker"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// DBAdapter implements a simple message queue using a SQL database table via GORM.
type DBAdapter struct {
	db           *gorm.DB
	driver       string
	dsn          string
	workerName   string
	isShared     bool
	pollInterval time.Duration
	poolLimit    int
	maxRetry     int
}

// Ensure DBAdapter implements broker.Broker
var _ broker.Broker = (*DBAdapter)(nil)

func NewDBAdapter(config map[string]interface{}) (broker.Broker, error) {
	workerName, _ := config["worker_name"].(string)
	if workerName == "" {
		workerName = "default-worker"
	}

	// Default poll interval 2 seconds
	pollInterval := 2 * time.Second
	if interval, ok := config["poll_interval"].(time.Duration); ok {
		pollInterval = interval
	} else if intervalMs, ok := config["poll_interval_ms"].(int); ok {
		pollInterval = time.Duration(intervalMs) * time.Millisecond
	}

	// Default pool limit to 10
	poolLimit := 10
	if limit, ok := config["pool_limit"].(int); ok && limit > 0 {
		poolLimit = limit
	}

	// Default max retry to 3
	maxRetry := 3
	if retry, ok := config["max_retry"].(int); ok && retry >= 0 {
		maxRetry = retry
	}

	adapter := &DBAdapter{
		workerName:   workerName,
		pollInterval: pollInterval,
		poolLimit:    poolLimit,
		maxRetry:     maxRetry,
	}

	if dbInstance, ok := config["db_instance"].(*gorm.DB); ok {
		adapter.db = dbInstance
		adapter.isShared = true
		return adapter, nil
	}

	driver, _ := config["driver"].(string)
	dsn, _ := config["dsn"].(string)

	if driver == "" || dsn == "" {
		return nil, fmt.Errorf("db driver and dsn required")
	}

	adapter.driver = driver
	adapter.dsn = dsn

	return adapter, nil
}

func (d *DBAdapter) Connect() error {
	if d.isShared && d.db != nil {
		return nil
	}

	var dialector gorm.Dialector

	switch d.driver {
	case "mysql":
		dialector = mysql.Open(d.dsn)
	case "postgres":
		dialector = postgres.Open(d.dsn)
	default:
		return fmt.Errorf("unsupported driver: %s", d.driver)
	}

	var err error
	d.db, err = gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		return err
	}

	// Schema migration should be handled externally
	return nil
}

func (d *DBAdapter) Disconnect() error {
	if d.isShared {
		return nil
	}
	if d.db != nil {
		sqlDB, err := d.db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	return nil
}

func (d *DBAdapter) Publish(ctx context.Context, topic string, msg broker.Message) error {
	payloadBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	payload := string(payloadBytes)

	// Optimized fan-out using single SQL query.
	// We use table alias 'b' to avoid any ambiguity.
	query := `
		INSERT INTO broker_messages (worker_name, topic, payload, status, created_at, retry_count)
		SELECT b.worker_name, b.topic, ?, 'pending', now(), 0
		FROM broker b
		WHERE b.topic = ? AND b.status = 'active'
	`

	// Note: We use explicit column selection instead of * to be safe
	result := d.db.WithContext(ctx).Exec(query, payload, topic)
	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		log.Printf("Warning: Publish to topic '%s' resulted in 0 messages (no active workers found)", topic)
	}

	return nil
}

func (d *DBAdapter) Subscribe(ctx context.Context, topic string, handler broker.Handler) error {
	// Auto-register this worker for the topic if not exists
	err := d.registerWorker(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}

	go func() {
		// Use configured poll interval
		ticker := time.NewTicker(d.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.poll(ctx, topic, handler); err != nil {
					log.Printf("DB poll error: %v", err)
				}
			}
		}
	}()
	return nil
}

// registerWorker ensures the current worker is in the broker table for fan-out.
func (d *DBAdapter) registerWorker(ctx context.Context, topic string) error {
	// Use FirstOrCreate to avoid duplicates
	// Note: We assume d.workerName is set (defaults to "default-worker")
	b := Broker{
		WorkerName: d.workerName,
		Topic:      topic,
		Status:     "active",
	}

	return d.db.WithContext(ctx).
		Where(Broker{WorkerName: d.workerName, Topic: topic}).
		FirstOrCreate(&b).Error
}

func (d *DBAdapter) poll(ctx context.Context, topic string, handler broker.Handler) error {
	// 1. Fetch pending messages FOR THIS WORKER
	// Also filter by retry_count < maxRetry
	var messages []BrokerMessage

	// Transaction to safely pick up messages
	// Ideally we would lock rows (FOR UPDATE), but keeping it simple with GORM first.
	result := d.db.WithContext(ctx).
		Where("topic = ? AND worker_name = ? AND status = ? AND retry_count < ?", topic, d.workerName, "pending", d.maxRetry).
		Limit(d.poolLimit).
		Find(&messages)

	if result.Error != nil {
		return result.Error
	}

	if len(messages) == 0 {
		return nil
	}

	// 2. Mark as processing
	// We update in memory then save individually inside goroutine or batch update here.
	// Batch update status to 'processing' to claim them processing.
	ids := make([]uint, len(messages))
	for i, m := range messages {
		ids[i] = m.ID
	}

	if err := d.db.WithContext(ctx).Model(&BrokerMessage{}).
		Where("id IN ?", ids).
		Update("status", "processing").Error; err != nil {
		return err
	}

	// 3. Process messages
	var wg sync.WaitGroup

	for _, m := range messages {
		wg.Add(1)
		go func(msg BrokerMessage) {
			defer wg.Done()

			var brokerMsg broker.Message
			if msg.Payload != nil {
				if err := json.Unmarshal([]byte(*msg.Payload), &brokerMsg); err != nil {
					log.Printf("Failed to unmarshal message ID %d: %v", msg.ID, err)
					d.updateStatus(context.Background(), msg.ID, "failed", err.Error())
					return
				}
			}

			// Call the handler
			if err := handler(ctx, brokerMsg); err != nil {
				log.Printf("Handler failed for message ID %d: %v", msg.ID, err)
				// Increment retry count
				d.incrementRetry(context.Background(), msg.ID, err.Error())
			} else {
				// Success
				d.updateStatus(context.Background(), msg.ID, "done", "")
			}
		}(m)
	}

	// Wait for all goroutines in this batch to complete
	wg.Wait()

	return nil
}

func (d *DBAdapter) updateStatus(ctx context.Context, id uint, status string, errorMsg string) {
	updates := map[string]interface{}{
		"status": status,
	}
	if errorMsg != "" {
		updates["error_message"] = errorMsg
	}
	d.db.WithContext(ctx).Model(&BrokerMessage{}).Where("id = ?", id).Updates(updates)
}

func (d *DBAdapter) incrementRetry(ctx context.Context, id uint, errorMsg string) {
	// Custom increment logic might be needed or fetch-update
	// Using gorm expression for atomic increment
	d.db.WithContext(ctx).Model(&BrokerMessage{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":        "pending", // Set back to pending so it can be picked up again
		"retry_count":   gorm.Expr("retry_count + ?", 1),
		"error_message": errorMsg,
	})
}

func init() {
	broker.Register("db", NewDBAdapter)
}
