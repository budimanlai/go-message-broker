package db

import "time"

type Broker struct {
	WorkerName  string  `gorm:"column:worker_name;type:varchar(25);not null"`
	Topic       string  `gorm:"column:topic;type:varchar(255);not null;index:idx-broker-topic"`
	Description *string `gorm:"column:description;type:text"`
	Status      string  `gorm:"column:status;type:varchar(15);not null;default:'active'"`
}

func (Broker) TableName() string {
	return "broker"
}

type BrokerMessage struct {
	ID           uint      `gorm:"primaryKey"`
	WorkerName   string    `gorm:"column:worker_name;type:varchar(255);not null"`
	Topic        string    `gorm:"column:topic;type:varchar(255);not null"`
	Payload      *string   `gorm:"column:payload;type:text"`
	Status       string    `gorm:"column:status;type:varchar(15);not null;default:'pending'"`
	RetryCount   int       `gorm:"column:retry_count;type:int(11);not null;default:0"`
	ErrorMessage *string   `gorm:"column:error_message;type:text"`
	CreatedAt    time.Time `gorm:"column:created_at;type:datetime;not null"`
}

func (BrokerMessage) TableName() string {
	return "broker_messages"
}
