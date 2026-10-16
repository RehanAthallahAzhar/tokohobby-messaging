package messaging

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNewProducer_UnsupportedBroker(t *testing.T) {
	cfg := &ProducerConfig{
		Config: Config{
			BrokerType: "redis",
		},
	}

	prod, err := NewProducer(cfg)
	if err == nil {
		t.Fatal("expected error for unsupported broker type, got nil")
	}
	if prod != nil {
		t.Errorf("expected nil producer, got %v", prod)
	}
	if !strings.Contains(err.Error(), "unsupported broker type") {
		t.Errorf("expected error message to contain 'unsupported broker type', got %v", err)
	}
}

func TestNewProducer_Kafka(t *testing.T) {
	cfg := &ProducerConfig{
		Config: Config{
			BrokerType: "kafka",
			Brokers:    []string{"localhost:9092"},
			Topic:      "test-topic",
		},
		AsyncMode: true,
		BatchSize: 10,
	}

	prod, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("unexpected error creating kafka producer: %v", err)
	}
	if prod == nil {
		t.Fatal("expected non-nil producer, got nil")
	}

	// Should close cleanly
	if err := prod.Close(); err != nil {
		t.Errorf("failed to close kafka producer: %v", err)
	}
}

func TestNewProducer_RabbitMQ_InvalidBroker(t *testing.T) {
	cfg := &ProducerConfig{
		Config: Config{
			BrokerType: "rabbitmq",
			Brokers:    []string{"amqp://invalid-host-not-found:5672/"},
			Topic:      "test-queue",
		},
	}

	prod, err := NewProducer(cfg)
	if err == nil {
		t.Fatal("expected error connecting to invalid rabbitmq broker, got nil")
	}
	if prod != nil {
		t.Errorf("expected nil producer, got %v", prod)
	}
	if !strings.Contains(err.Error(), "rabbitmq producer") {
		t.Errorf("expected error to wrap 'rabbitmq producer', got: %v", err)
	}
}

func TestProducerConfig_Fields(t *testing.T) {
	cfg := ProducerConfig{
		Config: Config{
			BrokerType:   "kafka",
			Brokers:      []string{"127.0.0.1:9092"},
			Topic:        "orders",
			GroupID:      "order-group",
			ClientID:     "order-service",
			RetryEnabled: true,
			MaxRetries:   3,
		},
		AsyncMode: true,
		BatchSize: 100,
	}

	if cfg.BrokerType != "kafka" || len(cfg.Brokers) != 1 || cfg.Topic != "orders" {
		t.Errorf("unexpected config values: %+v", cfg)
	}
	if !cfg.AsyncMode || cfg.BatchSize != 100 {
		t.Errorf("unexpected producer config values: %+v", cfg)
	}
}

func TestMessage_Fields(t *testing.T) {
	now := time.Now().Unix()
	msg := &Message{
		Topic:     "events",
		Key:       []byte("key-1"),
		Value:     []byte("payload-1"),
		Headers:   map[string]string{"trace-id": "1234"},
		Metadata:  map[string]interface{}{"retry_count": 1},
		Timestamp: now,
	}

	if msg.Topic != "events" || string(msg.Key) != "key-1" || string(msg.Value) != "payload-1" {
		t.Errorf("unexpected message values: %+v", msg)
	}
	if msg.Headers["trace-id"] != "1234" {
		t.Errorf("unexpected header: %v", msg.Headers)
	}
}

func TestKafkaProducerAdapter_Publish(t *testing.T) {
	cfg := &ProducerConfig{
		Config: Config{
			BrokerType: "kafka",
			Brokers:    []string{"localhost:9092"},
			Topic:      "test-topic",
		},
		AsyncMode: true,
		BatchSize: 10,
	}

	prod, err := NewProducer(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer prod.Close() //nolint:errcheck

	// Context with timeout to avoid hanging if network call is triggered
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msg := &Message{
		Topic:     "test-topic",
		Key:       []byte("key-1"),
		Value:     []byte(`{"hello":"world"}`),
		Headers:   map[string]string{"h1": "v1"},
		Timestamp: time.Now().Unix(),
	}

	// In async mode, writer buffers or writes async
	_ = prod.Publish(ctx, msg)
	_ = prod.PublishBatch(ctx, []*Message{msg})
}
