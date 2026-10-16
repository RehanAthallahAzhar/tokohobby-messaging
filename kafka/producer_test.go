package kafka

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestNewProducer(t *testing.T) {
	cfg := &ProducerConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "test-topic",
		AsyncMode: true,
		BatchSize: 50,
	}

	prod := NewProducer(cfg)
	if prod == nil {
		t.Fatal("expected non-nil producer, got nil")
	}
	if prod.writer == nil {
		t.Fatal("expected non-nil writer, got nil")
	}

	if err := prod.Close(); err != nil {
		t.Errorf("failed to close producer: %v", err)
	}
}

func TestNewActivityProducer(t *testing.T) {
	brokers := []string{"localhost:9092"}
	ap := NewActivityProducer(brokers)
	if ap == nil {
		t.Fatal("expected non-nil activity producer, got nil")
	}
	if ap.producer == nil {
		t.Fatal("expected non-nil inner producer, got nil")
	}

	if err := ap.Close(); err != nil {
		t.Errorf("failed to close activity producer: %v", err)
	}
}

func TestUserActivityEvent_JSON(t *testing.T) {
	userID := "user-123"
	now := time.Now()
	event := &UserActivityEvent{
		EventID:   "evt-001",
		EventType: "LOGIN",
		UserID:    &userID,
		SessionID: "sess-abc",
		Timestamp: now,
		Metadata: map[string]interface{}{
			"ip": "127.0.0.1",
		},
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	var decoded UserActivityEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if decoded.EventID != "evt-001" || decoded.EventType != "LOGIN" || *decoded.UserID != "user-123" {
		t.Errorf("mismatch in decoded event: %+v", decoded)
	}
}

func TestProducer_Publish_Async(t *testing.T) {
	prod := NewProducerFromMessaging([]string{"localhost:9092"}, "test-topic", true, 10)
	defer prod.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	msg := &Message{
		Topic:     "test-topic",
		Key:       []byte("k1"),
		Value:     []byte("v1"),
		Headers:   map[string]string{"correlation_id": "c1"},
		Timestamp: time.Now().Unix(),
	}

	// In async mode, writer buffers without blocking on network connect
	_ = prod.Publish(ctx, msg)
	_ = prod.PublishBatch(ctx, []*Message{msg})
}

func TestActivityProducer_PublishActivity_Async(t *testing.T) {
	ap := NewActivityProducer([]string{"localhost:9092"})
	defer ap.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	uid := "u1"
	event := &UserActivityEvent{
		EventID:   "evt-1",
		EventType: "CLICK",
		UserID:    &uid,
		SessionID: "s1",
		Timestamp: time.Now(),
	}

	_ = ap.PublishActivity(ctx, event)
}
