package rabbitmq

import (
	"testing"
)

func TestRabbitMQ_GetChannel_ClosedOrUninitialized(t *testing.T) {
	// Case 1: Uninitialized channel
	rmq := &RabbitMQ{}
	ch, err := rmq.GetChannel()
	if err == nil {
		t.Fatal("expected error on uninitialized channel, got nil")
	}
	if ch != nil {
		t.Errorf("expected nil channel, got %v", ch)
	}

	// Case 2: Closed instance
	rmqClosed := &RabbitMQ{closed: true}
	ch, err = rmqClosed.GetChannel()
	if err == nil {
		t.Fatal("expected error on closed RabbitMQ, got nil")
	}
	if ch != nil {
		t.Errorf("expected nil channel, got %v", ch)
	}
}

func TestRabbitMQ_Close_Idempotent(t *testing.T) {
	rmq := &RabbitMQ{
		closeChan: make(chan struct{}),
	}

	// First close
	if err := rmq.Close(); err != nil {
		t.Fatalf("unexpected error on first Close: %v", err)
	}

	// Channel should be closed
	select {
	case <-rmq.closeChan:
		// OK
	default:
		t.Error("closeChan was not closed")
	}

	// Second close should be safe and return nil immediately
	if err := rmq.Close(); err != nil {
		t.Fatalf("unexpected error on second Close: %v", err)
	}
}

func TestNewRabbitMQ_InvalidURL(t *testing.T) {
	cfg := &RabbitMQConfig{
		URL: "amqp://invalid-host-that-does-not-exist:5672/",
	}

	rmq, err := NewRabbitMQ(cfg)
	if err == nil {
		t.Fatal("expected error when connecting to invalid host, got nil")
	}
	if rmq != nil {
		t.Errorf("expected nil RabbitMQ on error, got %v", rmq)
	}
}
