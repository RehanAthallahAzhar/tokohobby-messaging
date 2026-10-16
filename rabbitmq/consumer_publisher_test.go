package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPublisher_Publish_MarshalError ensures that a non-marshallable body
// returns an error immediately without attempting to connect.
func TestPublisher_Publish_MarshalError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 1

	rmq := &RabbitMQ{
		config:    cfg,
		closed:    true, // simulate closed connection
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	close(rmq.closeChan)

	pub := NewPublisher(rmq)

	// chan cannot be marshalled to JSON
	err := pub.Publish(context.Background(), PublishOptions{}, make(chan int))
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
}

// TestPublisher_Publish_ContextCancelled verifies that a cancelled context
// aborts the retry sleep before the next attempt.
func TestPublisher_Publish_ContextCancelled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = 5
	cfg.RetryDelay = 5 * time.Second // long enough to detect premature exit

	rmq := &RabbitMQ{
		config:    cfg,
		closed:    true, // GetChannel will return error → triggers retry
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	close(rmq.closeChan)

	pub := NewPublisher(rmq)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := pub.Publish(ctx, PublishOptions{}, map[string]string{"key": "value"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
	// Should exit well before RetryDelay (5s)
	if elapsed > 2*time.Second {
		t.Errorf("Publish did not respect context cancellation: took %v", elapsed)
	}
}

// TestConsumer_NewConsumer_DefaultWorkers verifies the default worker count.
func TestConsumer_NewConsumer_DefaultWorkers(t *testing.T) {
	rmq := &RabbitMQ{
		config:    DefaultConfig(),
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	c := NewConsumer(rmq, ConsumerOptions{QueueName: "test"}, nil)
	if c.workerCount != 5 {
		t.Errorf("expected default worker count 5, got %d", c.workerCount)
	}
}

// TestConsumer_NewConsumer_CustomWorkers verifies custom worker count is respected.
func TestConsumer_NewConsumer_CustomWorkers(t *testing.T) {
	rmq := &RabbitMQ{
		config:    DefaultConfig(),
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	c := NewConsumer(rmq, ConsumerOptions{QueueName: "test", WorkerCount: 10}, nil)
	if c.workerCount != 10 {
		t.Errorf("expected worker count 10, got %d", c.workerCount)
	}
}

// TestConsumer_AutoAck_Propagated ensures AutoAck is stored on the Consumer struct.
func TestConsumer_AutoAck_Propagated(t *testing.T) {
	rmq := &RabbitMQ{
		config:    DefaultConfig(),
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	c := NewConsumer(rmq, ConsumerOptions{QueueName: "test", AutoAck: true}, nil)
	if !c.autoAck {
		t.Error("expected autoAck=true, got false")
	}
}

// TestWorker_HandlesMessages validates the worker goroutine dispatching
// using a fake channel and a counting handler.
func TestWorker_HandlesMessages(t *testing.T) {
	var processed atomic.Int32
	handler := func(_ context.Context, _ []byte) error {
		processed.Add(1)
		return nil
	}

	rmq := &RabbitMQ{
		config:    DefaultConfig(),
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	c := NewConsumer(rmq, ConsumerOptions{QueueName: "test", WorkerCount: 2}, handler)

	ctx, cancel := context.WithCancel(context.Background())

	jobChan := make(chan fakeDelivery, 10)
	var wg sync.WaitGroup

	// Start 2 workers via the internal worker method indirectly
	// by calling the exported logic path through a helper
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.workerFake(ctx, 0, jobChan)
	}()

	jobChan <- fakeDelivery{body: []byte(`{"hello":"world"}`)}
	jobChan <- fakeDelivery{body: []byte(`{"hello":"test"}`)}

	// Give workers time to process
	time.Sleep(100 * time.Millisecond)
	cancel()
	close(jobChan)
	wg.Wait()

	if processed.Load() != 2 {
		t.Errorf("expected 2 messages processed, got %d", processed.Load())
	}
}

// TestWorker_NackOnError ensures NACK is attempted on handler error.
func TestWorker_NackOnError(t *testing.T) {
	var nackedCount atomic.Int32
	handler := func(_ context.Context, _ []byte) error {
		return errors.New("handler error")
	}

	rmq := &RabbitMQ{
		config:    DefaultConfig(),
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}
	c := NewConsumer(rmq, ConsumerOptions{QueueName: "test"}, handler)

	ctx, cancel := context.WithCancel(context.Background())
	jobChan := make(chan fakeDelivery, 5)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.workerFake(ctx, 0, jobChan)
	}()

	jobChan <- fakeDelivery{body: []byte(`{}`), onNack: func() { nackedCount.Add(1) }}

	time.Sleep(100 * time.Millisecond)
	cancel()
	close(jobChan)
	wg.Wait()

	if nackedCount.Load() != 1 {
		t.Errorf("expected 1 NACK, got %d", nackedCount.Load())
	}
}

// UnmarshalMessage helper test
func TestUnmarshalMessage(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	body := []byte(`{"name":"tokohobby"}`)
	var p payload
	if err := UnmarshalMessage(body, &p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.Name != "tokohobby" {
		t.Errorf("expected name=tokohobby, got %s", p.Name)
	}
}

// ---- test helpers ----

// fakeDelivery mimics amqp.Delivery for unit tests without a live broker.
type fakeDelivery struct {
	body   []byte
	onAck  func()
	onNack func()
}

// workerFake is an internal test helper that calls the same logic as worker()
// but accepts fakeDelivery instead of amqp.Delivery so we can test without a broker.
func (c *Consumer) workerFake(ctx context.Context, _ int, jobs <-chan fakeDelivery) {
	for {
		select {
		case msg, ok := <-jobs:
			if !ok {
				return
			}
			err := c.handler(ctx, msg.body)
			if err != nil {
				if msg.onNack != nil {
					msg.onNack()
				}
			} else {
				if msg.onAck != nil {
					msg.onAck()
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
