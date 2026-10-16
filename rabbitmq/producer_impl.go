package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message represents a message payload for use with the generic Producer.
// This mirrors messaging.Message to avoid an import cycle between packages.
type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp int64
}

// Producer implements the messaging.Producer interface for RabbitMQ.
// It is intended to be used via the messaging factory (messaging.NewProducer).
type Producer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewProducerFromMessaging creates a RabbitMQ Producer from a broker address list.
// Returns an error if the connection or channel cannot be established.
func NewProducerFromMessaging(brokers []string, _ string) (*Producer, error) {
	conn, err := amqp.Dial("amqp://" + brokers[0])
	if err != nil {
		return nil, fmt.Errorf("rabbitmq: dial %s: %w", brokers[0], err)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	return &Producer{
		conn:    conn,
		channel: channel,
	}, nil
}

// Publish sends a single message to RabbitMQ.
func (p *Producer) Publish(ctx context.Context, msg *Message) error {
	headers := make(amqp.Table)
	for k, v := range msg.Headers {
		headers[k] = v
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         msg.Value,
		Headers:      headers,
		DeliveryMode: amqp.Persistent,
	}

	return p.channel.PublishWithContext(
		ctx,
		"",        // exchange (default — direct to queue by routing key)
		msg.Topic, // routing key / queue name
		false,
		false,
		publishing,
	)
}

// PublishBatch sends multiple messages sequentially.
func (p *Producer) PublishBatch(ctx context.Context, messages []*Message) error {
	for _, msg := range messages {
		if err := p.Publish(ctx, msg); err != nil {
			return err
		}
	}
	return nil
}

// Close releases channel and connection resources.
func (p *Producer) Close() error {
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			slog.Error("Failed to close RabbitMQ channel", slog.Any("error", err))
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			return fmt.Errorf("close RabbitMQ connection: %w", err)
		}
	}
	return nil
}
