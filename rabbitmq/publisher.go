package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher sends messages to a RabbitMQ exchange.
type Publisher struct {
	rmq *RabbitMQ
}

// NewPublisher creates a new Publisher backed by the given RabbitMQ connection.
func NewPublisher(rmq *RabbitMQ) *Publisher {
	return &Publisher{rmq: rmq}
}

// PublishOptions holds the routing metadata for a single publish call.
type PublishOptions struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// Publish marshals body to JSON and sends it to RabbitMQ with retry logic.
// It respects context cancellation both during sleep between retries and during
// the publish call itself.
func (p *Publisher) Publish(ctx context.Context, opts PublishOptions, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= p.rmq.config.MaxRetries; attempt++ {
		if attempt > 0 {
			slog.InfoContext(ctx, "Retry attempt",
				slog.Int("attempt", attempt),
				slog.Int("max_retries", p.rmq.config.MaxRetries),
			)
			// Context-aware sleep: cancel immediately if ctx is done
			select {
			case <-time.After(p.rmq.config.RetryDelay):
			case <-ctx.Done():
				return fmt.Errorf("publish cancelled: %w", ctx.Err())
			}
		}

		ch, err := p.rmq.GetChannel()
		if err != nil {
			lastErr = err
			continue
		}

		// Each publish gets its own timeout context — cancel is called
		// synchronously (not via defer) so it's released before the next iteration.
		publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = ch.PublishWithContext(
			publishCtx,
			opts.Exchange,   // exchange
			opts.RoutingKey, // routing key
			opts.Mandatory,  // mandatory
			opts.Immediate,  // immediate
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         data,
				DeliveryMode: amqp.Persistent, // survive broker restart
				Timestamp:    time.Now(),
			},
		)
		cancel() // always release immediately — NOT defer inside a loop

		if err == nil {
			slog.InfoContext(ctx, "Message published",
				slog.String("exchange", opts.Exchange),
				slog.String("routing_key", opts.RoutingKey),
			)
			return nil
		}

		lastErr = err
		slog.ErrorContext(ctx, "Publish failed",
			slog.Int("attempt", attempt+1),
			slog.Any("error", err),
		)
	}

	return fmt.Errorf("failed to publish after %d attempts: %w",
		p.rmq.config.MaxRetries, lastErr)
}

// DeclareExchange declares an exchange if it does not already exist.
func (p *Publisher) DeclareExchange(name, kind string, durable bool) error {
	ch, err := p.rmq.GetChannel()
	if err != nil {
		return err
	}
	return ch.ExchangeDeclare(
		name,    // name
		kind,    // type: direct, topic, fanout, headers
		durable, // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
}
