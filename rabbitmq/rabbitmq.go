package rabbitmq

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	config    *RabbitMQConfig
	conn      *amqp.Connection // Koneksi TCP ke RabbitMQ
	channel   *amqp.Channel    // Channel untuk komunikasi dengan RabbitMQ
	mu        sync.RWMutex     // for thread-safe operations from multiple goroutines (race condition)
	closed    bool             // flag to indicate if connection is closed
	closeChan chan struct{}    // channel to signal connection close
	reconnect chan struct{}    // channel to signal reconnect
}

func NewRabbitMQ(config *RabbitMQConfig) (*RabbitMQ, error) {
	if config == nil {
		config = DefaultConfig()
	}

	rmq := &RabbitMQ{
		config:    config,
		closeChan: make(chan struct{}),
		reconnect: make(chan struct{}, 1),
	}

	if err := rmq.connect(); err != nil {
		return nil, err
	}

	go rmq.handleReconnect()
	return rmq, nil
}

func (r *RabbitMQ) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var err error

	// Create connection
	r.conn, err = amqp.Dial(r.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// Create channel
	r.channel, err = r.conn.Channel()
	if err != nil {
		// Close connection immediately — no defer needed here
		_ = r.conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS (Quality of Service) for consumer
	/*
		Qos is a way to control the flow of messages between the client and the server.
		It is used to prevent the server from sending messages to the client faster than the client can process them.

		Prefetch count -> restrict message count
			example: if we set prefetch count is 10, it will send max 10 messages to consumer
					if consumer is processing 1 message, it will send 1 next message
		Prefetch size -> restrict message byte size,
			example: dont send more than 1024 bytes if prefetch size is 1024
		Global -> false means only for this consumer, true means for all consumer
	*/
	err = r.channel.Qos(
		r.config.PrefetchCount, // prefetch count
		0,                      // prefetch size
		false,                  // global
	)
	if err != nil {
		_ = r.channel.Close()
		_ = r.conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	slog.Info("Connected to RabbitMQ successfully")

	// Monitor connection close — pass a done channel so goroutine can be stopped
	// on reconnect without leaking
	go r.monitorConnection(r.closeChan)

	return nil
}

// monitorConnection monitors if connection is closed.
// It accepts a snapshot of closeChan at the moment it's spawned so that
// each reconnect cycle gets its own goroutine tied to the right signal.
func (r *RabbitMQ) monitorConnection(done <-chan struct{}) {
	connClose := r.conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case err, ok := <-connClose:
		if ok && err != nil {
			slog.Error("RabbitMQ connection closed unexpectedly", slog.String("error", err.Error()))
			// Trigger reconnect (non-blocking)
			select {
			case r.reconnect <- struct{}{}:
			default:
			}
		}
	case <-done:
		return
	}
}

// handleReconnect handles automatic reconnection with exponential backoff.
func (r *RabbitMQ) handleReconnect() {
	attempt := 0
	for {
		select {
		case <-r.reconnect:
			r.mu.RLock()
			if r.closed {
				r.mu.RUnlock()
				return
			}
			r.mu.RUnlock()

			// Exponential backoff: delay = base * 2^attempt, capped at MaxReconnectDelay
			delay := exponentialBackoff(r.config.ReconnectDelay, r.config.MaxReconnectDelay, attempt)
			slog.Info("Attempting to reconnect to RabbitMQ...",
				slog.Int("attempt", attempt+1),
				slog.Duration("backoff", delay),
			)

			select {
			case <-time.After(delay):
			case <-r.closeChan:
				return
			}

			if err := r.connect(); err != nil {
				slog.Error("Reconnect failed", slog.Any("error", err))
				attempt++
				// Schedule next retry
				select {
				case r.reconnect <- struct{}{}:
				default:
				}
			} else {
				slog.Info("Reconnected to RabbitMQ successfully")
				attempt = 0 // reset backoff counter on success
			}

		case <-r.closeChan:
			return
		}
	}
}

// exponentialBackoff calculates delay = base * 2^attempt, capped at maxDelay.
func exponentialBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		return base
	}
	multiplier := math.Pow(2, float64(attempt))
	delay := time.Duration(float64(base) * multiplier)
	if delay > maxDelay || delay < 0 {
		return maxDelay
	}
	return delay
}

// GetChannel returns channel for use.
func (r *RabbitMQ) GetChannel() (*amqp.Channel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, fmt.Errorf("RabbitMQ connection is closed")
	}

	if r.channel == nil {
		return nil, fmt.Errorf("channel is not initialized")
	}

	return r.channel, nil
}

// Close closes connection gracefully.
func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.closeChan)

	if r.channel != nil {
		if closeErr := r.channel.Close(); closeErr != nil {
			slog.Error("Failed to close RabbitMQ channel", slog.Any("error", closeErr))
		}
	}
	if r.conn != nil {
		if closeErr := r.conn.Close(); closeErr != nil {
			slog.Error("Failed to close RabbitMQ connection cleanly", slog.Any("error", closeErr))
		}
	}

	slog.Info("RabbitMQ connection closed")
	return nil
}
