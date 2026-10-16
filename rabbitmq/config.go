package rabbitmq

import (
	"os"
	"strconv"
	"time"
)

// RabbitMQConfig holds all configuration for a RabbitMQ connection.
type RabbitMQConfig struct {
	// URL is the AMQP connection string.
	// Format: amqp://user:password@host:port/vhost
	URL string

	// MaxRetries is the number of publish retry attempts.
	MaxRetries int

	// RetryDelay is the wait duration between publish retries.
	RetryDelay time.Duration

	// PrefetchCount limits max unacknowledged messages per consumer.
	PrefetchCount int

	// ReconnectDelay is the base delay before attempting reconnection.
	// Used as the starting point for exponential backoff.
	ReconnectDelay time.Duration

	// MaxReconnectDelay caps the exponential backoff ceiling.
	MaxReconnectDelay time.Duration
}

// DefaultConfig returns a config suitable for LOCAL DEVELOPMENT ONLY.
//
// WARNING: Never use in production. Credentials are hard-coded for convenience.
// Use NewConfigFromEnv() for all non-local environments.
func DefaultConfig() *RabbitMQConfig {
	return &RabbitMQConfig{
		// #nosec G101 - default local development credentials only
		URL:               "amqp://user:user123@localhost:5672/tokohobby",
		MaxRetries:        3,
		RetryDelay:        2 * time.Second,
		PrefetchCount:     10,
		ReconnectDelay:    2 * time.Second,
		MaxReconnectDelay: 60 * time.Second,
	}
}

// NewConfigFromEnv builds a RabbitMQConfig from environment variables.
//
// Required environment variable:
//   - RABBITMQ_URL — full AMQP URL (e.g. amqp://user:pass@host:5672/vhost)
//
// Optional environment variables (fall back to DefaultConfig values):
//   - RABBITMQ_MAX_RETRIES        (default: 3)
//   - RABBITMQ_PREFETCH_COUNT     (default: 10)
//   - RABBITMQ_RETRY_DELAY_MS     milliseconds (default: 2000)
//   - RABBITMQ_RECONNECT_DELAY_MS milliseconds (default: 2000)
//   - RABBITMQ_MAX_RECONNECT_DELAY_MS milliseconds (default: 60000)
func NewConfigFromEnv() *RabbitMQConfig {
	cfg := DefaultConfig()

	if url := os.Getenv("RABBITMQ_URL"); url != "" {
		cfg.URL = url
	}
	if v := envInt("RABBITMQ_MAX_RETRIES"); v > 0 {
		cfg.MaxRetries = v
	}
	if v := envInt("RABBITMQ_PREFETCH_COUNT"); v > 0 {
		cfg.PrefetchCount = v
	}
	if v := envInt("RABBITMQ_RETRY_DELAY_MS"); v > 0 {
		cfg.RetryDelay = time.Duration(v) * time.Millisecond
	}
	if v := envInt("RABBITMQ_RECONNECT_DELAY_MS"); v > 0 {
		cfg.ReconnectDelay = time.Duration(v) * time.Millisecond
	}
	if v := envInt("RABBITMQ_MAX_RECONNECT_DELAY_MS"); v > 0 {
		cfg.MaxReconnectDelay = time.Duration(v) * time.Millisecond
	}

	return cfg
}

func envInt(key string) int {
	v, _ := strconv.Atoi(os.Getenv(key))
	return v
}
