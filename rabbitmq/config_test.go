package rabbitmq

import (
	"os"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.URL == "" {
		t.Error("DefaultConfig: URL must not be empty")
	}
	if cfg.MaxRetries <= 0 {
		t.Errorf("DefaultConfig: MaxRetries must be > 0, got %d", cfg.MaxRetries)
	}
	if cfg.PrefetchCount <= 0 {
		t.Errorf("DefaultConfig: PrefetchCount must be > 0, got %d", cfg.PrefetchCount)
	}
	if cfg.ReconnectDelay <= 0 {
		t.Error("DefaultConfig: ReconnectDelay must be > 0")
	}
	if cfg.MaxReconnectDelay <= 0 {
		t.Error("DefaultConfig: MaxReconnectDelay must be > 0")
	}
	if cfg.MaxReconnectDelay < cfg.ReconnectDelay {
		t.Error("DefaultConfig: MaxReconnectDelay must be >= ReconnectDelay")
	}
}

func TestNewConfigFromEnv_Defaults(t *testing.T) {
	// No env vars set — should fall back to defaults
	cfg := NewConfigFromEnv()

	def := DefaultConfig()
	if cfg.MaxRetries != def.MaxRetries {
		t.Errorf("expected MaxRetries=%d, got %d", def.MaxRetries, cfg.MaxRetries)
	}
	if cfg.PrefetchCount != def.PrefetchCount {
		t.Errorf("expected PrefetchCount=%d, got %d", def.PrefetchCount, cfg.PrefetchCount)
	}
}

func TestNewConfigFromEnv_Override(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://testuser:testpass@testhost:5672/testvhost")
	t.Setenv("RABBITMQ_MAX_RETRIES", "5")
	t.Setenv("RABBITMQ_PREFETCH_COUNT", "20")
	t.Setenv("RABBITMQ_RETRY_DELAY_MS", "3000")
	t.Setenv("RABBITMQ_RECONNECT_DELAY_MS", "4000")
	t.Setenv("RABBITMQ_MAX_RECONNECT_DELAY_MS", "120000")

	cfg := NewConfigFromEnv()

	if cfg.URL != "amqp://testuser:testpass@testhost:5672/testvhost" {
		t.Errorf("unexpected URL: %s", cfg.URL)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", cfg.MaxRetries)
	}
	if cfg.PrefetchCount != 20 {
		t.Errorf("expected PrefetchCount=20, got %d", cfg.PrefetchCount)
	}
	if cfg.RetryDelay != 3*time.Second {
		t.Errorf("expected RetryDelay=3s, got %v", cfg.RetryDelay)
	}
	if cfg.ReconnectDelay != 4*time.Second {
		t.Errorf("expected ReconnectDelay=4s, got %v", cfg.ReconnectDelay)
	}
	if cfg.MaxReconnectDelay != 120*time.Second {
		t.Errorf("expected MaxReconnectDelay=120s, got %v", cfg.MaxReconnectDelay)
	}
}

func TestNewConfigFromEnv_EmptyURL(t *testing.T) {
	// Unset RABBITMQ_URL explicitly
	os.Unsetenv("RABBITMQ_URL") //nolint:errcheck
	cfg := NewConfigFromEnv()
	if cfg.URL == "" {
		t.Error("expected fallback URL from DefaultConfig, got empty string")
	}
}

func TestExponentialBackoff(t *testing.T) {
	base := 2 * time.Second
	maxDelay := 60 * time.Second

	tests := []struct {
		attempt int
		wantMin time.Duration
		wantMax time.Duration
	}{
		{0, base, base},
		{1, 4 * time.Second, 4 * time.Second},
		{2, 8 * time.Second, 8 * time.Second},
		{3, 16 * time.Second, 16 * time.Second},
		{10, maxDelay, maxDelay}, // should be capped
		{100, maxDelay, maxDelay},
	}

	for _, tc := range tests {
		got := exponentialBackoff(base, maxDelay, tc.attempt)
		if got < tc.wantMin || got > tc.wantMax {
			t.Errorf("attempt=%d: got %v, want [%v, %v]", tc.attempt, got, tc.wantMin, tc.wantMax)
		}
	}
}
