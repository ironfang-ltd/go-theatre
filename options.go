package theatre

import "time"

type DeadLetterHandler func(msg InboxMessage)

type Option func(*hostConfig)

type hostConfig struct {
	idleTimeout       time.Duration
	requestTimeout    time.Duration
	cleanupInterval   time.Duration
	drainTimeout      time.Duration
	deadLetterHandler DeadLetterHandler
}

func defaultHostConfig() hostConfig {
	return hostConfig{
		idleTimeout:     15 * time.Second,
		requestTimeout:  5 * time.Second,
		cleanupInterval: 1 * time.Second,
		drainTimeout:    5 * time.Second,
	}
}

func WithIdleTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.idleTimeout = d
	}
}

func WithRequestTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.requestTimeout = d
	}
}

func WithCleanupInterval(d time.Duration) Option {
	return func(c *hostConfig) {
		c.cleanupInterval = d
	}
}

func WithDrainTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.drainTimeout = d
	}
}

func WithDeadLetterHandler(h DeadLetterHandler) Option {
	return func(c *hostConfig) {
		c.deadLetterHandler = h
	}
}
