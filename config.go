package pgdlock

import (
	"time"
)

const (
	lockAttributeKeyPrefix = "distributed_lock_"
	heartbeatMultiplier    = 2.5
)

// Config holds the configuration options for the distributed lock system.
type Config struct {
	// MaxRetries is the maximum number of attempts to acquire the lock.
	MaxRetries int

	// RetryDelay is the duration to wait between lock acquisition attempts.
	RetryDelay time.Duration

	// HeartbeatInterval is the duration between heartbeats to maintain the lock.
	HeartbeatInterval time.Duration

	// LeaseTime is the duration for which the lock is considered valid.
	LeaseTime time.Duration

	// TablePrefix is the prefix to use for database tables.
	// For example, with TablePrefix "myapp_", the table will be "myapp_distributed_locks".
	TablePrefix string

	// SchemaName is the database schema to use.
	SchemaName string
}

// DefaultConfig provides default configuration values.
var DefaultConfig = Config{
	MaxRetries:        2,
	RetryDelay:        1 * time.Millisecond,
	HeartbeatInterval: 10 * time.Second,
	LeaseTime:         60 * time.Second,
	TablePrefix:       "",
	SchemaName:        "",
}
