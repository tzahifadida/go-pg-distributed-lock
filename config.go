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
	// MaxAttempts is the maximum number of attempts to acquire the lock.
	MaxAttempts int

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

	// CleanupInterval is the interval at which to run the cleanup process.
	// Default is 24 hours.
	CleanupInterval time.Duration

	// InactivityThreshold is the duration of inactivity after which a lock is considered stale and can be cleaned up.
	// Default is 12 hours.
	InactivityThreshold time.Duration

	// EnableAutomaticCleanup determines whether to automatically run the cleanup process.
	// Default is true.
	EnableAutomaticCleanup bool
}

// DefaultConfig provides default configuration values.
var DefaultConfig = Config{
	MaxAttempts:            1,
	RetryDelay:             1 * time.Millisecond,
	HeartbeatInterval:      10 * time.Second,
	LeaseTime:              60 * time.Second,
	TablePrefix:            "",
	SchemaName:             "",
	CleanupInterval:        24 * time.Hour,
	InactivityThreshold:    12 * time.Hour,
	EnableAutomaticCleanup: true,
}
