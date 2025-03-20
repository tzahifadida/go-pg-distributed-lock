# go-pg-distributed-lock

A robust PostgreSQL-based distributed locking library for Go applications.

## Overview

`go-pg-distributed-lock` provides a reliable distributed locking mechanism using PostgreSQL as the coordination backend. It's designed for distributed systems that need synchronization across multiple nodes or processes.

Key features:
- Lease-based locks with configurable timeouts
- Heartbeat mechanism to maintain locks during long operations
- Automatic lock cleanup for crashed nodes
- Configurable retry strategies

## Installation

```bash
go get github.com/yourusername/go-pg-distributed-lock
```

## Quick Start

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	pgdlock "github.com/yourusername/go-pg-distributed-lock"
)

func main() {
	// Connect to PostgreSQL
	db, err := sql.Open("pgx", "postgres://username:password@localhost:5432/dbname")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Create context
	ctx := context.Background()

	// Create lock manager with default config
	lockManager, err := pgdlock.NewLockManager(ctx, db, nil)
	if err != nil {
		log.Fatalf("Failed to create lock manager: %v", err)
	}

	// Create a lock for a specific resource
	lock := lockManager.NewDistributedLock("my-resource")

	// Try to acquire the lock
	if err := lock.Lock(ctx); err != nil {
		log.Printf("Could not acquire lock: %v", err)
		return
	}

	log.Println("Lock acquired, performing work...")

	// Simulate work
	time.Sleep(5 * time.Second)

	// Release the lock
	if err := lock.Unlock(ctx); err != nil {
		log.Printf("Error releasing lock: %v", err)
		return
	}

	log.Println("Lock released")
}
```

## Configuration Options

You can customize the lock behavior with the `Config` struct:

```go
config := pgdlock.Config{
MaxRetries:        3,                  // Maximum attempts to acquire a lock
RetryDelay:        100 * time.Millisecond, // Delay between retry attempts
HeartbeatInterval: 5 * time.Second,    // How often to send heartbeats
LeaseTime:         30 * time.Second,   // How long a lock is valid
SchemaName:        "public",           // Database schema
TablePrefix:       "myapp_",           // Prefix for the locks table (results in "myapp_distributed_locks")
}

lockManager, err := pgdlock.NewLockManager(ctx, db, &config)
```

The `TablePrefix` is particularly useful when:
- You need to separate locks used by different applications in the same database
- You want to avoid table name collisions in your database
- You need to namespace different lock sets for different components

For example, you might use different prefixes for different services:
```go
// For the auth service
authConfig := pgdlock.Config{
TablePrefix: "auth_",  // Table will be "auth_distributed_locks"
// ... other config options
}

// For the payment service
paymentConfig := pgdlock.Config{
TablePrefix: "payment_",  // Table will be "payment_distributed_locks"
// ... other config options
}
```

## Use Cases

- Coordinating access to shared resources in distributed systems
- Implementing leader election
- Ensuring exclusive access to critical sections
- Preventing race conditions in distributed job systems
- Implementing distributed cron jobs that should run on only one node

## Error Handling

The library provides specific error types to handle common locking scenarios:

```go
if err := lock.Lock(ctx); err != nil {
if errors.Is(err, pgdlock.ErrLockAlreadyHeld) {
// Resource is already locked by another process
} else if errors.Is(err, pgdlock.ErrLockExpired) {
// Lock has expired
} else {
// Other errors
}
}
```

## Advanced Usage

### Extending a Lock Lease

```go
// Extend the current lock by an additional 30 seconds
if err := lock.ExtendLease(ctx, 30 * time.Second); err != nil {
log.Printf("Failed to extend lease: %v", err)
}
```

### Checking Lock Status

```go
isLocked, err := lock.IsLocked(ctx)
if err != nil {
log.Printf("Error checking lock status: %v", err)
} else if isLocked {
log.Println("Lock is currently held by this node")
} else {
log.Println("Lock is not currently held")
}
```

### Customizing Lock Retry Behavior

You can customize the retry behavior for individual locks:

```go
// Create a lock with custom retry options
lock := lockManager.NewDistributedLock(
    "my-critical-resource",
    WithMaxRetries(10),           // Try more times than the default
    WithRetryDelay(50 * time.Millisecond), // Use a shorter retry delay
)

// Acquire the lock with the custom retry behavior
if err := lock.Lock(ctx); err != nil {
    log.Printf("Failed to acquire lock despite 10 attempts: %v", err)
}
```

## Performance Considerations

- The library uses a heartbeat mechanism that requires periodic database operations
- Consider tuning the `HeartbeatInterval` and `LeaseTime` based on your workload
- For high-frequency lock operations, you may need to adjust your PostgreSQL connection pool size

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.