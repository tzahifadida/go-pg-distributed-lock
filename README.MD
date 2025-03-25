# go-pg-distributed-lock

A robust PostgreSQL-based distributed locking library for Go applications.

⭐️ **Star This Project** ⭐️

If you find this project helpful, please give it a star on GitHub! Your support is greatly appreciated.

## Overview

`go-pg-distributed-lock` provides a reliable distributed locking mechanism using PostgreSQL as the coordination backend. It's designed for distributed systems that need synchronization across multiple nodes or processes.

Key features:
- Lease-based locks with configurable timeouts
- Heartbeat mechanism to maintain locks during long operations
- Automatic cleanup of inactive/stale locks
- Automatic lock cleanup for crashed nodes
- Configurable retry strategies
- Per-lock customization of timing parameters

## Installation

```bash
go get github.com/tzahifadida/go-pg-distributed-lock
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
	pgdlock "github.com/tzahifadida/go-pg-distributed-lock"
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
	// Properly clean up resources when done
	defer lockManager.Close()

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
    MaxAttempts:       3,                  // Maximum attempts to acquire a lock (default: 1)
    RetryDelay:        100 * time.Millisecond, // Delay between retry attempts
    HeartbeatInterval: 5 * time.Second,    // How often to send heartbeats
    LeaseTime:         30 * time.Second,   // How long a lock is valid
    SchemaName:        "public",           // Database schema
    TablePrefix:       "myapp_",           // Prefix for the locks table (results in "myapp_distributed_locks")
    CleanupInterval:       24 * time.Hour,     // How often to cleanup inactive locks (default: 24h)
    InactivityThreshold:   12 * time.Hour,     // Time after which inactive locks are cleaned up (default: 12h)
    EnableAutomaticCleanup: true,              // Whether to automatically run cleanup (default: true)
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

### Managing Lock Cleanup

The library includes functionality to clean up locks that haven't been used for a specified period. This helps prevent database bloat from abandoned locks.

Automatic cleanup is enabled by default with a 24-hour cleanup interval and a 12-hour inactivity threshold. You can customize these settings:

```go
config := pgdlock.Config{
    // ... other options
    CleanupInterval:       12 * time.Hour,    // Run cleanup every 12 hours
    InactivityThreshold:   6 * time.Hour,     // Clean locks inactive for 6+ hours
    EnableAutomaticCleanup: true,             // Enable automatic cleanup
}
```

You can also manually stop, start, or trigger the cleanup process:

```go
// Manually run cleanup once
if err := lockManager.Cleanup(ctx); err != nil {
    log.Printf("Failed to run cleanup: %v", err)
}

// Stop automatic cleanup
lockManager.StopCleanup()

// Restart automatic cleanup
if err := lockManager.StartCleanup(ctx); err != nil {
    log.Printf("Failed to start cleanup: %v", err)
}
```

### Customizing Lock Acquisition Attempts

By default, the library does not retry lock acquisition if it fails (MaxAttempts=1). If you want automatic retries, you can either:

1. Configure it globally for all locks:

```go
config := pgdlock.Config{
    MaxAttempts: 5,                  // Try 5 times before giving up
    RetryDelay: 50 * time.Millisecond, // Wait 50ms between attempts
    // other config options...
}
lockManager, err := pgdlock.NewLockManager(ctx, db, &config)
```

2. Configure it for individual locks:

```go
// Create a lock with custom retry options
lock := lockManager.NewDistributedLock(
    "my-critical-resource",
    pgdlock.WithMaxAttempts(5),              // Try up to 5 times
    pgdlock.WithRetryDelay(50 * time.Millisecond), // Use a shorter retry delay
)

// Acquire the lock with the custom retry behavior
if err := lock.Lock(ctx); err != nil {
    log.Printf("Failed to acquire lock despite 5 attempts: %v", err)
}
```

### Customizing Timing Parameters For Individual Locks

You can customize the heartbeat interval and lease time for individual locks:

```go
// Create a lock with custom timing parameters
lock := lockManager.NewDistributedLock(
    "my-long-running-task",
    pgdlock.WithHeartbeatInterval(3 * time.Second),  // Send heartbeats more frequently
    pgdlock.WithLeaseTime(2 * time.Minute),         // Use a longer lease time
)

// You can combine multiple options
lock := lockManager.NewDistributedLock(
    "my-critical-resource",
    pgdlock.WithMaxAttempts(5),                // Try up to 5 times
    pgdlock.WithRetryDelay(50 * time.Millisecond),   // Use a shorter retry delay
    pgdlock.WithHeartbeatInterval(1 * time.Second),  // Send frequent heartbeats
    pgdlock.WithLeaseTime(10 * time.Second)          // Use a shorter lease time
)
```

This gives you fine-grained control over lock behavior based on specific resource requirements:
- Use shorter lease times for high-contention resources
- Use longer lease times for long-running operations
- Adjust heartbeat frequency based on network reliability and operation duration

## Performance Considerations

- The library uses a heartbeat mechanism that requires periodic database operations.
- Consider tuning the `HeartbeatInterval` and `LeaseTime` based on your workload.
- Adjust `CleanupInterval` and `InactivityThreshold` based on your application's lock usage patterns. Using shorter cleanup intervals in high-churn systems can help maintain database performance.
- For high-frequency lock operations, you may need to adjust your PostgreSQL connection pool size.
- Per-lock customization allows you to optimize timing parameters for different usage patterns.
- Always call `lockManager.Close()` when shutting down your application to properly release resources.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Thank you for using the library! If you have any questions, suggestions, or encounter any issues, please don't hesitate to open an issue on the GitHub repository. Your feedback and contributions are greatly appreciated and help make this library better for everyone.