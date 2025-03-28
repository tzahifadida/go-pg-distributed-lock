package pgdlock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DistributedLock represents a distributed lock implementation.
type DistributedLock struct {
	lm                *LockManager
	resource          string
	heartbeatTicker   *time.Ticker
	mutex             sync.Mutex
	heartbeatCtx      context.Context
	heartbeatCancel   context.CancelFunc
	expirationTimer   *time.Timer
	leaseExpiration   time.Time
	heartbeatStopped  bool
	maxAttempts       int           // Maximum number of attempts when acquiring the lock
	retryDelay        time.Duration // Delay between retry attempts
	heartbeatInterval time.Duration // Custom heartbeat interval for this lock
	leaseTime         time.Duration // Custom lease time for this lock
}

// NewDistributedLock creates a new DistributedLock instance.
//
// Parameters:
//   - resource: The name of the resource being locked.
//   - options: Optional parameters for customizing the lock behavior.
//
// Returns:
//   - A pointer to the newly created DistributedLock.
func (lm *LockManager) NewDistributedLock(resource string, options ...LockOption) *DistributedLock {
	lock := &DistributedLock{
		lm:                lm,
		resource:          resource,
		heartbeatStopped:  true,
		maxAttempts:       lm.Config.MaxAttempts,
		retryDelay:        lm.Config.RetryDelay,
		heartbeatInterval: lm.Config.HeartbeatInterval, // Initialize with default from config
		leaseTime:         lm.Config.LeaseTime,         // Initialize with default from config
	}

	// Apply any provided options
	for _, option := range options {
		option(lock)
	}

	return lock
}

// LockOption defines a function type for customizing a DistributedLock
type LockOption func(*DistributedLock)

// WithMaxAttempts sets a custom maximum number of attempts for lock acquisition
func WithMaxAttempts(maxAttempts int) LockOption {
	return func(lock *DistributedLock) {
		if maxAttempts > 0 {
			lock.maxAttempts = maxAttempts
		}
	}
}

// WithRetryDelay sets a custom delay between retry attempts for lock acquisition
func WithRetryDelay(retryDelay time.Duration) LockOption {
	return func(lock *DistributedLock) {
		if retryDelay > 0 {
			lock.retryDelay = retryDelay
		}
	}
}

// WithHeartbeatInterval sets a custom heartbeat interval for the lock
func WithHeartbeatInterval(heartbeatInterval time.Duration) LockOption {
	return func(lock *DistributedLock) {
		if heartbeatInterval > 0 {
			lock.heartbeatInterval = heartbeatInterval
		}
	}
}

// WithLeaseTime sets a custom lease time for the lock
func WithLeaseTime(leaseTime time.Duration) LockOption {
	return func(lock *DistributedLock) {
		if leaseTime > 0 {
			lock.leaseTime = leaseTime
		}
	}
}

// Lock attempts to acquire the distributed lock.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - An error if the lock cannot be acquired, nil otherwise.
//   - By default, no retries are performed unless configured differently.
func (dl *DistributedLock) Lock(ctx context.Context) error {
	// Check context cancellation first
	if ctx.Err() != nil {
		return ctx.Err()
	}

	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	for attempt := 0; attempt < dl.maxAttempts; attempt++ {
		// Check for context cancellation at each iteration
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := dl.attemptLock(ctx)
		if err == nil {
			dl.leaseExpiration = dl.lm.clock.Now().Add(dl.leaseTime) // Use custom lease time
			dl.startHeartbeat(ctx)
			return nil
		}
		if errors.Is(err, ErrLockAlreadyHeld) {
			return err
		}
		if attempt < dl.maxAttempts-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(dl.retryDelay):
				// Continue to next iteration
			}
		}
	}

	return fmt.Errorf("failed to acquire lock after %d attempts", dl.maxAttempts)
}

func (dl *DistributedLock) attemptLock(ctx context.Context) error {
	now := dl.lm.clock.Now()
	expiresAt := now.Add(dl.leaseTime) // Use lock-specific lease time instead of config

	// First try to get the current lock information
	query := fmt.Sprintf(`
		SELECT "node_id", "resource", "expires_at", "last_heartbeat"
		FROM %s
		WHERE "resource" = $1
	`, dl.lm.tableName)

	var currentLock LockInfo
	err := dl.lm.db.GetContext(ctx, &currentLock, query, dl.resource)
	if err == nil {
		// Lock exists, check if it's expired or stale
		if now.Before(currentLock.ExpiresAt) {
			if now.Sub(currentLock.LastHeartbeat) <= time.Duration(heartbeatMultiplier*float64(dl.leaseTime)) {
				return ErrLockAlreadyHeld
			}
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to get current lock info: %w", err)
	}

	// Try to acquire the lock with an upsert
	query = fmt.Sprintf(`
		INSERT INTO %s ("resource", "node_id", "expires_at", "last_heartbeat")
		VALUES ($1, $2, $3, $4)
		ON CONFLICT ("resource") DO UPDATE
		SET "node_id" = EXCLUDED.node_id,
			"expires_at" = EXCLUDED.expires_at,
			"last_heartbeat" = EXCLUDED.last_heartbeat
		WHERE %s."expires_at" < $5 OR %s."last_heartbeat" < $6
	`, dl.lm.tableName, dl.lm.tableName, dl.lm.tableName)

	result, err := dl.lm.db.ExecContext(
		ctx,
		query,
		dl.resource,
		dl.lm.nodeID,
		expiresAt,
		now,
		now, // For expired locks
		now.Add(-time.Duration(heartbeatMultiplier*float64(dl.leaseTime))), // For stale locks, using custom lease time
	)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return ErrLockAlreadyHeld
	}

	return nil
}

func (dl *DistributedLock) startHeartbeat(ctx context.Context) {
	dl.stopHeartbeatLocked()

	// Use lock-specific heartbeat interval and lease time
	heartbeatInterval := dl.heartbeatInterval
	if heartbeatInterval >= dl.leaseTime/2 {
		return
	}

	dl.heartbeatCtx, dl.heartbeatCancel = context.WithCancel(ctx)
	dl.heartbeatTicker = time.NewTicker(heartbeatInterval)
	dl.expirationTimer = time.NewTimer(time.Until(dl.leaseExpiration))
	dl.heartbeatStopped = false

	go func() {
		defer dl.stopHeartbeat()
		for {
			select {
			case <-dl.heartbeatTicker.C:
				dl.mutex.Lock()
				if dl.heartbeatStopped {
					dl.mutex.Unlock()
					return
				}
				// Update lease expiration time to extend it
				dl.leaseExpiration = dl.lm.clock.Now().Add(dl.leaseTime)
				if dl.expirationTimer != nil {
					dl.expirationTimer.Reset(dl.leaseTime)
				}
				err := dl.sendHeartbeat(dl.heartbeatCtx)
				dl.mutex.Unlock()
				if err != nil {
					dl.lm.logger.Warn("Failed to send heartbeat", "error", err)
					return
				}
			case <-dl.expirationTimer.C:
				dl.mutex.Lock()
				dl.lm.logger.Info("Lock lease expired, stopping heartbeat")
				dl.heartbeatStopped = true
				dl.mutex.Unlock()
				return
			case <-dl.heartbeatCtx.Done():
				return
			}
		}
	}()
}

// Unlock releases the distributed lock.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - An error if the lock cannot be released, nil otherwise.
func (dl *DistributedLock) Unlock(ctx context.Context) error {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	dl.stopHeartbeatLocked()

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE "resource" = $1 AND "node_id" = $2
		RETURNING "resource"
	`, dl.lm.tableName)

	var resource string
	err := dl.lm.db.QueryRowContext(ctx, query, dl.resource, dl.lm.nodeID).Scan(&resource)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to unlock: %w", err)
	}

	return nil
}

// ExtendLease extends the lease time of the lock.
//
// Parameters:
//   - ctx: The context for the operation.
//   - extension: The duration by which to extend the lease.
//
// Returns:
//   - An error if the lease cannot be extended, nil otherwise.
func (dl *DistributedLock) ExtendLease(ctx context.Context, extension time.Duration) error {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	now := dl.lm.clock.Now()
	if now.After(dl.leaseExpiration) {
		return ErrLockExpired
	}

	newExpiresAt := dl.leaseExpiration.Add(extension)

	query := fmt.Sprintf(`
		UPDATE %s
		SET "expires_at" = $1, "last_heartbeat" = $2
		WHERE "resource" = $3 AND "node_id" = $4
		RETURNING "resource"
	`, dl.lm.tableName)

	var resource string
	err := dl.lm.db.QueryRowContext(ctx, query, newExpiresAt, now, dl.resource, dl.lm.nodeID).Scan(&resource)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to extend lease: %w", err)
	}

	dl.leaseExpiration = newExpiresAt

	// Update the expiration timer
	if dl.expirationTimer != nil {
		if !dl.expirationTimer.Stop() {
			select {
			case <-dl.expirationTimer.C:
			default:
			}
		}
		dl.expirationTimer.Reset(time.Until(newExpiresAt))
	}

	return nil
}

func (dl *DistributedLock) sendHeartbeat(ctx context.Context) error {
	now := dl.lm.clock.Now()

	query := fmt.Sprintf(`
		UPDATE %s
		SET "last_heartbeat" = $1
		WHERE "resource" = $2 AND "node_id" = $3
		RETURNING "resource"
	`, dl.lm.tableName)

	var resource string
	err := dl.lm.db.QueryRowContext(ctx, query, now, dl.resource, dl.lm.nodeID).Scan(&resource)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to send heartbeat: %w", err)
	}

	return nil
}

func (dl *DistributedLock) stopHeartbeat() {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	dl.stopHeartbeatLocked()
}

func (dl *DistributedLock) stopHeartbeatLocked() {
	if !dl.heartbeatStopped {
		if dl.heartbeatCancel != nil {
			dl.heartbeatCancel()
		}
		if dl.heartbeatTicker != nil {
			dl.heartbeatTicker.Stop()
		}
		if dl.expirationTimer != nil {
			dl.expirationTimer.Stop()
		}
		dl.heartbeatStopped = true
	}
}

// IsLocked checks if the lock is currently held by this node.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - A boolean indicating whether the lock is held, and an error if the check fails.
func (dl *DistributedLock) IsLocked(ctx context.Context) (bool, error) {
	query := fmt.Sprintf(`
		SELECT "node_id", "expires_at", "last_heartbeat"
		FROM %s
		WHERE "resource" = $1
	`, dl.lm.tableName)

	var lockInfo LockInfo
	err := dl.lm.db.GetContext(ctx, &lockInfo, query, dl.resource)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check lock status: %w", err)
	}

	now := dl.lm.clock.Now()
	if lockInfo.NodeID == dl.lm.nodeID && now.Before(lockInfo.ExpiresAt) {
		return true, nil
	}

	return false, nil
}
