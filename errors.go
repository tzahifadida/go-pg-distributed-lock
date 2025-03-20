package pgdlock

import (
	"errors"
)

var (
	// ErrLockAlreadyHeld is returned when attempting to acquire a lock that is already held by another node.
	ErrLockAlreadyHeld = errors.New("lock is already held by another node")

	// ErrLockNotHeld is returned when attempting to perform an operation on a lock that is not held by the current node.
	ErrLockNotHeld = errors.New("lock is not held by this node")

	// ErrLockExpired is returned when attempting to perform an operation on a lock that has expired.
	ErrLockExpired = errors.New("lock has expired")

	// ErrDatabaseError is returned when a database operation fails.
	ErrDatabaseError = errors.New("database operation failed")
)
