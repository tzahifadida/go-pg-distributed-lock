package pgdlock

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"log/slog"
	"sync"
	"time"
)

// LockManager is responsible for managing distributed locks in PostgreSQL.
type LockManager struct {
	Config    *Config
	db        *sqlx.DB
	nodeID    uuid.UUID
	clock     clockwork.Clock
	logger    *slog.Logger
	ctx       context.Context
	tableName string

	// For cleanup process
	cleanupTicker  *time.Ticker
	cleanupCtx     context.Context
	cleanupCancel  context.CancelFunc
	cleanupMutex   sync.Mutex
	cleanupStopped bool
}

// LockInfo holds information about a lock.
type LockInfo struct {
	NodeID        uuid.UUID `json:"nodeID" db:"node_id"`
	Resource      string    `json:"resource" db:"resource"`
	ExpiresAt     time.Time `json:"expiresAt" db:"expires_at"`
	LastHeartbeat time.Time `json:"lastHeartbeat" db:"last_heartbeat"`
}

// NewLockManager creates a new LockManager with the given configuration and database connection.
func NewLockManager(ctx context.Context, db *sql.DB, config *Config) (*LockManager, error) {
	if config == nil {
		cfg := DefaultConfig
		config = &cfg
	}

	nodeID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}

	sqlxDB := sqlx.NewDb(db, "pgx")

	lm := &LockManager{
		Config:         config,
		db:             sqlxDB,
		nodeID:         nodeID,
		clock:          clockwork.NewRealClock(),
		logger:         slog.Default(),
		ctx:            ctx,
		cleanupStopped: true,
	}

	if err := lm.ensureTable(); err != nil {
		return nil, err
	}

	// Start cleanup process if enabled
	if config.EnableAutomaticCleanup {
		if err := lm.StartCleanup(ctx); err != nil {
			return nil, fmt.Errorf("failed to start cleanup process: %w", err)
		}
	}

	return lm, nil
}

// ensureTable creates the necessary table for storing distributed locks if it doesn't exist.
func (lm *LockManager) ensureTable() error {
	tableName := lm.getTableName("distributed_locks")
	lm.tableName = tableName

	query := `
		CREATE TABLE IF NOT EXISTS %s (
			"resource" TEXT NOT NULL,
			"node_id" UUID NOT NULL,
			"expires_at" TIMESTAMP WITH TIME ZONE NOT NULL,
			"last_heartbeat" TIMESTAMP WITH TIME ZONE NOT NULL,
			PRIMARY KEY ("resource")
		);`

	_, err := lm.db.Exec(fmt.Sprintf(query, tableName))
	if err != nil {
		return fmt.Errorf("failed to create distributed locks table: %w", err)
	}

	// Create index for faster cleanup of expired locks
	indexQuery := `CREATE INDEX IF NOT EXISTS "%s_expires_at_idx" ON %s ("expires_at");`
	_, err = lm.db.Exec(fmt.Sprintf(indexQuery, lm.Config.TablePrefix+"distributed_locks", tableName))
	if err != nil {
		return fmt.Errorf("failed to create expires_at index: %w", err)
	}

	// Create index for faster cleanup based on last_heartbeat
	heartbeatIndexQuery := `CREATE INDEX IF NOT EXISTS "%s_last_heartbeat_idx" ON %s ("last_heartbeat");`
	_, err = lm.db.Exec(fmt.Sprintf(heartbeatIndexQuery, lm.Config.TablePrefix+"distributed_locks", tableName))
	if err != nil {
		return fmt.Errorf("failed to create last_heartbeat index: %w", err)
	}

	return nil
}

// getTableName returns the fully qualified table name with schema if specified.
func (lm *LockManager) getTableName(baseName string) string {
	if lm.Config.SchemaName != "" {
		return fmt.Sprintf(`"%s"."%s%s"`, lm.Config.SchemaName, lm.Config.TablePrefix, baseName)
	}
	return fmt.Sprintf(`"%s%s"`, lm.Config.TablePrefix, baseName)
}

// WithClock sets a custom clock for the LockManager (mainly for testing purposes).
func (lm *LockManager) WithClock(clock clockwork.Clock) *LockManager {
	lm.clock = clock
	return lm
}

// WithLogger sets a custom logger for the LockManager.
func (lm *LockManager) WithLogger(logger *slog.Logger) *LockManager {
	lm.logger = logger
	return lm
}

// StartCleanup starts the background process for cleaning up inactive locks.
// This method is automatically called by NewLockManager if EnableAutomaticCleanup is true.
func (lm *LockManager) StartCleanup(ctx context.Context) error {
	lm.cleanupMutex.Lock()
	defer lm.cleanupMutex.Unlock()

	if !lm.cleanupStopped {
		return nil // Cleanup already running
	}

	lm.cleanupCtx, lm.cleanupCancel = context.WithCancel(ctx)
	lm.cleanupTicker = time.NewTicker(lm.Config.CleanupInterval)
	lm.cleanupStopped = false

	go func() {
		lm.logger.Info("Starting lock cleanup process",
			"interval", lm.Config.CleanupInterval.String(),
			"inactivity_threshold", lm.Config.InactivityThreshold.String())

		// Run cleanup immediately on start
		if err := lm.Cleanup(lm.cleanupCtx); err != nil {
			lm.logger.Warn("Initial lock cleanup failed", "error", err)
		}

		for {
			select {
			case <-lm.cleanupTicker.C:
				if err := lm.Cleanup(lm.cleanupCtx); err != nil {
					lm.logger.Warn("Lock cleanup failed", "error", err)
				}
			case <-lm.cleanupCtx.Done():
				lm.logger.Info("Lock cleanup process stopped")
				return
			}
		}
	}()

	return nil
}

// StopCleanup stops the background cleanup process.
func (lm *LockManager) StopCleanup() {
	lm.cleanupMutex.Lock()
	defer lm.cleanupMutex.Unlock()

	if !lm.cleanupStopped {
		if lm.cleanupCancel != nil {
			lm.cleanupCancel()
		}
		if lm.cleanupTicker != nil {
			lm.cleanupTicker.Stop()
		}
		lm.cleanupStopped = true
		lm.logger.Info("Lock cleanup process stopped")
	}
}

// Cleanup removes locks that have been inactive for longer than the configured threshold.
// This method can be called manually if automatic cleanup is disabled.
func (lm *LockManager) Cleanup(ctx context.Context) error {
	now := lm.clock.Now()
	inactiveThreshold := now.Add(-lm.Config.InactivityThreshold)

	query := fmt.Sprintf(`
		DELETE FROM %s
		WHERE "last_heartbeat" < $1
		RETURNING "resource"
	`, lm.tableName)

	rows, err := lm.db.QueryContext(ctx, query, inactiveThreshold)
	if err != nil {
		return fmt.Errorf("failed to execute cleanup query: %w", err)
	}
	defer rows.Close()

	var resources []string
	for rows.Next() {
		var resource string
		if err := rows.Scan(&resource); err != nil {
			lm.logger.Warn("Failed to scan resource during cleanup", "error", err)
			continue
		}
		resources = append(resources, resource)
	}

	if len(resources) > 0 {
		lm.logger.Info("Cleaned up inactive locks", "count", len(resources))
	}

	return nil
}

// Close releases resources held by the LockManager, including stopping the cleanup process.
func (lm *LockManager) Close() error {
	lm.StopCleanup()
	return nil
}
