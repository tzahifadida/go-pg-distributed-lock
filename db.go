package pgdlock

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"log/slog"
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
		Config: config,
		db:     sqlxDB,
		nodeID: nodeID,
		clock:  clockwork.NewRealClock(),
		logger: slog.Default(),
		ctx:    ctx,
	}

	if err := lm.ensureTable(); err != nil {
		return nil, err
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
		return fmt.Errorf("failed to create index: %w", err)
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
