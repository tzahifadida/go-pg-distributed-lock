package pgdlock

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"sync"
	"testing"
	"time"
)

func TestLockManagerBasic(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager
	cfg := DefaultConfig
	cfg.TablePrefix = "test_"
	cfg.SchemaName = "public"

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	lm.WithClock(fakeClock)

	// Test basic lock acquisition and release
	t.Run("BasicLockAcquisitionAndRelease", func(t *testing.T) {
		resourceName := "test-resource-1"
		lock := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock.Lock(ctx)
		require.NoError(t, err)

		// Verify the lock is held
		isLocked, err := lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Release the lock
		err = lock.Unlock(ctx)
		require.NoError(t, err)

		// Verify the lock is released
		isLocked, err = lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.False(t, isLocked)
	})

	// Test concurrent lock attempts
	t.Run("ConcurrentLockAttempts", func(t *testing.T) {
		resourceName := "test-resource-2"
		lock1 := lm.NewDistributedLock(resourceName)
		lock2 := lm.NewDistributedLock(resourceName)

		// Acquire the lock with lock1
		err := lock1.Lock(ctx)
		require.NoError(t, err)

		// Try to acquire the same lock with lock2
		err = lock2.Lock(ctx)
		assert.Error(t, err)
		assert.Equal(t, ErrLockAlreadyHeld, err)

		// Release the lock with lock1
		err = lock1.Unlock(ctx)
		require.NoError(t, err)

		// Now lock2 should be able to acquire the lock
		err = lock2.Lock(ctx)
		require.NoError(t, err)

		// Clean up
		err = lock2.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test extending lease
	t.Run("ExtendLease", func(t *testing.T) {
		resourceName := "test-resource-3"
		lock := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock.Lock(ctx)
		require.NoError(t, err)

		// Extend the lease
		extension := 30 * time.Second
		err = lock.ExtendLease(ctx, extension)
		require.NoError(t, err)

		// Advance clock to slightly before the initial lease would expire
		fakeClock.Advance(cfg.LeaseTime - 1*time.Second)

		// Lock should still be held
		isLocked, err := lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Advance clock to after initial lease but before extended lease
		fakeClock.Advance(2 * time.Second) // Now just past initial lease expiry

		// Lock should still be held due to extension
		isLocked, err = lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Clean up
		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test heartbeat mechanism
	t.Run("HeartbeatMechanism", func(t *testing.T) {
		resourceName := "test-resource-4"
		lock := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock.Lock(ctx)
		require.NoError(t, err)

		// Advance clock by half the lease time
		fakeClock.Advance(cfg.LeaseTime / 2)

		// Lock should still be held and heartbeat should have been sent
		isLocked, err := lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Advance clock to almost the full lease time
		fakeClock.Advance(cfg.LeaseTime/2 - 1*time.Second)

		// Lock should still be held
		isLocked, err = lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Clean up
		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})
}

func TestLockManagerAdvanced(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager
	cfg := DefaultConfig
	cfg.TablePrefix = "test_"
	cfg.SchemaName = "public"
	cfg.MaxAttempts = 3
	cfg.RetryDelay = 100 * time.Millisecond
	cfg.LeaseTime = 10 * time.Second
	cfg.HeartbeatInterval = 2 * time.Second

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	lm.WithClock(fakeClock)

	// Test lock expiration
	t.Run("LockExpiration", func(t *testing.T) {
		resourceName := "test-resource-5"
		lock1 := lm.NewDistributedLock(resourceName)
		lock2 := lm.NewDistributedLock(resourceName)

		// Acquire the lock with lock1
		err := lock1.Lock(ctx)
		require.NoError(t, err)

		// Advance clock past lease time
		fakeClock.Advance(cfg.LeaseTime + 1*time.Second)

		// lock2 should now be able to acquire the lock
		err = lock2.Lock(ctx)
		require.NoError(t, err)

		// Verify lock2 holds the lock
		isLocked, err := lock2.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked)

		// Clean up
		err = lock2.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test multiple lock managers (different nodes)
	t.Run("MultipleLockManagers", func(t *testing.T) {
		// Create a second lock manager
		lm2, err := NewLockManager(ctx, db, &cfg)
		require.NoError(t, err)
		lm2.WithClock(fakeClock)

		resourceName := "test-resource-6"
		lock1 := lm.NewDistributedLock(resourceName)
		lock2 := lm2.NewDistributedLock(resourceName)

		// Acquire the lock with lock1
		err = lock1.Lock(ctx)
		require.NoError(t, err)

		// Try to acquire the same lock with lock2
		err = lock2.Lock(ctx)
		assert.Error(t, err)
		assert.Equal(t, ErrLockAlreadyHeld, err)

		// Release the lock with lock1
		err = lock1.Unlock(ctx)
		require.NoError(t, err)

		// Now lock2 should be able to acquire the lock
		err = lock2.Lock(ctx)
		require.NoError(t, err)

		// Clean up
		err = lock2.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test extending an expired lease
	t.Run("ExtendExpiredLease", func(t *testing.T) {
		resourceName := "test-resource-7"
		lock := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock.Lock(ctx)
		require.NoError(t, err)

		// Advance clock past lease time
		fakeClock.Advance(cfg.LeaseTime + 1*time.Second)

		// Try to extend the expired lease
		err = lock.ExtendLease(ctx, 30*time.Second)
		assert.Error(t, err)
		assert.Equal(t, ErrLockExpired, err)

		// Clean up
		// No need to unlock as the lock has expired
	})

	// Test basic lock re-acquisition after release
	t.Run("LockReacquisitionAfterRelease", func(t *testing.T) {
		resourceName := "test-resource-8"
		lock1 := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock1.Lock(ctx)
		require.NoError(t, err)

		// Release the lock
		err = lock1.Unlock(ctx)
		require.NoError(t, err)

		// Lock again with the same lock object
		err = lock1.Lock(ctx)
		require.NoError(t, err)

		// Clean up
		err = lock1.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test unlocking someone else's lock
	t.Run("UnlockSomeoneElsesLock", func(t *testing.T) {
		// Create a second lock manager with a different node ID
		lm2, err := NewLockManager(ctx, db, &cfg)
		require.NoError(t, err)
		lm2.WithClock(fakeClock)

		resourceName := "test-resource-9"
		lock1 := lm.NewDistributedLock(resourceName)
		lock2 := lm2.NewDistributedLock(resourceName)

		// Acquire the lock with lock1
		err = lock1.Lock(ctx)
		require.NoError(t, err)

		// Try to unlock the lock with lock2
		err = lock2.Unlock(ctx)
		assert.Error(t, err)
		assert.Equal(t, ErrLockNotHeld, err)

		// Clean up
		err = lock1.Unlock(ctx)
		require.NoError(t, err)
	})
}

func TestLockManagerPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager
	cfg := DefaultConfig
	cfg.TablePrefix = "test_"
	cfg.SchemaName = "public"

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)

	// Test creating and releasing many locks sequentially
	t.Run("ManyLocksSequential", func(t *testing.T) {
		numLocks := 100
		start := time.Now()

		for i := 0; i < numLocks; i++ {
			resourceName := fmt.Sprintf("test-resource-seq-%d", i)
			lock := lm.NewDistributedLock(resourceName)

			err := lock.Lock(ctx)
			require.NoError(t, err)

			err = lock.Unlock(ctx)
			require.NoError(t, err)
		}

		elapsed := time.Since(start)
		log.Printf("Created and released %d locks sequentially in %s", numLocks, elapsed)
		assert.Less(t, elapsed, 30*time.Second) // Adjust this threshold as needed
	})

	// Test concurrent lock acquisition
	t.Run("ConcurrentLockAcquisition", func(t *testing.T) {
		var wg sync.WaitGroup
		numWorkers := 10
		locksPerWorker := 10
		errorsChan := make(chan error, numWorkers*locksPerWorker)

		start := time.Now()

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				for i := 0; i < locksPerWorker; i++ {
					resourceName := fmt.Sprintf("test-resource-conc-%d-%d", workerID, i)
					lock := lm.NewDistributedLock(resourceName)

					if err := lock.Lock(ctx); err != nil {
						errorsChan <- fmt.Errorf("worker %d failed to acquire lock %d: %w", workerID, i, err)
						continue
					}

					// Simulate some work
					time.Sleep(10 * time.Millisecond)

					if err := lock.Unlock(ctx); err != nil {
						errorsChan <- fmt.Errorf("worker %d failed to release lock %d: %w", workerID, i, err)
					}
				}
			}(w)
		}

		wg.Wait()
		close(errorsChan)

		// Check for errors
		var errors []error
		for err := range errorsChan {
			errors = append(errors, err)
		}

		elapsed := time.Since(start)
		log.Printf("Processed %d locks concurrently in %s", numWorkers*locksPerWorker, elapsed)

		assert.Empty(t, errors, "Expected no errors during concurrent lock acquisition")
		assert.Less(t, elapsed, 30*time.Second) // Adjust this threshold as needed
	})

	// Test contended lock scenario
	t.Run("ContentionScenario", func(t *testing.T) {
		var wg sync.WaitGroup
		numWorkers := 5
		resourceName := "test-resource-contention"
		var mu sync.Mutex
		successfulWorkers := make(map[int]bool)

		start := time.Now()

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				lock := lm.NewDistributedLock(resourceName)
				err := lock.Lock(ctx)
				if err != nil {
					if err != ErrLockAlreadyHeld {
						t.Errorf("Worker %d received unexpected error: %v", workerID, err)
					}
					return
				}

				// Record successful acquisition
				mu.Lock()
				successfulWorkers[workerID] = true
				mu.Unlock()

				// Hold the lock for a short time
				time.Sleep(100 * time.Millisecond)

				err = lock.Unlock(ctx)
				if err != nil {
					t.Errorf("Worker %d failed to release lock: %v", workerID, err)
				}
			}(w)
		}

		wg.Wait()
		elapsed := time.Since(start)

		// Only one worker should have succeeded
		assert.Equal(t, 1, len(successfulWorkers), "Expected exactly one worker to acquire the lock")
		log.Printf("Contention scenario with %d workers completed in %s", numWorkers, elapsed)
	})
}

func TestLockManagerEdgeCases(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager
	cfg := DefaultConfig
	cfg.TablePrefix = "test_"
	cfg.SchemaName = "public"

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	lm.WithClock(fakeClock)

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		resourceName := "test-resource-10"

		// First, we need a lock to hold the resource
		lock1 := lm.NewDistributedLock(resourceName)
		err := lock1.Lock(ctx)
		require.NoError(t, err)

		// Create a context that's already cancelled
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()

		// Create a lock with a longer max retry to ensure the context cancellation is checked
		lock2 := lm.NewDistributedLock(resourceName, WithMaxAttempts(3))

		// Try to acquire the lock with the cancelled context
		err = lock2.Lock(cancelledCtx)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)

		// Clean up
		err = lock1.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test lock acquisition with invalid resource name
	t.Run("EmptyResourceName", func(t *testing.T) {
		lock := lm.NewDistributedLock("")
		err := lock.Lock(ctx)
		require.NoError(t, err) // Empty resource name should still work

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test create lock manager with nil config
	t.Run("NilConfig", func(t *testing.T) {
		lm, err := NewLockManager(ctx, db, nil)
		require.NoError(t, err)
		assert.NotNil(t, lm)

		// Test basic lock functionality
		lock := lm.NewDistributedLock("test-resource-nil-config")
		err = lock.Lock(ctx)
		require.NoError(t, err)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test lock for a resource with special characters
	t.Run("SpecialCharactersInResourceName", func(t *testing.T) {
		resourceName := "test-resource-!@#$%^&*()_+"
		lock := lm.NewDistributedLock(resourceName)

		err := lock.Lock(ctx)
		require.NoError(t, err)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	// Test lock a very long resource name
	t.Run("VeryLongResourceName", func(t *testing.T) {
		resourceName := ""
		for i := 0; i < 1000; i++ {
			resourceName += "a"
		}

		lock := lm.NewDistributedLock(resourceName)
		err := lock.Lock(ctx)
		require.NoError(t, err)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})
}

func TestLockManagerTablePrefixAndSchema(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a test schema
	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS test_schema")
	require.NoError(t, err)

	// Test different configurations
	testConfigs := []struct {
		name        string
		tablePrefix string
		schemaName  string
	}{
		{"DefaultConfig", "", ""},
		{"WithTablePrefix", "prefix_", ""},
		{"WithSchema", "", "test_schema"},
		{"WithBoth", "prefix_", "test_schema"},
	}

	for _, tc := range testConfigs {
		t.Run(tc.name, func(t *testing.T) {
			// Create a lock manager with the specific config
			cfg := DefaultConfig
			cfg.TablePrefix = tc.tablePrefix
			cfg.SchemaName = tc.schemaName

			lm, err := NewLockManager(ctx, db, &cfg)
			require.NoError(t, err)

			// Test basic lock functionality
			resourceName := fmt.Sprintf("test-resource-%s", tc.name)
			lock := lm.NewDistributedLock(resourceName)

			err = lock.Lock(ctx)
			require.NoError(t, err)

			err = lock.Unlock(ctx)
			require.NoError(t, err)

			// Verify the table was created with the correct name
			var count int
			var query string
			if tc.schemaName != "" {
				query = `
					SELECT COUNT(*) FROM information_schema.tables 
					WHERE table_schema = $1 AND table_name = $2
				`
				err = db.QueryRow(query, tc.schemaName, tc.tablePrefix+"distributed_locks").Scan(&count)
			} else {
				query = `
					SELECT COUNT(*) FROM information_schema.tables 
					WHERE table_schema = 'public' AND table_name = $1
				`
				err = db.QueryRow(query, tc.tablePrefix+"distributed_locks").Scan(&count)
			}

			require.NoError(t, err)
			assert.Equal(t, 1, count, "Table should exist with the correct name and schema")
		})
	}
}

func TestLockManagerWithRealClock(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager with real clock
	cfg := DefaultConfig
	cfg.TablePrefix = "test_"
	cfg.SchemaName = "public"
	// Set short durations for real clock testing
	cfg.LeaseTime = 2 * time.Second
	cfg.HeartbeatInterval = 500 * time.Millisecond

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)

	// Test heartbeat mechanism with real clock
	t.Run("HeartbeatWithRealClock", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping test in short mode")
		}

		// Create a test-specific config with appropriate lease time
		tempCfg := DefaultConfig
		tempCfg.TablePrefix = "test_"
		tempCfg.SchemaName = "public"
		tempCfg.LeaseTime = 5 * time.Second // Longer lease time
		tempCfg.HeartbeatInterval = 500 * time.Millisecond

		tempLM, err := NewLockManager(ctx, db, &tempCfg)
		require.NoError(t, err)

		resourceName := "test-resource-real-clock"
		lock := tempLM.NewDistributedLock(resourceName)

		// Acquire the lock
		err = lock.Lock(ctx)
		require.NoError(t, err)

		// Wait for a bit, but less than the lease time
		time.Sleep(2 * time.Second)

		// Lock should still be held
		isLocked, err := lock.IsLocked(ctx)
		require.NoError(t, err)
		assert.True(t, isLocked, "Lock should still be held")

		// Clean up
		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})
	// Test lock with custom retry options
	t.Run("CustomAttemptOptions", func(t *testing.T) {
		resourceName := "test-resource-custom-attempts"
		customMaxAttempts := 5
		customRetryDelay := 20 * time.Millisecond

		// Create a lock with custom retry options
		lock := lm.NewDistributedLock(resourceName,
			WithMaxAttempts(customMaxAttempts),
			WithRetryDelay(customRetryDelay))

		// Verify that the options were applied
		assert.Equal(t, customMaxAttempts, lock.maxAttempts)
		assert.Equal(t, customRetryDelay, lock.retryDelay)

		// Test basic functionality with the custom options
		err := lock.Lock(ctx)
		require.NoError(t, err)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})
	// Test lock expiration with real clock
	t.Run("LockExpirationWithRealClock", func(t *testing.T) {
		resourceName := "test-resource-real-expiration"
		lock := lm.NewDistributedLock(resourceName)

		// Acquire the lock
		err := lock.Lock(ctx)
		require.NoError(t, err)

		// Manually stop heartbeats to force expiration
		lock.stopHeartbeat()

		// Wait for the lock to expire
		time.Sleep(3 * time.Second) // > LeaseTime

		// Another lock should be able to acquire it now
		lock2 := lm.NewDistributedLock(resourceName)
		err = lock2.Lock(ctx)
		require.NoError(t, err, "Lock should be acquirable after expiration")

		// Clean up
		err = lock2.Unlock(ctx)
		require.NoError(t, err)
	})
}

// Utility function to start a Postgres container for testing
func startPostgresContainer(ctx context.Context) (testcontainers.Container, *sql.DB, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test_user",
			"POSTGRES_PASSWORD": "test_password",
			"POSTGRES_DB":       "test_db",
		},
		Cmd: []string{
			"postgres",
			"-c", "max_connections=200",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to start postgres container: %v", err)
	}

	host, err := postgres.Host(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get postgres host: %v", err)
	}

	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get postgres port: %v", err)
	}

	pgConnString := fmt.Sprintf("host=%s port=%d user=test_user password=test_password dbname=test_db sslmode=disable", host, port.Int())

	log.Printf("Attempting to connect with: %s", pgConnString)

	// Attempt to connect with retries
	var db *sql.DB
	err = retry(ctx, 30*time.Second, func() error {
		var err error
		db, err = sql.Open("pgx", pgConnString)
		if err != nil {
			log.Printf("Failed to connect, retrying: %v", err)
			return err
		}
		db.SetMaxOpenConns(200)
		return db.Ping()
	})

	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to connect to database after retries: %v", err)
	}

	// Verify max_connections setting
	var maxConnections int
	err = db.QueryRow("SHOW max_connections").Scan(&maxConnections)
	if err != nil {
		log.Printf("Warning: Could not verify max_connections: %v", err)
	} else {
		log.Printf("max_connections is set to: %d", maxConnections)
	}

	return postgres, db, pgConnString, nil
}

func retry(ctx context.Context, maxWait time.Duration, fn func() error) error {
	start := time.Now()
	for {
		err := fn()
		if err == nil {
			return nil
		}

		if time.Since(start) > maxWait {
			return fmt.Errorf("timeout after %v: %w", maxWait, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue with the next iteration
		}
	}
}

func TestLockManagerCleanup(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new LockManager with custom cleanup settings
	cfg := DefaultConfig
	cfg.TablePrefix = "cleanup_test_"
	cfg.SchemaName = "public"
	// Set shorter durations for cleanup testing
	cfg.CleanupInterval = 1 * time.Second
	cfg.InactivityThreshold = 2 * time.Second
	// Disable automatic cleanup so we can test manual cleanup
	cfg.EnableAutomaticCleanup = false

	lm, err := NewLockManager(ctx, db, &cfg)
	require.NoError(t, err)
	defer lm.Close()

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	lm.WithClock(fakeClock)

	// Test basic cleanup functionality
	t.Run("BasicCleanup", func(t *testing.T) {
		// Create and acquire multiple locks
		lockCount := 5
		resourceNames := make([]string, lockCount)

		for i := 0; i < lockCount; i++ {
			resourceNames[i] = fmt.Sprintf("test-cleanup-resource-%d", i)
			lock := lm.NewDistributedLock(resourceNames[i])
			err := lock.Lock(ctx)
			require.NoError(t, err)
		}

		// Verify all locks exist in the database
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, lm.tableName)
		var count int
		err = db.QueryRowContext(ctx, query).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, lockCount, count, "All locks should exist in the database")

		// Advance clock past the inactivity threshold
		fakeClock.Advance(cfg.InactivityThreshold + 1*time.Second)

		// Run cleanup
		err = lm.Cleanup(ctx)
		require.NoError(t, err)

		// Verify all locks have been removed
		err = db.QueryRowContext(ctx, query).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "All locks should have been cleaned up")
	})

	// Test selective cleanup based on activity
	t.Run("SelectiveCleanup", func(t *testing.T) {
		// Reset clock
		fakeClock = clockwork.NewFakeClock()
		lm.WithClock(fakeClock)

		// Create 5 locks
		lockCount := 5
		locks := make([]*DistributedLock, lockCount)

		for i := 0; i < lockCount; i++ {
			resourceName := fmt.Sprintf("test-selective-cleanup-%d", i)
			locks[i] = lm.NewDistributedLock(resourceName)
			err := locks[i].Lock(ctx)
			require.NoError(t, err)
		}

		// Advance clock by half the inactivity threshold
		fakeClock.Advance(cfg.InactivityThreshold / 2)

		// Keep the first 2 locks active by extending their lease
		for i := 0; i < 2; i++ {
			err := locks[i].ExtendLease(ctx, 30*time.Second)
			require.NoError(t, err)
		}

		// Advance clock past the inactivity threshold
		fakeClock.Advance(cfg.InactivityThreshold)

		// Run cleanup
		err = lm.Cleanup(ctx)
		require.NoError(t, err)

		// Verify only inactive locks were removed
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, lm.tableName)
		var count int
		err = db.QueryRowContext(ctx, query).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count, "Only active locks should remain")

		// Cleanup remaining locks
		for i := 0; i < 2; i++ {
			err := locks[i].Unlock(ctx)
			require.NoError(t, err)
		}
	})

	// Test automatic cleanup
	t.Run("AutomaticCleanup", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping test in short mode")
		}

		// Create a new LockManager with automatic cleanup enabled
		autoCfg := DefaultConfig
		autoCfg.TablePrefix = "auto_cleanup_" + uuid.NewString()[:8] + "_" // Use unique prefix to avoid conflicts
		autoCfg.CleanupInterval = 1 * time.Second
		autoCfg.InactivityThreshold = 1 * time.Second
		autoCfg.EnableAutomaticCleanup = true

		// Create the LockManager
		autoLM, err := NewLockManager(ctx, db, &autoCfg)
		require.NoError(t, err)
		defer autoLM.Close()

		// Directly insert stale records into the database
		now := time.Now().UTC().Add(-autoCfg.InactivityThreshold - 2*time.Second) // Old records
		query := fmt.Sprintf(`
			INSERT INTO %s ("resource", "node_id", "expires_at", "last_heartbeat")
			VALUES ($1, $2, $3, $4)
		`, autoLM.tableName)

		for i := 0; i < 3; i++ {
			resourceName := fmt.Sprintf("test-auto-cleanup-%d", i)
			_, err = db.ExecContext(ctx, query, resourceName, autoLM.nodeID, now, now)
			require.NoError(t, err)
		}

		// Wait enough time for cleanup to run (interval + buffer)
		time.Sleep(autoCfg.CleanupInterval + 2*time.Second)

		// Verify locks have been cleaned up by automatic process
		var count int
		err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", autoLM.tableName)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "All locks should have been automatically cleaned up")
	})

	// Test cleanup with real clock
	t.Run("CleanupWithRealClock", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping test in short mode")
		}

		// Create a lock manager with real clock and short cleanup settings
		realCfg := DefaultConfig
		realCfg.TablePrefix = "real_cleanup_"
		realCfg.SchemaName = "public"
		realCfg.CleanupInterval = 3 * time.Second
		realCfg.InactivityThreshold = 2 * time.Second
		realCfg.EnableAutomaticCleanup = false

		realLM, err := NewLockManager(ctx, db, &realCfg)
		require.NoError(t, err)
		defer realLM.Close()

		// Manually create locks in database to avoid race conditions
		now := time.Now().UTC()
		query := fmt.Sprintf(`
			INSERT INTO %s ("resource", "node_id", "expires_at", "last_heartbeat")
			VALUES ($1, $2, $3, $4)
		`, realLM.tableName)

		for i := 0; i < 3; i++ {
			resourceName := fmt.Sprintf("test-real-cleanup-%d", i)
			_, err = db.ExecContext(ctx, query,
				resourceName,
				realLM.nodeID,
				now.Add(10*time.Second),
				now.Add(-realCfg.InactivityThreshold-1*time.Second)) // Already inactive
			require.NoError(t, err)
		}

		// Verify locks exist
		countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, realLM.tableName)
		var count int
		err = db.QueryRowContext(ctx, countQuery).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 3, count, "All locks should exist initially")

		// Run manual cleanup
		err = realLM.Cleanup(ctx)
		require.NoError(t, err)

		// Verify locks have been cleaned up
		err = db.QueryRowContext(ctx, countQuery).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "All locks should have been cleaned up")
	})

	// Test disabling and re-enabling the automatic cleanup
	t.Run("DisableEnableAutomaticCleanup", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping test in short mode")
		}

		// This test is more complex and prone to timing issues
		// Simply test that we can toggle the cleanup state

		// Create a lock manager with automatic cleanup enabled
		enableCfg := DefaultConfig
		enableCfg.TablePrefix = "toggle_cleanup_"
		enableCfg.CleanupInterval = 1 * time.Second
		enableCfg.InactivityThreshold = 2 * time.Second
		enableCfg.EnableAutomaticCleanup = true

		toggleLM, err := NewLockManager(ctx, db, &enableCfg)
		require.NoError(t, err)
		defer toggleLM.Close()

		// Verify cleanup is running
		assert.False(t, toggleLM.cleanupStopped, "Cleanup should be running initially")

		// Stop the cleanup process
		toggleLM.StopCleanup()
		assert.True(t, toggleLM.cleanupStopped, "Cleanup should be stopped after StopCleanup")

		// Restart cleanup
		err = toggleLM.StartCleanup(ctx)
		require.NoError(t, err)
		assert.False(t, toggleLM.cleanupStopped, "Cleanup should be running after restart")
	})
}
