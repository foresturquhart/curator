package storage

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"time"
)

// Database represents a PostgreSQL storage connection pool
type Database struct {
	pool *pgxpool.Pool
}

// NewDatabase creates and configures a new storage connection
func NewDatabase(url string) (*Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Parse the connection config
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("unable to parse storage config: %w", err)
	}

	// Register pgvector types for vector operations
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		return pgxvec.RegisterTypes(ctx, conn)
	}

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to create storage connection pool: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("unable to connect to storage: %w", err)
	}

	return &Database{pool: pool}, nil
}

// GetPool returns the underlying connection pool
func (db *Database) GetPool() *pgxpool.Pool {
	return db.pool
}

// Close closes the storage connection pool
func (db *Database) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}
