package storage

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Postgres struct {
	Pool *pgxpool.Pool
	dsn  string
}

func NewPostgres(dsn string) (*Postgres, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse postgres config: %w", err)
	}

	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector;"); err != nil {
			return fmt.Errorf("unable to load pgvector extension: %w", err)
		}

		return pgxvec.RegisterTypes(ctx, conn)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to connect to postgres: %w", err)
	}

	return &Postgres{
		Pool: pool,
		dsn:  dsn,
	}, nil
}

func (d *Postgres) Close() {
	d.Pool.Close()
}

func (d *Postgres) Migrate() error {
	source, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("unable to load embedded migrations: %v", err)
	}

	db, err := sql.Open("pgx", d.dsn)
	if err != nil {
		return fmt.Errorf("unable to open migration connection: %w", err)
	}
	defer db.Close()

	instance, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("unable to create migration instance: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", source, "postgres", instance)
	if err != nil {
		return fmt.Errorf("could not initialize migration: %w", err)
	}

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("unable to migrate: %w", err)
	}

	return nil
}
