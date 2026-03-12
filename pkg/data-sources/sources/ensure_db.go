package sources

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// EnsurePgDatabase connects to the PostgreSQL "postgres" maintenance database
// and creates the target database if it doesn't exist.
// This is idempotent — it does nothing if the database already exists.
func EnsurePgDatabase(ctx context.Context, host, port, user, password, dbName string) error {
	if dbName == "" {
		return fmt.Errorf("ensure_db: database name is required")
	}
	if port == "" {
		port = "5432"
	}

	// Connect to the "postgres" maintenance database
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("ensure_db: connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

	// Check if database exists
	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("ensure_db: check database existence: %w", err)
	}

	if exists {
		return nil
	}

	// Create database — identifiers cannot be parameterized, so we use quoting
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
	if err != nil {
		return fmt.Errorf("ensure_db: create database %q: %w", dbName, err)
	}

	return nil
}
