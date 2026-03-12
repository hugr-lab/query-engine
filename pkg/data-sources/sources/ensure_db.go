package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// EnsurePgDatabase connects to the PostgreSQL "postgres" maintenance database
// and creates the target database if it doesn't exist.
// This is idempotent — it does nothing if the database already exists.
//
// NOTE: The connection to the maintenance database uses sslmode=disable because
// it targets the same PostgreSQL instance as the main DuckLake connection.
// In production, the DuckLake PostgreSQL backend should be co-located (same network)
// with the Hugr query engine, so TLS is not required for the maintenance connection.
// If your PostgreSQL instance requires TLS, do not use ensure_db — create the
// database manually instead.
func EnsurePgDatabase(ctx context.Context, host, port, user, password, dbName string) error {
	if dbName == "" {
		return fmt.Errorf("ensure_db: database name is required")
	}
	if strings.ContainsAny(dbName, `"'`) {
		return fmt.Errorf("ensure_db: database name must not contain quotes: %q", dbName)
	}
	if port == "" {
		port = "5432"
	}

	// Connect to the "postgres" maintenance database.
	// Uses sslmode=disable — see function doc for rationale.
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=postgres sslmode=disable",
		host, port, user, password)

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("ensure_db: connect to postgres maintenance database at %s:%s: %w (note: connection uses sslmode=disable; if your PostgreSQL requires TLS, create the database manually)", host, port, err)
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
