package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// settingsConfig is the JSON structure stored in _schema_settings under key "config".
type settingsConfig struct {
	VecSize int `json:"vec_size"`
}

// ensureSettings creates the _schema_settings table if it does not exist.
func (p *Provider) ensureSettings(ctx context.Context) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("ensure settings: %w", err)
	}
	defer conn.Close()
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		key VARCHAR NOT NULL PRIMARY KEY,
		value JSON NOT NULL
	)`, p.table("_schema_settings")))
	return err
}

// readSettings reads the stored config from _schema_settings.
// Returns zero-value settingsConfig if no config row exists.
func (p *Provider) readSettings(ctx context.Context) (settingsConfig, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return settingsConfig{}, fmt.Errorf("read settings: %w", err)
	}
	defer conn.Close()

	var raw string
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT CAST(value AS VARCHAR) FROM %s WHERE key = 'config'`, p.table("_schema_settings"),
	)).Scan(&raw)
	if err == sql.ErrNoRows {
		return settingsConfig{}, nil
	}
	if err != nil {
		return settingsConfig{}, fmt.Errorf("read settings: %w", err)
	}

	var cfg settingsConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return settingsConfig{}, fmt.Errorf("parse settings: %w", err)
	}
	return cfg, nil
}

// writeSettings persists the config to _schema_settings.
func (p *Provider) writeSettings(ctx context.Context, cfg settingsConfig) error {
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}

	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`INSERT INTO %s (key, value) VALUES ('config', $1)
		 ON CONFLICT (key) DO UPDATE SET value = $1`,
		p.table("_schema_settings"),
	), string(data))
	if err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	return nil
}

// ensureVectorSize checks the stored vec_size against the configured one.
// If they differ, it drops and recreates vec columns on the 4 vector-bearing tables.
// If vecSize is 0, this is a no-op.
func (p *Provider) ensureVectorSize(ctx context.Context) error {
	if p.vecSize == 0 {
		return nil
	}

	stored, err := p.readSettings(ctx)
	if err != nil {
		return err
	}

	if stored.VecSize == p.vecSize {
		return nil
	}

	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("ensure vector size: %w", err)
	}
	defer conn.Close()

	tables := []string{"_schema_catalogs", "_schema_types", "_schema_fields", "_schema_modules"}
	for _, t := range tables {
		tbl := p.table(t)

		// DuckDB cannot DROP COLUMN on tables with indexes. Save and drop
		// indexes first, then restore them after the column change.
		var savedIndexSQL []string
		if !p.isPostgres {
			savedIndexSQL, err = p.saveAndDropIndexes(ctx, conn, t)
			if err != nil {
				return fmt.Errorf("save indexes for %s: %w", t, err)
			}
		}

		// Drop existing vec column
		_, dropErr := conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN IF EXISTS vec`, tbl))

		// Add vec column with new size
		var colType string
		if p.isPostgres {
			colType = fmt.Sprintf("vector(%d)", p.vecSize)
		} else {
			colType = fmt.Sprintf("FLOAT[%d]", p.vecSize)
		}
		_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN vec %s`, tbl, colType))
		if err != nil {
			// Column may already exist if schema.sql created it and DROP failed.
			// Accept "already exists" only when the stored size was 0 (fresh init).
			if strings.Contains(err.Error(), "already exists") && stored.VecSize == 0 {
				// Restore indexes we dropped before continuing
				for _, sql := range savedIndexSQL {
					_, _ = conn.Exec(ctx, sql)
				}
				continue
			}
			if dropErr != nil {
				return fmt.Errorf("drop vec column from %s: %w (add also failed: %v)", t, dropErr, err)
			}
			return fmt.Errorf("add vec column to %s: %w", t, err)
		}

		// Restore indexes
		for _, sql := range savedIndexSQL {
			if _, err := conn.Exec(ctx, sql); err != nil {
				return fmt.Errorf("restore index on %s: %w", t, err)
			}
		}
	}

	// Persist the new vec_size
	return p.writeSettings(ctx, settingsConfig{VecSize: p.vecSize})
}

// saveAndDropIndexes queries DuckDB's catalog for non-primary indexes on the
// given table, saves their CREATE INDEX SQL, drops them, and returns the SQL
// for later restoration. This works around DuckDB's inability to ALTER TABLE
// DROP COLUMN on tables that have indexes.
func (p *Provider) saveAndDropIndexes(ctx context.Context, conn *Connection, tableName string) ([]string, error) {
	// Determine DuckDB schema for the query (attached "core" vs default "main")
	schemaName := "main"
	if p.prefix != "" {
		schemaName = strings.TrimSuffix(p.prefix, ".")
	}

	rows, err := conn.Query(ctx,
		`SELECT index_name, sql FROM duckdb_indexes()
		 WHERE table_name = $1 AND schema_name = $2 AND is_primary = false AND sql IS NOT NULL`,
		tableName, schemaName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var saved []string
	var toDrop []string
	for rows.Next() {
		var name, createSQL string
		if err := rows.Scan(&name, &createSQL); err != nil {
			return nil, err
		}
		saved = append(saved, createSQL)
		toDrop = append(toDrop, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Drop indexes so ALTER TABLE can proceed
	for _, name := range toDrop {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`DROP INDEX IF EXISTS %s%s`, p.prefix, name)); err != nil {
			return nil, fmt.Errorf("drop index %s: %w", name, err)
		}
	}

	return saved, nil
}
