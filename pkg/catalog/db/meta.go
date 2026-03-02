package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
		// Drop existing vec column (ignore error if it doesn't exist)
		_, _ = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s DROP COLUMN IF EXISTS vec`, tbl))
		// Add vec column with new size
		var colType string
		if p.isPostgres {
			colType = fmt.Sprintf("vector(%d)", p.vecSize)
		} else {
			colType = fmt.Sprintf("FLOAT[%d]", p.vecSize)
		}
		_, err = conn.Exec(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN vec %s`, tbl, colType))
		if err != nil {
			return fmt.Errorf("add vec column to %s: %w", t, err)
		}
	}

	// Persist the new vec_size
	return p.writeSettings(ctx, settingsConfig{VecSize: p.vecSize})
}
