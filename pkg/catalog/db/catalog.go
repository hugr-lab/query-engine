package db

import (
	"context"
	"database/sql"
	"fmt"
)

// CatalogRecord holds metadata for a registered catalog.
type CatalogRecord struct {
	Name        string
	Version     string
	Description string
	Disabled    bool
	Suspended   bool
}

// GetCatalog returns the catalog record for the given name, or nil if not found.
func (p *Provider) GetCatalog(ctx context.Context, name string) (*CatalogRecord, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get catalog: %w", err)
	}
	defer conn.Close()

	var rec CatalogRecord
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT name, version, description, disabled, suspended FROM %s WHERE name = $1`,
		p.table("_schema_catalogs"),
	), name).Scan(&rec.Name, &rec.Version, &rec.Description, &rec.Disabled, &rec.Suspended)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get catalog: %w", err)
	}
	return &rec, nil
}

// ListCatalogs returns all registered catalog records.
func (p *Provider) ListCatalogs(ctx context.Context) ([]*CatalogRecord, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("list catalogs: %w", err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, version, description, disabled, suspended FROM %s ORDER BY name`,
		p.table("_schema_catalogs"),
	))
	if err != nil {
		return nil, fmt.Errorf("list catalogs: %w", err)
	}
	defer rows.Close()

	var catalogs []*CatalogRecord
	for rows.Next() {
		var rec CatalogRecord
		if err := rows.Scan(&rec.Name, &rec.Version, &rec.Description, &rec.Disabled, &rec.Suspended); err != nil {
			return nil, fmt.Errorf("list catalogs scan: %w", err)
		}
		catalogs = append(catalogs, &rec)
	}
	return catalogs, nil
}

// SetCatalogVersion updates the version of a catalog.
func (p *Provider) SetCatalogVersion(ctx context.Context, name, version string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set catalog version: %w", err)
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`UPDATE %s SET version = $2 WHERE name = $1`,
		p.table("_schema_catalogs"),
	), name, version)
	if err != nil {
		return fmt.Errorf("set catalog version: %w", err)
	}
	return nil
}

// SetCatalogDisabled enables or disables a catalog.
func (p *Provider) SetCatalogDisabled(ctx context.Context, name string, disabled bool) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set catalog disabled: %w", err)
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`UPDATE %s SET disabled = $2 WHERE name = $1`,
		p.table("_schema_catalogs"),
	), name, disabled)
	if err != nil {
		return fmt.Errorf("set catalog disabled: %w", err)
	}

	p.InvalidateCatalog(name)
	return nil
}

// SetCatalogSuspended suspends or unsuspends a catalog.
func (p *Provider) SetCatalogSuspended(ctx context.Context, name string, suspended bool) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("set catalog suspended: %w", err)
	}
	defer conn.Close()

	_, err = p.execWrite(ctx, conn, fmt.Sprintf(
		`UPDATE %s SET suspended = $2 WHERE name = $1`,
		p.table("_schema_catalogs"),
	), name, suspended)
	if err != nil {
		return fmt.Errorf("set catalog suspended: %w", err)
	}

	p.InvalidateCatalog(name)
	return nil
}
