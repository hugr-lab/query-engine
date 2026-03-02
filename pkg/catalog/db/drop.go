package db

import (
	"context"
	"fmt"
)

// DropCatalog removes all types, fields, arguments, enum values, and metadata
// for the named catalog. If cascade is true, dependent catalogs are suspended.
//
// Delete order respects DuckDB's lack of FK CASCADE:
//
//	cascade suspension → enum_values → arguments → fields → types →
//	data_object_queries → data_objects → module_catalogs → catalog_dependencies → catalog record
func (p *Provider) DropCatalog(ctx context.Context, name string, cascade bool) error {
	txCtx, err := p.pool.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("drop catalog: begin tx: %w", err)
	}
	defer p.pool.Rollback(txCtx)

	conn, err := p.pool.Conn(txCtx)
	if err != nil {
		return fmt.Errorf("drop catalog: %w", err)
	}
	defer conn.Close()

	// 1. Suspend dependent catalogs BEFORE deleting dependency records
	if cascade {
		if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
			`UPDATE %s SET suspended = true
			 WHERE name IN (
			   SELECT catalog_name FROM %s WHERE depends_on = $1
			 )`,
			p.table("_schema_catalogs"), p.table("_schema_catalog_dependencies"),
		), name); err != nil {
			return fmt.Errorf("drop catalog suspend dependents: %w", err)
		}
	}

	// 2. Delete enum values for types owned by this catalog
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_enum_values"), p.table("_schema_types"),
	), name); err != nil {
		return fmt.Errorf("drop catalog enum_values: %w", err)
	}

	// 3. Delete arguments for types owned by this catalog
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_arguments"), p.table("_schema_types"),
	), name); err != nil {
		return fmt.Errorf("drop catalog arguments: %w", err)
	}

	// 3b. Delete arguments for extension fields depending on this catalog
	// (step 3 only deletes args for types owned by this catalog, not for
	// extension fields on types owned by OTHER catalogs)
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE (type_name, field_name) IN (
		   SELECT type_name, name FROM %s WHERE dependency_catalog = $1
		 )`,
		p.table("_schema_arguments"), p.table("_schema_fields"),
	), name); err != nil {
		return fmt.Errorf("drop catalog extension field arguments: %w", err)
	}

	// 4. Delete fields owned by this catalog + extension fields depending on it
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE type_name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), name); err != nil {
		return fmt.Errorf("drop catalog fields (owned types): %w", err)
	}
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE dependency_catalog = $1`,
		p.table("_schema_fields"),
	), name); err != nil {
		return fmt.Errorf("drop catalog extension fields: %w", err)
	}

	// 5. Delete types owned by this catalog
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog = $1`,
		p.table("_schema_types"),
	), name); err != nil {
		return fmt.Errorf("drop catalog types: %w", err)
	}

	// 6. Clean up data_object_queries and data_objects (orphan cleanup)
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE object_name NOT IN (SELECT name FROM %s)`,
		p.table("_schema_data_object_queries"), p.table("_schema_types"),
	)); err != nil {
		return fmt.Errorf("drop catalog data_object_queries: %w", err)
	}
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE name NOT IN (SELECT name FROM %s)`,
		p.table("_schema_data_objects"), p.table("_schema_types"),
	)); err != nil {
		return fmt.Errorf("drop catalog data_objects: %w", err)
	}

	// 7. Clean up module_catalogs for this catalog
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog_name = $1`,
		p.table("_schema_module_catalogs"),
	), name); err != nil {
		return fmt.Errorf("drop catalog module_catalogs: %w", err)
	}

	// 8. Clean up catalog_dependencies
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog_name = $1 OR depends_on = $1`,
		p.table("_schema_catalog_dependencies"),
	), name); err != nil {
		return fmt.Errorf("drop catalog dependencies: %w", err)
	}

	// 9. Delete the catalog record itself
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE name = $1`,
		p.table("_schema_catalogs"),
	), name); err != nil {
		return fmt.Errorf("drop catalog record: %w", err)
	}

	if err := p.pool.Commit(txCtx); err != nil {
		return fmt.Errorf("drop catalog: commit: %w", err)
	}

	p.InvalidateCatalog(name)
	return nil
}
