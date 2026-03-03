package db

import (
	"context"
	"fmt"
)

// deleteSchemaObjectsForCatalog removes all schema objects (types, fields,
// arguments, enum values, data objects, module links) for a catalog using
// a single multi-statement SQL script.
//
// Must be called within a transaction. Does NOT delete the catalog record
// or dependency metadata — those are handled by DropCatalog.
//
// Delete order respects DuckDB's lack of FK CASCADE:
//
//	enum_values → arguments (by field.catalog + dependency_catalog) →
//	fields (by catalog + dependency_catalog) → types →
//	data_object_queries (orphan) → data_objects (orphan) → module_catalogs
func (p *Provider) deleteSchemaObjectsForCatalog(ctx context.Context, conn *Connection, name string) error {
	script := fmt.Sprintf(`
DELETE FROM %s WHERE type_name IN (SELECT name FROM %s WHERE catalog = $1);
DELETE FROM %s WHERE (type_name, field_name) IN (SELECT type_name, name FROM %s WHERE catalog = $1);
DELETE FROM %s WHERE (type_name, field_name) IN (SELECT type_name, name FROM %s WHERE dependency_catalog = $1);
DELETE FROM %s WHERE catalog = $1;
DELETE FROM %s WHERE dependency_catalog = $1;
DELETE FROM %s WHERE catalog = $1;
DELETE FROM %s WHERE object_name NOT IN (SELECT name FROM %s);
DELETE FROM %s WHERE name NOT IN (SELECT name FROM %s);
DELETE FROM %s WHERE catalog_name = $1;
DELETE FROM %s WHERE name != '' AND name NOT IN (SELECT DISTINCT module_name FROM %s)`,
		// 1. enum_values for owned types
		p.table("_schema_enum_values"), p.table("_schema_types"),
		// 2. arguments for catalog-owned fields (on owned types AND module extension fields)
		p.table("_schema_arguments"), p.table("_schema_fields"),
		// 3. arguments for extension fields from other catalogs depending on this one
		p.table("_schema_arguments"), p.table("_schema_fields"),
		// 4. fields owned by this catalog (covers owned types + module extension fields on Query/Mutation/_join etc.)
		p.table("_schema_fields"),
		// 5. extension fields from other catalogs that depend on this catalog
		p.table("_schema_fields"),
		// 6. types
		p.table("_schema_types"),
		// 7. orphan data_object_queries
		p.table("_schema_data_object_queries"), p.table("_schema_types"),
		// 8. orphan data_objects
		p.table("_schema_data_objects"), p.table("_schema_types"),
		// 9. module_catalogs for this catalog
		p.table("_schema_module_type_catalogs"),
		// 10. orphan modules (no remaining catalog links, excluding root module '')
		p.table("_schema_modules"), p.table("_schema_module_type_catalogs"),
	)

	_, err := p.execWrite(ctx, conn, script, name)
	return err
}

// dropCatalogSchemaObjects removes all schema objects for a catalog
// WITHOUT deleting the catalog record or dependency metadata.
//
// Used by suspendDependents and AddCatalog (version mismatch) to clean
// schema objects while preserving:
//   - catalog record (for suspended state tracking)
//   - dependency info (for reactivation dependency checks)
func (p *Provider) dropCatalogSchemaObjects(ctx context.Context, name string) error {
	txCtx, err := p.pool.WithTx(ctx)
	if err != nil {
		return fmt.Errorf("drop schema objects: begin tx: %w", err)
	}
	defer p.pool.Rollback(txCtx)

	conn, err := p.pool.Conn(txCtx)
	if err != nil {
		return fmt.Errorf("drop schema objects: %w", err)
	}
	defer conn.Close()

	if err := p.deleteSchemaObjectsForCatalog(txCtx, conn, name); err != nil {
		return fmt.Errorf("drop schema objects: %w", err)
	}

	if err := p.pool.Commit(txCtx); err != nil {
		return fmt.Errorf("drop schema objects: commit: %w", err)
	}

	p.InvalidateCatalog(name)
	return nil
}

// DropCatalog removes all types, fields, arguments, enum values, and metadata
// for the named catalog. If cascade is true, dependent catalogs are suspended.
//
// Delete order:
//
//	cascade suspension → schema objects → catalog_dependencies → catalog record
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

	// 1. Suspend dependent catalogs BEFORE deleting dependency records.
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

	// 2. Delete all schema objects (shared with dropCatalogSchemaObjects).
	if err := p.deleteSchemaObjectsForCatalog(txCtx, conn, name); err != nil {
		return fmt.Errorf("drop catalog schema objects: %w", err)
	}

	// 3. Delete catalog dependencies and the catalog record itself.
	if _, err := p.execWrite(txCtx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog_name = $1 OR depends_on = $1;
		 DELETE FROM %s WHERE name = $1`,
		p.table("_schema_catalog_dependencies"),
		p.table("_schema_catalogs"),
	), name); err != nil {
		return fmt.Errorf("drop catalog deps+record: %w", err)
	}

	if err := p.pool.Commit(txCtx); err != nil {
		return fmt.Errorf("drop catalog: commit: %w", err)
	}

	p.InvalidateCatalog(name)
	return nil
}
