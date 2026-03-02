package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
)

// reconcileMetadata populates derived metadata tables from compiled types
// for the given catalog. Called within the Update transaction.
//
// Tables reconciled:
//   - _schema_modules (from types with @module_root directive)
//   - _schema_module_catalogs (catalog → module associations)
//   - _schema_catalog_dependencies (from @dependency directives)
//   - _schema_data_objects (from types classified as Table/View with filter types)
//   - _schema_data_object_queries (from query root fields referencing data objects)
func (p *Provider) reconcileMetadata(ctx context.Context, catalogName string) error {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	defer conn.Close()

	// Reconcile modules: find types with @module_root directive for this catalog
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, CAST(directives AS VARCHAR) FROM %s WHERE catalog = $1`,
		p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return fmt.Errorf("reconcile modules query: %w", err)
	}

	for rows.Next() {
		var typeName, dirJSON string
		if err := rows.Scan(&typeName, &dirJSON); err != nil {
			continue
		}

		// Check if this type has @module_root directive
		if !strings.Contains(dirJSON, base.ModuleRootDirectiveName) {
			continue
		}

		// Extract module name from @module directive
		moduleName := ""
		if idx := strings.Index(dirJSON, `"module"`); idx >= 0 {
			// Extract module from the type's module column
			var mod string
			err := conn.QueryRow(ctx, fmt.Sprintf(
				`SELECT module FROM %s WHERE name = $1`,
				p.table("_schema_types"),
			), typeName).Scan(&mod)
			if err == nil && mod != "" {
				moduleName = mod
			}
		}
		if moduleName == "" {
			continue
		}

		// Upsert module record
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1)
			 ON CONFLICT (name) DO NOTHING`,
			p.table("_schema_modules"),
		), moduleName); err != nil {
			return fmt.Errorf("reconcile module upsert: %w", err)
		}

		// Link module to catalog
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (module_name, catalog_name) VALUES ($1, $2)
			 ON CONFLICT (module_name, catalog_name) DO NOTHING`,
			p.table("_schema_module_catalogs"),
		), moduleName, catalogName); err != nil {
			return fmt.Errorf("reconcile module_catalog: %w", err)
		}
	}
	rows.Close()

	// Reconcile catalog dependencies: scan fields with dependency_catalog
	depRows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT dependency_catalog FROM %s
		 WHERE dependency_catalog IS NOT NULL AND dependency_catalog != $1
		   AND type_name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return fmt.Errorf("reconcile dependencies query: %w", err)
	}

	for depRows.Next() {
		var depCatalog string
		if err := depRows.Scan(&depCatalog); err != nil {
			continue
		}
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (catalog_name, depends_on) VALUES ($1, $2)
			 ON CONFLICT (catalog_name, depends_on) DO NOTHING`,
			p.table("_schema_catalog_dependencies"),
		), catalogName, depCatalog); err != nil {
			return fmt.Errorf("reconcile dependency: %w", err)
		}
	}
	depRows.Close()

	// Reconcile data_objects: types classified as Table or View
	doRows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT t.name, t.hugr_type FROM %s t
		 WHERE t.catalog = $1
		   AND t.hugr_type IN ('Table', 'View', 'ParameterizedView')`,
		p.table("_schema_types"),
	), catalogName)
	if err == nil {
		for doRows.Next() {
			var typeName, hugrType string
			if err := doRows.Scan(&typeName, &hugrType); err != nil {
				continue
			}
			// Look for corresponding filter type
			filterTypeName := typeName + "_filter"
			var filterExists int
			_ = conn.QueryRow(ctx, fmt.Sprintf(
				`SELECT count(*) FROM %s WHERE name = $1`,
				p.table("_schema_types"),
			), filterTypeName).Scan(&filterExists)

			var filterPtr any
			if filterExists > 0 {
				filterPtr = filterTypeName
			}

			// Look for args type (for parameterized views)
			argsTypeName := typeName + "_args"
			var argsExists int
			_ = conn.QueryRow(ctx, fmt.Sprintf(
				`SELECT count(*) FROM %s WHERE name = $1`,
				p.table("_schema_types"),
			), argsTypeName).Scan(&argsExists)

			var argsPtr any
			if argsExists > 0 {
				argsPtr = argsTypeName
			}

			_, _ = p.execWrite(ctx, conn, fmt.Sprintf(
				`INSERT INTO %s (name, filter_type_name, args_type_name) VALUES ($1, $2, $3)
				 ON CONFLICT (name) DO UPDATE SET filter_type_name=$2, args_type_name=$3`,
				p.table("_schema_data_objects"),
			), typeName, filterPtr, argsPtr)
		}
		doRows.Close()
	}

	// Also record reverse dependencies: other catalogs that extend types in this catalog
	revDepRows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT f.catalog FROM %s f
		 INNER JOIN %s t ON f.type_name = t.name
		 WHERE f.dependency_catalog = $1 AND f.catalog IS NOT NULL AND f.catalog != $1`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), catalogName)
	if err == nil {
		for revDepRows.Next() {
			var depCatalog string
			if err := revDepRows.Scan(&depCatalog); err != nil {
				continue
			}
			_, _ = p.execWrite(ctx, conn, fmt.Sprintf(
				`INSERT INTO %s (catalog_name, depends_on) VALUES ($1, $2)
				 ON CONFLICT (catalog_name, depends_on) DO NOTHING`,
				p.table("_schema_catalog_dependencies"),
			), depCatalog, catalogName)
		}
		revDepRows.Close()
	}

	return nil
}
