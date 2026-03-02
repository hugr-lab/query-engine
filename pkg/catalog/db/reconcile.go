package db

import (
	"context"
	"fmt"
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

	// Delete stale metadata for this catalog before reinserting.
	// This ensures removed modules/dependencies/data_objects are cleaned up.
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog_name = $1`,
		p.table("_schema_module_catalogs"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean module_catalogs: %w", err)
	}
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE catalog_name = $1`,
		p.table("_schema_catalog_dependencies"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean catalog_dependencies: %w", err)
	}
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_data_objects"), p.table("_schema_types"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean data_objects: %w", err)
	}

	// Step 1: Collect module names from types that define modules (have @module_root).
	// Query the module column directly instead of parsing directive JSON in Go.
	moduleNames, err := p.collectModuleNames(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect modules: %w", err)
	}

	// Write module records and links
	for _, moduleName := range moduleNames {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name) VALUES ($1)
			 ON CONFLICT (name) DO NOTHING`,
			p.table("_schema_modules"),
		), moduleName); err != nil {
			return fmt.Errorf("reconcile module upsert: %w", err)
		}
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (module_name, catalog_name) VALUES ($1, $2)
			 ON CONFLICT (module_name, catalog_name) DO NOTHING`,
			p.table("_schema_module_catalogs"),
		), moduleName, catalogName); err != nil {
			return fmt.Errorf("reconcile module_catalog: %w", err)
		}
	}

	// Step 2: Collect catalog dependencies (fields with dependency_catalog).
	depCatalogs, err := p.collectDependencies(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect dependencies: %w", err)
	}

	for _, depCatalog := range depCatalogs {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (catalog_name, depends_on) VALUES ($1, $2)
			 ON CONFLICT (catalog_name, depends_on) DO NOTHING`,
			p.table("_schema_catalog_dependencies"),
		), catalogName, depCatalog); err != nil {
			return fmt.Errorf("reconcile dependency: %w", err)
		}
	}

	// Step 3: Collect and write data_objects for Table/View/ParameterizedView types.
	dataObjects, err := p.collectDataObjects(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect data_objects: %w", err)
	}

	for _, do := range dataObjects {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, filter_type_name, args_type_name) VALUES ($1, $2, $3)
			 ON CONFLICT (name) DO UPDATE SET filter_type_name=$2, args_type_name=$3`,
			p.table("_schema_data_objects"),
		), do.name, do.filterType, do.argsType); err != nil {
			return fmt.Errorf("reconcile data_object: %w", err)
		}
	}

	// Step 4: Collect and write reverse dependencies.
	revDeps, err := p.collectReverseDependencies(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect reverse deps: %w", err)
	}

	for _, depCatalog := range revDeps {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (catalog_name, depends_on) VALUES ($1, $2)
			 ON CONFLICT (catalog_name, depends_on) DO NOTHING`,
			p.table("_schema_catalog_dependencies"),
		), depCatalog, catalogName); err != nil {
			return fmt.Errorf("reconcile reverse dependency: %w", err)
		}
	}

	return nil
}

// dataObjectInfo holds collected data object metadata.
type dataObjectInfo struct {
	name       string
	filterType any // nil or string
	argsType   any // nil or string
}

// collectModuleNames queries types with @module_root directive and returns their module names.
func (p *Provider) collectModuleNames(ctx context.Context, conn *Connection, catalogName string) ([]string, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT module FROM %s
		 WHERE catalog = $1 AND module != ''
		   AND CAST(directives AS VARCHAR) LIKE '%%module_root%%'`,
		p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var modules []string
	for rows.Next() {
		var mod string
		if err := rows.Scan(&mod); err != nil {
			continue
		}
		modules = append(modules, mod)
	}
	return modules, nil
}

// collectDependencies returns distinct catalog names that this catalog depends on.
// Detected by finding types owned by OTHER catalogs where this catalog contributes fields.
// E.g., ext catalog adds fields to base_Item (owned by base) → ext depends on base.
func (p *Provider) collectDependencies(ctx context.Context, conn *Connection, catalogName string) ([]string, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT t.catalog FROM %s f
		 INNER JOIN %s t ON f.type_name = t.name
		 WHERE f.catalog = $1 AND t.catalog IS NOT NULL AND t.catalog != $1`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			continue
		}
		deps = append(deps, dep)
	}
	return deps, nil
}

// collectDataObjects returns data object info for Table/View/ParameterizedView types.
func (p *Provider) collectDataObjects(ctx context.Context, conn *Connection, catalogName string) ([]dataObjectInfo, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT t.name FROM %s t
		 WHERE t.catalog = $1
		   AND t.hugr_type IN ('Table', 'View', 'ParameterizedView')`,
		p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var typeNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		typeNames = append(typeNames, name)
	}

	// Now check filter/args types for each data object
	var objects []dataObjectInfo
	for _, typeName := range typeNames {
		do := dataObjectInfo{name: typeName}

		filterTypeName := typeName + "_filter"
		var filterExists int
		_ = conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT count(*) FROM %s WHERE name = $1`,
			p.table("_schema_types"),
		), filterTypeName).Scan(&filterExists)
		if filterExists > 0 {
			do.filterType = filterTypeName
		}

		argsTypeName := typeName + "_args"
		var argsExists int
		_ = conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT count(*) FROM %s WHERE name = $1`,
			p.table("_schema_types"),
		), argsTypeName).Scan(&argsExists)
		if argsExists > 0 {
			do.argsType = argsTypeName
		}

		objects = append(objects, do)
	}
	return objects, nil
}

// collectReverseDependencies returns catalogs that extend types owned by this catalog.
func (p *Provider) collectReverseDependencies(ctx context.Context, conn *Connection, catalogName string) ([]string, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT DISTINCT f.catalog FROM %s f
		 INNER JOIN %s t ON f.type_name = t.name
		 WHERE f.dependency_catalog = $1 AND f.catalog IS NOT NULL AND f.catalog != $1`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deps []string
	for rows.Next() {
		var dep string
		if err := rows.Scan(&dep); err != nil {
			continue
		}
		deps = append(deps, dep)
	}
	return deps, nil
}
