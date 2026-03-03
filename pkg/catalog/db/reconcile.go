package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/db/schema"
)

// reconcileMetadata populates derived metadata tables from compiled types
// for the given catalog. Called within the Update transaction.
//
// Tables reconciled:
//   - _schema_modules (from types with @module_root directive)
//   - _schema_module_type_catalogs (catalog → module associations)
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
		p.table("_schema_module_type_catalogs"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean module_catalogs: %w", err)
	}
	// Delete orphan modules (no remaining catalog links, excluding root module '').
	// This correctly handles the case where a module was solely provided by this catalog:
	// the module record is removed and will be re-created fresh below with only the
	// root types that actually exist (avoiding stale COALESCE issues).
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE name != '' AND name NOT IN (SELECT DISTINCT module_name FROM %s)`,
		p.table("_schema_modules"), p.table("_schema_module_type_catalogs"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean orphan modules: %w", err)
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
	if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
		`DELETE FROM %s WHERE object_name IN (SELECT name FROM %s WHERE catalog = $1)`,
		p.table("_schema_data_object_queries"), p.table("_schema_types"),
	), catalogName); err != nil {
		return fmt.Errorf("reconcile clean data_object_queries: %w", err)
	}

	// Step 0: For system types catalog, ensure root module (empty name) exists
	// pointing to system root types (Query, Mutation, Function, MutationFunction).
	if catalogName == SystemCatalogName {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, query_root, mutation_root, function_root, mut_function_root)
			 VALUES ('', $1, $2, $3, $4)
			 ON CONFLICT (name) DO UPDATE SET
			   query_root=$1, mutation_root=$2, function_root=$3, mut_function_root=$4`,
			p.table("_schema_modules"),
		), base.QueryBaseName, base.MutationBaseName, base.FunctionTypeName, base.FunctionMutationTypeName); err != nil {
			return fmt.Errorf("reconcile root module: %w", err)
		}
	}

	// Step 1: Collect module info from types classified as modules (hugr_type='module').
	// Parse @module_root and @module_catalog directives from the type's JSON directives.
	moduleRoots, err := p.collectModuleInfo(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect modules: %w", err)
	}

	// Group by module name — a module may have multiple root types (query, mutation, etc.)
	type moduleRecord struct {
		queryRoot    string
		mutationRoot string
		functionRoot string
		mutFuncRoot  string
		typeNames    map[string]struct{} // all module type names
		catalogs     map[string]struct{}
	}
	moduleMap := make(map[string]*moduleRecord)

	for _, mi := range moduleRoots {
		m, ok := moduleMap[mi.moduleName]
		if !ok {
			m = &moduleRecord{typeNames: make(map[string]struct{}), catalogs: make(map[string]struct{})}
			moduleMap[mi.moduleName] = m
		}
		switch mi.rootKind {
		case base.ModuleQuery:
			m.queryRoot = mi.typeName
		case base.ModuleMutation:
			m.mutationRoot = mi.typeName
		case base.ModuleFunction:
			m.functionRoot = mi.typeName
		case base.ModuleMutationFunction:
			m.mutFuncRoot = mi.typeName
		}
		m.typeNames[mi.typeName] = struct{}{}
		for _, cat := range mi.catalogs {
			m.catalogs[cat] = struct{}{}
		}
	}

	// Collect module catalog associations from module entry fields
	// (fields with @module_catalog on types not owned by this catalog).
	fieldModCats, err := p.collectFieldModuleCatalogs(ctx, conn, catalogName)
	if err != nil {
		return fmt.Errorf("reconcile collect field module_catalogs: %w", err)
	}
	for moduleName, cats := range fieldModCats {
		m, ok := moduleMap[moduleName]
		if !ok {
			m = &moduleRecord{typeNames: make(map[string]struct{}), catalogs: make(map[string]struct{})}
			moduleMap[moduleName] = m
			// Look up existing root type names from _schema_modules for this module.
			var qr, mr, fr, mfr *string
			err := conn.QueryRow(ctx, fmt.Sprintf(
				`SELECT query_root, mutation_root, function_root, mut_function_root FROM %s WHERE name = $1`,
				p.table("_schema_modules"),
			), moduleName).Scan(&qr, &mr, &fr, &mfr)
			if err == nil {
				for _, ptr := range []*string{qr, mr, fr, mfr} {
					if ptr != nil && *ptr != "" {
						m.typeNames[*ptr] = struct{}{}
					}
				}
			}
		}
		for _, cat := range cats {
			m.catalogs[cat] = struct{}{}
		}
	}

	// Write module records and collect (module_name, type_name, catalog_name) triples.
	// Each module root type gets its own entry per catalog.
	var moduleTypeCatalogTriples [][3]string
	for moduleName, m := range moduleMap {
		if _, err := p.execWrite(ctx, conn, fmt.Sprintf(
			`INSERT INTO %s (name, query_root, mutation_root, function_root, mut_function_root)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (name) DO UPDATE SET
			   query_root=COALESCE($2, %[1]s.query_root),
			   mutation_root=COALESCE($3, %[1]s.mutation_root),
			   function_root=COALESCE($4, %[1]s.function_root),
			   mut_function_root=COALESCE($5, %[1]s.mut_function_root)`,
			p.table("_schema_modules"),
		), moduleName, nullStr(m.queryRoot), nullStr(m.mutationRoot), nullStr(m.functionRoot), nullStr(m.mutFuncRoot)); err != nil {
			return fmt.Errorf("reconcile module upsert: %w", err)
		}
		// Create a triple for each (type name, catalog) combination.
		for cat := range m.catalogs {
			for tn := range m.typeNames {
				moduleTypeCatalogTriples = append(moduleTypeCatalogTriples, [3]string{moduleName, tn, cat})
			}
		}
	}
	if err := p.batchInsertModuleTypeCatalogs(ctx, conn, moduleTypeCatalogTriples); err != nil {
		return fmt.Errorf("reconcile module_type_catalogs: %w", err)
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

	// Step 5: Collect and write data_object_queries.
	if err := p.reconcileDataObjectQueries(ctx, conn, catalogName); err != nil {
		return fmt.Errorf("reconcile data_object_queries: %w", err)
	}

	return nil
}

// dataObjectInfo holds collected data object metadata.
type dataObjectInfo struct {
	name       string
	filterType any // nil or string
	argsType   any // nil or string
}

// moduleRootInfo holds extracted module root type information.
type moduleRootInfo struct {
	moduleName string
	typeName   string
	rootKind   base.ModuleObjectType
	catalogs   []string // catalog names from @module_catalog directives
}

// collectModuleInfo queries types classified as modules (hugr_type='module') and
// parses @module_root and @module_catalog directives to extract module metadata.
// Module root types have module='' but hugr_type='module' with @module_root(name, type).
func (p *Provider) collectModuleInfo(ctx context.Context, conn *Connection, catalogName string) ([]moduleRootInfo, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, module, CAST(directives AS VARCHAR) FROM %s
		 WHERE catalog = $1 AND hugr_type = 'module'`,
		p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []moduleRootInfo
	for rows.Next() {
		var typeName, moduleCol, dirJSON string
		if err := rows.Scan(&typeName, &moduleCol, &dirJSON); err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}

		// Parse @module_root directive for module name and root type slot.
		// Well-known root type names (Query, Mutation, Function, MutationFunction)
		// may not have @module_root directive — infer from name.
		info := moduleRootInfo{typeName: typeName}
		mrDir := dirs.ForName(base.ModuleRootDirectiveName)
		if mrDir != nil {
			if a := mrDir.Arguments.ForName("name"); a != nil && a.Value != nil {
				info.moduleName = a.Value.Raw
			}
			if a := mrDir.Arguments.ForName("type"); a != nil && a.Value != nil {
				switch a.Value.Raw {
				case "QUERY":
					info.rootKind = base.ModuleQuery
				case "MUTATION":
					info.rootKind = base.ModuleMutation
				case "FUNCTION":
					info.rootKind = base.ModuleFunction
				case "MUT_FUNCTION":
					info.rootKind = base.ModuleMutationFunction
				}
			}
		}

		// Fallback: if no module name from @module_root, use the module column
		// (set from @module directive during write).
		if info.moduleName == "" && moduleCol != "" {
			info.moduleName = moduleCol
		}

		// Fallback: if root kind is not set, infer from well-known type names.
		if info.rootKind == 0 {
			switch typeName {
			case base.QueryBaseName:
				info.rootKind = base.ModuleQuery
			case base.MutationBaseName:
				info.rootKind = base.ModuleMutation
			case base.FunctionTypeName:
				info.rootKind = base.ModuleFunction
			case base.FunctionMutationTypeName:
				info.rootKind = base.ModuleMutationFunction
			default:
				if mrDir == nil {
					continue // Unknown module type without @module_root — skip.
				}
			}
		}

		// Parse @module_catalog directives for catalog associations.
		for _, d := range dirs {
			if d.Name == base.ModuleCatalogDirectiveName {
				if a := d.Arguments.ForName("name"); a != nil && a.Value != nil {
					info.catalogs = append(info.catalogs, a.Value.Raw)
				}
			}
		}
		// If no explicit @module_catalog directives, the owning catalog is the default.
		if len(info.catalogs) == 0 {
			info.catalogs = []string{catalogName}
		}

		results = append(results, info)
	}
	return results, nil
}

// preloadModuleNames loads all module root type→module name mappings in a single query.
// Returns map[typeName]moduleName by parsing @module_root(name: ...) directives.
func (p *Provider) preloadModuleNames(ctx context.Context, conn *Connection) (map[string]string, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, CAST(directives AS VARCHAR) FROM %s WHERE hugr_type = 'module'`,
		p.table("_schema_types"),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var typeName, dirJSON string
		if err := rows.Scan(&typeName, &dirJSON); err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}
		mrDir := dirs.ForName(base.ModuleRootDirectiveName)
		if mrDir == nil {
			continue
		}
		if a := mrDir.Arguments.ForName("name"); a != nil && a.Value != nil {
			result[typeName] = a.Value.Raw
		}
	}
	return result, nil
}

// collectFieldModuleCatalogs scans fields owned by this catalog for @module_catalog
// directives and returns module→catalog associations. This handles cross-catalog
// module contributions where a field on a module root type (owned by another catalog)
// has catalog = $1 and @module_catalog pointing to the contributing catalog.
//
// For module entry fields on system types (Query/Mutation), the module name is
// resolved from the field's target type using the preloaded module names map.
func (p *Provider) collectFieldModuleCatalogs(ctx context.Context, conn *Connection, catalogName string) (map[string][]string, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT f.type_name, f.field_type, CAST(f.directives AS VARCHAR) FROM %s f
		 WHERE f.catalog = $1 AND CAST(f.directives AS VARCHAR) LIKE '%%module_catalog%%'`,
		p.table("_schema_fields"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type fieldEntry struct {
		parentType string
		fieldType  string // marshaled field type reference
		catalogs   []string
	}
	var entries []fieldEntry

	for rows.Next() {
		var parentType, fieldType, dirJSON string
		if err := rows.Scan(&parentType, &fieldType, &dirJSON); err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}
		var cats []string
		for _, d := range dirs {
			if d.Name == base.ModuleCatalogDirectiveName {
				if a := d.Arguments.ForName("name"); a != nil && a.Value != nil {
					cats = append(cats, a.Value.Raw)
				}
			}
		}
		if len(cats) > 0 {
			entries = append(entries, fieldEntry{parentType: parentType, fieldType: fieldType, catalogs: cats})
		}
	}

	if len(entries) == 0 {
		return nil, nil
	}

	// Preload module name→type name map (single query instead of N+1).
	moduleNames, err := p.preloadModuleNames(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("preload module names: %w", err)
	}

	result := make(map[string][]string)
	for _, e := range entries {
		// Try parent type first (for fields within module root types).
		moduleName := moduleNames[e.parentType]
		if moduleName == "" {
			// For module entry fields on Query/Mutation: resolve via field's target type.
			targetType, err := schema.UnmarshalType(e.fieldType)
			if err == nil && targetType != nil {
				moduleName = moduleNames[targetType.Name()]
			}
		}
		if moduleName != "" {
			result[moduleName] = append(result[moduleName], e.catalogs...)
		}
	}
	return result, nil
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
// Uses a single LEFT JOIN query instead of N+1 individual queries per type.
func (p *Provider) collectDataObjects(ctx context.Context, conn *Connection, catalogName string) ([]dataObjectInfo, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT t.name,
		        CASE WHEN f.name IS NOT NULL THEN t.name || '_filter' END,
		        CASE WHEN a.name IS NOT NULL THEN t.name || '_args' END
		 FROM %s t
		 LEFT JOIN %s f ON f.name = t.name || '_filter'
		 LEFT JOIN %s a ON a.name = t.name || '_args'
		 WHERE t.catalog = $1
		   AND t.hugr_type IN ('Table', 'View', 'ParameterizedView')`,
		p.table("_schema_types"),
		p.table("_schema_types"),
		p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []dataObjectInfo
	for rows.Next() {
		var name string
		var filterType, argsType *string
		if err := rows.Scan(&name, &filterType, &argsType); err != nil {
			continue
		}
		do := dataObjectInfo{name: name}
		if filterType != nil {
			do.filterType = *filterType
		}
		if argsType != nil {
			do.argsType = *argsType
		}
		objects = append(objects, do)
	}
	return objects, nil
}

// batchInsertModuleTypeCatalogs inserts (module_name, type_name, catalog_name) triples
// into _schema_module_type_catalogs in a single multi-row INSERT.
func (p *Provider) batchInsertModuleTypeCatalogs(ctx context.Context, conn *Connection, triples [][3]string) error {
	if len(triples) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		`INSERT INTO %s (module_name, type_name, catalog_name) VALUES `,
		p.table("_schema_module_type_catalogs"),
	))
	args := make([]any, 0, len(triples)*3)
	for i, t := range triples {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d)", i*3+1, i*3+2, i*3+3))
		args = append(args, t[0], t[1], t[2])
	}
	sb.WriteString(" ON CONFLICT (type_name, catalog_name) DO NOTHING")

	_, err := p.execWrite(ctx, conn, sb.String(), args...)
	return err
}

// reconcileDataObjectQueries collects query fields (select, select_one, aggregate, bucket_agg)
// from module root types and system types (Query, Mutation), and populates _schema_data_object_queries.
func (p *Provider) reconcileDataObjectQueries(ctx context.Context, conn *Connection, catalogName string) error {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT f.name, f.field_type, f.type_name, f.hugr_type
		 FROM %s f
		 JOIN %s t ON t.name = f.type_name
		 WHERE f.catalog = $1
		   AND f.hugr_type IN ('select', 'select_one', 'aggregate', 'bucket_agg')
		   AND (t.hugr_type = 'module' OR t.name IN ('Query', 'Mutation'))`,
		p.table("_schema_fields"), p.table("_schema_types"),
	), catalogName)
	if err != nil {
		return err
	}
	defer rows.Close()

	type doqEntry struct {
		name      string // field name (query name)
		object    string // resolved data object type name
		queryRoot string // type name of the root type containing this field
		queryType string // hugr_type (select, select_one, etc.)
	}
	var entries []doqEntry

	for rows.Next() {
		var fieldName, fieldTypeStr, queryRoot, hugrType string
		if err := rows.Scan(&fieldName, &fieldTypeStr, &queryRoot, &hugrType); err != nil {
			continue
		}
		// Parse field type to get the base type name (unwrapping [Type!]!)
		ft, err := schema.UnmarshalType(fieldTypeStr)
		if err != nil || ft == nil {
			continue
		}
		objectName := ft.Name()
		if objectName == "" {
			continue
		}
		entries = append(entries, doqEntry{
			name:      fieldName,
			object:    objectName,
			queryRoot: queryRoot,
			queryType: hugrType,
		})
	}

	if len(entries) == 0 {
		return nil
	}

	// Batch INSERT all entries
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		`INSERT INTO %s (name, object_name, query_root, query_type) VALUES `,
		p.table("_schema_data_object_queries"),
	))
	args := make([]any, 0, len(entries)*4)
	for i, e := range entries {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4))
		args = append(args, e.name, e.object, e.queryRoot, e.queryType)
	}
	sb.WriteString(" ON CONFLICT (name, object_name) DO UPDATE SET query_root=EXCLUDED.query_root, query_type=EXCLUDED.query_type")

	_, err = p.execWrite(ctx, conn, sb.String(), args...)
	return err
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
