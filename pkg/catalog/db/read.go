package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"log"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/db/schema"
	dbpkg "github.com/hugr-lab/query-engine/pkg/db"
)

// ForName returns the type definition with the given name.
// Checks LRU cache first, falls back to DB query.
// Returns nil if the type doesn't exist or its catalog is disabled/suspended.
func (p *Provider) ForName(ctx context.Context, name string) *ast.Definition {
	if def := p.cache.getType(name); def != nil {
		return def
	}

	d, err, _ := p.sf.Do("DEF:"+name, func() (interface{}, error) {
		if def := p.cache.getType(name); def != nil {
			return def, nil
		}
		conn, err := p.pool.Conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		return p.forName(ctx, conn, name), nil
	})
	if err != nil {
		log.Printf("[LOAD TYPE]: %s", err.Error())
		return nil
	}
	if d == nil {
		return nil
	}
	return d.(*ast.Definition)
}

func (p *Provider) forName(ctx context.Context, conn *dbpkg.Connection, name string) *ast.Definition {
	if def := p.cache.getType(name); def != nil {
		return def
	}

	def, catalog, err := p.loadTypeFromDB(ctx, conn, name)
	if err != nil {
		log.Printf("[LOAD TYPE] loadTypeFromDB(%q): %v", name, err)
		return nil
	}
	if def == nil {
		return nil
	}

	// Populate cache
	p.cache.putType(name, catalog, def)
	return def
}

// DirectiveForName returns the directive definition with the given name.
func (p *Provider) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	// Check cache
	if dir := p.cache.getDirective(name); dir != nil {
		return dir
	}

	d, err, _ := p.sf.Do("DIR:"+name, func() (interface{}, error) {
		if dir := p.cache.getDirective(name); dir != nil {
			return dir, nil
		}
		dir, err := p.loadDirectiveFromDB(ctx, name)
		if err != nil {
			return nil, err
		}
		if dir != nil {
			p.cache.putDirective(name, dir)
		}
		return dir, nil
	})
	if err != nil {
		log.Printf("ERR: DirectiveForName(%q): %v", name, err)
		return nil
	}

	return d.(*ast.DirectiveDefinition)
}

// Definitions returns an iterator over all type definitions.
// Collects type names from DB first, then loads each definition
// via ForName (which uses cache + DB fallback).
func (p *Provider) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		// Collect names first to avoid holding a connection while
		// ForName/loadTypeFromDB acquires its own connection.
		conn, err := p.pool.Conn(ctx)
		if err != nil {
			log.Printf("[LOAD TYPE NAMES] open connection: %s", err.Error())
			return
		}
		defer conn.Close()
		for name := range p.collectActiveTypeNames(ctx) {
			def := p.forName(ctx, conn, name)
			if def != nil {
				if !yield(def) {
					return
				}
			}
		}
	}
}

// collectActiveTypeNames returns names of all types not in disabled/suspended catalogs.
func (p *Provider) collectActiveTypeNames(ctx context.Context) iter.Seq[string] {
	return func(yield func(string) bool) {
		conn, err := p.pool.Conn(ctx)
		if err != nil {
			log.Printf("[LOAD TYPE NAMES] open connection: %s", err.Error())
			return
		}
		defer conn.Close()

		rows, err := conn.Query(ctx, fmt.Sprintf(
			`SELECT t.name FROM %s t
		 WHERE
		   CASE
		     WHEN t.catalog = '%s' THEN true
		     WHEN t.hugr_type = 'module' THEN
		       EXISTS (
		         SELECT 1 FROM %s mtc
		         INNER JOIN %s c ON mtc.catalog_name = c.name
		         WHERE mtc.type_name = t.name
		           AND c.disabled = false AND c.suspended = false
		       )
		     ELSE
		       t.catalog IS NULL OR t.catalog = ''
		       OR NOT EXISTS (
		         SELECT 1 FROM %s c
		         WHERE c.name = t.catalog AND (c.disabled = true OR c.suspended = true)
		       )
		   END
		ORDER BY t.name;`,
			p.table("_schema_types"),
			SystemCatalogName,
			p.table("_schema_module_type_catalogs"), p.table("_schema_catalogs"),
			p.table("_schema_catalogs"),
		))
		if err != nil {
			log.Printf("[LOAD TYPE NAMES] query error: %s", err.Error())
			return
		}
		defer rows.Close()

		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				continue
			}
			if !yield(name) {
				return
			}
		}
	}
}

// DirectiveDefinitions returns an iterator over all directive definitions.
func (p *Provider) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		conn, err := p.pool.Conn(ctx)
		if err != nil {
			return
		}
		defer conn.Close()

		rows, err := conn.Query(ctx, fmt.Sprintf(
			`SELECT name, description, locations, is_repeatable, arguments FROM %s`,
			p.table("_schema_directives"),
		))
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var name, desc, locs, argsJSON string
			var repeatable bool
			if err := rows.Scan(&name, &desc, &locs, &repeatable, &argsJSON); err != nil {
				return
			}
			dir := &ast.DirectiveDefinition{
				Name:         name,
				Description:  desc,
				IsRepeatable: repeatable,
			}
			if locs != "" {
				for _, loc := range strings.Split(locs, "|") {
					dir.Locations = append(dir.Locations, ast.DirectiveLocation(loc))
				}
			}
			if argsJSON != "" && argsJSON != "[]" {
				args, err := schema.UnmarshalArgumentDefinitions([]byte(argsJSON))
				if err == nil {
					dir.Arguments = args
				}
			}
			if !yield(name, dir) {
				return
			}
		}
	}
}

// QueryType returns the Query root type definition.
func (p *Provider) QueryType(ctx context.Context) *ast.Definition {
	p.ensureRoots(ctx)
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.queryType
}

// MutationType returns the Mutation root type definition.
func (p *Provider) MutationType(ctx context.Context) *ast.Definition {
	p.ensureRoots(ctx)
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mutationType
}

// SubscriptionType returns nil (subscriptions not supported).
func (p *Provider) SubscriptionType(_ context.Context) *ast.Definition {
	return nil
}

// PossibleTypes returns implementations of an interface or union type.
func (p *Provider) PossibleTypes(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		def := p.ForName(ctx, name)
		if def == nil {
			return
		}

		// Check implements cache first
		if cached, ok := p.cache.getImplements(name); ok {
			for _, implName := range cached {
				implDef := p.ForName(ctx, implName)
				if implDef != nil {
					if !yield(implDef) {
						return
					}
				}
			}
			return
		}

		var implementors []string

		switch def.Kind {
		case ast.Interface:
			// Collect implementor names in a separate scope so the connection
			// is released before we call ForName below.
			implNames, err := p.findInterfaceImplementors(ctx, name)
			if err != nil {
				return
			}
			implementors = implNames

		case ast.Union:
			// Union types: members are in the Types field
			implementors = append(implementors, def.Types...)
		}

		// Cache the result
		p.cache.putImplements(name, implementors)

		// Yield definitions
		for _, implName := range implementors {
			implDef := p.ForName(ctx, implName)
			if implDef != nil {
				if !yield(implDef) {
					return
				}
			}
		}
	}
}

// Implements returns interfaces that the named type implements.
func (p *Provider) Implements(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		def := p.ForName(ctx, name)
		if def == nil {
			return
		}
		for _, iface := range def.Interfaces {
			ifaceDef := p.ForName(ctx, iface)
			if ifaceDef != nil {
				if !yield(ifaceDef) {
					return
				}
			}
		}
	}
}

// Types returns an iterator over all type (name, definition) pairs.
func (p *Provider) Types(ctx context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for def := range p.Definitions(ctx) {
			if !yield(def.Name, def) {
				return
			}
		}
	}
}

// findInterfaceImplementors returns type names that implement the given interface.
func (p *Provider) findInterfaceImplementors(ctx context.Context, ifaceName string) ([]string, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT t.name, t.interfaces FROM %s t
		 WHERE t.kind = 'OBJECT' AND t.interfaces != ''
		   AND (t.catalog IS NULL OR t.catalog = ''
		     OR NOT EXISTS (SELECT 1 FROM %s c WHERE c.name = t.catalog AND (c.disabled = true OR c.suspended = true)))`,
		p.table("_schema_types"), p.table("_schema_catalogs"),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var implementors []string
	for rows.Next() {
		var typeName, ifaces string
		if err := rows.Scan(&typeName, &ifaces); err != nil {
			continue
		}
		for _, iface := range strings.Split(ifaces, "|") {
			if iface == ifaceName {
				implementors = append(implementors, typeName)
				break
			}
		}
	}
	return implementors, nil
}

// ensureRoots loads the Query and Mutation root type pointers on first access.
func (p *Provider) ensureRoots(ctx context.Context) {
	p.mu.RLock()
	if p.rootsLoaded {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	// Load outside the lock to avoid holding the write lock during
	// potentially slow DB queries.
	q := p.ForName(ctx, base.QueryBaseName)
	m := p.ForName(ctx, base.MutationBaseName)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rootsLoaded {
		return // another goroutine loaded while we were querying
	}
	p.queryType = q
	p.mutationType = m
	p.rootsLoaded = true
}

// JSON record types for single-query type loading.
type typeDefJSON struct {
	Name        string         `json:"name"`
	Kind        string         `json:"kind"`
	Description string         `json:"description"`
	Catalog     *string        `json:"catalog"`
	Directives  string         `json:"directives"`
	Module      string         `json:"module"`
	Interfaces  string         `json:"interfaces"`
	UnionTypes  string         `json:"union_types"`
	HugrType    string         `json:"hugr_type"`
	Fields      []fieldDefJSON `json:"fields"`
	EnumValues  []enumValJSON  `json:"enum_values"`
}

type fieldDefJSON struct {
	Name        string       `json:"name"`
	FieldType   string       `json:"field_type"`
	Description string       `json:"description"`
	Directives  string       `json:"directives"`
	Arguments   []argDefJSON `json:"arguments"`
}

type argDefJSON struct {
	Name         string  `json:"name"`
	ArgType      string  `json:"arg_type"`
	DefaultValue *string `json:"default_value"`
	Description  string  `json:"description"`
	Directives   string  `json:"directives"`
}

type enumValJSON struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Directives  string `json:"directives"`
}

// loadTypeFromDB reconstructs a *ast.Definition from the database using a single
// CTE query that fetches the type, its fields (with arguments), and enum values.
// Returns (nil, "", nil) if the type doesn't exist or is in a disabled/suspended catalog.
//
// For DuckDB: uses struct literals + try_cast(... AS JSON) with LATERAL JOINs.
// For PostgreSQL: wraps the PG-native query in postgres_query('core', ...).
func (p *Provider) loadTypeFromDB(ctx context.Context, conn *dbpkg.Connection, name string) (*ast.Definition, string, error) {
	if name == "" {
		return nil, "", errors.New("[LOAD TYPE] query: name cannot be empty")
	}
	var q string
	if p.isPostgres {
		q = p.buildTypeQueryPG(name)
	} else {
		q = p.buildTypeQueryDuckDB(name)
	}

	var defJSON string
	err := conn.QueryRow(ctx, q).Scan(&defJSON)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("query: %w", err)
	}

	var rec typeDefJSON
	if err := json.Unmarshal([]byte(defJSON), &rec); err != nil {
		return nil, "", fmt.Errorf("unmarshal: %w", err)
	}

	def, catalog := rec.toDefinition()
	return def, catalog, nil
}

// toDefinition converts a JSON record to an ast.Definition and its catalog name.
func (rec *typeDefJSON) toDefinition() (*ast.Definition, string) {
	dirs, err := schema.UnmarshalDirectives([]byte(rec.Directives))
	if err != nil {
		return nil, ""
	}

	def := &ast.Definition{
		Kind:        ast.DefinitionKind(rec.Kind),
		Name:        rec.Name,
		Description: rec.Description,
		Directives:  dirs,
	}

	if rec.Interfaces != "" {
		def.Interfaces = strings.Split(rec.Interfaces, "|")
	}
	if rec.UnionTypes != "" {
		def.Types = strings.Split(rec.UnionTypes, "|")
	}

	for _, f := range rec.Fields {
		fd, err := f.toFieldDefinition()
		if err != nil {
			continue
		}
		def.Fields = append(def.Fields, fd)
	}

	for _, e := range rec.EnumValues {
		ev, err := e.toEnumValueDefinition()
		if err != nil {
			continue
		}
		def.EnumValues = append(def.EnumValues, ev)
	}

	var catalog string
	if rec.Catalog != nil {
		catalog = *rec.Catalog
	}
	return def, catalog
}

func (p *Provider) buildTypeQueryDuckDB(name string) string {
	name = strings.ReplaceAll(name, `'`, `''`) // escape single quotes for SQL string literal

	q := fmt.Sprintf(typeQueryDuckDB,
		p.table("_schema_types"),
		name,
		SystemCatalogName,
		p.table("_schema_module_type_catalogs"), p.table("_schema_catalogs"),
		p.table("_schema_arguments"),
		p.table("_schema_fields"),
		p.table("_schema_enum_values"),
	)
	return q
}

// buildTypeQueryPG constructs the single-query SQL for PostgreSQL CoreDB.
// The inner SQL uses jsonb_agg + jsonb_build_object, wrapped in
// postgres_query('core', ...) to push the query directly to PostgreSQL.
// The name parameter is inlined into the SQL because postgres_query()
// does not support parameterized queries ($1 inside the string literal
// would be sent literally to PostgreSQL without binding).
func (p *Provider) buildTypeQueryPG(name string) string {
	name = strings.ReplaceAll(name, `'`, `''`) // escape single quotes for SQL string literal
	q := fmt.Sprintf(typeQueryPG,
		"_schema_types",
		name,
		SystemCatalogName,
		"_schema_module_type_catalogs", "_schema_catalogs",
		"_schema_arguments",
		"_schema_fields",
		"_schema_enum_values",
	)
	// Escape remaining single quotes for the outer postgres_query wrapper
	return q
}

func (f *fieldDefJSON) toFieldDefinition() (*ast.FieldDefinition, error) {
	fieldType, err := schema.UnmarshalType(f.FieldType)
	if err != nil {
		return nil, err
	}
	dirs, err := schema.UnmarshalDirectives([]byte(f.Directives))
	if err != nil {
		return nil, err
	}

	fd := &ast.FieldDefinition{
		Name:        f.Name,
		Type:        fieldType,
		Description: f.Description,
		Directives:  dirs,
	}

	for _, a := range f.Arguments {
		ad, err := a.toArgumentDefinition()
		if err != nil {
			continue
		}
		fd.Arguments = append(fd.Arguments, ad)
	}

	return fd, nil
}

func (a *argDefJSON) toArgumentDefinition() (*ast.ArgumentDefinition, error) {
	argType, err := schema.UnmarshalType(a.ArgType)
	if err != nil {
		return nil, err
	}
	dirs, err := schema.UnmarshalDirectives([]byte(a.Directives))
	if err != nil {
		return nil, err
	}

	ad := &ast.ArgumentDefinition{
		Name:        a.Name,
		Type:        argType,
		Description: a.Description,
		Directives:  dirs,
	}

	if a.DefaultValue != nil && *a.DefaultValue != "" {
		val, err := schema.UnmarshalValue(json.RawMessage(*a.DefaultValue))
		if err != nil {
			ad.DefaultValue = &ast.Value{
				Raw:  *a.DefaultValue,
				Kind: ast.StringValue,
			}
		} else {
			ad.DefaultValue = val
		}
	}

	return ad, nil
}

func (e *enumValJSON) toEnumValueDefinition() (*ast.EnumValueDefinition, error) {
	dirs, err := schema.UnmarshalDirectives([]byte(e.Directives))
	if err != nil {
		return nil, err
	}
	return &ast.EnumValueDefinition{
		Name:        e.Name,
		Description: e.Description,
		Directives:  dirs,
	}, nil
}

// loadDirectiveFromDB loads a directive definition from the database.
// Returns (nil, nil) if not found.
func (p *Provider) loadDirectiveFromDB(ctx context.Context, name string) (*ast.DirectiveDefinition, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("conn: %w", err)
	}
	defer conn.Close()

	var desc, locs, argsJSON string
	var repeatable bool
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT description, locations, is_repeatable, arguments FROM %s WHERE name = $1`,
		p.table("_schema_directives"),
	), name).Scan(&desc, &locs, &repeatable, &argsJSON)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	dir := &ast.DirectiveDefinition{
		Name:         name,
		Description:  desc,
		IsRepeatable: repeatable,
	}
	if locs != "" {
		for _, loc := range strings.Split(locs, "|") {
			dir.Locations = append(dir.Locations, ast.DirectiveLocation(loc))
		}
	}
	if argsJSON != "" && argsJSON != "[]" {
		args, err := schema.UnmarshalArgumentDefinitions([]byte(argsJSON))
		if err != nil {
			return nil, fmt.Errorf("unmarshal directive arguments: %w", err)
		}
		dir.Arguments = args
	}
	return dir, nil
}

// Connection is an alias for the db package Connection type.
type Connection = dbpkg.Connection

// typeQueryDuckDB loads a type definition with fields, arguments, and enum values
// in a single query. Returns one row with a JSON object, or zero rows if type
// is not found or not visible. Uses DuckDB struct literals + try_cast(... AS JSON).
//
// Parameters: 10 table references via fmt.Sprintf:
//  1. _schema_types
//  2. type_name filter (string literal, may be empty for no filtering)
//  3. SystemCatalogName (string literal)
//  4. _schema_module_type_catalogs
//  5. _schema_catalogs
//  6. _schema_arguments
//  7. _schema_fields
//  8. _schema_enum_values
const typeQueryDuckDB = `
WITH disabled_catalogs AS (
	SELECT name FROM %[5]s WHERE disabled = true OR suspended = true
), enabled_mtc AS (
	SELECT mtc.type_name
	FROM %[4]s mtc
		INNER JOIN %[5]s c ON mtc.catalog_name = c.name
	WHERE c.disabled = false AND c.suspended = false
), type_info AS (
    SELECT
        t.name, t.kind, t.description, t.catalog, CAST(t.directives AS VARCHAR) AS directives,
        t.module, t.interfaces, t.union_types, t.hugr_type
    FROM %[1]s t
    WHERE t.name = '%[2]s' AND (
        t.catalog = '%[3]s' OR t.catalog = '' OR t.catalog IS NULL OR
        (t.hugr_type = 'module' AND EXISTS (
            SELECT 1 FROM enabled_mtc emtc WHERE emtc.type_name = t.name
        )) OR
        (t.hugr_type != 'module' AND NOT EXISTS (
            SELECT 1 FROM disabled_catalogs dc WHERE dc.name = t.catalog
        ))
    )
), field_args AS (
    SELECT a.type_name, a.field_name, a.name, a.arg_type, a.default_value, a.description,
           CAST(a.directives AS VARCHAR) AS directives, a.ordinal
    FROM %[6]s a
		INNER JOIN type_info ti ON a.type_name = ti.name
), fields_info AS (
    SELECT
        f.name, f.field_type, f.description, CAST(f.directives AS VARCHAR) AS directives,
        args.args, f.ordinal
    FROM %[7]s f
    	INNER JOIN type_info ti ON f.type_name = ti.name AND ti.kind IN ('OBJECT', 'INPUT_OBJECT', 'INTERFACE')
    LEFT JOIN LATERAL (
        SELECT array_agg({
            name: a.name, arg_type: a.arg_type, default_value: a.default_value,
            description: a.description, directives: a.directives
        } ORDER BY a.ordinal, a.name) AS args
        FROM field_args a
        WHERE a.field_name = f.name
    ) AS args ON TRUE
    WHERE (
        f.hugr_type = 'submodule' AND EXISTS (
            SELECT 1 FROM enabled_mtc emtc WHERE emtc.type_name = f.field_type
        )
    ) OR (
        f.hugr_type != 'submodule' AND (
            f.dependency_catalog IS NULL OR NOT EXISTS (
                SELECT 1 FROM disabled_catalogs dc  WHERE dc.name = f.dependency_catalog
            )
        )
    )
), enum_vals AS (
    SELECT e.name, e.description, CAST(e.directives AS VARCHAR) AS directives, e.ordinal
    FROM %[8]s e
    	INNER JOIN type_info ti ON e.type_name = ti.name
    WHERE ti.kind = 'ENUM'
)
SELECT CAST(try_cast({
    name: ti.name, kind: ti.kind, description: ti.description, catalog: ti.catalog,
    directives: ti.directives, module: ti.module, interfaces: ti.interfaces,
    union_types: ti.union_types, hugr_type: ti.hugr_type,
    fields: flds.info, enum_values: evs.vals
} AS JSON) AS VARCHAR) AS def
FROM type_info ti
	LEFT JOIN LATERAL (
		SELECT array_agg({
			name: f.name, field_type: f.field_type, description: f.description,
			directives: f.directives, arguments: f.args
		} ORDER BY f.ordinal, f.name) AS info
		FROM fields_info f
	) AS flds ON ti.kind IN ('OBJECT', 'INPUT_OBJECT', 'INTERFACE')
	LEFT JOIN LATERAL (
		SELECT array_agg({
			name: e.name, description: e.description, directives: e.directives
		} ORDER BY e.ordinal, e.name) AS vals
		FROM enum_vals e
	) AS evs ON ti.kind = 'ENUM'
`

// typeQueryPG is the PostgreSQL version of the type query, intended for use
// with postgres_query('core', '<sql>') when core DB is attached PostgreSQL.
// Parameters match typeQueryDuckDB.
const typeQueryPG = `
FROM postgres_query('core', $metaquery$
WITH disabled_catalogs AS MATERIALIZED(
	SELECT name FROM %[5]s WHERE disabled = true OR suspended = true
), enabled_mtc AS MATERIALIZED (
	SELECT mtc.type_name
	FROM %[4]s mtc
		INNER JOIN %[5]s c ON mtc.catalog_name = c.name
	WHERE c.disabled = false AND c.suspended = false
),type_info AS (
    SELECT
        t.name, t.kind, t.description, t.catalog, CAST(t.directives AS VARCHAR) AS directives,
        t.module, t.interfaces, t.union_types, t.hugr_type
    FROM %[1]s t
    WHERE t.name = '%[2]s' AND (
        t.catalog = '%[3]s' OR t.catalog = '' OR t.catalog IS NULL OR 
        (t.hugr_type = 'module' AND EXISTS (
            SELECT 1 FROM enabled_mtc emtc WHERE emtc.type_name = t.name
        )) OR
        (t.hugr_type != 'module' AND NOT EXISTS (
            SELECT 1 FROM disabled_catalogs dc WHERE dc.name = t.catalog
        ))
    )
), field_args AS (
    SELECT a.field_name, a.name, a.arg_type, a.default_value, a.description,
           CAST(a.directives AS VARCHAR) AS directives, a.ordinal
    FROM %[6]s a, type_info ti
    WHERE a.type_name = ti.name
), fields_info AS (
    SELECT
        f.name, f.field_type, f.description, CAST(f.directives AS VARCHAR) AS directives,
        args.args, f.ordinal
    FROM %[7]s f
    JOIN type_info ti ON f.type_name = ti.name AND ti.kind IN ('OBJECT', 'INPUT_OBJECT', 'INTERFACE')
		LEFT JOIN LATERAL (
			SELECT array_agg(jsonb_build_object(
				'name', a.name, 'arg_type', a.arg_type, 'default_value', a.default_value,
				'description', a.description, 'directives', a.directives
			) ORDER BY a.ordinal, a.name) AS args
			FROM field_args a
			WHERE a.field_name = f.name
		) AS args ON TRUE
    WHERE (
        f.hugr_type = 'submodule' AND EXISTS (
            SELECT 1 FROM enabled_mtc emtc WHERE emtc.type_name = f.field_type
        )
    ) OR (
        f.hugr_type != 'submodule' AND (
            f.dependency_catalog IS NULL OR NOT EXISTS (
                SELECT 1 FROM disabled_catalogs dc WHERE dc.name = f.dependency_catalog
            )
        )
    )
), enum_vals AS (
    SELECT e.name, e.description, CAST(e.directives AS VARCHAR) AS directives, e.ordinal
    FROM %[8]s e
    	INNER JOIN type_info ti ON e.type_name = ti.name AND ti.kind = 'ENUM'
)
SELECT jsonb_build_object(
    'name', ti.name, 'kind', ti.kind, 'description', ti.description, 'catalog', ti.catalog,
    'directives', ti.directives, 'module', ti.module, 'interfaces', ti.interfaces,
    'union_types', ti.union_types, 'hugr_type', ti.hugr_type,
    'fields', flds.info, 'enum_values', evs.vals
) AS def
FROM type_info ti
	LEFT JOIN LATERAL (
		SELECT array_agg(jsonb_build_object(
			'name', f.name, 'field_type', f.field_type, 'description', f.description,
			'directives', f.directives, 'arguments', f.args
		) ORDER BY f.ordinal, f.name) AS info
		FROM fields_info f
	) AS flds ON ti.kind IN ('OBJECT', 'INPUT_OBJECT', 'INTERFACE')
	LEFT JOIN LATERAL (
		SELECT array_agg(jsonb_build_object(
			'name', e.name, 'description', e.description, 'directives', e.directives
		) ORDER BY e.ordinal, e.name) AS vals
		FROM enum_vals e
	) AS evs ON ti.kind = 'ENUM'
$metaquery$)
`
