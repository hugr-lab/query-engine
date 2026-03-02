package db

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
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
	// Check cache first
	if def := p.cache.getType(name); def != nil {
		return def
	}

	// Load from DB
	def, catalog := p.loadTypeFromDB(ctx, name)
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

	// Load from DB
	dir := p.loadDirectiveFromDB(ctx, name)
	if dir != nil {
		p.cache.putDirective(name, dir)
	}
	return dir
}

// Definitions returns an iterator over all type definitions.
// Collects type names from DB first, then loads each definition
// via ForName (which uses cache + DB fallback).
func (p *Provider) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		// Collect names first to avoid holding a connection while
		// ForName/loadTypeFromDB acquires its own connection.
		names, err := p.collectActiveTypeNames(ctx)
		if err != nil {
			return
		}
		for _, name := range names {
			def := p.ForName(ctx, name)
			if def != nil {
				if !yield(def) {
					return
				}
			}
		}
	}
}

// collectActiveTypeNames returns names of all types not in disabled/suspended catalogs.
func (p *Provider) collectActiveTypeNames(ctx context.Context) ([]string, error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT t.name FROM %s t
		 WHERE t.catalog IS NULL OR t.catalog = ''
		    OR t.catalog NOT IN (SELECT name FROM %s WHERE disabled = true OR suspended = true)`,
		p.table("_schema_types"), p.table("_schema_catalogs"),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		names = append(names, name)
	}
	return names, nil
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
		   AND (t.catalog IS NULL OR t.catalog NOT IN
		     (SELECT name FROM %s WHERE disabled = true OR suspended = true))`,
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

// loadTypeFromDB reconstructs a *ast.Definition from the database.
// Returns (nil, "") if the type doesn't exist or is in a disabled/suspended catalog.
func (p *Provider) loadTypeFromDB(ctx context.Context, name string) (*ast.Definition, string) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, ""
	}
	defer conn.Close()

	// Load type record
	var kind, desc, catalog, dirJSON, module, ifaces, unionTypes string
	var catalogPtr *string
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT kind, description, catalog, CAST(directives AS VARCHAR), module, interfaces, union_types FROM %s WHERE name = $1`,
		p.table("_schema_types"),
	), name).Scan(&kind, &desc, &catalogPtr, &dirJSON, &module, &ifaces, &unionTypes)
	if err != nil {
		return nil, ""
	}
	if catalogPtr != nil {
		catalog = *catalogPtr
	}

	// Check if catalog is disabled or suspended
	if catalog != "" {
		var disabled, suspended bool
		err = conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT disabled, suspended FROM %s WHERE name = $1`,
			p.table("_schema_catalogs"),
		), catalog).Scan(&disabled, &suspended)
		if err == nil && (disabled || suspended) {
			return nil, ""
		}
	}

	// Parse directives
	dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
	if err != nil {
		return nil, ""
	}

	def := &ast.Definition{
		Kind:        ast.DefinitionKind(kind),
		Name:        name,
		Description: desc,
		Directives:  dirs,
	}

	// Load interfaces and union types from stored columns
	if ifaces != "" {
		def.Interfaces = strings.Split(ifaces, "|")
	}
	if unionTypes != "" {
		def.Types = strings.Split(unionTypes, "|")
	}

	// Load fields (excluding fields from disabled/suspended dependency catalogs)
	def.Fields = p.loadFieldsFromDB(ctx, conn, name)

	// Load enum values
	def.EnumValues = p.loadEnumValuesFromDB(ctx, conn, name)

	return def, catalog
}

// loadFieldsFromDB loads all active fields for a type.
func (p *Provider) loadFieldsFromDB(ctx context.Context, conn *Connection, typeName string) ast.FieldList {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT f.name, f.field_type, f.description, CAST(f.directives AS VARCHAR)
		 FROM %s f
		 WHERE f.type_name = $1
		   AND (f.dependency_catalog IS NULL
		     OR f.dependency_catalog NOT IN
		       (SELECT name FROM %s WHERE disabled = true OR suspended = true))
		 ORDER BY f.ordinal, f.name`,
		p.table("_schema_fields"), p.table("_schema_catalogs"),
	), typeName)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var fields ast.FieldList
	for rows.Next() {
		var name, fieldTypeStr, desc, dirJSON string
		if err := rows.Scan(&name, &fieldTypeStr, &desc, &dirJSON); err != nil {
			continue
		}

		fieldType, err := schema.UnmarshalType(fieldTypeStr)
		if err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}

		field := &ast.FieldDefinition{
			Name:        name,
			Type:        fieldType,
			Description: desc,
			Directives:  dirs,
		}

		// Load arguments for this field
		field.Arguments = p.loadArgumentsFromDB(ctx, conn, typeName, name)

		fields = append(fields, field)
	}
	return fields
}

// loadArgumentsFromDB loads all arguments for a field.
func (p *Provider) loadArgumentsFromDB(ctx context.Context, conn *Connection, typeName, fieldName string) ast.ArgumentDefinitionList {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, arg_type, default_value, description, CAST(directives AS VARCHAR)
		 FROM %s
		 WHERE type_name = $1 AND field_name = $2
		 ORDER BY ordinal, name`,
		p.table("_schema_arguments"),
	), typeName, fieldName)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var args ast.ArgumentDefinitionList
	for rows.Next() {
		var name, argTypeStr, desc, dirJSON string
		var defaultVal sql.NullString
		if err := rows.Scan(&name, &argTypeStr, &defaultVal, &desc, &dirJSON); err != nil {
			continue
		}

		argType, err := schema.UnmarshalType(argTypeStr)
		if err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}

		arg := &ast.ArgumentDefinition{
			Name:        name,
			Type:        argType,
			Description: desc,
			Directives:  dirs,
		}

		if defaultVal.Valid && defaultVal.String != "" {
			arg.DefaultValue = &ast.Value{
				Raw:  defaultVal.String,
				Kind: ast.StringValue,
			}
		}

		args = append(args, arg)
	}
	return args
}

// loadEnumValuesFromDB loads all enum values for a type.
func (p *Provider) loadEnumValuesFromDB(ctx context.Context, conn *Connection, typeName string) ast.EnumValueList {
	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT name, description, CAST(directives AS VARCHAR) FROM %s WHERE type_name = $1 ORDER BY ordinal, name`,
		p.table("_schema_enum_values"),
	), typeName)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var evs ast.EnumValueList
	for rows.Next() {
		var name, desc, dirJSON string
		if err := rows.Scan(&name, &desc, &dirJSON); err != nil {
			continue
		}
		dirs, err := schema.UnmarshalDirectives([]byte(dirJSON))
		if err != nil {
			continue
		}
		evs = append(evs, &ast.EnumValueDefinition{
			Name:        name,
			Description: desc,
			Directives:  dirs,
		})
	}
	return evs
}

// loadDirectiveFromDB loads a directive definition from the database.
func (p *Provider) loadDirectiveFromDB(ctx context.Context, name string) *ast.DirectiveDefinition {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()

	var desc, locs, argsJSON string
	var repeatable bool
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT description, locations, is_repeatable, arguments FROM %s WHERE name = $1`,
		p.table("_schema_directives"),
	), name).Scan(&desc, &locs, &repeatable, &argsJSON)
	if err != nil {
		return nil
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
	return dir
}

// Connection is an alias for the db package Connection type.
type Connection = dbpkg.Connection
