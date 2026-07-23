package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructFunction rebuilds a stored function / mutation / subscription as
// the root-field definition the module-root synthesis places on its module
// root. Directives come from the function pair table (pairs_function.go),
// arguments from emitFunctionArgs (@arg_default / @deprecated re-attached);
// @catalog(name, engine) is attached by the root rules (gen_roots.go).
func reconstructFunction(ctx context.Context, g *genContext, module, name string) (*ast.FieldDefinition, error) {
	row, err := g.readFunction(ctx, module, name)
	if err != nil || row == nil {
		return nil, err
	}
	return functionField(row), nil
}

// functionField builds the root-field definition from a function row.
func functionField(row *function) *ast.FieldDefinition {
	return &ast.FieldDefinition{
		Name:        row.Name,
		Type:        parseFieldType(row.Returns),
		Description: row.Description,
		Arguments:   emitFunctionArgs(row),
		Directives:  emitFunctionDirectives(row),
		Position:    reconPos,
	}
}

// readFunction reads one function row by its (module, name) primary key;
// absent = (nil, nil).
func (g *genContext) readFunction(ctx context.Context, module, name string) (*function, error) {
	key := pkKey(module, name)
	if row, ok := g.function[key]; ok {
		return row, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read function %s.%s: %w", module, name, err)
	}
	defer conn.Close()
	row, err := scanFunction(conn.QueryRow(ctx, `SELECT f.module, f.name, f.kind, f.data_source, f.returns, f.is_table,
		f.args::JSON::VARCHAR, f.properties::JSON::VARCHAR, f.deprecation_reason, f.description
		FROM core.catalog.functions f`+activeMeta("m", "f.data_source")+`
		WHERE f.module = `+lit(module)+` AND f.name = `+lit(name)).Scan)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("read function %s.%s: %w", module, name, err)
		}
		if g.function == nil {
			g.function = map[string]*function{}
		}
		g.function[key] = nil
		return nil, nil
	}
	if g.function == nil {
		g.function = map[string]*function{}
	}
	g.function[key] = row
	g.touch(row.DataSource)
	return row, nil
}

// scanFunction fills a function row from the canonical column list (module,
// name, kind, data_source, returns, is_table, args, properties,
// deprecation_reason, description).
func scanFunction(scan func(...any) error) (*function, error) {
	r := function{Properties: &functionProperties{}}
	var args, props, deprecated, desc sql.NullString
	err := scan(&r.Module, &r.Name, &r.Kind, &r.DataSource, &r.Returns, &r.IsTable, &args, &props, &deprecated, &desc)
	if err != nil {
		return nil, err
	}
	r.DeprecationReason = deprecated.String
	r.Description = desc.String
	if args.Valid {
		_ = json.Unmarshal([]byte(args.String), &r.Args)
	}
	if props.Valid {
		_ = json.Unmarshal([]byte(props.String), r.Properties)
	}
	return &r, nil
}
