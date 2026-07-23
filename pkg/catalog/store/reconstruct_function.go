package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructFunction rebuilds a stored function / mutation / subscription as
// the root-field definition the module-root synthesis places on its module
// root. Directives come from the function pair table (pairs_function.go),
// arguments from emitFunctionArgs (@arg_default / @deprecated re-attached);
// @catalog(name, engine) is attached by the root rules (gen_roots.go).
func reconstructFunction(ctx context.Context, s *Store, module, name string) *ast.FieldDefinition {
	row, ok := s.readFunction(ctx, module, name)
	if !ok {
		return nil
	}
	return functionField(row)
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

func (s *Store) readFunction(ctx context.Context, module, name string) (*function, bool) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		readErr("function", err)
		return nil, false
	}
	defer conn.Close()
	row, err := scanFunction(conn.QueryRow(ctx, `SELECT f.module, f.name, f.kind, f.data_source, f.returns, f.is_table,
		f.args::JSON::VARCHAR, f.properties::JSON::VARCHAR, f.deprecation_reason, f.description
		FROM core.catalog.functions f`+activeMeta("m", "f.data_source")+`
		WHERE f.module = `+lit(module)+` AND f.name = `+lit(name)).Scan)
	if err != nil {
		if err != sql.ErrNoRows {
			readErr("function", err)
		}
		return nil, false
	}
	return row, true
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
