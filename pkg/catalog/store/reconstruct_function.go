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
// arguments from emitFunctionArgs (@arg_default / @deprecated re-attached).
//
// Still to come (generation layer): @catalog(name, engine) from
// data_source_meta.
func reconstructFunction(ctx context.Context, s *Store, module, name string) *ast.FieldDefinition {
	row, ok := s.readFunction(ctx, module, name)
	if !ok {
		return nil
	}
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
		return nil, false
	}
	defer conn.Close()
	r := function{Properties: &functionProperties{}}
	var args, props, deprecated, desc sql.NullString
	err = conn.QueryRow(ctx, `SELECT module, name, kind, data_source, returns, is_table,
		args::JSON::VARCHAR, properties::JSON::VARCHAR, deprecation_reason, description
		FROM core.catalog.functions WHERE module = `+lit(module)+` AND name = `+lit(name)).
		Scan(&r.Module, &r.Name, &r.Kind, &r.DataSource, &r.Returns, &r.IsTable, &args, &props, &deprecated, &desc)
	if err != nil {
		return nil, false
	}
	r.DeprecationReason = deprecated.String
	r.Description = desc.String
	if args.Valid {
		_ = json.Unmarshal([]byte(args.String), &r.Args)
	}
	if props.Valid {
		_ = json.Unmarshal([]byte(props.String), r.Properties)
	}
	return &r, true
}
