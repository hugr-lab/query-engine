package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructDataObject rebuilds the BASE (pre-generate) object definition from
// catalog.* — the logical-model "source" that the generation rules compile. The
// directive↔bag pair tables (pairs_object.go / pairs_field.go) re-attach every
// stored directive; on top of them:
//   - @catalog(name, engine) — from the row's data_source + data_source_meta
//     (object level always; field level for virtual and foreign-owned fields,
//     mirroring the compiler);
//   - @references — re-emitted from the object's physical relation rows
//     (relations.go); the m2m/back projections stay generated, never attached.
//
// Generated fields (nav, aggregation, filters) are NOT added here — they are
// produced per name by the generation layer. All reads are activity-gated.
func reconstructDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	row, ok := s.readDataObject(ctx, name)
	if !ok {
		return nil
	}
	engines := s.activeEngines(ctx)
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        row.Name,
		Description: row.Description,
		Directives:  emitObjectDirectives(row),
		Position:    reconPos,
	}
	def.Directives = append(def.Directives, catalogDirective(row.DataSource, engines[row.DataSource]))
	for _, r := range s.relationsBySource(ctx, name) {
		def.Directives = append(def.Directives, referencesDirective(r))
	}
	for _, f := range s.readFields(ctx, name) {
		fd := reconstructField(f)
		if f.DataSource != row.DataSource ||
			f.Properties.FunctionCall != nil || f.Properties.TableFunctionCallJoin != nil {
			fd.Directives = append(fd.Directives, catalogDirective(f.DataSource, engines[f.DataSource]))
		}
		def.Fields = append(def.Fields, fd)
	}
	return def
}

// reconstructField rebuilds a base field definition via the field pair table.
func reconstructField(f *field) *ast.FieldDefinition {
	return &ast.FieldDefinition{
		Name:        f.Name,
		Type:        parseFieldType(f.FieldType),
		Description: f.Description,
		Directives:  emitFieldDirectives(f),
		Position:    reconPos,
	}
}

// catalogDirective rebuilds @catalog(name, engine) — name IS the data source;
// the engine comes from data_source_meta (activeEngines).
func catalogDirective(dataSource, engine string) *ast.Directive {
	return directive(base.CatalogDirectiveName,
		strArg(base.ArgName, dataSource), strArg(argEngine, engine))
}

// referencesDirective rebuilds an object-level @references from a stored
// relation row (values are normalized at collect time — no defaults here).
func referencesDirective(r *relation) *ast.Directive {
	args := []*ast.Argument{
		strArg(base.ArgName, r.Name),
		strArg(base.ArgReferencesName, r.Destination),
		strListArg(base.ArgSourceFields, r.SourceKeys),
		strListArg(base.ArgReferencesFields, r.DestinationKeys),
		strArg(base.ArgQuery, r.SourceField),
		strArg(base.ArgReferencesQuery, r.DestinationField),
	}
	if r.SourceFieldDescription != "" {
		args = append(args, strArg(base.ArgDescription, r.SourceFieldDescription))
	}
	if r.DestinationFieldDescription != "" {
		args = append(args, strArg(base.ArgReferencesDescription, r.DestinationFieldDescription))
	}
	return directive(base.ReferencesDirectiveName, args...)
}

// activeEngines maps ACTIVE data sources to their engine type strings.
func (s *Store) activeEngines(ctx context.Context) map[string]string {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source, engine FROM core.catalog.data_source_meta
		WHERE loaded AND NOT disabled AND NOT suspended`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var source, engine string
		if err := rows.Scan(&source, &engine); err != nil {
			return out
		}
		out[source] = engine
	}
	return out
}

// --- catalog.* readers (fill the shared entity models) ---

func (s *Store) readDataObject(ctx context.Context, name string) (*dataObject, bool) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	r := dataObject{Properties: &dataObjectProperties{}}
	var props, desc sql.NullString
	err = conn.QueryRow(ctx, `SELECT o.name, o.original_name, o.data_source, o.module, o.kind,
		o.properties::JSON::VARCHAR, o.description
		FROM core.catalog.data_objects o`+activeMeta("m", "o.data_source")+`
		WHERE o.name = `+lit(name)).
		Scan(&r.Name, &r.OriginalName, &r.DataSource, &r.Module, &r.Kind, &props, &desc)
	if err != nil {
		return nil, false
	}
	r.Description = desc.String
	if props.Valid {
		_ = json.Unmarshal([]byte(props.String), r.Properties)
	}
	return &r, true
}

func (s *Store) readFields(ctx context.Context, typeName string) []*field {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT f.name, f.field_type, f.properties::JSON::VARCHAR,
		f.data_source, f.is_pk, f.ordinal, f.deprecation_reason, f.description
		FROM core.catalog.fields f`+activeMeta("m", "f.data_source")+`
		WHERE f.type_name = `+lit(typeName)+` ORDER BY f.ordinal, f.name`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*field
	for rows.Next() {
		f := field{TypeName: typeName, Properties: &fieldProperties{}}
		var props, deprecated, desc sql.NullString
		if err := rows.Scan(&f.Name, &f.FieldType, &props, &f.DataSource, &f.IsPK, &f.Ordinal, &deprecated, &desc); err != nil {
			return out
		}
		f.DeprecationReason = deprecated.String
		f.Description = desc.String
		if props.Valid {
			_ = json.Unmarshal([]byte(props.String), f.Properties)
		}
		out = append(out, &f)
	}
	return out
}
