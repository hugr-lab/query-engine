package store

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructDataObject rebuilds the BASE (pre-generate) object definition from
// catalog.* — the logical-model "source" that the generation rules compile. The
// directive↔bag pair tables (pairs_object.go / pairs_field.go) re-attach every
// stored directive; generated fields (nav, aggregation, filters) are NOT added
// here — they are produced per name by the generation layer.
//
// Still to come (logical model): relations → @references / @field_references
// re-emission, @catalog(name, engine) from data_source_meta.
func reconstructDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	row, ok := s.readDataObject(ctx, name)
	if !ok {
		return nil
	}
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        row.Name,
		Description: row.Description,
		Directives:  emitObjectDirectives(row),
		Position:    reconPos,
	}
	for _, f := range s.readFields(ctx, name) {
		def.Fields = append(def.Fields, reconstructField(f))
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

// --- catalog.* readers (fill the shared entity models) ---

func (s *Store) readDataObject(ctx context.Context, name string) (*dataObject, bool) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	r := dataObject{Properties: &dataObjectProperties{}}
	var props, desc sql.NullString
	err = conn.QueryRow(ctx, `SELECT name, original_name, data_source, module, kind,
		properties::JSON::VARCHAR, description
		FROM core.catalog.data_objects WHERE name = `+lit(name)).
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
	rows, err := conn.Query(ctx, `SELECT name, field_type, properties::JSON::VARCHAR,
		data_source, is_pk, ordinal, deprecation_reason, description
		FROM core.catalog.fields WHERE type_name = `+lit(typeName)+` ORDER BY ordinal, name`)
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
