package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructDataObject rebuilds the BASE (pre-generate) object definition from
// catalog.* — the logical-model "source" that the generation rules compile. It
// reattaches the directives decomposed into the property bags (@original_name,
// @module, @table/@view, @field_source, @pk, …). Generated fields (nav,
// aggregation, filters) are NOT added here — they are produced per name.
//
// M3 (base slice): object shell + core directives + base fields. Field-level
// bag directives (@sql/@default/@join/@function_call) and relations follow.
func reconstructDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	row, ok := s.readDataObject(ctx, name)
	if !ok {
		return nil
	}
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        row.Name,
		Description: row.Description,
		Position:    reconPos,
	}

	def.Directives = append(def.Directives, directive("original_name", strArg("name", row.OriginalName)))
	if row.Module != "" {
		def.Directives = append(def.Directives, directive("module", strArg("name", row.Module)))
	}
	def.Directives = append(def.Directives, objectKindDirective(row.Kind, row.Properties))

	for _, f := range s.readFields(ctx, name) {
		def.Fields = append(def.Fields, reconstructField(f))
	}
	return def
}

// objectKindDirective rebuilds @table / @view from the property bag.
func objectKindDirective(kind string, p *dataObjectProperties) *ast.Directive {
	var args []*ast.Argument
	if p != nil && p.Name != "" {
		args = append(args, strArg("name", p.Name))
	}
	switch kind {
	case "view":
		if p != nil && p.SQL != "" {
			args = append(args, strArg("sql", p.SQL))
		}
		return directive("view", args...)
	default: // table
		if p != nil && p.IsM2M {
			args = append(args, boolArg("is_m2m", true))
		}
		return directive("table", args...)
	}
}

// reconstructField rebuilds a base field definition (core directives only).
func reconstructField(f *fieldRow) *ast.FieldDefinition {
	fd := &ast.FieldDefinition{
		Name:        f.Name,
		Type:        parseFieldType(f.FieldType),
		Description: f.Description,
		Position:    reconPos,
	}
	if f.IsPK {
		fd.Directives = append(fd.Directives, directive("pk"))
	}
	if f.Properties != nil && f.Properties.Source != "" {
		fd.Directives = append(fd.Directives, directive("field_source", strArg("field", f.Properties.Source)))
	}
	if f.DeprecationReason != "" {
		fd.Directives = append(fd.Directives, directive("deprecated", strArg("reason", f.DeprecationReason)))
	}
	return fd
}

// --- catalog.* readers ---

// dataObjectRow is a data object read back from storage (properties decoded).
type dataObjectRow struct {
	Name         string
	OriginalName string
	Module       string
	Kind         string
	Properties   *dataObjectProperties
	Description  string
}

// fieldRow is a field read back from storage (properties decoded).
type fieldRow struct {
	Name              string
	FieldType         string
	Properties        *fieldProperties
	IsPK              bool
	Ordinal           int
	DeprecationReason string
	Description       string
}

func (s *Store) readDataObject(ctx context.Context, name string) (*dataObjectRow, bool) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	var r dataObjectRow
	var props, desc sql.NullString
	err = conn.QueryRow(ctx, `SELECT name, original_name, module, kind,
		properties::JSON::VARCHAR, description
		FROM core.catalog.data_objects WHERE name = `+lit(name)).
		Scan(&r.Name, &r.OriginalName, &r.Module, &r.Kind, &props, &desc)
	if err != nil {
		return nil, false
	}
	r.Description = desc.String
	if props.Valid {
		var p dataObjectProperties
		if json.Unmarshal([]byte(props.String), &p) == nil {
			r.Properties = &p
		}
	}
	return &r, true
}

func (s *Store) readFields(ctx context.Context, typeName string) []*fieldRow {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT name, field_type, properties::JSON::VARCHAR, is_pk, ordinal, deprecation_reason, description
		FROM core.catalog.fields WHERE type_name = `+lit(typeName)+` ORDER BY ordinal, name`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*fieldRow
	for rows.Next() {
		var f fieldRow
		var props, deprecated, desc sql.NullString
		if err := rows.Scan(&f.Name, &f.FieldType, &props, &f.IsPK, &f.Ordinal, &deprecated, &desc); err != nil {
			return out
		}
		f.DeprecationReason = deprecated.String
		f.Description = desc.String
		if props.Valid {
			var p fieldProperties
			if json.Unmarshal([]byte(props.String), &p) == nil {
				f.Properties = &p
			}
		}
		out = append(out, &f)
	}
	return out
}

// --- AST building helpers ---

var reconPos = &ast.Position{Src: &ast.Source{Name: "catalog:reconstruct"}}

func directive(name string, args ...*ast.Argument) *ast.Directive {
	return &ast.Directive{Name: name, Arguments: args, Position: reconPos}
}

func strArg(name, val string) *ast.Argument {
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: val, Kind: ast.StringValue, Position: reconPos}, Position: reconPos}
}

func boolArg(name string, val bool) *ast.Argument {
	raw := "false"
	if val {
		raw = "true"
	}
	return &ast.Argument{Name: name, Value: &ast.Value{Raw: raw, Kind: ast.BooleanValue, Position: reconPos}, Position: reconPos}
}

// parseFieldType turns a stored type string (Int!, [tags], [Int!]!) into ast.Type.
func parseFieldType(s string) *ast.Type {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "!") {
		t := parseFieldType(s[:len(s)-1])
		t.NonNull = true
		return t
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		return &ast.Type{Elem: parseFieldType(s[1 : len(s)-1]), Position: reconPos}
	}
	return &ast.Type{NamedType: s, Position: reconPos}
}
