package planner

import (
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// GeomInfoFromField extracts per-field geometry metadata from a GraphQL AST
// field. Same mechanism the IPC handler uses when building the
// X-Hugr-Geometry-Fields header, but returns structured types.GeometryInfo
// entries keyed by dotted field path ("" means the whole value).
//
// Used by the planner to attach GeometryInfo to every ArrowTable /
// *JsonValue leaf it returns, so downstream consumers (RecordToJSON,
// IPC writer, Arrow scanner) read from a single source of truth.
//
// Format values produced:
//
//   - "GeoJSON"       — top-level scalar Geometry return (the whole value).
//   - "GeoJSONString" — nested Geometry inside a struct / row field.
//   - "WKB"           — not produced here; emitted on the IPC wire by the
//     handler when it detects a raw Geometry table column. RecordToJSON
//     discovers WKB via the Arrow extension name directly on the column,
//     not via GeometryInfo.
//   - "H3Cell"        — not a geometry; returned so callers that care (like
//     the IPC header builder) can still see the hint.
func GeomInfoFromField(query *ast.Field) map[string]types.GeometryInfo {
	raw := geomFieldsInfoFromQuery(query)
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]types.GeometryInfo, len(raw))
	for k, v := range raw {
		// Top-level scalar Geometry selects produce a `WKB` entry from
		// geomFieldsInfoFromQuery; the IPC handler upgrades it to
		// `GeoJSON` based on query.Definition.Type.NamedType. Mirror
		// that so in-memory consumers see the same value.
		format := v.format
		if k == "" && query.Definition.Type.NamedType != "" && format == "WKB" {
			format = "GeoJSON"
		}
		out[k] = types.GeometryInfo{SRID: v.srid, Format: format}
	}
	return out
}

// internal shape used by the AST walk — unexported so callers go through
// the typed GeomInfoFromField.
type geomFieldMeta struct {
	srid   string
	format string
}

// geomFieldsInfoFromQuery walks a GraphQL field's selection set producing a
// map keyed by dotted alias path. Logic mirrors the original implementation
// at ipc-query.go:232-273; extracted here so the planner and IPC writer
// share one source of truth.
func geomFieldsInfoFromQuery(query *ast.Field) map[string]geomFieldMeta {
	if sdl.IsScalarType(query.Definition.Type.Name()) {
		if query.Definition.Type.NamedType == base.H3CellTypeName {
			return map[string]geomFieldMeta{
				"": {format: "H3Cell"},
			}
		}
		if query.Definition.Type.NamedType != base.GeometryTypeName {
			return nil
		}
		fi := sdl.FieldInfo(query)
		if fi == nil {
			return nil
		}
		return map[string]geomFieldMeta{
			"": {format: "WKB", srid: fi.GeometrySRID()},
		}
	}
	meta := map[string]geomFieldMeta{}
	for _, s := range engines.SelectedFields(query.SelectionSet) {
		info := geomFieldsInfoFromQuery(s.Field)
		if len(info) == 0 {
			continue
		}
		for field, m := range info {
			if field != "" {
				field = s.Field.Alias + "." + field
				m.format = "GeoJSONString"
			}
			if field == "" {
				field = s.Field.Alias
			}
			meta[field] = m
		}
	}
	return meta
}
