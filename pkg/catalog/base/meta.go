package base

import (
	"slices"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

// The META SURFACE: hugr's logical-model introspection, and GraphQL's own.
//
// A meta-field is resolved on the metadata path (like __schema / __type),
// never planned as a data query, and sits OUTSIDE the role-governed surface —
// the same reason no one writes a permission rule for __typename. What those
// resolvers RETURN is still filtered per role; it is the ENTRY POINT that is
// not something a permission row addresses.
//
// The set is a Go slice rather than an SDL directive on purpose. It is closed
// and owned by the binary: a marker in SDL would be a permission exemption any
// source could write, and containing that would mean a validator rule to
// forbid it everywhere else. A slice cannot be forged, and pairing it with the
// "must be a field of Query" test closes the question completely — a source
// that manages to define a field named _catalog somewhere else gets no
// exemption from it.
//
// Three consumers read this, so registering a meta-field is one edit rather
// than three:
//
//   - sdl.QueryRequestInfo routes it to the metadata path;
//   - perm.PermissionFieldRule leaves it out of the role-governed surface;
//   - the introspection resolvers keep it out of the schema's own listings.

// introspectionPrefix is GraphQL's own marker for meta-fields and
// introspection types. __schema and __type are never DECLARED by hugr —
// gqlparser appends them to the root type while validating the system schema —
// so the prefix, not a list, is what recognises them.
const introspectionPrefix = "__"

// MetaQueryFields is the closed set of hugr's meta-fields on Query. Adding a
// logical-model meta query means adding it here; MetaTypesReachable (in the
// tests) is the guard that the type list below keeps up.
var MetaQueryFields = []string{
	MetadataCatalogQuery,
	MetadataModuleQuery,
	MetadataDataObjectQuery,
	MetadataFunctionQuery,
	MetadataTypesQuery,
	MetadataDataSourceQuery,
	MetadataDataSourcesQuery,
	MetadataSearchQuery,
}

// MetaTypes is the closed set of types reachable ONLY from a meta-field. It
// exists because the exemption has to compose: the walker checks every hop, so
// covering just the entry field would let a wildcard permission row cut the
// query one level in, at _Module.dataObjects.
//
// It is enumerated rather than inferred. "@system and starts with _" would
// look like the same set and is not: _h3_query and _h3_data_query match that
// shape and lead straight into DATA — exempting them would be a hole.
var MetaTypes = []string{
	"_Module",
	"_DataObject",
	"_DataObjectProperties",
	"_DataObjectQuery",
	"_Relation",
	"_Function",
	"_DataSource",
	"_SearchResult",
	"_SearchHit",
}

// IsMetaSelection reports whether a SELECTED field is a meta-field: one of
// GraphQL's own, or one of hugr's on the Query root.
//
// The parent test is not a formality. It is what makes the slice safe to
// consult by name at all: a meta-field is a field OF Query, and nothing named
// like one anywhere else inherits its exemption.
func IsMetaSelection(parentDef *ast.Definition, field *ast.Field) bool {
	if field == nil {
		return false
	}
	if strings.HasPrefix(field.Name, introspectionPrefix) {
		return true
	}
	return IsMetaQueryField(parentDef, field.Name)
}

// IsMetaQueryField reports whether a field NAME on a parent type is one of
// hugr's meta-fields. Separate from IsMetaSelection because the introspection
// listings walk field DEFINITIONS, where there is no selection to inspect.
func IsMetaQueryField(parentDef *ast.Definition, name string) bool {
	if parentDef == nil || parentDef.Name != QueryBaseName {
		return false
	}
	return slices.Contains(MetaQueryFields, name)
}

// IsHugrMetaType reports whether a type belongs to hugr's meta surface,
// EXCLUDING GraphQL's own introspection types.
//
// The distinction matters exactly once, in the schema's type listing:
// __Schema and friends must stay in __schema.types — the spec puts them there
// and tooling reads them — while hugr's meta types have no business in a
// listing of the served schema, any more than __schema has in Query.fields.
func IsHugrMetaType(def *ast.Definition) bool {
	return def != nil && slices.Contains(MetaTypes, def.Name)
}

// IsMetaType reports whether a type definition belongs to the meta surface:
// one of hugr's meta types, or one of GraphQL's introspection types.
func IsMetaType(def *ast.Definition) bool {
	if def == nil {
		return false
	}
	return strings.HasPrefix(def.Name, introspectionPrefix) || slices.Contains(MetaTypes, def.Name)
}

// InMetaSurface reports whether a field being walked lies outside the
// role-governed surface — either because it is a meta-field itself, or because
// the walk has already entered one and this field belongs to the subtree that
// meta-field opened.
//
// The PARENT check is what makes the exemption compose. Asking "is my parent
// part of the meta surface" answers for a whole subtree at once, which is
// sound precisely because the meta types are reachable from nowhere else.
func InMetaSurface(parentDef *ast.Definition, field *ast.Field) bool {
	return IsMetaSelection(parentDef, field) || IsMetaType(parentDef)
}
