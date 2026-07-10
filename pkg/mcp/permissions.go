package mcp

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

// mcpFilter provides role-based filtering for MCP discovery results.
// If nil (no role or no permissions), all items are visible.
type mcpFilter struct {
	perm *perm.RolePermissions
}

func newMCPFilter(ctx context.Context) *mcpFilter {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return nil
	}
	if auth.IsFullAccess(ctx) {
		return nil
	}
	return &mcpFilter{perm: p}
}

func (f *mcpFilter) visibleModule(name string) bool {
	if f == nil {
		return true
	}
	_, ok := f.perm.Visible("mcp:modules", name)
	return ok
}

func (f *mcpFilter) visibleDataSource(name string) bool {
	if f == nil {
		return true
	}
	_, ok := f.perm.Visible("mcp:data-sources", name)
	return ok
}

// visibleType checks mcp:tables:query, GraphQL type-level, and data-object
// (table-level) visibility. A data object hidden or disabled via a
// data-object:query permission row is not exposed as an MCP tool — mirroring
// how GraphQL introspection hides it.
func (f *mcpFilter) visibleType(typeName string) bool {
	if f == nil {
		return true
	}
	if _, ok := f.perm.Visible("mcp:tables:query", typeName); !ok {
		return false
	}
	if _, ok := f.perm.Visible(typeName, "*"); !ok {
		return false
	}
	return !f.dataObjectDenied(typeName)
}

// dataObjectDenied reports whether a data object is hidden or query-disabled by
// a table-level (data-object:query) permission rule — either makes it invisible
// in MCP discovery (a disabled object cannot be queried, a hidden one is meant
// to be discovery-invisible). typeName must be a data object's GraphQL type
// name; callers gate on that (table listings query only table/view types,
// field checks gate on a data-object-reference field hugr_type) so a wildcard
// data-object:query field:"*" rule never matches a scalar or struct type.
func (f *mcpFilter) dataObjectDenied(typeName string) bool {
	if f == nil || typeName == "" {
		return false
	}
	return f.perm.DataObjectHidden(typeName) || f.perm.DataObjectDisabled(typeName, perm.OpQuery)
}

func (f *mcpFilter) visibleField(typeName, fieldName string) bool {
	if f == nil {
		return true
	}
	_, ok := f.perm.Visible(typeName, fieldName)
	return ok
}

// visibleFieldOfType is visibleField plus, for fields that REFERENCE a data
// object (relations, joins, aggregations — identified by the field's
// hugr_type), a check that the referenced data object is not hidden/disabled.
// A scalar or embedded-struct field is never a data-object reference, so it is
// left to the plain field-level check and never matched by a table-level rule.
// baseTypeName is the field's base return type name (the catalog's
// field_type_name — list/non-null markers already unwrapped).
func (f *mcpFilter) visibleFieldOfType(typeName, fieldName, baseTypeName, fieldHugrType string) bool {
	if f == nil {
		return true
	}
	if !f.visibleField(typeName, fieldName) {
		return false
	}
	if !isDataObjectRefField(fieldHugrType) {
		return true
	}
	return !f.dataObjectDenied(baseTypeName)
}

// isDataObjectRefField reports whether a field's hugr_type means the field's
// RETURN type is itself the base data object — a forward/reverse reference or a
// _join. For those, the field's return type name is the data object and a
// data-object rule keys on it directly.
//
// Deliberately excluded: aggregate/bucket_agg fields (return a synthetic
// _X_aggregation type, not the base object) and @function_call fields (return
// type may be a scalar/struct, which MCP cannot distinguish from a data object
// without the AST). Those derived fields can still surface a hidden object's
// NAME in discovery, but the query is blocked by the planner; hiding them here
// would require mapping the derived type back to the base object. Same gap as
// GraphQL introspection, which also only hides direct data-object return types.
func isDataObjectRefField(hugrType string) bool {
	switch base.HugrTypeField(hugrType) {
	case base.HugrTypeFieldSelect, base.HugrTypeFieldSelectOne, base.HugrTypeFieldJoin:
		return true
	}
	return false
}

// visibleFunction checks mcp:function permission with fully qualified name.
func (f *mcpFilter) visibleFunction(moduleName, funcName string) bool {
	if f == nil {
		return true
	}
	fqn := moduleName + "." + funcName
	_, ok := f.perm.Visible("mcp:function", fqn)
	return ok
}

// visibleMutationFunction checks mcp:function:mutation permission.
func (f *mcpFilter) visibleMutationFunction(moduleName, funcName string) bool {
	if f == nil {
		return true
	}
	fqn := moduleName + "." + funcName
	_, ok := f.perm.Visible("mcp:function:mutation", fqn)
	return ok
}
