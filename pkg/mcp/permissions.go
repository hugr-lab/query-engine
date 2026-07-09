package mcp

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/auth"
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
// to be discovery-invisible).
func (f *mcpFilter) dataObjectDenied(typeName string) bool {
	if f == nil {
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

// visibleFieldOfType is visibleField plus a check that the field's return type
// is not a hidden/disabled data object — so a relation to a hidden table is
// hidden along with the table itself. fieldType is the field's GraphQL return
// type as written (list / non-null markers are stripped here).
func (f *mcpFilter) visibleFieldOfType(typeName, fieldName, fieldType string) bool {
	if f == nil {
		return true
	}
	if !f.visibleField(typeName, fieldName) {
		return false
	}
	return !f.dataObjectDenied(baseGraphQLTypeName(fieldType))
}

// baseGraphQLTypeName strips list and non-null markers from a GraphQL type
// reference: "[Type!]!" -> "Type".
func baseGraphQLTypeName(t string) string {
	t = strings.TrimSpace(t)
	t = strings.TrimSuffix(t, "!")
	t = strings.TrimPrefix(t, "[")
	t = strings.TrimSuffix(t, "]")
	t = strings.TrimSuffix(t, "!")
	return strings.TrimSpace(t)
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
