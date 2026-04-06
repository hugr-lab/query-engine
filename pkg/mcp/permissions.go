package mcp

import (
	"context"

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

// visibleType checks both mcp:tables:query and GraphQL type-level visibility.
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
	return true
}

func (f *mcpFilter) visibleField(typeName, fieldName string) bool {
	if f == nil {
		return true
	}
	_, ok := f.perm.Visible(typeName, fieldName)
	return ok
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
