package schema

import (
	"github.com/hugr-lab/query-engine/pkg/schema/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// QueryRequestInfo recursively classifies an operation's selection set into
// typed query requests (meta, data, mutation, function, jq, h3).
func QueryRequestInfo(ss ast.SelectionSet) ([]sdl.QueryRequest, sdl.QueryType) {
	return sdl.QueryRequestInfo(ss)
}
