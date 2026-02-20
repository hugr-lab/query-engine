package schema

import (
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Operation is the result of ParseQuery.
// Contains an enriched AST operation ready for the planner.
type Operation struct {
	// Enriched AST operation definition
	Definition *ast.OperationDefinition

	// All fragments from the document (needed by the planner for FragmentSpread resolution)
	Fragments ast.FragmentDefinitionList

	// Transformed variables
	Variables map[string]any

	// Classified top-level query requests from the selection set
	Queries []base.QueryRequest

	// Combined bitmask of all query types in the operation
	QueryType base.QueryType
}
