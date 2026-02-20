package base

import "github.com/vektah/gqlparser/v2/ast"

// QueryType is a bitmask for classifying operation-level query requests.
type QueryType int

const (
	QueryTypeNone QueryType = 0
	QueryTypeMeta QueryType = 1 << iota
	QueryTypeQuery
	QueryTypeJQTransform
	QueryTypeMutation
	QueryTypeFunction
	QueryTypeFunctionMutation
	QueryTypeH3Aggregation
)

// QueryRequest is a classified entry from an operation's selection set.
type QueryRequest struct {
	Name      string
	OrderNum  int
	QueryType QueryType
	Field     *ast.Field
	Subset    []QueryRequest
}

const (
	MetadataSchemaQuery   = "__schema"
	MetadataTypeQuery     = "__type"
	MetadataTypeNameQuery = "__typename"

	JQTransformQueryName = "jq"

	FieldAggregationQueryDirectiveName = "aggregation_query"
)
