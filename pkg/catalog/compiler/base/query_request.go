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
	QueryTypeSubscription
)

// QueryRequest is a classified entry from an operation's selection set.
type QueryRequest struct {
	Name      string
	OrderNum  int
	QueryType QueryType
	Field     *ast.Field
	Subset    []QueryRequest
}

// Metadata query constants.
const (
	MetadataSchemaQuery   = "__schema"
	MetadataTypeQuery     = "__type"
	MetadataTypeNameQuery = "__typename"

	// Logical-model introspection (catalog) meta queries. Single "_" prefix:
	// GraphQL hard-reserves "__" names (graphql-js rejects them even in
	// introspection results), so these are ordinary system fields on Query —
	// like jq/h3 — dispatched to the metadata path by name.
	MetadataCatalogQuery     = "_catalog"
	MetadataModuleQuery      = "_module"
	MetadataDataObjectQuery  = "_dataObject"
	MetadataFunctionQuery    = "_function"
	MetadataTypesQuery       = "_types"
	MetadataDataSourceQuery  = "_dataSource"
	MetadataDataSourcesQuery = "_dataSources"

	JQTransformQueryName = "jq"
)
