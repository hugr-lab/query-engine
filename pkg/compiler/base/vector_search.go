package base

import (
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	VectorTypeName               = "Vector"
	VectorSearchInputName        = "VectorSearchInput"
	VectorDistanceTypeEnumName   = "VectorDistanceType"
	VectorSearchDistanceL2       = "L2"
	VectorSearchDistanceIP       = "Inner"
	VectorSearchDistanceCosine   = "Cosine"
	SimilaritySearchArgumentName = "similarity"

	EmbeddingsDirectiveName        = "embeddings"
	SemanticSearchArgumentName     = "semantic"
	SemanticSearchInputName        = "SemanticSearchInput"
	SummaryForEmbeddedArgumentName = "summary"

	DistanceFieldNameSuffix              = "distance"
	VectorDistanceExtraFieldName         = "VectorDistance"
	QueryEmbeddingDistanceExtraFieldName = "QueryEmbeddingDistance"
	QueryEmbeddingsDistanceFieldName     = "_distance_to_query"
)

func VectorEmbeddingsExtraField(vectorFieldName string) *ast.FieldDefinition {
	fieldName := QueryEmbeddingsDistanceFieldName
	sql := "[" + vectorFieldName + "]"

	return &ast.FieldDefinition{
		Name:        fieldName,
		Description: "Calculate vector distance to the specified vector for field " + vectorFieldName,
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "query",
				Description: "Query to calculate distance to",
				Type:        ast.NonNullNamedType("String", CompiledPos("compiled-instruction")),
				Position:    CompiledPos("compiled-instruction"),
			},
		},
		Directives: ast.DirectiveList{
			SqlFieldDirective(sql),
			ExtraFieldDirective(QueryEmbeddingDistanceExtraFieldName, vectorFieldName, VectorTypeName),
		},
		Type:     ast.NamedType("Float", CompiledPos("compiled-instruction")),
		Position: CompiledPos("compiled-instruction"),
	}

}
