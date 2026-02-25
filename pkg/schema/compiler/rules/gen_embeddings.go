package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ base.DefinitionRule = (*EmbeddingsRule)(nil)

type EmbeddingsRule struct{}

func (r *EmbeddingsRule) Name() string     { return "EmbeddingsRule" }
func (r *EmbeddingsRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *EmbeddingsRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("embeddings") != nil
}

func (r *EmbeddingsRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	dir := def.Directives.ForName("embeddings")
	pos := compiledPos(def.Name)

	// Extract and validate directive arguments
	model := base.DirectiveArgString(dir, base.ArgModel)
	vectorFieldName := base.DirectiveArgString(dir, base.ArgVector)
	distance := base.DirectiveArgString(dir, base.ArgDistance)
	if model == "" || vectorFieldName == "" || distance == "" {
		return gqlerror.ErrorPosf(dir.Position, "object %s: @embeddings requires model, vector, and distance arguments", def.Name)
	}

	// Validate the vector field exists and is of type Vector
	vectorField := def.Fields.ForName(vectorFieldName)
	if vectorField == nil {
		return gqlerror.ErrorPosf(dir.Position, "object %s: @embeddings vector field %q not found", def.Name, vectorFieldName)
	}
	if vectorField.Type.Name() != "Vector" {
		return gqlerror.ErrorPosf(dir.Position, "object %s: @embeddings vector field %q must be of type Vector, got %s", def.Name, vectorFieldName, vectorField.Type.Name())
	}

	semanticArg := &ast.ArgumentDefinition{
		Name:        "semantic",
		Description: "Search for semantic similarity, works only for single object queries. Uses the OpenAI API to compute embeddings and find the most relevant object.",
		Type:        ast.NamedType("SemanticSearchInput", pos),
		Position:    pos,
	}

	// 1. Add _distance_to_query field to both object and its aggregation type
	distanceField := &ast.FieldDefinition{
		Name:        "_distance_to_query",
		Description: "Calculate vector distance to the specified vector for field " + vectorFieldName,
		Type:        ast.NamedType("Float", pos),
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "query",
				Description: "Query to calculate distance to",
				Type:        ast.NonNullNamedType("String", pos),
				Position:    pos,
			},
		},
		Directives: ast.DirectiveList{
			{Name: "sql", Arguments: ast.ArgumentList{
				{Name: "exp", Value: &ast.Value{Raw: "[" + vectorFieldName + "]", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			{Name: "extra_field", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "QueryEmbeddingDistance", Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "base_field", Value: &ast.Value{Raw: vectorFieldName, Kind: ast.StringValue, Position: pos}, Position: pos},
				{Name: "base_type", Value: &ast.Value{Raw: "Vector", Kind: ast.EnumValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
		Position: pos,
	}
	ctx.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: pos,
		Fields:   ast.FieldList{distanceField},
	})

	// Also add _distance_to_query to aggregation type (as FloatAggregation with @field_aggregation)
	aggTypeName := "_" + def.Name + "_aggregation"
	if ctx.LookupType(aggTypeName) != nil {
		aggDistField := &ast.FieldDefinition{
			Name:     "_distance_to_query",
			Type:     ast.NamedType("FloatAggregation", pos),
			Position: pos,
			Arguments: ast.ArgumentDefinitionList{
				{
					Name:     "query",
					Type:     ast.NonNullNamedType("String", pos),
					Position: pos,
				},
			},
			Directives: ast.DirectiveList{
				fieldAggregationDirective("_distance_to_query", pos),
			},
		}
		ctx.AddExtension(&ast.Definition{
			Kind:     ast.Object,
			Name:     aggTypeName,
			Position: pos,
			Fields:   ast.FieldList{aggDistField},
		})
	}

	// 2. Modify ALL query fields: add semantic argument
	// (similarity is already added by VectorSearchRule for any Vector field)
	queryFields := ctx.QueryFields()[def.Name]
	for _, qf := range queryFields {
		// Skip by_pk (SELECT_ONE) queries
		if isSelectOneQuery(qf) {
			continue
		}
		qf.Arguments = append(qf.Arguments, semanticArg)
	}

	// 3. Modify mutation fields: add summary argument and make data nullable
	mutationFields := ctx.MutationFields()[def.Name]
	for _, mf := range mutationFields {
		mutDir := mf.Directives.ForName("mutation")
		if mutDir == nil {
			continue
		}
		mutType := base.DirectiveArgString(mutDir, base.ArgType)
		if mutType == "INSERT" || mutType == "UPDATE" {
			// Make data argument nullable for embedded objects
			for _, arg := range mf.Arguments {
				if arg.Name == "data" {
					arg.Type.NonNull = false
				}
			}
			mf.Arguments = append(mf.Arguments, &ast.ArgumentDefinition{
				Name:        "summary",
				Description: mutType + " summary embedding",
				Type:        ast.NamedType("String", pos),
				Position:    pos,
			})
		}
	}

	// Note: similarity/semantic args for _join/_join_aggregation are handled
	// by JoinSpatialRule which detects @embeddings/Vector fields.

	return nil
}

// isSelectOneQuery checks if a query field is a SELECT_ONE query (by_pk or unique).
func isSelectOneQuery(qf *ast.FieldDefinition) bool {
	queryDir := qf.Directives.ForName(base.QueryDirectiveName)
	if queryDir != nil {
		return base.DirectiveArgString(queryDir, base.ArgType) == "SELECT_ONE"
	}
	return false
}
