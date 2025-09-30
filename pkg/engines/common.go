package engines

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/queries"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

func commonVectorTransform(ctx context.Context, e EngineVectorDistanceCalculator, qe types.Querier, sql string, field *ast.Field, args compiler.FieldQueryArguments, params []any) (string, []any, error) {
	if len(args) == 0 {
		return "NULL", params, nil
	}
	// only for extra field
	if !compiler.IsExtraField(field.Definition) {
		return sql, params, nil
	}
	var vec types.Vector
	var dist string
	switch compiler.ExtraFieldName(field.Definition) {
	case base.VectorDistanceExtraFieldName:
		if v := args.ForName("vector"); v != nil {
			v, ok := v.Value.(types.Vector)
			if !ok {
				return "", nil, fmt.Errorf("invalid vector argument")
			}
			vec = v
		}
		if d := args.ForName("distance"); d != nil {
			d, ok := d.Value.(string)
			if !ok {
				return "", nil, fmt.Errorf("invalid distance argument")
			}
			dist = d
		}
	case base.QueryEmbeddingDistanceExtraFieldName:
		d := field.ObjectDefinition.Directives.ForName(base.EmbeddingsDirectiveName)
		if d == nil {
			return "", nil, compiler.ErrorPosf(field.Position, "The embeddings field and model is not defined for the data object %s", field.ObjectDefinition.Name)
		}
		model := compiler.DirectiveArgValue(d, "model", nil)
		var query string
		if d := args.ForName("query"); d != nil {
			d, ok := d.Value.(string)
			if !ok {
				return "", nil, fmt.Errorf("invalid distance argument")
			}
			query = d
		}
		var err error
		vec, err = queries.CreateEmbedding(ctx, qe, model, query)
		if err != nil {
			return "", nil, err
		}
		dist = compiler.DirectiveArgValue(d, "distance", nil)
	default:
		return "", nil, fmt.Errorf("unsupported vector extra field: %s", compiler.ExtraFieldName(field.Definition))
	}
	if vec == nil || dist == "" {
		return "NULL", params, nil
	}
	return e.VectorDistanceSQL(sql, dist, vec, params)
}
