package engines

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/queries"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// timestampTransformer is the slice of Engine commonDateTransform needs — the
// whole interface is out of reach here, since the engines are passed by value
// and Capabilities takes a pointer.
type timestampTransformer interface {
	TimestampTransform(sql string, field *ast.Field, args sdl.FieldQueryArguments) string
}

// commonDateTransform applies the arguments of a Date field. Date declares only
// `bucket`, and date_trunc widens DATE to a timestamp on every backend we speak
// to, so the truncated value is cast back: the column has to keep the type the
// GraphQL field promises, or the key of a bucket aggregation comes back as a
// timestamp. The cast is the same in every dialect, hence one body.
func commonDateTransform(e timestampTransformer, sql string, field *ast.Field, args sdl.FieldQueryArguments) string {
	if args.ForName("bucket") == nil {
		return sql
	}
	return fmt.Sprintf("CAST(%s AS DATE)", e.TimestampTransform(sql, field, args))
}

func commonVectorTransform(ctx context.Context, e EngineVectorDistanceCalculator, qe types.Querier, sql string, field *ast.Field, args sdl.FieldQueryArguments, params []any) (string, []any, error) {
	if len(args) == 0 {
		return "NULL", params, nil
	}
	// only for extra field
	if !sdl.IsExtraField(field.Definition) {
		return sql, params, nil
	}
	var vec types.Vector
	var dist string
	switch sdl.ExtraFieldName(field.Definition) {
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
			return "", nil, sdl.ErrorPosf(field.Position, "The embeddings field and model is not defined for the data object %s", field.ObjectDefinition.Name)
		}
		model := sdl.DirectiveArgValue(d, "model", nil)
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
		dist = sdl.DirectiveArgValue(d, "distance", nil)
	default:
		return "", nil, fmt.Errorf("unsupported vector extra field: %s", sdl.ExtraFieldName(field.Definition))
	}
	if vec == nil || dist == "" {
		return "NULL", params, nil
	}
	return e.VectorDistanceSQL(sql, dist, vec, params)
}
