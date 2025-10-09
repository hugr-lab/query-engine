package planner

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/queries"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	vectorDistanceNodeName    = "vectorSearchDistance"
	vectorSearchLimitNodeName = "vectorSearchLimit"
)

// vector similarity search nodes
func vectorSearchNodes(e engines.Engine, info *compiler.Object, query *ast.Field, prefix string, param any) (QueryPlanNodes, error) {
	if param == nil && info == nil {
		return nil, nil
	}
	// parse value
	sm, ok := param.(map[string]any)
	if sm == nil || !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector search must be an object")
	}
	field, ok := sm["name"].(string)
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector field name is required")
	}
	distance, ok := sm["distance"].(string)
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector distance is required")
	}
	limit, ok := sm["limit"].(int64)
	if !ok || limit <= 0 {
		return nil, compiler.ErrorPosf(query.Position, "vector limit is required")
	}

	def := info.Definition()
	if def == nil {
		return nil, compiler.ErrorPosf(query.Position, "vector search is not supported on this field")
	}

	fieldDef := info.Definition().Fields.ForName(field)
	if fieldDef == nil || fieldDef.Type.NamedType != base.VectorTypeName {
		return nil, compiler.ErrorPosf(query.Position, "unknown vector field %s", field)
	}

	fi := compiler.FieldDefinitionInfo(fieldDef, info.Definition())
	if fi == nil {
		return nil, compiler.ErrorPosf(query.Position, "unknown vector field %s", field)
	}

	ec, ok := e.(engines.EngineVectorDistanceCalculator)
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector distance calculation is not supported by query engine %s", e.Type())
	}

	vec, ok := sm["vector"]
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector is required")
	}
	vector, err := types.ParseVector(vec)
	if err != nil {
		return nil, compiler.ErrorPosf(query.Position, "invalid vector: %v", err)
	}
	if fi.Dim > 0 && len(vector) != fi.Dim {
		return nil, compiler.ErrorPosf(query.Position, "vector dimension mismatch: expected %d, got %d", fi.Dim, len(vector))
	}

	var nodes QueryPlanNodes
	// create a vector search node
	nodes = QueryPlanNodes{
		&QueryPlanNode{
			Name:  vectorDistanceNodeName,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				// calculate distance to sort by
				dist, params, err := ec.VectorDistanceSQL(fi.SQL(prefix), distance, vector, params)
				if err != nil {
					return "", params, err
				}
				return dist, params, nil
			},
		},
		&QueryPlanNode{
			Name:  vectorSearchLimitNodeName,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				// calculate distance to sort by
				return strconv.Itoa(int(limit)), params, nil
			},
		},
	}
	return nodes, nil
}

// vector similarity search nodes
func semanticSearchNodes(e engines.Engine, info *compiler.Object, query *ast.Field, prefix string, param any) (QueryPlanNodes, error) {
	if param == nil && info == nil {
		// add empty fields node with distance NULL
		return nil, nil
	}
	// parse value
	sm, ok := param.(map[string]any)
	if sm == nil || !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector search must be an object")
	}
	queryText, ok := sm["query"].(string)
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector field name is required")
	}
	limit, ok := sm["limit"].(int64)
	if !ok || limit <= 0 {
		return nil, compiler.ErrorPosf(query.Position, "vector limit is required")
	}

	d := info.Definition().Directives.ForName(base.EmbeddingsDirectiveName)
	if d == nil {
		return nil, compiler.ErrorPosf(query.Position, "semantic search is not supported on object %s", info.Name)
	}

	field := compiler.DirectiveArgValue(d, "vector", nil)
	model := compiler.DirectiveArgValue(d, "model", nil)
	distance := compiler.DirectiveArgValue(d, "distance", nil)

	fieldDef := info.Definition().Fields.ForName(field)
	if fieldDef == nil || fieldDef.Type.NamedType != base.VectorTypeName {
		return nil, compiler.ErrorPosf(query.Position, "unknown vector field %s", field)
	}

	fi := compiler.FieldDefinitionInfo(fieldDef, info.Definition())
	if fi == nil {
		return nil, compiler.ErrorPosf(query.Position, "unknown vector field %s", field)
	}

	ec, ok := e.(engines.EngineVectorDistanceCalculator)
	if !ok {
		return nil, compiler.ErrorPosf(query.Position, "vector distance calculation is not supported by query engine %s", e.Type())
	}

	var nodes QueryPlanNodes
	// create a vector search node
	nodes = QueryPlanNodes{
		&QueryPlanNode{
			Name:  vectorDistanceNodeName,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				vec, err := queries.CreateEmbedding(context.Background(), node.Querier(), model, queryText)
				if err != nil {
					return "", params, compiler.ErrorPosf(query.Position, "failed to create embedding: %v", err)
				}
				if fi.Dim > 0 && len(vec) != fi.Dim {
					return "", params, compiler.ErrorPosf(query.Position, "vector dimension mismatch: expected %d, got %d", fi.Dim, len(vec))
				}
				// calculate distance to sort by
				dist, params, err := ec.VectorDistanceSQL(fi.SQL(prefix), distance, vec, params)
				if err != nil {
					return "", params, err
				}
				return dist, params, nil
			},
		},
		&QueryPlanNode{
			Name:  vectorSearchLimitNodeName,
			Query: query,
			CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
				// calculate distance to sort by
				return strconv.Itoa(int(limit)), params, nil
			},
		},
	}
	return nodes, nil
}

func createEmbeddingForTable(ctx context.Context, qe types.Querier, info *compiler.Object, text string) (string, types.Vector, error) {
	d := info.Definition().Directives.ForName(base.EmbeddingsDirectiveName)
	if d == nil {
		return "", nil, errors.New("semantic search is not supported for this object")
	}

	field := compiler.DirectiveArgValue(d, "vector", nil)
	model := compiler.DirectiveArgValue(d, "model", nil)
	fi := info.FieldForName(field) // to check field existence
	if fi == nil || fi.Definition().Type.NamedType != base.VectorTypeName {
		return "", nil, errors.New("unknown vector field " + field)
	}

	vec, err := queries.CreateEmbedding(ctx, qe, model, text)
	if err != nil {
		return "", nil, err
	}
	if fi.Dim > 0 && len(vec) != fi.Dim {
		return "", nil, fmt.Errorf("vector dimension mismatch: expected %d, got %d", fi.Dim, len(vec))
	}
	return fi.Name, vec, nil
}
