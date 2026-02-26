package hugr

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/vektah/gqlparser/v2/ast"
)

var ErrEmptyRequest = errors.New("empty request")

type ChunkProcessFunc func(ctx context.Context, path string, field *ast.Field, rec arrow.RecordBatch) error

func (s *Service) ProcessStreamQuery(ctx context.Context, query string, vars map[string]any) (db.ArrowTable, func(), error) {
	ctx, err := s.perm.ContextWithPermissions(ctx)
	if err != nil {
		return nil, nil, err
	}

	op, err := s.schema.ParseQuery(ctx, query, vars, "")
	if err != nil {
		return nil, nil, err
	}

	if op.QueryType&(base.QueryTypeMeta|base.QueryTypeMutation|base.QueryTypeJQTransform|base.QueryTypeFunction|base.QueryTypeFunctionMutation) != 0 {
		return nil, nil, errors.New("streaming is not supported for mutations, JQ transforms, or functions")
	}
	if len(op.Queries) != 1 {
		return nil, nil, fmt.Errorf("streaming is only supported for single queries, found %d queries", len(op.Queries))
	}

	flatQuery := sdl.FlatQuery(op.Queries)
	var q base.QueryRequest
	for _, qq := range flatQuery {
		q = qq
		break
	}

	provider := s.schema.Provider()
	ctx = planner.ContextWithRawResultsFlag(ctx)

	plan, err := s.planner.Plan(ctx, provider, q.Field, op.Variables)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to plan query: %w", err)
	}

	err = plan.Compile()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile query: %w", err)
	}

	if s.config.Debug {
		ai := auth.AuthInfoFromContext(ctx)
		if ai != nil {
			log.Printf("Stream: User: %s, Role: %s, Query: %s (%s), SQL: %s",
				ai.UserName,
				ai.Role,
				q.Field.Alias,
				q.Field.Name,
				plan.Log(),
			)
		}
		if auth.IsFullAccess(ctx) {
			log.Printf("Stream: Internal query: %s (%s), SQL: %s",
				q.Field.Alias,
				q.Field.Name,
				plan.Log(),
			)
		}
	}

	return plan.ExecuteStream(ctx, s.db)
}
