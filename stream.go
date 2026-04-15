package hugr

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/trace"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// recoverStreamPanic converts a panic into an error for stream handlers.
func recoverStreamPanic(errp *error) {
	if r := recover(); r != nil {
		if e, ok := r.(error); ok {
			*errp = fmt.Errorf("internal error: %w", e)
		} else {
			*errp = fmt.Errorf("internal error: %v", r)
		}
		log.Printf("panic recovered in stream: %v\n%s", r, debug.Stack())
	}
}

var ErrEmptyRequest = errors.New("empty request")

type ChunkProcessFunc func(ctx context.Context, path string, field *ast.Field, rec arrow.RecordBatch) error

func (s *Service) ProcessStreamQuery(ctx context.Context, query string, vars map[string]any) (table types.ArrowTable, finalize func(), err error) {
	defer recoverStreamPanic(&err)
	ctx, err = s.perm.ContextWithPermissions(ctx)
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

	logger := trace.LoggerFromContext(ctx)
	logger.Debug("stream.sql", "field", q.Field.Name, "alias", q.Field.Alias, "sql", plan.Log())
	if ai := auth.AuthInfoFromContext(ctx); ai != nil {
		logger.Debug("stream.user", "user", ai.UserName, "role", ai.Role, "field", q.Field.Name)
	}

	return plan.ExecuteStream(ctx, s.db)
}
