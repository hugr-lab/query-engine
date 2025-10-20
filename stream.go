package hugr

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/planner"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

var ErrEmptyRequest = errors.New("empty request")

type ChunkProcessFunc func(ctx context.Context, path string, field *ast.Field, rec arrow.RecordBatch) error

func (s *Service) ProcessStreamQuery(ctx context.Context, query string, vars map[string]any) (db.ArrowTable, func(), error) {
	ctx, err := s.perm.ContextWithPermissions(ctx)
	if err != nil {
		return nil, nil, err
	}
	schema := s.catalog.Schema()
	qd, errs := gqlparser.LoadQueryWithRules(schema, query, types.GraphQLQueryRules)
	if len(errs) > 0 {
		return nil, nil, errs
	}

	if len(qd.Operations) == 0 {
		return nil, nil, errors.New("no operations found")
	}
	if len(qd.Operations) > 1 {
		return nil, nil, errors.New("multiple operations found, please specify operation name")
	}
	op := qd.Operations[0]
	if op == nil {
		return nil, nil, ErrEmptyRequest
	}
	queries, qtt := compiler.QueryRequestInfo(op.SelectionSet)
	if qtt&(compiler.QueryTypeMeta|compiler.QueryTypeMutation|compiler.QueryTypeJQTransform|compiler.QueryTypeFunction|compiler.QueryTypeFunctionMutation) != 0 {
		return nil, nil, errors.New("streaming is not supported for mutations, JQ transforms, or functions")
	}
	// authorize queries
	if len(queries) != 1 {
		return nil, nil, fmt.Errorf("streaming is only supported for single queries, found %d queries", len(queries))
	}
	p := perm.PermissionsFromCtx(ctx)
	if p != nil {
		if err := p.CheckQuery(queries[0].Field); err != nil {
			return nil, nil, err
		}
	}

	flatQuery := compiler.FlatQuery(queries)
	var q compiler.QueryRequest
	for _, qq := range flatQuery {
		q = qq
		break
	}

	ctx = planner.ContextWithRawResultsFlag(ctx)

	plan, err := s.planner.Plan(ctx, schema, q.Field, vars)
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
