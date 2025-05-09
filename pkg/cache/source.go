package cache

import (
	"context"
	"database/sql/driver"

	_ "embed"

	"github.com/hugr-lab/query-engine/pkg/auth"
	cs "github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/marcboeker/go-duckdb/v2"
)

// implement sources.RuntimeSource interface

var _ sources.RuntimeSource = (*Service)(nil)

//go:embed schema.graphql
var schema string

func (s *Service) Name() string {
	return "cache"
}

func (s *Service) Engine() engines.Engine {
	return s.engine
}

func (s *Service) IsReadonly() bool {
	return false
}

func (s *Service) AsModule() bool {
	return false
}

func (s *Service) Attach(ctx context.Context, pool *db.Pool) error {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		return err
	}
	t, err = duckdb.NewListInfo(t)
	if err != nil {
		return err
	}
	return db.RegisterScalarFunctionSet(auth.ContextWithFullAccess(ctx), pool, db.ScalarFunctionSet[string]{
		Name:        "invalidate_cache",
		Module:      "cache",
		Description: "Invalidate the cache for the given tags.",
		OutputType:  types.DuckDBOperationResult(),
		ConvertOutput: func(out string) (any, error) {
			return types.Result(out, 1, 0).ToDuckdb(), nil
		},
		Funcs: []db.ScalarFunctionCaster[string]{
			&db.ScalarFunctionSetItemNoArgs[string]{
				Execute: func(ctx context.Context) (string, error) {
					err := s.Invalidate(ctx)
					if err != nil {
						return "", err
					}
					return "cache invalidated", nil
				},
			},
			&db.ScalarFunctionSetItem[[]string, string]{
				InputTypes: []duckdb.TypeInfo{t},
				Execute: func(ctx context.Context, tags []string) (string, error) {
					err := s.Invalidate(ctx, tags...)
					if err != nil {
						return "", err
					}
					return "cache invalidated", nil
				},
				ConvertInput: func(args []driver.Value) ([]string, error) {
					if len(args) != 1 {
						return nil, &duckdb.Error{
							Type: duckdb.ErrorTypeParameterNotResolved,
							Msg:  "invalid number of arguments",
						}
					}
					vv, ok := args[0].([]any)
					if !ok {
						return nil, &duckdb.Error{
							Type: duckdb.ErrorTypeInvalidInput,
							Msg:  "invalid argument type for cache invalidation",
						}
					}
					tags := make([]string, len(vv))
					for i, v := range vv {
						tags[i], ok = v.(string)
						if !ok {
							return nil, &duckdb.Error{
								Type: duckdb.ErrorTypeInvalidInput,
								Msg:  "invalid argument type for cache invalidation",
							}
						}
					}
					return tags, nil
				},
			},
		},
	})
}

func (s *Service) Catalog(ctx context.Context) cs.Source {
	return cs.NewStringSource("cache/schema.graphql", schema)
}
