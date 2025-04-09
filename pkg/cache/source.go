package cache

import (
	"context"
	"database/sql/driver"
	"strings"

	_ "embed"

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

func (s *Service) Attach(ctx context.Context, db *db.Pool) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	err = duckdb.RegisterScalarUDFSet(c.DBConn(), "invalidate_cache", &invalidateCacheUDF{s: s, ctx: ctx}, &invalidateCacheTagsUDF{s: s, ctx: ctx})
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) Catalog(ctx context.Context) cs.Source {
	return cs.NewStringSource("cache/schema.graphql", schema)
}

type invalidateCacheTagsUDF struct {
	s   *Service
	ctx context.Context
}

func (f *invalidateCacheTagsUDF) Config() duckdb.ScalarFuncConfig {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	t, _ = duckdb.NewListInfo(t)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{t},
		ResultTypeInfo: types.DuckDBOperationResult(),
		Volatile:       true,
	}
}

func (f *invalidateCacheTagsUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			vv, ok := values[0].([]any)
			if !ok {
				return nil, &duckdb.Error{
					Type: duckdb.ErrorTypeInvalidInput,
					Msg:  "invalid argument type for cache invalidation",
				}
			}
			if len(vv) == 0 {
				err := f.s.Invalidate(f.ctx)
				if err != nil {
					return nil, err
				}
				return types.Result("cache invalidated", 1, 0).ToDuckdb(), nil
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
			err := f.s.Invalidate(f.ctx, tags...)
			if err != nil {
				return nil, err
			}

			return types.Result("cache invalidated for tags: "+strings.Join(tags, ","), 1, 0).ToDuckdb(), nil
		},
	}
}

type invalidateCacheUDF struct {
	s   *Service
	ctx context.Context
}

func (f *invalidateCacheUDF) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{
		ResultTypeInfo: types.DuckDBOperationResult(),
		Volatile:       true,
	}
}

func (f *invalidateCacheUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			if len(values) != 0 {
				return nil, &duckdb.Error{
					Type: duckdb.ErrorTypeInvalidInput,
					Msg:  "invalid argument type for cache invalidation",
				}
			}
			err := f.s.Invalidate(f.ctx)
			if err != nil {
				return nil, err
			}
			return types.Result("cache invalidated", 1, 0).ToDuckdb(), nil
		},
	}
}
