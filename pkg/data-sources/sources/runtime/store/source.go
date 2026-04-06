package store

import (
	"context"
	"database/sql/driver"
	_ "embed"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

//go:embed schema.graphql
var schema string

type Source struct {
	db       *db.Pool
	resolver sources.DataSourceResolver
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string           { return "core.store" }
func (s *Source) Engine() engines.Engine { return engines.NewDuckDB() }
func (s *Source) IsReadonly() bool       { return false }
func (s *Source) AsModule() bool         { return true }

func (s *Source) DataSourceServiceSetup(resolver sources.DataSourceResolver) {
	s.resolver = resolver
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	return s.registerUDFs(ctx)
}

func (s *Source) Catalog(_ context.Context) (cs.Catalog, error) {
	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		Prefix:       "core_store",
		ReadOnly:     s.IsReadonly(),
		AsModule:     s.AsModule(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return cs.NewStringSource(s.Name(), e, opts, schema)
}

func (s *Source) resolveStore(name string) (sources.StoreSource, error) {
	ds, err := s.resolver.Resolve(name)
	if err != nil {
		return nil, fmt.Errorf("store %q not found: %w", name, err)
	}
	st, ok := ds.(sources.StoreSource)
	if !ok {
		return nil, fmt.Errorf("data source %q is not a store source", name)
	}
	return st, nil
}

func (s *Source) registerUDFs(ctx context.Context) error {
	// core_store_get(store, key) → VARCHAR (nullable)
	type getArgs struct {
		store string
		key   string
	}
	err := db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[getArgs, *string]{
		Name: "core_store_get",
		Execute: func(ctx context.Context, args getArgs) (*string, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return nil, err
			}
			val, exists, err := st.Get(ctx, args.key)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, nil
			}
			return &val, nil
		},
		ConvertInput: func(args []driver.Value) (getArgs, error) {
			return getArgs{store: args[0].(string), key: args[1].(string)}, nil
		},
		ConvertOutput: func(out *string) (any, error) {
			if out == nil {
				return nil, nil
			}
			return *out, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
	})
	if err != nil {
		return fmt.Errorf("register core_store_get: %w", err)
	}

	// core_store_set(store, key, value, ttl) → OperationResult
	type setArgs struct {
		store string
		key   string
		value string
		ttl   int32
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[setArgs, *types.OperationResult]{
		Name: "core_store_set",
		Execute: func(ctx context.Context, args setArgs) (*types.OperationResult, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return types.ErrResult(err), nil
			}
			if err := st.Set(ctx, args.key, args.value, int(args.ttl)); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("OK", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (setArgs, error) {
			a := setArgs{store: args[0].(string), key: args[1].(string), value: args[2].(string)}
			if args[3] != nil {
				a.ttl = args[3].(int32)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
		},
		OutputType:            db.DuckDBOperationResult(),
		IsSpecialNullHandling: true,
	})
	if err != nil {
		return fmt.Errorf("register core_store_set: %w", err)
	}

	// core_store_del(store, key) → OperationResult
	type delArgs struct {
		store string
		key   string
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[delArgs, *types.OperationResult]{
		Name: "core_store_del",
		Execute: func(ctx context.Context, args delArgs) (*types.OperationResult, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return types.ErrResult(err), nil
			}
			if err := st.Del(ctx, args.key); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("OK", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (delArgs, error) {
			return delArgs{store: args[0].(string), key: args[1].(string)}, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
	if err != nil {
		return fmt.Errorf("register core_store_del: %w", err)
	}

	// core_store_incr(store, key) → BIGINT
	type incrArgs struct {
		store string
		key   string
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[incrArgs, int64]{
		Name: "core_store_incr",
		Execute: func(ctx context.Context, args incrArgs) (int64, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return 0, err
			}
			return st.Incr(ctx, args.key)
		},
		ConvertInput: func(args []driver.Value) (incrArgs, error) {
			return incrArgs{store: args[0].(string), key: args[1].(string)}, nil
		},
		ConvertOutput: func(out int64) (any, error) { return out, nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: runtime.DuckDBTypeInfoByNameMust("BIGINT"),
	})
	if err != nil {
		return fmt.Errorf("register core_store_incr: %w", err)
	}

	// core_store_expire(store, key, ttl) → OperationResult
	type expireArgs struct {
		store string
		key   string
		ttl   int32
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[expireArgs, *types.OperationResult]{
		Name: "core_store_expire",
		Execute: func(ctx context.Context, args expireArgs) (*types.OperationResult, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return types.ErrResult(err), nil
			}
			if err := st.Expire(ctx, args.key, int(args.ttl)); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("OK", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (expireArgs, error) {
			return expireArgs{store: args[0].(string), key: args[1].(string), ttl: args[2].(int32)}, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
	if err != nil {
		return fmt.Errorf("register core_store_expire: %w", err)
	}

	// core_store_keys(store, pattern) → LIST(VARCHAR)
	type keysArgs struct {
		store   string
		pattern string
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[keysArgs, []string]{
		Name: "core_store_keys",
		Execute: func(ctx context.Context, args keysArgs) ([]string, error) {
			st, err := s.resolveStore(args.store)
			if err != nil {
				return nil, err
			}
			return st.Keys(ctx, args.pattern)
		},
		ConvertInput: func(args []driver.Value) (keysArgs, error) {
			return keysArgs{store: args[0].(string), pattern: args[1].(string)}, nil
		},
		ConvertOutput: func(out []string) (any, error) {
			return out, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: runtime.DuckDBListInfoByNameMust("VARCHAR"),
	})
	if err != nil {
		return fmt.Errorf("register core_store_keys: %w", err)
	}

	return nil
}

var _ sources.RuntimeSourceDataSourceUser = (*Source)(nil)
