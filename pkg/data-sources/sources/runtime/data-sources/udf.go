package dssource

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"

	_ "embed"
)

func (s *Source) registerUDF(ctx context.Context) error {
	ctx = auth.ContextWithFullAccess(ctx)
	err := s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, string]{
		Name: "data_source_status",
		Execute: func(ctx context.Context, name string) (string, error) {
			return s.qe.DataSourceStatus(ctx, name)
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			name := args[0].(string)
			return name, nil
		},
		ConvertOutput: func(out string) (any, error) {
			return out, nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
	})
	if err != nil {
		return err
	}

	err = s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "load_data_source",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			err := s.qe.LoadDataSource(ctx, name)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Datasource was loaded", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			name := args[0].(string)
			return name, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: db.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	type unloadArgs struct {
		name string
		hard bool
	}
	err = s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[unloadArgs, *types.OperationResult]{
		Name: "unload_data_source",
		Execute: func(ctx context.Context, args unloadArgs) (*types.OperationResult, error) {
			var opts []types.UnloadOpt
			if args.hard {
				opts = append(opts, types.WithHardUnload())
			}
			err := s.qe.UnloadDataSource(ctx, args.name, opts...)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Datasource was unloaded", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (unloadArgs, error) {
			a := unloadArgs{name: args[0].(string)}
			if len(args) > 1 && args[1] != nil {
				a.hard = args[1].(bool)
			}
			return a, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType:            db.DuckDBOperationResult(),
		IsSpecialNullHandling: true,
	})
	if err != nil {
		return err
	}

	err = s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "checkpoint_db",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			if name != "" {
				name = base.Ident(name)
			}
			_, err := s.db.Exec(ctx, "CHECKPOINT "+name)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Datasource was checkpointed", 0, 0), nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", errors.New("invalid number of arguments")
			}
			name := args[0].(string)
			return name, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType: db.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	type describeArgs struct {
		name string
		self bool
		log  bool
	}

	err = s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[describeArgs, string]{
		Name: "describe_data_source_schema",
		Execute: func(ctx context.Context, p describeArgs) (string, error) {
			out, err := s.qe.DescribeDataSource(ctx, p.name, p.self)
			if err != nil {
				return "", err
			}
			if p.log {
				log.Printf("describe_data_source_schema output: %s", out)
			}
			return out, nil
		},
		ConvertInput: func(args []driver.Value) (describeArgs, error) {
			if len(args) != 3 {
				return describeArgs{}, fmt.Errorf("invalid number of arguments: expected 3, got %d", len(args))
			}
			return describeArgs{
				name: args[0].(string),
				self: args[1].(bool),
				log:  args[2].(bool),
			}, nil
		},
		ConvertOutput: func(out string) (any, error) {
			return out, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
		},
		OutputType: runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
	})

	return err
}
