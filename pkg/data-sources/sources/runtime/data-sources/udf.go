package dssource

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/marcboeker/go-duckdb/v2"

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
		OutputType: types.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	err = s.db.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "unload_data_source",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			err := s.qe.UnloadDataSource(ctx, name)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Datasource was unloaded", 0, 0), nil
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
		OutputType: types.DuckDBOperationResult(),
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
				fmt.Println("describe_data_source_schema output:", out)
			}
			return out, nil
		},
		ConvertInput: func(args []driver.Value) (describeArgs, error) {
			if len(args) != 3 {
				return describeArgs{}, errors.New("invalid number of arguments")
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
