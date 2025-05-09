package dssource

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/marcboeker/go-duckdb/v2"

	_ "embed"
)

func (s *Source) registerUDF(ctx context.Context) error {
	t, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		return err
	}
	ctx = auth.ContextWithFullAccess(ctx)
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[string, string]{
		Name:        "data_source_status",
		Description: "Get the status of a data source",
		Module:      "core",
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
		InputTypes: []duckdb.TypeInfo{t},
		OutputType: t,
	})
	if err != nil {
		return err
	}

	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name:        "load_data_source",
		Description: "Load/Reload data source",
		Module:      "core",
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
		InputTypes: []duckdb.TypeInfo{t},
		OutputType: types.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name:        "unload_data_source",
		Description: "Unload data source",
		Module:      "core",
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
		InputTypes: []duckdb.TypeInfo{t},
		OutputType: types.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	return nil
}
