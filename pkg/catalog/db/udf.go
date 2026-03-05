package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
)

// CatalogChecker checks whether a catalog (data source) has an active engine.
type CatalogChecker interface {
	ExistsCatalog(name string) bool
}

// RegisterUDFs registers all schema management UDFs on the Provider's DB pool.
// The checker is used by _schema_hard_remove to verify the catalog is not loaded.
func (p *Provider) RegisterUDFs(ctx context.Context, checker CatalogChecker) error {
	pool := p.pool

	// _schema_update_type_desc(name, description, long_description)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[descArgs, *types.OperationResult]{
		Name: "_schema_update_type_desc",
		Execute: func(ctx context.Context, args descArgs) (*types.OperationResult, error) {
			err := p.SetDefinitionDescription(ctx, args.name, args.description, args.longDescription)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("type description updated", 1, 0), nil
		},
		ConvertInput:  convertDescArgs,
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_update_field_desc(type_name, name, description, long_description)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[fieldDescArgs, *types.OperationResult]{
		Name: "_schema_update_field_desc",
		Execute: func(ctx context.Context, args fieldDescArgs) (*types.OperationResult, error) {
			err := p.SetFieldDescription(ctx, args.typeName, args.name, args.description, args.longDescription)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("field description updated", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (fieldDescArgs, error) {
			if len(args) != 4 {
				return fieldDescArgs{}, fmt.Errorf("expected 4 arguments, got %d", len(args))
			}
			typeName, ok := args[0].(string)
			if !ok {
				return fieldDescArgs{}, fmt.Errorf("expected string for type_name, got %T", args[0])
			}
			name, ok := args[1].(string)
			if !ok {
				return fieldDescArgs{}, fmt.Errorf("expected string for name, got %T", args[1])
			}
			description, ok := args[2].(string)
			if !ok {
				return fieldDescArgs{}, fmt.Errorf("expected string for description, got %T", args[2])
			}
			longDescription, ok := args[3].(string)
			if !ok {
				return fieldDescArgs{}, fmt.Errorf("expected string for long_description, got %T", args[3])
			}
			return fieldDescArgs{
				typeName:        typeName,
				name:            name,
				description:     description,
				longDescription: longDescription,
			}, nil
		},
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_update_module_desc(name, description, long_description)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[descArgs, *types.OperationResult]{
		Name: "_schema_update_module_desc",
		Execute: func(ctx context.Context, args descArgs) (*types.OperationResult, error) {
			err := p.SetModuleDescription(ctx, args.name, args.description, args.longDescription)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("module description updated", 1, 0), nil
		},
		ConvertInput:  convertDescArgs,
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_update_catalog_desc(name, description, long_description)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[descArgs, *types.OperationResult]{
		Name: "_schema_update_catalog_desc",
		Execute: func(ctx context.Context, args descArgs) (*types.OperationResult, error) {
			err := p.SetCatalogDescription(ctx, args.name, args.description, args.longDescription)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("catalog description updated", 1, 0), nil
		},
		ConvertInput:  convertDescArgs,
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_hard_remove(name)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "_schema_hard_remove",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			if checker != nil && checker.ExistsCatalog(name) {
				return types.ErrResult(errors.New("catalog is loaded: unload it before hard removal")), nil
			}
			err := p.DropCatalog(ctx, name, true)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("catalog hard removed", 1, 0), nil
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_version_clean(name)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "_schema_version_clean",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			err := p.SetCatalogVersion(ctx, name, "")
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("catalog version cleared", 1, 0), nil
		},
		ConvertInput:  convertStringArg,
		ConvertOutput: convertOperationResult,
		InputTypes:    []duckdb.TypeInfo{runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		OutputType:    types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_reset_summarized(name, scope)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[resetSummarizedArgs, *types.OperationResult]{
		Name: "_schema_reset_summarized",
		Execute: func(ctx context.Context, args resetSummarizedArgs) (*types.OperationResult, error) {
			count, err := p.ResetSummarized(ctx, args.name, args.scope)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(fmt.Sprintf("reset %d entities", count), count, 0), nil
		},
		ConvertInput: func(args []driver.Value) (resetSummarizedArgs, error) {
			if len(args) != 2 {
				return resetSummarizedArgs{}, fmt.Errorf("expected 2 arguments, got %d", len(args))
			}
			name, ok := args[0].(string)
			if !ok {
				return resetSummarizedArgs{}, fmt.Errorf("expected string for name, got %T", args[0])
			}
			scope, ok := args[1].(string)
			if !ok {
				return resetSummarizedArgs{}, fmt.Errorf("expected string for scope, got %T", args[1])
			}
			return resetSummarizedArgs{
				name:  name,
				scope: scope,
			}, nil
		},
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	// _schema_reindex(name, batch_size)
	if err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[reindexArgs, *types.OperationResult]{
		Name: "_schema_reindex",
		Execute: func(ctx context.Context, args reindexArgs) (*types.OperationResult, error) {
			if !p.HasEmbeddings() {
				return types.ErrResult(errors.New("embeddings not configured (EMBEDDER_URL not set)")), nil
			}
			batchSize := int(args.batchSize)
			if batchSize <= 0 {
				batchSize = 50
			}
			if batchSize > 200 {
				batchSize = 200
			}
			count, err := p.ReindexEmbeddings(ctx, args.name, batchSize)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(fmt.Sprintf("reindexed %d entities", count), count, 0), nil
		},
		ConvertInput: func(args []driver.Value) (reindexArgs, error) {
			if len(args) != 2 {
				return reindexArgs{}, fmt.Errorf("expected 2 arguments, got %d", len(args))
			}
			name, ok := args[0].(string)
			if !ok {
				return reindexArgs{}, fmt.Errorf("expected string for name, got %T", args[0])
			}
			batchSize, ok := args[1].(int32)
			if !ok {
				return reindexArgs{}, fmt.Errorf("expected int32 for batch_size, got %T", args[1])
			}
			return reindexArgs{
				name:      name,
				batchSize: batchSize,
			}, nil
		},
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
		},
		OutputType: types.DuckDBOperationResult(),
	}); err != nil {
		return err
	}

	return nil
}

// --- Shared types and converters ---

type descArgs struct {
	name            string
	description     string
	longDescription string
}

type fieldDescArgs struct {
	typeName        string
	name            string
	description     string
	longDescription string
}

type reindexArgs struct {
	name      string
	batchSize int32
}

type resetSummarizedArgs struct {
	name  string
	scope string
}

func convertDescArgs(args []driver.Value) (descArgs, error) {
	if len(args) != 3 {
		return descArgs{}, fmt.Errorf("expected 3 arguments, got %d", len(args))
	}
	name, ok := args[0].(string)
	if !ok {
		return descArgs{}, fmt.Errorf("expected string for name, got %T", args[0])
	}
	description, ok := args[1].(string)
	if !ok {
		return descArgs{}, fmt.Errorf("expected string for description, got %T", args[1])
	}
	longDescription, ok := args[2].(string)
	if !ok {
		return descArgs{}, fmt.Errorf("expected string for long_description, got %T", args[2])
	}
	return descArgs{
		name:            name,
		description:     description,
		longDescription: longDescription,
	}, nil
}

func convertStringArg(args []driver.Value) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("expected 1 argument, got %d", len(args))
	}
	s, ok := args[0].(string)
	if !ok {
		return "", fmt.Errorf("expected string, got %T", args[0])
	}
	return s, nil
}

func convertOperationResult(out *types.OperationResult) (any, error) {
	return out.ToDuckdb(), nil
}
