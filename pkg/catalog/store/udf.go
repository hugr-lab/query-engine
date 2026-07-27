package store

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

// The schema-management UDFs are the SQL (and therefore GraphQL) entry points
// to curation and catalog maintenance. The four `_schema_update_*_desc`
// functions the summarizer already calls keep their names and signatures, so
// the tooling works unchanged when the engine switches to entity storage; what
// changes is where they write — one annotations row instead of a column on the
// compiled type.
//
// Entity storage curates TWO surfaces (annotate.go), and only one of them was
// reachable before: the LOGICAL model (data objects, their fields, functions,
// modules, source types, data sources) and the GENERATED GraphQL surface
// (types, fields, arguments produced by generation). The legacy names address
// the generated surface — that is what the old provider stored — so the logical
// kinds get their own `_schema_update_*_desc` names alongside them.
//
// Every function returns an OperationResult and reports failures THROUGH it
// (types.ErrResult) rather than as a DuckDB error, so a failed curation does
// not abort the calling statement.

// CatalogChecker reports whether a catalog (data source) currently has an
// active engine — _schema_hard_remove refuses to unregister a loaded source.
// Implemented by catalog.Service.
type CatalogChecker interface {
	ExistsCatalog(name string) bool
}

// curationUDF is one description-curation function: an all-VARCHAR signature
// whose last two arguments are always description / long_description.
type curationUDF struct {
	name string
	argc int
	msg  string
	set  func(ctx context.Context, args []string) error
}

// RegisterUDFs registers the schema-management UDFs on the CoreDB pool. The
// checker gates _schema_hard_remove; nil skips the gate.
func (s *Store) RegisterUDFs(ctx context.Context, checker CatalogChecker) error {
	curations := []curationUDF{
		// --- generated GraphQL surface (legacy names, unchanged signatures) ---
		{"_schema_update_type_desc", 3, "type description updated", func(ctx context.Context, a []string) error {
			return s.SetDefinitionDescription(ctx, a[0], a[1], a[2])
		}},
		{"_schema_update_field_desc", 4, "field description updated", func(ctx context.Context, a []string) error {
			return s.SetFieldDescription(ctx, a[0], a[1], a[2], a[3])
		}},
		{"_schema_update_argument_desc", 5, "argument description updated", func(ctx context.Context, a []string) error {
			return s.SetArgumentDescription(ctx, a[0], a[1], a[2], a[3], a[4])
		}},

		// --- logical model ---
		{"_schema_update_module_desc", 3, "module description updated", func(ctx context.Context, a []string) error {
			return s.SetModuleDescription(ctx, a[0], a[1], a[2])
		}},
		{"_schema_update_catalog_desc", 3, "catalog description updated", func(ctx context.Context, a []string) error {
			return s.SetDataSourceDescription(ctx, a[0], a[1], a[2])
		}},
		{"_schema_update_data_object_desc", 3, "data object description updated", func(ctx context.Context, a []string) error {
			return s.SetDataObjectDescription(ctx, a[0], a[1], a[2])
		}},
		{"_schema_update_data_object_field_desc", 4, "data object field description updated", func(ctx context.Context, a []string) error {
			return s.SetObjectFieldDescription(ctx, a[0], a[1], a[2], a[3])
		}},
		{"_schema_update_source_type_desc", 3, "source type description updated", func(ctx context.Context, a []string) error {
			return s.SetSourceTypeDescription(ctx, a[0], a[1], a[2])
		}},
		{"_schema_update_function_desc", 4, "function description updated", func(ctx context.Context, a []string) error {
			return s.SetFunctionDescription(ctx, a[0], a[1], a[2], a[3])
		}},
	}
	for _, c := range curations {
		if err := s.registerCuration(ctx, c); err != nil {
			return err
		}
	}

	// _schema_hard_remove(name) — unregister: rows, metadata, dependencies and
	// seed annotations go; curations stay as tolerated orphans.
	if err := s.registerOperation(ctx, "_schema_hard_remove", 1,
		func(ctx context.Context, a []string) (*types.OperationResult, error) {
			if checker != nil && checker.ExistsCatalog(a[0]) {
				return types.ErrResult(errors.New("catalog is loaded: unload it before hard removal")), nil
			}
			if err := s.RemoveCatalog(ctx, a[0]); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("catalog hard removed", 1, 0), nil
		}); err != nil {
		return err
	}

	// _schema_version_clean(name) — invalidate the stored version so the next
	// load recompiles and rewrites the source instead of taking the gate.
	if err := s.registerOperation(ctx, "_schema_version_clean", 1,
		func(ctx context.Context, a []string) (*types.OperationResult, error) {
			if err := s.ClearSourceVersion(ctx, a[0]); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("catalog version cleared", 1, 0), nil
		}); err != nil {
		return err
	}

	// _schema_reindex(name, batch_size) — recompute embedding vectors.
	return db.RegisterScalarFunction(ctx, s.pool, &db.ScalarFunctionWithArgs[reindexArgs, *types.OperationResult]{
		Name: "_schema_reindex",
		Execute: func(ctx context.Context, args reindexArgs) (*types.OperationResult, error) {
			count, err := s.ReindexEmbeddings(ctx, args.name, int(args.batchSize))
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
			return reindexArgs{name: name, batchSize: batchSize}, nil
		},
		ConvertOutput: convertOperationResult,
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
		},
		OutputType: db.DuckDBOperationResult(),
	})
}

type reindexArgs struct {
	name      string
	batchSize int32
}

// registerCuration registers one description-curation function: the annotator
// call plus the fixed "1 row affected" success result.
func (s *Store) registerCuration(ctx context.Context, c curationUDF) error {
	return s.registerOperation(ctx, c.name, c.argc,
		func(ctx context.Context, args []string) (*types.OperationResult, error) {
			if err := c.set(ctx, args); err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result(c.msg, 1, 0), nil
		})
}

// registerOperation registers an all-VARCHAR scalar function returning an
// OperationResult.
func (s *Store) registerOperation(ctx context.Context, name string, argc int,
	exec func(ctx context.Context, args []string) (*types.OperationResult, error),
) error {
	return db.RegisterScalarFunction(ctx, s.pool, &db.ScalarFunctionWithArgs[[]string, *types.OperationResult]{
		Name:          name,
		Execute:       exec,
		ConvertInput:  stringArgs(argc),
		ConvertOutput: convertOperationResult,
		InputTypes:    varcharTypes(argc),
		OutputType:    db.DuckDBOperationResult(),
	})
}

// stringArgs converts exactly n VARCHAR arguments.
func stringArgs(n int) func(args []driver.Value) ([]string, error) {
	return func(args []driver.Value) ([]string, error) {
		if len(args) != n {
			return nil, fmt.Errorf("expected %d arguments, got %d", n, len(args))
		}
		out := make([]string, n)
		for i, a := range args {
			v, ok := a.(string)
			if !ok {
				return nil, fmt.Errorf("expected string for argument %d, got %T", i+1, a)
			}
			out[i] = v
		}
		return out, nil
	}
}

func varcharTypes(n int) []duckdb.TypeInfo {
	out := make([]duckdb.TypeInfo, n)
	for i := range out {
		out[i] = runtime.DuckDBTypeInfoByNameMust("VARCHAR")
	}
	return out
}

func convertOperationResult(out *types.OperationResult) (any, error) {
	return out.ToDuckdb(), nil
}
