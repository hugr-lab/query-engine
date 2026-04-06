package datasources

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
)

func (s *Service) RegisterUDF(ctx context.Context) error {
	type httpDataSourceRequestArgs struct {
		source  string // source (catalog)
		path    string // path
		method  string // method
		headers string // headers JSON
		params  string // params JSON
		body    string // body JSON
		jq      string // jq string
	}
	err := db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[httpDataSourceRequestArgs, any]{
		Name: "http_data_source_request_scalar",
		Execute: func(ctx context.Context, args httpDataSourceRequestArgs) (any, error) {
			return s.HttpRequest(ctx, args.source, args.path, args.method, args.headers, args.params, args.body, args.jq)
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // source (catalog)
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // path
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // method
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // headers JSON
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // params JSON
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // body JSON
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // jq string
		},
		ConvertInput: func(args []driver.Value) (httpDataSourceRequestArgs, error) {
			if len(args) != 7 {
				return httpDataSourceRequestArgs{}, errors.New("invalid number of arguments")
			}
			return httpDataSourceRequestArgs{
				source:  args[0].(string), // source (catalog)
				path:    args[1].(string), // path
				method:  args[2].(string), // method
				headers: args[3].(string), // headers JSON
				params:  args[4].(string), // params JSON
				body:    args[5].(string), // body JSON
				jq:      args[6].(string), // jq string
			}, nil
		},
		ConvertOutput: func(out any) (any, error) {
			b, err := json.Marshal(out)
			if err != nil {
				return "", fmt.Errorf("marshal data: %w", err)
			}
			return string(b), nil
		},
		OutputType: runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
	})
	if err != nil {
		return fmt.Errorf("register http_data_source_request_scalar function: %w", err)
	}

	type embeddingArgs struct {
		source string // source (model)
		input  string // input text
	}

	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[embeddingArgs, ctypes.Vector]{
		Name: "create_embedding",
		Execute: func(ctx context.Context, args embeddingArgs) (ctypes.Vector, error) {
			result, err := s.CreateEmbedding(ctx, args.source, args.input)
			if err != nil {
				return nil, err
			}
			return result.Vector, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // source (catalog)
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"), // input text
		},
		ConvertInput: func(args []driver.Value) (embeddingArgs, error) {
			if len(args) != 2 {
				return embeddingArgs{}, errors.New("invalid number of arguments")
			}
			return embeddingArgs{
				source: args[0].(string), // source (catalog)
				input:  args[1].(string), // input text
			}, nil
		},
		ConvertOutput: func(out ctypes.Vector) (any, error) {
			return out, nil
		},
		OutputType: runtime.DuckDBListInfoByNameMust("FLOAT"),
	})
	if err != nil {
		return fmt.Errorf("register create_embedding function: %w", err)
	}

	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionNoArgs[EmbedderSettings]{
		Name: "hugr_embedder_settings",
		Execute: func(ctx context.Context) (EmbedderSettings, error) {
			return s.embedderSettings, nil
		},
		ConvertOutput: func(out EmbedderSettings) (any, error) {
			return map[string]any{
				"name":       out.Name,
				"model":      out.Model,
				"dimensions": out.Dimensions,
				"is_enabled": out.IsEnabled,
			}, nil
		},
		OutputType: duckDBEmbedderSettingsType,
	})
	if err != nil {
		return fmt.Errorf("register hugr_embedder_settings function: %w", err)
	}

	return nil
}

var duckDBEmbedderSettingsType = runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
	"is_enabled": duckdb.TYPE_BOOLEAN,
	"name":       duckdb.TYPE_VARCHAR,
	"model":      duckdb.TYPE_VARCHAR,
	"dimensions": duckdb.TYPE_BIGINT,
})
