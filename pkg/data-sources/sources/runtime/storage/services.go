package storage

// Runtime source that provides functionality to manage file storages
// register and unregister s3 file storages to access data
import (
	"context"
	"database/sql/driver"
	_ "embed"
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

//go:embed schema.graphql
var schema string

type Source struct {
	db *db.Pool
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string {
	return "core.storage"
}

func (s *Source) Engine() engines.Engine {
	return engines.NewDuckDB()
}

func (s *Source) IsReadonly() bool {
	return false
}

func (s *Source) AsModule() bool {
	return true
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	err := pool.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[SecretInfo, *types.OperationResult]{
		Name: "register_object_storage",
		Execute: func(ctx context.Context, info SecretInfo) (*types.OperationResult, error) {
			err := s.RegisterSecret(ctx, info)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Object storage registered", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (SecretInfo, error) {
			if len(args) != 12 {
				return SecretInfo{}, fmt.Errorf("invalid number of arguments")
			}
			params := make(map[string]any)
			for i := 3; i < 12; i++ {
				if args[i] == nil {
					continue
				}
				switch i {
				case 3:
					params["KEY_ID"] = args[i].(string)
				case 4:
					params["SECRET"] = args[i].(string)
				case 5:
					if args[i].(string) != "" {
						params["REGION"] = args[i].(string)
					}
				case 6:
					params["ENDPOINT"] = args[i].(string)
				case 7:
					params["USE_SSL"] = args[i].(bool)
				case 8:
					params["URL_STYLE"] = args[i].(string)
				case 9:
					if args[i].(bool) {
						params["URL_COMPATIBILITY_MODE"] = true
					}
				case 10:
					if args[i].(string) != "" {
						params["KMS_KEY_ID"] = args[i].(string)
					}
				case 11:
					if args[i].(string) != "" {
						params["ACCOUNT_ID"] = args[i].(string)
					}
				}
			}
			return SecretInfo{
				Type:       args[0].(string),
				Name:       args[1].(string),
				Scope:      args[2].(string),
				Parameters: params,
			}, nil
		},
		ConvertOutput: func(out *types.OperationResult) (any, error) {
			return out.ToDuckdb(), nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	err = pool.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "unregister_object_storage",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			err := s.UnregisterSecret(ctx, name)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("Object storage unregistered", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (string, error) {
			if len(args) != 1 {
				return "", fmt.Errorf("invalid number of arguments")
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
	// register views
	duckdb.RegisterReplacementScan(pool.Connector(), func(tableName string) (string, []any, error) {
		if tableName != "core_registered_object_storages" {
			return "", nil, &duckdb.Error{
				Type: duckdb.ErrorTypeCatalog,
			}
		}
		return "core_object_storages", nil, nil
	})

	err = pool.RegisterTableRowFunction(ctx, &db.TableRowFunctionNoArgs[secrets]{
		Name: "core_object_storages",
		ColumnInfos: []duckdb.ColumnInfo{
			{Name: "name", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "type", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "scope", T: runtime.DuckDBListInfoByNameMust("VARCHAR")},
			{Name: "parameters", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
		},
		Execute: func(ctx context.Context) ([]secrets, error) {
			var data []secrets
			err := pool.QueryTableToSlice(ctx, &data, `
				SELECT name, "type", "scope", secret_string
				FROM duckdb_secrets()
				WHERE "type" = 's3'
			`)
			return data, err
		},
		FillRow: func(out secrets, row duckdb.Row) error {
			err := duckdb.SetRowValue(row, 0, out.Name)
			if err != nil {
				return err
			}
			err = duckdb.SetRowValue(row, 1, out.Type)
			if err != nil {
				return err
			}
			err = duckdb.SetRowValue(row, 2, out.Scope)
			if err != nil {
				return err
			}
			err = duckdb.SetRowValue(row, 3, out.Value)
			return err
		},
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Source) Catalog(ctx context.Context) sources.Source {
	return sources.NewStringSource("storage", schema)
}

func (s *Source) RegisterSecret(ctx context.Context, info SecretInfo) error {
	if info.Type != "S3" && info.Type != "R2" && info.Type != "GCS" {
		return fmt.Errorf("unsupported secret type: %s", info.Type)
	}
	if len(info.Parameters) == 0 {
		return fmt.Errorf("no parameters provided")
	}

	var params []string
	for k, v := range info.Parameters {
		vv, err := s.Engine().SQLValue(v)
		if err != nil {
			return fmt.Errorf("invalid parameter value for %s: %w", k, err)
		}
		params = append(params, fmt.Sprintf("%s %s", k, vv))
	}

	_, err := s.db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE PERSISTENT SECRET %s (
			TYPE %s,
			SCOPE '%s',
			%s
		);
	`, info.Name, info.Type, info.Scope, strings.Join(params, ", ")))
	if err != nil {
		return err
	}
	return nil
}

func (s *Source) UnregisterSecret(ctx context.Context, name string) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(`DROP PERSISTENT SECRET %s`, name))
	return err
}

type SecretInfo struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Scope      string         `json:"scope"`
	Parameters map[string]any `json:"parameters"`
}

type secrets struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Scope []string `json:"scope"`
	Value string   `json:"secret_string"`
}
