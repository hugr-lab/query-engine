package storage

// Runtime source that provides functionality to manage file storages
// register and unregister s3 file storages to access data
import (
	"context"
	"database/sql/driver"
	_ "embed"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/marcboeker/go-duckdb/v2"
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
	err := db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[S3Info, *types.OperationResult]{
		Name: "register_s3",
		Execute: func(ctx context.Context, info S3Info) (*types.OperationResult, error) {
			err := s.RegisterS3(ctx, info)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("S3 storage registered", 1, 0), nil
		},
		ConvertInput: func(args []driver.Value) (S3Info, error) {
			if len(args) != 8 {
				return S3Info{}, fmt.Errorf("invalid number of arguments")
			}
			return S3Info{
				Name:     args[0].(string),
				Type:     "s3",
				KeyID:    args[1].(string),
				Secret:   args[2].(string),
				Region:   args[3].(string),
				Endpoint: args[4].(string),
				UseSSL:   args[5].(bool),
				URLStyle: args[6].(string),
				Scope:    args[7].(string),
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
			runtime.DuckDBTypeInfoByNameMust("BOOLEAN"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: types.DuckDBOperationResult(),
	})
	if err != nil {
		return err
	}

	err = db.RegisterScalarFunction(ctx, pool, &db.ScalarFunctionWithArgs[string, *types.OperationResult]{
		Name: "unregister_s3",
		Execute: func(ctx context.Context, name string) (*types.OperationResult, error) {
			err := s.UnregisterS3(ctx, name)
			if err != nil {
				return types.ErrResult(err), nil
			}
			return types.Result("S3 storage unregistered", 1, 0), nil
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
		if tableName != "core_registered_s3" {
			return "", nil, &duckdb.Error{
				Type: duckdb.ErrorTypeCatalog,
			}
		}
		return "s3_secrets", nil, nil
	})

	err = db.RegisterTableRowFunction(ctx, pool, &db.TableRowFunctionNoArgs[secrets]{
		Name: "s3_secrets",
		ColumnInfos: []duckdb.ColumnInfo{
			{Name: "name", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "type", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "key", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "scope", T: runtime.DuckDBListInfoByNameMust("VARCHAR")},
			{Name: "region", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "endpoint", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "use_ssl", T: runtime.DuckDBTypeInfoByNameMust("BOOLEAN")},
			{Name: "url_style", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
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
			err = duckdb.SetRowValue(row, 3, out.Scope)
			if err != nil {
				return err
			}
			for v := range strings.SplitSeq(out.Value, ";") {
				switch {
				case strings.HasPrefix(v, "key_id="):
					err = duckdb.SetRowValue(row, 2, strings.TrimPrefix(v, "key_id="))
				case strings.HasPrefix(v, "region="):
					err = duckdb.SetRowValue(row, 4, strings.TrimPrefix(v, "region="))
				case strings.HasPrefix(v, "endpoint="):
					err = duckdb.SetRowValue(row, 5, strings.TrimPrefix(v, "endpoint="))
				case strings.HasPrefix(v, "use_ssl="):
					err = duckdb.SetRowValue(row, 6, v == "use_ssl=true")
				case strings.HasPrefix(v, "url_style="):
					err = duckdb.SetRowValue(row, 7, strings.TrimPrefix(v, "url_style="))
				default:
					continue
				}
				if err != nil {
					return err
				}
			}
			return nil
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

type S3Info struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	KeyID    string `json:"key_id"`
	Secret   string `json:"secret"`
	Region   string `json:"region"`
	Endpoint string `json:"endpoint"`
	UseSSL   bool   `json:"use_ssl"`
	URLStyle string `json:"url_style"`
	Scope    string `json:"scope"`
}

func (s *Source) RegisterS3(ctx context.Context, info S3Info) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE PERSISTENT SECRET %s (
			TYPE %s,
			KEY_ID '%s',
			SECRET '%s',
			REGION '%s',
			ENDPOINT '%s',
			USE_SSL %v,
			URL_STYLE '%s',
			SCOPE '%s'
		);
	`, info.Name, info.Type, info.KeyID, info.Secret, info.Region, info.Endpoint, info.UseSSL, info.URLStyle, info.Scope))
	if err != nil {
		return err
	}
	return nil
}

func (s *Source) UnregisterS3(ctx context.Context, name string) error {
	_, err := s.db.Exec(ctx, fmt.Sprintf(`DROP PERSISTENT SECRET %s`, name))
	return err
}

type secrets struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Scope []string `json:"scope"`
	Value string   `json:"secret_string"`
}
