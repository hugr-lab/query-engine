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
	return "storage"
}

func (s *Source) Engine() engines.Engine {
	return engines.NewDuckDB()
}

func (s *Source) IsReadonly() bool {
	return false
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) error {
	s.db = db
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	// register views
	duckdb.RegisterReplacementScan(db.Connector(), s.storagesScan)

	err = duckdb.RegisterTableUDF(c.DBConn(), "s3_secrets", duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (duckdb.RowTableSource, error) {
			return &storagesUDF{s: s, ctx: ctx}, nil
		},
	})
	if err != nil {
		return err
	}

	// register UDFs
	err = duckdb.RegisterScalarUDFSet(c.DBConn(), "register_s3", &registerUDF{s: s, ctx: ctx})
	if err != nil {
		return err
	}

	err = duckdb.RegisterScalarUDFSet(c.DBConn(), "unregister_s3", &unregisterUDF{s: s, ctx: ctx})
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

func (s *Source) storagesScan(tableName string) (string, []any, error) {
	if tableName != "core_registered_s3" {
		return "", nil, &duckdb.Error{
			Type: duckdb.ErrorTypeCatalog,
		}
	}
	return "s3_secrets", nil, nil
}

type storagesUDF struct {
	s   *Source
	ctx context.Context

	err    error
	data   []secrets
	curRow int
}

type secrets struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Scope []string `json:"scope"`
	Value string   `json:"secret_string"`
}

func (f *storagesUDF) Init() {
	f.curRow = 0

	f.err = f.s.db.QueryTableToSlice(f.ctx, &f.data, `
		SELECT name, "type", "scope", secret_string
		FROM duckdb_secrets()
		WHERE "type" = 's3'
	`)
}

func (f *storagesUDF) ColumnInfos() []duckdb.ColumnInfo {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	tb, _ := duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	tl, _ := duckdb.NewListInfo(t)
	return []duckdb.ColumnInfo{
		{Name: "name", T: t},
		{Name: "type", T: t},
		{Name: "key", T: t},
		{Name: "scope", T: tl},
		{Name: "region", T: t},
		{Name: "endpoint", T: t},
		{Name: "use_ssl", T: tb},
		{Name: "url_style", T: t},
	}
}

func (f *storagesUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(len(f.data)),
		Exact:       true,
	}
}

func (f *storagesUDF) FillRow(row duckdb.Row) (bool, error) {
	if f.err != nil {
		return false, f.err
	}
	if f.curRow >= len(f.data) {
		return false, nil
	}

	f.curRow++
	err := duckdb.SetRowValue(row, 0, f.data[f.curRow-1].Name)
	if err != nil {
		return false, err
	}
	err = duckdb.SetRowValue(row, 1, f.data[f.curRow-1].Type)
	if err != nil {
		return false, err
	}
	err = duckdb.SetRowValue(row, 3, f.data[f.curRow-1].Scope)
	if err != nil {
		return false, err
	}
	for v := range strings.SplitSeq(f.data[f.curRow-1].Value, ";") {
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
			return false, err
		}
	}
	return true, nil
}

type registerUDF struct {
	s   *Source
	ctx context.Context
}

func (f *registerUDF) Config() duckdb.ScalarFuncConfig {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	tb, _ := duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{
			t, t, t, t, t, tb, t, t,
		},
		ResultTypeInfo: types.DuckDBOperationResult(),
		Volatile:       true,
	}
}

func (f *registerUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			info := S3Info{
				Name:     values[0].(string),
				Type:     "s3",
				KeyID:    values[1].(string),
				Secret:   values[2].(string),
				Region:   values[3].(string),
				Endpoint: values[4].(string),
				UseSSL:   values[5].(bool),
				URLStyle: values[6].(string),
				Scope:    values[7].(string),
			}
			err := f.s.RegisterS3(f.ctx, info)
			if err != nil {
				return types.ErrResult(err).ToDuckdb(), nil
			}
			return types.Result("s3 storage registered", 1, 0).ToDuckdb(), nil
		},
	}
}

type unregisterUDF struct {
	s   *Source
	ctx context.Context
}

func (f *unregisterUDF) Config() duckdb.ScalarFuncConfig {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{t},
		ResultTypeInfo: types.DuckDBOperationResult(),
		Volatile:       true,
	}
}

func (f *unregisterUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			name := values[0].(string)
			err := f.s.UnregisterS3(f.ctx, name)
			if err != nil {
				return types.ErrResult(err).ToDuckdb(), nil
			}
			return types.Result("s3 storage unregistered", 1, 0).ToDuckdb(), nil
		},
	}
}
