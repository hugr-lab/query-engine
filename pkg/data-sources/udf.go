package datasources

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/marcboeker/go-duckdb/v2"

	_ "embed"
)

//go:embed udf.graphql
var schema string

func (s *Service) RegisterUDF(ctx context.Context) error {
	c, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx = auth.ContextWithFullAccess(ctx)
	err = duckdb.RegisterTableUDF(c.DBConn(), "http_data_source_request", s.httpRequestTableUDF())
	if err != nil {
		return err
	}

	err = duckdb.RegisterScalarUDF(c.DBConn(), "http_data_source_request_scalar", s.httpRequestScalarUDF())
	if err != nil {
		return err
	}
	return s.registerUDFCatalog(ctx)
}

func (s *Service) registerUDFCatalog(ctx context.Context) error {
	c, err := catalogs.NewCatalog(ctx, "data_sources",
		"",
		&engines.DuckDB{},
		sources.NewStringSource(schema),
		false,
		false,
	)
	if err != nil {
		return err
	}
	err = s.catalogs.AddCatalog(ctx, "data_sources", c)
	if err != nil {
		return fmt.Errorf("register data_sources catalog: %w", err)
	}
	return s.catalogs.RebuildSchema(ctx)
}

func (s *Service) httpRequestScalarUDF() duckdb.ScalarFunc {
	return &httpRequestScalarUDF{s: s}
}

func (s *Service) httpRequestTableUDF() duckdb.RowTableFunction {
	ts, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)

	return duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{
				ts, // source (catalog)
				ts, // path
				ts, // method
				ts, // headers JSON
				ts, // params JSON
				ts, // body JSON
				ts, // jq string
			},
		},
		BindArguments: func(named map[string]any, args ...any) (duckdb.RowTableSource, error) {
			// check if named arguments are provided
			if len(args) < 7 {
				return nil, &duckdb.Error{Type: duckdb.ErrorTypeBinder, Msg: "invalid number of arguments"}
			}
			var source, path, method, headers, params, body, jqq string
			for i, v := range args {
				switch i {
				case 0:
					source, _ = v.(string)
				case 1:
					path, _ = v.(string)
				case 2:
					method, _ = v.(string)
				case 3:
					headers, _ = v.(string)
				case 4:
					params, _ = v.(string)
				case 5:
					body, _ = v.(string)
				case 6:
					jqq, _ = v.(string)
				}
			}

			data, err := s.HttpRequest(context.Background(), source, path, method, headers, params, body, jqq)
			if err != nil {
				return nil, err
			}
			if data == nil {
				t, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
				return &httpRequestTableReader{
					data: []any{data},
					cols: []duckdb.ColumnInfo{{Name: "value", T: t}},
				}, nil
			}
			if _, ok := data.([]any); !ok {
				data = []any{data}
			}
			cols, err := columnInfosFromData(data)
			if err != nil {
				return nil, err
			}
			slices.SortFunc(cols, func(a, b duckdb.ColumnInfo) int {
				if a.Name < b.Name {
					return -1
				}
				if a.Name > b.Name {
					return 1
				}
				return 0
			})

			return &httpRequestTableReader{
				data: data.([]any),
				cols: cols,
			}, nil
		},
	}
}

type httpRequestTableReader struct {
	data []any
	cols []duckdb.ColumnInfo
	idx  int
	err  error
}

func (udf *httpRequestTableReader) Init() {
	udf.idx = 0
}

func (udf *httpRequestTableReader) ColumnInfos() []duckdb.ColumnInfo {
	for i, v := range udf.cols {
		log.Printf("column %d: %s - %v", i, v.Name, v.T.InternalType())
	}
	return udf.cols
}

func (udf *httpRequestTableReader) FillRow(row duckdb.Row) (bool, error) {
	if udf.err != nil || udf.data == nil || udf.idx >= len(udf.data) {
		return false, nil
	}
	if udf.cols == nil {
		return false, errors.New("column info is nil")
	}
	if len(udf.cols) == 1 {
		row.SetRowValue(0, udf.data[udf.idx])
		return true, nil
	}

	dm, ok := udf.data[udf.idx].(map[string]any)
	if !ok {
		return false, errors.New("data is not an object")
	}
	for i, col := range udf.cols {
		v, ok := dm[col.Name]
		if !ok || v == nil {
			continue
		}
		log.Printf("Row: %d: projected:%v, val: %v", i, row.IsProjected(i), v)
		err := duckdb.SetRowValue(row, i, v)
		if err != nil {
			return false, err
		}
	}
	udf.idx++

	return true, nil
}

func (udf *httpRequestTableReader) Cardinality() *duckdb.CardinalityInfo {

	return &duckdb.CardinalityInfo{
		Exact:       true,
		Cardinality: uint(len(udf.data)),
	}
}

func columnInfosFromData(data any) ([]duckdb.ColumnInfo, error) {
	if data == nil {
		return nil, nil
	}
	switch data := data.(type) {
	case []any:
		var cols []duckdb.ColumnInfo
		for _, v := range data {
			vm, ok := v.(map[string]any)
			if !ok {
				t, err := typeInfo(v)
				if err != nil {
					return nil, err
				}
				return []duckdb.ColumnInfo{{Name: "value", T: t}}, nil
			}
			sub, err := columnInfosFromData(vm)
			if err != nil {
				return nil, err
			}
			for _, se := range sub {
				if !slices.ContainsFunc(cols, func(entry duckdb.ColumnInfo) bool {
					return entry.Name == se.Name
				}) {
					cols = append(cols, se)
				}
			}
		}
		if len(cols) == 0 {
			t, err := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			return []duckdb.ColumnInfo{{Name: "value", T: t}}, nil
		}
		return cols, nil
	case map[string]any:
		see, err := structEntries(data)
		if err != nil {
			return nil, err
		}
		var cols []duckdb.ColumnInfo
		for _, se := range see {
			cols = append(cols, duckdb.ColumnInfo{Name: se.Name(), T: se.Info()})
		}
		if len(cols) == 0 {
			t, err := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
			if err != nil {
				return nil, err
			}
			return []duckdb.ColumnInfo{{Name: "value", T: t}}, nil
		}
		return cols, nil
	default:
		t, err := typeInfo(data)
		if err != nil {
			return nil, err
		}
		return []duckdb.ColumnInfo{{Name: "value", T: t}}, nil
	}
}

func typeInfo(data any) (duckdb.TypeInfo, error) {
	if data == nil {
		return duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	}
	switch data := data.(type) {
	case string:
		return duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	case int, int64:
		return duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	case int16, int32, int8:
		return duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	case float64:
		return duckdb.NewTypeInfo(duckdb.TYPE_FLOAT)
	case bool:
		return duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	case []any:
		var out []duckdb.StructEntry
		for _, v := range data {
			vm, ok := v.(map[string]any)
			if !ok {
				ch, err := typeInfo(v)
				if err != nil {
					return nil, err
				}
				return duckdb.NewListInfo(ch)
			}
			see, err := structEntries(vm)
			if err != nil {
				return nil, err
			}
			for _, se := range see {
				if !slices.ContainsFunc(out, func(entry duckdb.StructEntry) bool {
					return entry.Name() == se.Name()
				}) {
					out = append(out, se)
				}
			}
		}
		if len(out) == 0 {
			return duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
		}
		t, err := duckdb.NewStructInfo(out[0], out[1:]...)
		if err != nil {
			return nil, err
		}
		return duckdb.NewListInfo(t)
	case map[string]any:
		see, err := structEntries(data)
		if err != nil {
			return nil, err
		}
		if len(see) == 0 {
			return duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
		}
		return duckdb.NewStructInfo(see[0], see[1:]...)
	default:
		return duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	}
}

func structEntries(data map[string]any) ([]duckdb.StructEntry, error) {
	var out []duckdb.StructEntry
	for k, v := range data {
		ch, err := typeInfo(v)
		if err != nil {
			return nil, err
		}
		se, err := duckdb.NewStructEntry(ch, k)
		if err != nil {
			return nil, err
		}
		out = append(out, se)
	}
	return out, nil
}

type httpRequestScalarUDF struct {
	s *Service
}

func (udf *httpRequestScalarUDF) Config() duckdb.ScalarFuncConfig {
	ts, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{
			ts, // source (catalog)
			ts, // path
			ts, // method
			ts, // headers JSON
			ts, // params JSON
			ts, // body JSON
			ts, // jq string
		},
		ResultTypeInfo: ts,
		Volatile:       true,
	}
}

func (udf *httpRequestScalarUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowExecutor: func(values []driver.Value) (any, error) {
			if len(values) != 7 {
				return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid number of arguments"}
			}
			res, err := udf.s.HttpRequest(context.Background(),
				values[0].(string),
				values[1].(string),
				values[2].(string),
				values[3].(string),
				values[4].(string),
				values[5].(string),
				values[6].(string))
			if err != nil {
				return nil, err
			}
			b, err := json.Marshal(res)
			if err != nil {
				return nil, err
			}
			return string(b), nil
		},
	}
}
