package db

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"

	"github.com/marcboeker/go-duckdb/v2"
)

func TestNewPool(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if pool == nil {
		t.Fatalf("expected pool to be non-nil")
	}
}

func TestPool_SetMaxOpenConns(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	pool.SetMaxOpenConns(10)
	if pool.db.Stats().MaxOpenConnections != 10 {
		t.Fatalf("expected max open connections to be 10, got %v", pool.db.Stats().MaxOpenConnections)
	}
}

func TestPool_Close(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = pool.Close()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestPool_Conn(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Conn(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if conn == nil {
		t.Fatalf("expected conn to be non-nil")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestPool_Arrow(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	ctx := context.Background()
	arrow, err := pool.Arrow(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if arrow == nil {
		t.Fatalf("expected arrow to be non-nil")
	}

	err = arrow.Close()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestPool_Conn_Concurrent(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	pool.SetMaxOpenConns(5)

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := pool.Conn(ctx)
			if err != nil {
				t.Logf("expected no error, got %v", err)
				return
			}
			if conn == nil {
				t.Logf("expected conn to be non-nil")
				return
			}
			err = conn.Close()
			if err != nil {
				t.Logf("expected no error, got %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestPool_Arrow_Concurrent(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	pool.SetMaxOpenConns(5)

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			arrow, err := pool.Arrow(ctx)
			if err != nil {
				t.Logf("expected no error, got %v", err)
				return
			}
			if arrow == nil {
				t.Logf("expected arrow to be non-nil")
				return
			}
			err = arrow.Close()
			if err != nil {
				t.Logf("expected no error, got %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestUDF(t *testing.T) {
	pool, err := NewPool("")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer pool.Close()

	c, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer c.Close()

	ti, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	tudf := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{ti},
		},
		BindArguments: testUDFBind,
	}
	udf := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{ti},
		},
		BindArguments: bindTableUDF,
	}

	err = duckdb.RegisterTableUDF(c.DBConn(), "test_udf", udf)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	err = duckdb.RegisterTableUDF(c.DBConn(), "udf", tudf)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	c.Close()

	c, err = pool.Conn(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer c.Close()

	rows, err := c.Query(context.Background(), "FROM udf(15)")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d int64
		var n string
		err = rows.Scan(&d, &n)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		t.Log(d, n)
	}

	c.Close()
	c, err = pool.Conn(context.Background())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer c.Close()

	rows, err = c.Query(context.Background(), "SELECT d::JSON FROM duckdb_functions() as d where function_name  like '%udf%' ;")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		err = rows.Scan(&d)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		t.Log(d)
	}

	_, err = c.Exec(context.Background(), "LOAD spatial;")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	ta, err := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	udfA := duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments: []duckdb.TypeInfo{ti, ta},
		},
		BindArguments: bindAnyArgsTableUDF,
	}

	err = duckdb.RegisterTableUDF(c.DBConn(), "table_any_args", udfA)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	rows, err = c.Query(context.Background(), "FROM table_any_args(2050, {a: 1, b:{val:ARRAY[{val: 13}]}}::JSON)")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var n int64
		var d any
		err = rows.Scan(&n, &d)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if n > 2048 {
			t.Logf("%d: %v (%[2]T)", n, d)
		}
	}
}

func testUDFBind(named map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	if len(args) != 1 {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid number of arguments"}
	}
	v, ok := args[0].(int64)
	if !ok {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid argument type"}
	}
	return &testTableReader{max: int(v)}, nil
}

var _ duckdb.RowTableSource = &testTableReader{}

type testTableReader struct {
	max int
}

func (r *testTableReader) ColumnInfos() []duckdb.ColumnInfo {
	ti, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	ts, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	return []duckdb.ColumnInfo{
		{Name: "id", T: ti},
		{Name: "name", T: ts},
	}
}

func (r *testTableReader) Init() {
}

func (r *testTableReader) FillRow(row duckdb.Row) (bool, error) {
	if r.max == 0 {
		return false, nil
	}
	row.SetRowValue(0, int64(r.max))
	row.SetRowValue(1, "test"+strconv.Itoa(r.max))
	r.max--
	return true, nil
}

func (r *testTableReader) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(r.max),
		Exact:       true,
	}
}

func testTableFuncConfig() duckdb.TableFunctionConfig {
	ti, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	return duckdb.TableFunctionConfig{
		Arguments: []duckdb.TypeInfo{ti},
	}
}

type incrementTableUDF struct {
	tableSize  int64
	currentRow int64
}

func bindTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	return &incrementTableUDF{
		currentRow: 0,
		tableSize:  args[0].(int64),
	}, nil
}

func (udf *incrementTableUDF) ColumnInfos() []duckdb.ColumnInfo {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	return []duckdb.ColumnInfo{{Name: "result", T: t}}
}

func (udf *incrementTableUDF) Init() {}

func (udf *incrementTableUDF) FillRow(row duckdb.Row) (bool, error) {
	if udf.currentRow+1 > udf.tableSize {
		return false, nil
	}
	udf.currentRow++
	err := duckdb.SetRowValue(row, 0, udf.currentRow)
	return true, err
}

func (udf *incrementTableUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(udf.tableSize),
		Exact:       true,
	}
}

type testTableAnyArgsUDF struct {
	vars any
	n    int64
	data map[string]any

	c int64
}

func bindAnyArgsTableUDF(namedArgs map[string]any, args ...interface{}) (duckdb.RowTableSource, error) {
	if len(args) != 2 {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid number of arguments"}
	}
	n, ok := args[0].(int64)
	if !ok {
		return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid argument type"}
	}
	var data map[string]any
	if args[1] != nil {
		s, ok := args[1].(string)
		if !ok {
			return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid argument type"}
		}
		err := json.Unmarshal([]byte(s), &data)
		if err != nil {
			return nil, err
		}
	}

	return &testTableAnyArgsUDF{
		n:    n,
		vars: args[1],
		data: data,
	}, nil
}

func parseDuckDBType(val any) (duckdb.TypeInfo, error) {
	switch v := val.(type) {
	case string:
		return duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	case int64, int:
		return duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	case float64:
		return duckdb.NewTypeInfo(duckdb.TYPE_FLOAT)
	case bool:
		return duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	case []byte:
		return duckdb.NewTypeInfo(duckdb.TYPE_BLOB)
	case []any:
		if len(val.([]any)) == 0 {
			return duckdb.NewTypeInfo(duckdb.TYPE_ANY)
		}
		t, err := parseDuckDBType(val.([]any)[0])
		if err != nil {
			return nil, err
		}
		return duckdb.NewListInfo(t)
	case map[string]any:
		if len(v) == 0 {
			return duckdb.NewTypeInfo(duckdb.TYPE_ANY)
		}
		var str []duckdb.StructEntry
		for k, v := range v {
			t, err := parseDuckDBType(v)
			if err != nil {
				return nil, err
			}
			se, err := duckdb.NewStructEntry(t, k)
			if err != nil {
				return nil, err
			}
			str = append(str, se)
		}
		return duckdb.NewStructInfo(str[0], str[1:]...)
	default:
		return duckdb.NewTypeInfo(duckdb.TYPE_ANY)
	}
}

func (udf *testTableAnyArgsUDF) ColumnInfos() []duckdb.ColumnInfo {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	cc := []duckdb.ColumnInfo{{Name: "result", T: t}}

	t, err := parseDuckDBType(udf.data)
	if err != nil {
		return nil
	}
	cc = append(cc, duckdb.ColumnInfo{Name: "vals", T: t})

	return cc
}

func (udf *testTableAnyArgsUDF) Init() {
	udf.c = 0
}

func (udf *testTableAnyArgsUDF) FillRow(row duckdb.Row) (bool, error) {
	if udf.c >= udf.n {
		return false, nil
	}
	udf.c++

	err := row.SetRowValue(0, udf.c)
	if err != nil {
		return false, err
	}
	err = row.SetRowValue(1, udf.data)
	if err != nil {
		return false, err
	}
	return true, err
}

func (udf *testTableAnyArgsUDF) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(udf.n),
		Exact:       true,
	}
}

func Test_print(t *testing.T) {
	t.Log("[{\"address\":\"ул. Мира - ул. Мичурина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.530669834,66.644954681]}\",\"id\":\"0381c536-6efc-49e8-b2ec-87303ba0d4f4\",\"isManaged\":false,\"name\":\"C-005\",\"number\":\"C005\"},{\"address\":\"Пермь, улица КИМ, 72\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[58.021034275,56.293535829]}\",\"id\":\"9cc382ea-5cc5-4b50-bb4f-11ea2de0a393\",\"isManaged\":false,\"name\":\"ДК КДУ-КМД\",\"number\":\"2001\"},{\"address\":\"ул. Мира- Чубынина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.530721108,66.613830328]}\",\"id\":\"2472a103-5e54-49f5-aa78-e690785ea83a\",\"isManaged\":false,\"name\":\"ИДК1-02\",\"number\":\"002\"},{\"address\":\"ул. Чубынина - Мира\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.531680341,66.612414122]}\",\"id\":\"6aa3eaa4-cfd9-431d-a45f-dc88820604aa\",\"isManaged\":false,\"name\":\"ИДК1-03\",\"number\":\"103\"},{\"address\":\" Арктическая-Губкина-Матросова\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.538271784,66.62610814]}\",\"id\":\"506b8ea2-615c-429d-bb29-bd0dc15e9996\",\"isManaged\":true,\"name\":\"ИДКЗ-01\",\"number\":\"301\"},{\"address\":\" Губкина - Зои Космодемьянской\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.537984428,66.633174419]}\",\"id\":\"841dd2cb-64a1-4d53-b76c-74869a4d01e6\",\"isManaged\":true,\"name\":\"ИДКЗ-02\",\"number\":\"302\"},{\"address\":\"Броднева -Губкина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.537782631,66.639154851]}\",\"id\":\"345ce8d4-65b4-46d3-92e5-b2a48876413e\",\"isManaged\":false,\"name\":\"ИДКЗ-03\",\"number\":\"303\"},{\"address\":\" Матросова - Подшибякина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.536092423,66.625503302]}\",\"id\":\"9f7591dd-aa75-4d29-8627-e8e9486986c9\",\"isManaged\":false,\"name\":\"ИДКЗ-04\",\"number\":\"304\"},{\"address\":\" Космодемьянской - Подшибякина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.535844447,66.632648706]}\",\"id\":\"bb0ad7e1-50e3-4982-96c7-70dc2ea54fb8\",\"isManaged\":false,\"name\":\"ИДКЗ-05\",\"number\":\"305\"},{\"address\":\"Броднева-Подшибякина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.535609528,66.638635397]}\",\"id\":\"eac5e822-ce2a-4425-8a27-47b1f88d9836\",\"isManaged\":false,\"name\":\"ИДКЗ-06\",\"number\":\"306\"},{\"address\":\"Ямальская - Матросова\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.534233819,66.625117064]}\",\"id\":\"af482037-3796-4d53-b59c-7e82dfeb454d\",\"isManaged\":false,\"name\":\"ИДКЗ-07\",\"number\":\"307\"},{\"address\":\"Ямальская - Космодемьянской\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.534003136,66.632198095]}\",\"id\":\"86952cf0-947d-4b9e-84c8-cfa44a3dd5a7\",\"isManaged\":false,\"name\":\"ИДКЗ-08\",\"number\":\"308\"},{\"address\":\"Ямальская- Броднева\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.533776997,66.638131142]}\",\"id\":\"498a3164-6e27-4908-b811-5a6b710be17d\",\"isManaged\":false,\"name\":\"ИДКЗ-09\",\"number\":\"309\"},{\"address\":\"улица Чубынина/улица Республики\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.529226826,66.616048263]}\",\"id\":\"d538df50-9193-4a1e-a02c-89f6fd4854cb\",\"isManaged\":false,\"name\":\"Инвиан 1\",\"number\":\"Инв-01\"},{\"address\":\"Пермь\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[58.019055099,56.290804077]}\",\"id\":\"962efde4-62e2-4128-a438-18f43f8b56e4\",\"isManaged\":true,\"name\":\"Инвиан-02\",\"number\":\"Инв-02\"},{\"address\":\"ул. Богдана Кнунянца\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.549909425,66.579353213]}\",\"id\":\"6d736df2-e46a-4698-9ac2-b02a99eaefd6\",\"isManaged\":false,\"name\":\"С-001\",\"number\":\"С001\"},{\"address\":\"ул. Почтовая - просп. Молодежи\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.538073574,66.596063375]}\",\"id\":\"7db4d259-75a7-4e68-a765-e6c0e91573e4\",\"isManaged\":false,\"name\":\"С-002\",\"number\":\"С002\"},{\"address\":\"ул. Объездная - просп. Молодежи\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.542511344,66.621533632]}\",\"id\":\"bafa44ff-e40d-460c-9f0c-3c3732833245\",\"isManaged\":false,\"name\":\"С-003\",\"number\":\"С003\"},{\"address\":\"ул. Республики - ул. Подшибякина\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.534745795,66.652078629]}\",\"id\":\"3fdb1c7f-d69e-40fd-a29b-316dc95faf65\",\"isManaged\":false,\"name\":\"С-004\",\"number\":\"С004\"},{\"address\":\"ул. Броднева\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.531127024,66.637487411]}\",\"id\":\"c4558a8e-4752-4131-b8a5-1bcf97c925e3\",\"isManaged\":false,\"name\":\"С-006\",\"number\":\"С006\"},{\"address\":\"ул. Артеева\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.531383389,66.631565094]}\",\"id\":\"62a25f70-b9c3-41a3-acc4-46344ab75e08\",\"isManaged\":false,\"name\":\"С-007\",\"number\":\"С007\"},{\"address\":\"ул. Мира\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[66.53162266,66.624526978]}\",\"id\":\"dc0e2a05-1df5-4854-ab12-78623105abae\",\"isManaged\":false,\"name\":\"С-008\",\"number\":\"С008\"},{\"address\":\"Пермь, улица КИМ, 74А\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[58.019861248,56.291919989]}\",\"id\":\"cea96a45-b726-4e9e-8c86-a507f020257b\",\"isManaged\":true,\"name\":\"Спектр2-01\",\"number\":\"С-01\"},{\"address\":\"Пермь\",\"coordinates\":\"{\\\"type\\\":\\\"Point\\\",\\\"coordinates\\\":[58.020333595,56.292587678]}\",\"id\":\"6cc55c55-87f6-4918-a7ac-46172539d73b\",\"isManaged\":true,\"name\":\"Спектр2-02\",\"number\":\"С-02\"}]")
}
