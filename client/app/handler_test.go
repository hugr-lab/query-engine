package app

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// --- Scalar function Execute ---

func TestScalarFunc_Execute(t *testing.T) {
	mux := New()
	err := mux.HandleFunc("math", "add", func(w *Result, r *Request) error {
		return w.Set(r.Int64("a") + r.Int64("b"))
	}, Arg("a", Int64), Arg("b", Int64), Return(Int64))
	if err != nil {
		t.Fatal(err)
	}

	schema, _ := mux.Schema(context.Background(), "math")
	fns, _ := schema.ScalarFunctions(context.Background())
	fn := fns[0]

	// Build input RecordBatch: a=[1,2,3], b=[10,20,30]
	mem := memory.DefaultAllocator
	aBldr := array.NewInt64Builder(mem)
	bBldr := array.NewInt64Builder(mem)
	defer aBldr.Release()
	defer bBldr.Release()

	aBldr.AppendValues([]int64{1, 2, 3}, nil)
	bBldr.AppendValues([]int64{10, 20, 30}, nil)

	aArr := aBldr.NewArray()
	bArr := bBldr.NewArray()
	defer aArr.Release()
	defer bArr.Release()

	inputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	batch := array.NewRecord(inputSchema, []arrow.Array{aArr, bArr}, 3)
	defer batch.Release()

	result, err := fn.Execute(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Len() != 3 {
		t.Fatalf("expected 3 rows, got %d", result.Len())
	}

	got := result.(*array.Int64)
	expected := []int64{11, 22, 33}
	for i, want := range expected {
		if got.Value(i) != want {
			t.Errorf("row %d: got %d, want %d", i, got.Value(i), want)
		}
	}
}

func TestScalarFunc_Execute_String(t *testing.T) {
	mux := New()
	mux.HandleFunc("text", "greet", func(w *Result, r *Request) error {
		return w.Set("hello " + r.String("name"))
	}, Arg("name", String), Return(String))

	schema, _ := mux.Schema(context.Background(), "text")
	fns, _ := schema.ScalarFunctions(context.Background())
	fn := fns[0]

	mem := memory.DefaultAllocator
	bldr := array.NewStringBuilder(mem)
	defer bldr.Release()
	bldr.AppendValues([]string{"Alice", "Bob"}, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	inputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	batch := array.NewRecord(inputSchema, []arrow.Array{arr}, 2)
	defer batch.Release()

	result, err := fn.Execute(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	got := result.(*array.String)
	if got.Value(0) != "hello Alice" {
		t.Errorf("row 0: got %q", got.Value(0))
	}
	if got.Value(1) != "hello Bob" {
		t.Errorf("row 1: got %q", got.Value(1))
	}
}

func TestScalarFunc_Execute_HandlerError(t *testing.T) {
	mux := New()
	mux.HandleFunc("err", "fail", func(w *Result, r *Request) error {
		return fmt.Errorf("intentional error")
	}, Arg("x", Int64), Return(Int64))

	schema, _ := mux.Schema(context.Background(), "err")
	fns, _ := schema.ScalarFunctions(context.Background())
	fn := fns[0]

	mem := memory.DefaultAllocator
	bldr := array.NewInt64Builder(mem)
	defer bldr.Release()
	bldr.Append(1)
	arr := bldr.NewArray()
	defer arr.Release()

	inputSchema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Int64}}, nil)
	batch := array.NewRecord(inputSchema, []arrow.Array{arr}, 1)
	defer batch.Release()

	_, err := fn.Execute(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error")
	}
	mustContain(t, err.Error(), "intentional error")
}

// --- Table function Execute ---

func TestTableFunc_Execute(t *testing.T) {
	mux := New()
	err := mux.HandleTableFunc("users", "search", func(w *Result, r *Request) error {
		query := r.String("query")
		if query == "alice" {
			w.Append(int64(1), "Alice", 30.5)
		}
		w.Append(int64(2), "Bob", 25.0)
		return nil
	},
		Arg("query", String),
		ColPK("id", Int64),
		Col("name", String),
		ColNullable("score", Float64),
	)
	if err != nil {
		t.Fatal(err)
	}

	schema, _ := mux.Schema(context.Background(), "users")
	fns, _ := schema.TableFunctions(context.Background())
	fn := fns[0]

	// Check schema
	outSchema, err := fn.SchemaForParameters(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if outSchema.NumFields() != 3 {
		t.Fatalf("expected 3 fields, got %d", outSchema.NumFields())
	}
	if outSchema.Field(0).Name != "id" {
		t.Errorf("field 0: expected id, got %s", outSchema.Field(0).Name)
	}

	// Execute with query="alice"
	reader, err := fn.Execute(context.Background(), []any{"alice"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected at least one batch")
	}
	rec := reader.Record()
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}

	ids := rec.Column(0).(*array.Int64)
	names := rec.Column(1).(*array.String)
	scores := rec.Column(2).(*array.Float64)

	if ids.Value(0) != 1 || ids.Value(1) != 2 {
		t.Errorf("ids: got %d, %d", ids.Value(0), ids.Value(1))
	}
	if names.Value(0) != "Alice" || names.Value(1) != "Bob" {
		t.Errorf("names: got %q, %q", names.Value(0), names.Value(1))
	}
	if scores.Value(0) != 30.5 || scores.Value(1) != 25.0 {
		t.Errorf("scores: got %f, %f", scores.Value(0), scores.Value(1))
	}
}

func TestTableFunc_Execute_NoArgs(t *testing.T) {
	mux := New()
	mux.HandleTableFunc("data", "constants", func(w *Result, r *Request) error {
		w.Append(int64(1), "pi", 3.14159)
		w.Append(int64(2), "e", 2.71828)
		return nil
	}, ColPK("id", Int64), Col("name", String), Col("value", Float64))

	schema, _ := mux.Schema(context.Background(), "data")
	fns, _ := schema.TableFunctions(context.Background())
	fn := fns[0]

	reader, err := fn.Execute(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected batch")
	}
	rec := reader.Record()
	if rec.NumRows() != 2 {
		t.Fatalf("expected 2 rows, got %d", rec.NumRows())
	}
}

func TestTableFunc_Execute_EmptyResult(t *testing.T) {
	mux := New()
	mux.HandleTableFunc("data", "empty", func(w *Result, r *Request) error {
		// no rows
		return nil
	}, ColPK("id", Int64))

	schema, _ := mux.Schema(context.Background(), "data")
	fns, _ := schema.TableFunctions(context.Background())
	fn := fns[0]

	reader, err := fn.Execute(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()

	if !reader.Next() {
		t.Fatal("expected batch")
	}
	rec := reader.Record()
	if rec.NumRows() != 0 {
		t.Fatalf("expected 0 rows, got %d", rec.NumRows())
	}
}

func TestTableFunc_Execute_HandlerError(t *testing.T) {
	mux := New()
	mux.HandleTableFunc("data", "fail", func(w *Result, r *Request) error {
		return fmt.Errorf("table error")
	}, ColPK("id", Int64))

	schema, _ := mux.Schema(context.Background(), "data")
	fns, _ := schema.TableFunctions(context.Background())
	fn := fns[0]

	_, err := fn.Execute(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	mustContain(t, err.Error(), "table error")
}

// --- Request typed accessors ---

func TestRequest_TypedAccessors(t *testing.T) {
	r := &Request{
		ctx: context.Background(),
		args: map[string]any{
			"b":   true,
			"i32": int32(42),
			"i64": int64(100),
			"f64": 3.14,
			"s":   "hello",
		},
	}

	if !r.Bool("b") {
		t.Error("Bool failed")
	}
	if r.Int32("i32") != 42 {
		t.Error("Int32 failed")
	}
	if r.Int64("i64") != 100 {
		t.Error("Int64 failed")
	}
	if r.Float64("f64") != 3.14 {
		t.Error("Float64 failed")
	}
	if r.String("s") != "hello" {
		t.Error("String failed")
	}
	// Missing key returns zero value
	if r.Int64("missing") != 0 {
		t.Error("expected 0 for missing key")
	}
	if r.String("missing") != "" {
		t.Error("expected empty for missing key")
	}
}

// --- Result ---

func TestResult_Set(t *testing.T) {
	r := &Result{}
	r.Set(int64(42))
	if r.value != int64(42) {
		t.Errorf("expected 42, got %v", r.value)
	}
}

func TestResult_Append(t *testing.T) {
	r := &Result{}
	r.Append(int64(1), "a")
	r.Append(int64(2), "b")
	if len(r.rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.rows))
	}
	if r.rows[0][0] != int64(1) || r.rows[0][1] != "a" {
		t.Error("row 0 mismatch")
	}
}

// --- All scalar types round-trip ---

func TestScalarFunc_AllTypes(t *testing.T) {
	tests := []struct {
		name    string
		typ     Type
		input   any
		buildFn func(mem memory.Allocator) arrow.Array
	}{
		{"bool", Boolean, true, func(mem memory.Allocator) arrow.Array {
			b := array.NewBooleanBuilder(mem); b.Append(true); return b.NewArray()
		}},
		{"int32", Int32, int32(42), func(mem memory.Allocator) arrow.Array {
			b := array.NewInt32Builder(mem); b.Append(42); return b.NewArray()
		}},
		{"int64", Int64, int64(100), func(mem memory.Allocator) arrow.Array {
			b := array.NewInt64Builder(mem); b.Append(100); return b.NewArray()
		}},
		{"float64", Float64, 3.14, func(mem memory.Allocator) arrow.Array {
			b := array.NewFloat64Builder(mem); b.Append(3.14); return b.NewArray()
		}},
		{"string", String, "hello", func(mem memory.Allocator) arrow.Array {
			b := array.NewStringBuilder(mem); b.Append("hello"); return b.NewArray()
		}},
		{"uint64", Uint64, uint64(999), func(mem memory.Allocator) arrow.Array {
			b := array.NewUint64Builder(mem); b.Append(999); return b.NewArray()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := New()
			mux.HandleFunc("t", "f", func(w *Result, r *Request) error {
				return w.Set(r.Get("x"))
			}, Arg("x", tt.typ), Return(tt.typ))

			schema, _ := mux.Schema(context.Background(), "t")
			fns, _ := schema.ScalarFunctions(context.Background())
			fn := fns[0]

			mem := memory.DefaultAllocator
			arr := tt.buildFn(mem)
			defer arr.Release()

			inputSchema := arrow.NewSchema([]arrow.Field{{Name: "x", Type: tt.typ.dt}}, nil)
			batch := array.NewRecord(inputSchema, []arrow.Array{arr}, 1)
			defer batch.Release()

			result, err := fn.Execute(context.Background(), batch)
			if err != nil {
				t.Fatal(err)
			}
			defer result.Release()

			if result.Len() != 1 {
				t.Fatalf("expected 1 row, got %d", result.Len())
			}
		})
	}
}

// --- Signature ---

func TestScalarFunc_Signature(t *testing.T) {
	mux := New()
	mux.HandleFunc("m", "f", func(w *Result, r *Request) error {
		return w.Set(int64(0))
	}, Arg("a", Int64), Arg("b", String), Return(Float64))

	schema, _ := mux.Schema(context.Background(), "m")
	fns, _ := schema.ScalarFunctions(context.Background())
	sig := fns[0].Signature()

	if len(sig.Parameters) != 2 {
		t.Fatalf("expected 2 params, got %d", len(sig.Parameters))
	}
	if sig.Parameters[0] != arrow.PrimitiveTypes.Int64 {
		t.Error("param 0 should be Int64")
	}
	if sig.Parameters[1] != arrow.BinaryTypes.String {
		t.Error("param 1 should be String")
	}
	if sig.ReturnType != arrow.PrimitiveTypes.Float64 {
		t.Error("return type should be Float64")
	}
}

func TestTableFunc_Signature(t *testing.T) {
	mux := New()
	mux.HandleTableFunc("s", "f", func(w *Result, r *Request) error {
		return nil
	}, Arg("q", String), ColPK("id", Int64))

	schema, _ := mux.Schema(context.Background(), "s")
	fns, _ := schema.TableFunctions(context.Background())
	sig := fns[0].Signature()

	if len(sig.Parameters) != 1 {
		t.Fatalf("expected 1 param, got %d", len(sig.Parameters))
	}
	if sig.ReturnType != nil {
		t.Error("table func return type should be nil")
	}
}

// --- SetJSON / SetJSONValue / Request.JSON ---

func TestSetJSON_RuntimeCheck_NonJSONReturn(t *testing.T) {
	def := &funcDef{retType: &Int64}
	w := &Result{def: def}
	err := w.SetJSON(map[string]any{"foo": "bar"})
	if err == nil {
		t.Fatal("expected error for SetJSON on Int64 return type")
	}
}

func TestSetJSON_StructReturn(t *testing.T) {
	weather := Struct("weather").Field("temp", Float64)
	rt := weather.AsType()
	def := &funcDef{retType: &rt}
	w := &Result{def: def}

	if err := w.SetJSON(map[string]any{"temp": 22.5}); err != nil {
		t.Fatalf("expected SetJSON to succeed for struct return, got: %v", err)
	}
	s, ok := w.value.(string)
	if !ok {
		t.Fatalf("expected value to be string, got %T", w.value)
	}
	if s != `{"temp":22.5}` {
		t.Errorf("expected JSON %q, got %q", `{"temp":22.5}`, s)
	}
}

func TestSetJSON_RawJSONReturn(t *testing.T) {
	rt := JSON
	def := &funcDef{retType: &rt}
	w := &Result{def: def}

	if err := w.SetJSON([]string{"a", "b", "c"}); err != nil {
		t.Fatalf("expected SetJSON to succeed for JSON return, got: %v", err)
	}
	if w.value != `["a","b","c"]` {
		t.Errorf("got %v", w.value)
	}
}

func TestReturnList_NativeArrowWire(t *testing.T) {
	// ReturnList uses a native Arrow LIST wire (not JSON), so returnsJSON()
	// should be false and the handler should pass a Go slice via Set, not SetJSON.
	def := &funcDef{}
	ReturnList(String)(def)
	if def.returnsJSON() {
		t.Error("ReturnList should NOT use JSON wire — it uses native Arrow LIST")
	}
	if !def.returnsList {
		t.Error("expected returnsList = true")
	}
}

func TestSetJSONValue_String(t *testing.T) {
	rt := JSON
	def := &funcDef{retType: &rt}
	w := &Result{def: def}

	if err := w.SetJSONValue(`{"raw":"json"}`); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.value != `{"raw":"json"}` {
		t.Errorf("expected passthrough, got %v", w.value)
	}
}

func TestSetJSONValue_Bytes(t *testing.T) {
	rt := JSON
	def := &funcDef{retType: &rt}
	w := &Result{def: def}

	if err := w.SetJSONValue([]byte(`{"raw":"json"}`)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.value != `{"raw":"json"}` {
		t.Errorf("expected passthrough, got %v", w.value)
	}
}

func TestSetJSONValue_Marshal(t *testing.T) {
	rt := JSON
	def := &funcDef{retType: &rt}
	w := &Result{def: def}

	if err := w.SetJSONValue(map[string]any{"a": 1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.value != `{"a":1}` {
		t.Errorf("expected marshaled JSON, got %v", w.value)
	}
}

func TestRequest_JSON_Unmarshal(t *testing.T) {
	r := &Request{
		ctx: context.Background(),
		args: map[string]any{
			"input": `{"name":"alice","age":30}`,
		},
	}
	var out struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	if err := r.JSON("input", &out); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Name != "alice" || out.Age != 30 {
		t.Errorf("got %+v", out)
	}
}

func TestRequest_JSON_Empty(t *testing.T) {
	r := &Request{
		ctx:  context.Background(),
		args: map[string]any{"input": ""},
	}
	var out struct{ Name string }
	if err := r.JSON("input", &out); err != nil {
		t.Errorf("expected nil error for empty arg, got: %v", err)
	}
}
