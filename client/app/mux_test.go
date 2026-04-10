package app

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/airport-go/catalog"
)

func TestMux_Schemas(t *testing.T) {
	mux := New()

	schemas, err := mux.Schemas(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(schemas) != 0 {
		t.Fatalf("expected 0 schemas, got %d", len(schemas))
	}
}

func TestMux_HandleFunc(t *testing.T) {
	mux := New()

	err := mux.HandleFunc("math", "add", func(w *Result, r *Request) error {
		return w.Set(r.Int64("a") + r.Int64("b"))
	}, Arg("a", Int64), Arg("b", Int64), Return(Int64))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := mux.Schema(context.Background(), "math")
	if err != nil {
		t.Fatal(err)
	}
	if schema == nil {
		t.Fatal("schema 'math' not found")
	}

	fns, err := schema.ScalarFunctions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 scalar func, got %d", len(fns))
	}
	if fns[0].Name() != "ADD" {
		t.Errorf("expected name ADD, got %s", fns[0].Name())
	}
}

func TestMux_HandleFunc_MissingReturn(t *testing.T) {
	mux := New()
	err := mux.HandleFunc("math", "bad", func(w *Result, r *Request) error {
		return nil
	}, Arg("a", Int64))
	if err == nil {
		t.Fatal("expected error for missing Return()")
	}
}

func TestMux_HandleTableFunc(t *testing.T) {
	mux := New()

	err := mux.HandleTableFunc("users", "search", func(w *Result, r *Request) error {
		w.Append(int64(1), "Alice")
		w.Append(int64(2), "Bob")
		return nil
	}, Arg("query", String), ColPK("id", Int64), Col("name", String))
	if err != nil {
		t.Fatal(err)
	}

	schema, err := mux.Schema(context.Background(), "users")
	if err != nil {
		t.Fatal(err)
	}
	if schema == nil {
		t.Fatal("schema 'users' not found")
	}

	fns, err := schema.TableFunctions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatalf("expected 1 table func, got %d", len(fns))
	}
	if fns[0].Name() != "SEARCH" {
		t.Errorf("expected name SEARCH, got %s", fns[0].Name())
	}
}

func TestMux_HandleTableFunc_MissingCols(t *testing.T) {
	mux := New()
	err := mux.HandleTableFunc("users", "bad", func(w *Result, r *Request) error {
		return nil
	}, Arg("query", String))
	if err == nil {
		t.Fatal("expected error for missing Col()")
	}
}

func TestMux_HandleTableFunc_MutationRejected(t *testing.T) {
	mux := New()
	err := mux.HandleTableFunc("users", "bad", func(w *Result, r *Request) error {
		return nil
	}, ColPK("id", Int64), Mutation())
	if err == nil {
		t.Fatal("expected error: Mutation() is only valid for scalar functions")
	}
	if !strings.Contains(err.Error(), "Mutation()") {
		t.Errorf("error should mention Mutation(), got: %v", err)
	}
}

func TestMux_ArgFromContext_InvalidPlaceholder(t *testing.T) {
	mux := New()
	err := mux.HandleFunc("default", "bad", func(w *Result, r *Request) error {
		return nil
	}, Return(String), ArgFromContext("user_id", String, ContextPlaceholder("[$invalid]")))
	if err == nil {
		t.Fatal("expected error for unknown placeholder")
	}
	if !strings.Contains(err.Error(), "not a known context variable") {
		t.Errorf("error should mention unknown context variable, got: %v", err)
	}
}

func TestMux_ArgFromContext_DuplicateName(t *testing.T) {
	mux := New()
	err := mux.HandleFunc("default", "bad", func(w *Result, r *Request) error {
		return nil
	},
		Return(String),
		Arg("user_id", String),
		ArgFromContext("user_id", String, AuthUserID),
	)
	if err == nil {
		t.Fatal("expected error for duplicate argument name")
	}
	if !strings.Contains(err.Error(), "duplicate argument") {
		t.Errorf("error should mention duplicate argument, got: %v", err)
	}
}

func TestMux_ArgFromContext_TableFunc_Allowed(t *testing.T) {
	mux := New()
	err := mux.HandleTableFunc("default", "ok", func(w *Result, r *Request) error {
		return nil
	}, ArgFromContext("user_id", String, AuthUserID), ColPK("id", Int64))
	if err != nil {
		t.Fatalf("ArgFromContext should be allowed on table functions, got error: %v", err)
	}
}

func TestMux_Table_AutoSDL(t *testing.T) {
	mux := New()

	table := &mockTable{
		name: "events",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil),
	}
	mux.Table("app", table)

	sdl := mux.SDL()
	mustContain(t, sdl, "type app_events")
	mustContain(t, sdl, "@view")
	mustContain(t, sdl, "id: BigInt! @pk")
}

func TestMux_Table_WithOptions(t *testing.T) {
	mux := New()

	table := &mockTable{
		name: "orders",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "total", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		}, nil),
	}
	mux.Table("shop", table,
		WithPK("id"),
		WithFieldReferences("user_id", "shop_users", "user", "orders"),
		WithDescription("Order records"),
	)

	sdl := mux.SDL()
	mustContain(t, sdl, "Order records")
	mustContain(t, sdl, "@field_references")
	mustContain(t, sdl, `references_name: "shop_users"`)
}

func TestMux_SchemaWithTableRefs(t *testing.T) {
	mux := New()

	ref := &mockTableRef{
		name: "remote",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.BinaryTypes.String},
		}, nil),
	}
	mux.TableRef("ext", ref)

	schema, err := mux.Schema(context.Background(), "ext")
	if err != nil {
		t.Fatal(err)
	}

	schemaWithRefs, ok := schema.(catalog.SchemaWithTableRefs)
	if !ok {
		t.Fatal("schema does not implement SchemaWithTableRefs")
	}

	refs, err := schemaWithRefs.TableRefs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 table ref, got %d", len(refs))
	}
	if refs[0].Name() != "remote" {
		t.Errorf("expected name remote, got %s", refs[0].Name())
	}

	sdl := mux.SDL()
	mustContain(t, sdl, "type ext_remote")
	mustContain(t, sdl, "@view")
}

func TestMux_SDL_Collected(t *testing.T) {
	mux := New()

	mux.HandleFunc("math", "add", func(w *Result, r *Request) error {
		return w.Set(r.Int64("a") + r.Int64("b"))
	}, Arg("a", Int64), Arg("b", Int64), Return(Int64))

	mux.HandleTableFunc("users", "list", func(w *Result, r *Request) error {
		return nil
	}, ColPK("id", Int64), Col("name", String))

	sdl := mux.SDL()
	mustContain(t, sdl, "extend type Function")
	mustContain(t, sdl, `@function(name: "\"math\".\"ADD\"")`)
	mustContain(t, sdl, "type users_list")
	mustContain(t, sdl, "@view")
}

func TestMux_WithSDL_Override(t *testing.T) {
	mux := New()

	mux.HandleFunc("math", "add", func(w *Result, r *Request) error {
		return w.Set(int64(0))
	}, Arg("a", Int64), Return(Int64))

	custom := `type custom { id: Int! }`
	mux.WithSDL(custom)

	sdl := mux.SDL()
	if sdl != custom {
		t.Errorf("expected custom SDL, got:\n%s", sdl)
	}
}

func TestMux_MultipleSchemas(t *testing.T) {
	mux := New()

	mux.HandleFunc("math", "add", func(w *Result, r *Request) error {
		return w.Set(int64(0))
	}, Arg("a", Int64), Return(Int64))

	mux.HandleFunc("text", "upper", func(w *Result, r *Request) error {
		return w.Set("")
	}, Arg("s", String), Return(String))

	mux.Table("data", &mockTable{
		name: "items",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	})

	schemas, _ := mux.Schemas(context.Background())
	if len(schemas) != 3 {
		t.Fatalf("expected 3 schemas, got %d", len(schemas))
	}

	// Verify each schema is accessible
	for _, name := range []string{"math", "text", "data"} {
		s, _ := mux.Schema(context.Background(), name)
		if s == nil {
			t.Errorf("schema %q not found", name)
		}
	}

	// Non-existent schema
	s, _ := mux.Schema(context.Background(), "nope")
	if s != nil {
		t.Error("expected nil for non-existent schema")
	}
}

func TestMux_StructDedup_Identical(t *testing.T) {
	mux := New()

	user := Struct("user").Field("id", String).Field("name", String)
	err := mux.HandleFunc("app", "get_user_by_id",
		func(w *Result, r *Request) error { return w.SetJSON(map[string]any{"id": "1", "name": "Alice"}) },
		Arg("id", String), Return(user.AsType()))
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	user2 := Struct("user").Field("id", String).Field("name", String)
	err = mux.HandleFunc("app", "get_user_by_email",
		func(w *Result, r *Request) error { return w.SetJSON(map[string]any{"id": "1", "name": "Alice"}) },
		Arg("email", String), Return(user2.AsType()))
	if err != nil {
		t.Fatalf("second registration with identical struct should be allowed, got: %v", err)
	}

	if len(mux.structTypes) != 1 {
		t.Errorf("expected 1 struct type registered, got %d", len(mux.structTypes))
	}
}

func TestMux_StructDedup_Conflict(t *testing.T) {
	mux := New()

	v1 := Struct("user").Field("id", String)
	err := mux.HandleFunc("app", "f1",
		func(w *Result, r *Request) error { return w.SetJSON(map[string]any{"id": "1"}) },
		Return(v1.AsType()))
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	v2 := Struct("user").Field("name", String) // different fields → conflict
	err = mux.HandleFunc("app", "f2",
		func(w *Result, r *Request) error { return w.SetJSON(map[string]any{"name": "Alice"}) },
		Return(v2.AsType()))
	if err == nil {
		t.Fatal("expected conflict error for mismatched struct fields")
	}
	if !strings.Contains(err.Error(), "already registered with different fields") {
		t.Errorf("expected conflict message, got: %v", err)
	}
}

func TestMux_ReturnList_Struct_Rejected(t *testing.T) {
	mux := New()

	user := Struct("user").Field("id", String)
	err := mux.HandleFunc("app", "list_users",
		func(w *Result, r *Request) error { return nil },
		ReturnList(user.AsType()))
	if err == nil {
		t.Fatal("expected error for ReturnList(struct)")
	}
	if !strings.Contains(err.Error(), "ReturnList does not support struct") {
		t.Errorf("expected struct-rejection message, got: %v", err)
	}
	if !strings.Contains(err.Error(), "HandleTableFunc") {
		t.Errorf("expected error to mention HandleTableFunc, got: %v", err)
	}
}

func TestMux_SameSchemaMultipleRegistrations(t *testing.T) {
	mux := New()

	mux.HandleFunc("app", "func1", func(w *Result, r *Request) error {
		return w.Set(int64(0))
	}, Return(Int64))

	mux.HandleFunc("app", "func2", func(w *Result, r *Request) error {
		return w.Set("")
	}, Return(String))

	mux.Table("app", &mockTable{
		name: "table1",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	})

	schema, _ := mux.Schema(context.Background(), "app")
	fns, _ := schema.ScalarFunctions(context.Background())
	tables, _ := schema.Tables(context.Background())

	if len(fns) != 2 {
		t.Errorf("expected 2 scalar funcs, got %d", len(fns))
	}
	if len(tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(tables))
	}
}
