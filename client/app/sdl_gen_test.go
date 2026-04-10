package app

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/hugr-lab/airport-go/catalog"
)

// --- helpers ---

func mustContain(t *testing.T, sdl, substr string) {
	t.Helper()
	if !strings.Contains(sdl, substr) {
		t.Errorf("SDL missing %q\nGot:\n%s", substr, sdl)
	}
}

func mustNotContain(t *testing.T, sdl, substr string) {
	t.Helper()
	if strings.Contains(sdl, substr) {
		t.Errorf("SDL should not contain %q\nGot:\n%s", substr, sdl)
	}
}

// --- ToGraphQLName ---

func TestToGraphQLName(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"hello", "hello"},
		{"hello_world", "hello_world"},
		{"123start", "_123start"},
		{"with-dashes", "with_dashes"},
		{"with.dots", "with_dots"},
		{"CamelCase", "CamelCase"},
		{"", "_"},
		{"_ok", "_ok"},
		{"a b c", "a_b_c"},
	}
	for _, tt := range tests {
		if got := ToGraphQLName(tt.in); got != tt.want {
			t.Errorf("ToGraphQLName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestValidGraphQLName(t *testing.T) {
	if !ValidGraphQLName("hello_123") {
		t.Error("expected valid")
	}
	if ValidGraphQLName("123start") {
		t.Error("expected invalid")
	}
	if ValidGraphQLName("with-dash") {
		t.Error("expected invalid")
	}
}

// --- mock table ---

type mockTable struct {
	name    string
	comment string
	schema  *arrow.Schema
}

func (t *mockTable) Name() string                        { return t.name }
func (t *mockTable) Comment() string                     { return t.comment }
func (t *mockTable) ArrowSchema(cols []string) *arrow.Schema { return catalog.ProjectSchema(t.schema, cols) }
func (t *mockTable) Scan(_ context.Context, _ *catalog.ScanOptions) (array.RecordReader, error) {
	return nil, nil
}

// mockInsertableTable adds InsertableTable to make it mutable.
type mockInsertableTable struct {
	mockTable
}

func (t *mockInsertableTable) Insert(_ context.Context, _ array.RecordReader, _ *catalog.DMLOptions) (*catalog.DMLResult, error) {
	return nil, nil
}

// --- GenerateTableSDL ---

func TestGenerateTableSDL_ReadOnly(t *testing.T) {
	table := &mockTable{
		name: "events",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "ts", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		}, nil),
	}

	sdl := GenerateTableSDL("data", table)
	mustContain(t, sdl, `@view(name: "\"data\".\"EVENTS\""`)
	mustNotContain(t, sdl, "@table")
	mustContain(t, sdl, "id: BigInt! @pk")
	mustContain(t, sdl, "name: String!")
	mustContain(t, sdl, "ts: DateTime")
	mustContain(t, sdl, "type data_events")
}

func TestGenerateTableSDL_WithPK(t *testing.T) {
	table := &mockTable{
		name: "items",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "item_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table, WithPK("tenant_id", "item_id"))
	mustContain(t, sdl, "tenant_id: BigInt! @pk")
	mustContain(t, sdl, "item_id: BigInt! @pk")
	mustNotContain(t, sdl, "value: String @pk")
}

func TestGenerateTableSDL_WithDescription(t *testing.T) {
	table := &mockTable{
		name: "users",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "email", Type: arrow.BinaryTypes.String},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table,
		WithDescription("User accounts table"),
		WithFieldDescription("email", "User email address"),
	)
	mustContain(t, sdl, "User accounts table")
	mustContain(t, sdl, "User email address")
}

func TestGenerateTableSDL_WithReferences(t *testing.T) {
	table := &mockTable{
		name: "orders",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table,
		WithReferences("app_users", []string{"user_id"}, []string{"id"}, "user", "orders"),
	)
	mustContain(t, sdl, "@references")
	mustContain(t, sdl, `references_name: "app_users"`)
	mustContain(t, sdl, `source_fields: ["user_id"]`)
	mustContain(t, sdl, `references_fields: ["id"]`)
	mustContain(t, sdl, `query: "user"`)
	mustContain(t, sdl, `references_query: "orders"`)
}

func TestGenerateTableSDL_WithFieldReferences(t *testing.T) {
	table := &mockTable{
		name: "orders",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table,
		WithFieldReferences("user_id", "app_users", "user", "orders"),
	)
	mustContain(t, sdl, "@field_references")
	mustContain(t, sdl, `references_name: "app_users"`)
}

func TestGenerateTableSDL_WithM2M(t *testing.T) {
	table := &mockTable{
		name: "user_roles",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "user_id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "role_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table, WithM2M(), WithPK("user_id", "role_id"))
	mustContain(t, sdl, "is_m2m: true")
	mustContain(t, sdl, "user_id: BigInt! @pk")
	mustContain(t, sdl, "role_id: BigInt! @pk")
}

func TestGenerateTableSDL_WithFilterRequired(t *testing.T) {
	table := &mockTable{
		name: "logs",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "tenant_id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table, WithFilterRequired("tenant_id"))
	mustContain(t, sdl, "@filter_required")
}

func TestGenerateTableSDL_FieldSourceForInvalidNames(t *testing.T) {
	table := &mockTable{
		name: "data",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "user-name", Type: arrow.BinaryTypes.String},
			{Name: "123col", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
	}

	sdl := GenerateTableSDL("app", table)
	mustContain(t, sdl, `user_name: String! @field_source(field: "user-name")`)
	mustContain(t, sdl, `_123col: Int! @field_source(field: "123col")`)
}

func TestGenerateTableSDL_WithRawSDL(t *testing.T) {
	table := &mockTable{
		name: "custom",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		}, nil),
	}

	raw := `type my_custom @table(name: "custom") { id: Int! @pk }`
	sdl := GenerateTableSDL("app", table, WithRawSDL(raw))
	if sdl != raw {
		t.Errorf("expected raw SDL, got:\n%s", sdl)
	}
}

func TestGenerateTableSDL_Geometry(t *testing.T) {
	table := &mockTable{
		name: "places",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "location", Type: catalog.NewGeometryExtensionType(), Nullable: true},
		}, nil),
	}

	sdl := GenerateTableSDL("geo", table)
	mustContain(t, sdl, "location: Geometry")
}

// --- GenerateTableRefSDL ---

func TestGenerateTableRefSDL(t *testing.T) {
	ref := &mockTableRef{
		name: "remote_data",
		schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil),
	}

	sdl := GenerateTableRefSDL("ext", ref)
	mustContain(t, sdl, `@view(name: "\"ext\".\"REMOTE_DATA\""`)
	mustContain(t, sdl, "type ext_remote_data")
	mustContain(t, sdl, "id: BigInt! @pk")
	mustContain(t, sdl, "value: String")
}

type mockTableRef struct {
	name    string
	comment string
	schema  *arrow.Schema
}

func (r *mockTableRef) Name() string              { return r.name }
func (r *mockTableRef) Comment() string            { return r.comment }
func (r *mockTableRef) ArrowSchema() *arrow.Schema { return r.schema }
func (r *mockTableRef) FunctionCalls(_ context.Context, _ *catalog.FunctionCallRequest) ([]catalog.FunctionCall, error) {
	return nil, nil
}

// --- Scalar function SDL ---

func TestGenerateScalarFuncSDL(t *testing.T) {
	retType := Int64
	def := &funcDef{
		description: "Sum two numbers",
		args: []argDef{
			{name: "a", typ: Int64, description: "First operand"},
			{name: "b", typ: Int64, description: "Second operand"},
		},
		retType: &retType,
	}

	sdl := generateScalarFuncSDL("math", "add", def)
	mustContain(t, sdl, "extend type Function")
	mustContain(t, sdl, `@function(name: "\"math\".\"ADD\""`)
	mustContain(t, sdl, "Sum two numbers")
	mustContain(t, sdl, "First operand")
	mustContain(t, sdl, "a: BigInt!")
	mustContain(t, sdl, "b: BigInt!")
	mustContain(t, sdl, "): BigInt")
}

func TestGenerateScalarFuncSDL_Mutation(t *testing.T) {
	retType := String
	def := &funcDef{
		description: "Send a notification",
		args: []argDef{
			{name: "user_id", typ: String, description: "Recipient"},
			{name: "message", typ: String, description: "Message body"},
		},
		retType:    &retType,
		isMutation: true,
	}

	// Default schema: no @module directive expected
	sdl := generateScalarFuncSDL(DefaultSchema, "send", def)
	mustContain(t, sdl, "extend type MutationFunction")
	mustNotContain(t, sdl, "extend type Function ")
	mustContain(t, sdl, `@function(name: "\"default\".\"SEND\""`)
	mustContain(t, sdl, "Send a notification")
	mustContain(t, sdl, "user_id: String!")
	mustContain(t, sdl, "message: String!")
	mustContain(t, sdl, "): String")
	mustNotContain(t, sdl, "@module")
}

func TestGenerateScalarFuncSDL_Mutation_NamedSchema(t *testing.T) {
	retType := Int64
	def := &funcDef{
		retType:    &retType,
		isMutation: true,
	}

	// Named schema: both MutationFunction extension AND @module directive expected
	sdl := generateScalarFuncSDL("admin", "reset", def)
	mustContain(t, sdl, "extend type MutationFunction")
	mustContain(t, sdl, `@module(name: "admin")`)
	mustContain(t, sdl, `@function(name: "\"admin\".\"RESET\""`)
}

// --- Table function SDL (parameterized view) ---

func TestGenerateTableFuncSDL(t *testing.T) {
	def := &funcDef{
		description: "Search users",
		args: []argDef{
			{name: "query", typ: String, description: "Search query"},
		},
		cols: []colDef{
			{name: "id", typ: Int64, pk: true},
			{name: "name", typ: String, description: "Display name"},
			{name: "score", typ: Float64, nullable: true},
		},
	}

	sdl := generateTableFuncSDL("users", "search", def)

	// input type
	mustContain(t, sdl, "input users_search_args")
	mustContain(t, sdl, "query: String!")
	mustContain(t, sdl, "Search query")

	// result type as parameterized view
	mustContain(t, sdl, "type users_search")
	mustContain(t, sdl, `@view(name: "\"users\".\"SEARCH\""`)
	mustContain(t, sdl, `@args(name: "users_search_args"`)
	mustContain(t, sdl, "Search users")
	mustContain(t, sdl, "id: BigInt! @pk")
	mustContain(t, sdl, "Display name")
	mustContain(t, sdl, "score: Float")
}

func TestGenerateTableFuncSDL_NoArgs(t *testing.T) {
	def := &funcDef{
		cols: []colDef{
			{name: "id", typ: Int64, pk: true},
			{name: "name", typ: String},
		},
	}

	sdl := generateTableFuncSDL("data", "list_all", def)
	mustNotContain(t, sdl, "input ")
	mustNotContain(t, sdl, "@args")
	mustContain(t, sdl, `@view(name: "\"data\".\"LIST_ALL\""`)
}

// --- Arrow type mapping ---

func TestArrowToGraphQL(t *testing.T) {
	tests := []struct {
		dt   arrow.DataType
		want string
	}{
		{arrow.FixedWidthTypes.Boolean, "Boolean"},
		{arrow.PrimitiveTypes.Int8, "Int"},
		{arrow.PrimitiveTypes.Int16, "Int"},
		{arrow.PrimitiveTypes.Int32, "Int"},
		{arrow.PrimitiveTypes.Int64, "BigInt"},
		{arrow.PrimitiveTypes.Uint8, "UInt"},
		{arrow.PrimitiveTypes.Uint64, "BigUInt"},
		{arrow.PrimitiveTypes.Float32, "Float"},
		{arrow.PrimitiveTypes.Float64, "Float"},
		{arrow.BinaryTypes.String, "String"},
		{arrow.BinaryTypes.Binary, "Base64"},
		{arrow.FixedWidthTypes.Timestamp_us, "DateTime"},
		{arrow.FixedWidthTypes.Date32, "Date"},
		{catalog.NewGeometryExtensionType(), "Geometry"},
	}
	for _, tt := range tests {
		if got := arrowToGraphQL(tt.dt); got != tt.want {
			t.Errorf("arrowToGraphQL(%v) = %q, want %q", tt.dt, got, tt.want)
		}
	}
}
