package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
)

// Type wraps an arrow.DataType with a GraphQL type name for SDL generation.
// When structDef is non-nil, the type represents a custom struct that the SDL
// generator emits as a separate type definition; the wire representation is a
// JSON string (arrow.BinaryTypes.String).
type Type struct {
	dt        arrow.DataType
	graphql   string
	structDef *StructType
}

// Arrow type references — use these in Arg(), Return(), Col() options.
var (
	Boolean   = Type{dt: arrow.FixedWidthTypes.Boolean, graphql: "Boolean"}
	Int8      = Type{dt: arrow.PrimitiveTypes.Int8, graphql: "Int"}
	Int16     = Type{dt: arrow.PrimitiveTypes.Int16, graphql: "Int"}
	Int32     = Type{dt: arrow.PrimitiveTypes.Int32, graphql: "Int"}
	Int64     = Type{dt: arrow.PrimitiveTypes.Int64, graphql: "BigInt"}
	Uint8     = Type{dt: arrow.PrimitiveTypes.Uint8, graphql: "UInt"}
	Uint16    = Type{dt: arrow.PrimitiveTypes.Uint16, graphql: "UInt"}
	Uint32    = Type{dt: arrow.PrimitiveTypes.Uint32, graphql: "UInt"}
	Uint64    = Type{dt: arrow.PrimitiveTypes.Uint64, graphql: "BigUInt"}
	Float32   = Type{dt: arrow.PrimitiveTypes.Float32, graphql: "Float"}
	Float64   = Type{dt: arrow.PrimitiveTypes.Float64, graphql: "Float"}
	String    = Type{dt: arrow.BinaryTypes.String, graphql: "String"}
	Binary    = Type{dt: arrow.BinaryTypes.Binary, graphql: "Base64"}
	Timestamp = Type{dt: arrow.FixedWidthTypes.Timestamp_us, graphql: "DateTime"}
	Date      = Type{dt: arrow.FixedWidthTypes.Date32, graphql: "Date"}
	Geometry  = Type{dt: catalog.NewGeometryExtensionType(), graphql: "Geometry"}

	// JSON is the raw JSON scalar type. The wire representation is a string;
	// the handler is responsible for json.Marshal / json.Unmarshal of complex
	// values. In the public GraphQL schema this is the JSON scalar — clients
	// receive the raw JSON value.
	JSON = Type{dt: arrow.BinaryTypes.String, graphql: "JSON"}
)

// HandlerFunc is the function signature for scalar and table function handlers.
type HandlerFunc func(w *Result, r *Request) error

// Option configures function registration.
type Option func(*funcDef)

type argDef struct {
	name        string
	typ         Type
	description string

	// contextValue, if non-empty, marks this argument as server-injected from
	// a context placeholder (e.g. AuthUserID). The generated SDL emits an
	// @arg_default directive on the argument, hugr filters it from clients,
	// and the planner injects the resolved context value at request time.
	contextValue ContextPlaceholder
}

type colDef struct {
	name        string
	typ         Type
	nullable    bool
	pk          bool
	description string
}

type funcDef struct {
	description string // function description
	args        []argDef
	cols        []colDef // for table functions: result columns
	retType     *Type    // for scalar functions: return type
	isMutation  bool     // for scalar functions: extend MutationFunction instead of Function

	// returnsList is true when the function returns a list of scalars
	// (registered via app.ReturnList). The wire format is JSON.
	returnsList bool

	// invalidReturn carries a registration-time validation error message.
	// When non-empty, registerScalarFunc returns an error before SDL generation.
	invalidReturn string
}

// returnTypeName returns a human-readable return-type name for error messages,
// safely handling table functions (where retType is nil).
func returnTypeName(d *funcDef) string {
	if d == nil || d.retType == nil {
		return "<none>"
	}
	return d.retType.graphql
}

// returnsJSON reports whether the function's return type uses the JSON wire
// format (raw JSON or struct via JSON). Used by Result.SetJSON to validate
// that the handler is using the right helper.
//
// Note: list-of-scalars returns (registered via ReturnList) use a native
// Arrow LIST as the wire type, NOT JSON. The handler should call Set with a
// Go slice ([]string, []int64, etc.), not SetJSON.
func (d *funcDef) returnsJSON() bool {
	if d.retType == nil {
		return false
	}
	if d.retType.structDef != nil {
		return true
	}
	return d.retType.graphql == "JSON"
}

// Desc sets the function description.
func Desc(description string) Option {
	return func(d *funcDef) {
		d.description = description
	}
}

// Arg declares a function argument.
func Arg(name string, typ Type) Option {
	return func(d *funcDef) {
		d.args = append(d.args, argDef{name: name, typ: typ})
	}
}

// ArgDesc declares a function argument with a description.
func ArgDesc(name string, typ Type, description string) Option {
	return func(d *funcDef) {
		d.args = append(d.args, argDef{name: name, typ: typ, description: description})
	}
}

// Return declares the scalar function return type.
func Return(typ Type) Option {
	return func(d *funcDef) {
		d.retType = &typ
	}
}

// ReturnList declares that the scalar function returns a list of the given
// scalar element type (e.g. ReturnList(app.String) → [String!]).
//
// The wire format is a native Arrow LIST of the element's underlying type
// (NOT JSON), so DuckDB receives a proper LIST value and the planner does
// not need json_cast. The handler returns a Go slice via Set.
//
// ReturnList does NOT support struct element types — for lists of structs,
// register the function as a table function via HandleTableFunc instead.
// Passing a struct type sets a registration error that fails registration.
//
// The generated SDL preserves the scalar-return convention (outer-nullable,
// element-non-null): `[String!]`.
func ReturnList(typ Type) Option {
	return func(d *funcDef) {
		if typ.structDef != nil {
			d.invalidReturn = "ReturnList does not support struct element types; use HandleTableFunc for lists of structs"
			return
		}
		// Element graphql name is preserved in retType.graphql; the
		// returnsList flag tells the SDL generator to wrap it in a proper
		// ast.Type{Elem:...} list rather than stuffing brackets into a
		// named type.
		listType := Type{
			dt:      arrow.ListOfNonNullable(typ.dt),
			graphql: typ.graphql,
		}
		d.retType = &listType
		d.returnsList = true
	}
}

// Mutation marks a scalar function as a mutation. The generated SDL will
// extend MutationFunction instead of Function, exposing the function as a
// GraphQL mutation operation rather than a query.
func Mutation() Option {
	return func(d *funcDef) {
		d.isMutation = true
	}
}

// ArgFromContext declares a function argument whose value is server-injected
// from a context placeholder. The argument is hidden from the GraphQL schema —
// clients cannot pass a value for it. At handler execution time, read it via
// Request.String(name), Request.Int64(name), etc., the same as a regular argument.
//
// Use the predefined ContextPlaceholder constants (AuthUserID, AuthRole, etc.).
// Passing an unknown placeholder fails at registration time.
//
// Example:
//
//	mux.HandleFunc("default", "my_orders", handler,
//	    app.Arg("limit", app.Int64),
//	    app.ArgFromContext("user_id", app.String, app.AuthUserID),
//	    app.Return(app.String),
//	)
func ArgFromContext(name string, typ Type, placeholder ContextPlaceholder) Option {
	return func(d *funcDef) {
		d.args = append(d.args, argDef{
			name:         name,
			typ:          typ,
			contextValue: placeholder,
		})
	}
}

// Col declares a result column for a table function.
func Col(name string, typ Type) Option {
	return func(d *funcDef) {
		d.cols = append(d.cols, colDef{name: name, typ: typ})
	}
}

// ColDesc declares a result column with a description.
func ColDesc(name string, typ Type, description string) Option {
	return func(d *funcDef) {
		d.cols = append(d.cols, colDef{name: name, typ: typ, description: description})
	}
}

// ColPK declares a primary key result column for a table function.
func ColPK(name string, typ Type) Option {
	return func(d *funcDef) {
		d.cols = append(d.cols, colDef{name: name, typ: typ, pk: true})
	}
}

// ColNullable declares a nullable result column for a table function.
func ColNullable(name string, typ Type) Option {
	return func(d *funcDef) {
		d.cols = append(d.cols, colDef{name: name, typ: typ, nullable: true})
	}
}

// Request provides typed access to function arguments.
type Request struct {
	ctx  context.Context
	args map[string]any
}

// Context returns the request context.
func (r *Request) Context() context.Context { return r.ctx }

func (r *Request) Bool(name string) bool             { v, _ := r.args[name].(bool); return v }
func (r *Request) Int8(name string) int8             { v, _ := r.args[name].(int8); return v }
func (r *Request) Int16(name string) int16           { v, _ := r.args[name].(int16); return v }
func (r *Request) Int32(name string) int32           { v, _ := r.args[name].(int32); return v }
func (r *Request) Int64(name string) int64           { v, _ := r.args[name].(int64); return v }
func (r *Request) Uint8(name string) uint8           { v, _ := r.args[name].(uint8); return v }
func (r *Request) Uint16(name string) uint16         { v, _ := r.args[name].(uint16); return v }
func (r *Request) Uint32(name string) uint32         { v, _ := r.args[name].(uint32); return v }
func (r *Request) Uint64(name string) uint64         { v, _ := r.args[name].(uint64); return v }
func (r *Request) Float32(name string) float32       { v, _ := r.args[name].(float32); return v }
func (r *Request) Float64(name string) float64       { v, _ := r.args[name].(float64); return v }
func (r *Request) String(name string) string         { v, _ := r.args[name].(string); return v }
func (r *Request) Bytes(name string) []byte          { v, _ := r.args[name].([]byte); return v }
func (r *Request) Geometry(name string) orb.Geometry { v, _ := r.args[name].(orb.Geometry); return v }
func (r *Request) Get(name string) any               { return r.args[name] }

// JSON unmarshals the named argument's JSON string value into out.
// Use this for arguments declared with Arg(name, JSON) or Arg(name, struct.AsType()),
// where the planner inlines the GraphQL input value as a JSON literal that
// arrives at the handler as a string.
//
// Returns nil (no-op) if the argument is empty.
func (r *Request) JSON(name string, out any) error {
	s, _ := r.args[name].(string)
	if s == "" {
		return nil
	}
	return json.Unmarshal([]byte(s), out)
}

// Result writes function output.
// For scalar functions, use Set() to write the return value.
// For table functions, use Append() to write rows.
type Result struct {
	// scalar mode
	value any

	// table mode
	rows [][]any

	// def is a back-pointer to the function definition, used by SetJSON to
	// validate that the function's return type accepts JSON output.
	def *funcDef
}

// Set writes a scalar function return value.
func (w *Result) Set(v any) error {
	w.value = v
	return nil
}

// SetJSON marshals the value to a JSON string and stores it as the scalar
// return. Use this when the function's Return type is JSON or a struct (via
// Struct().AsType()).
//
// Returns an error if the function's return type is not JSON-compatible
// (including when called on a table function).
func (w *Result) SetJSON(v any) error {
	if w.def != nil && !w.def.returnsJSON() {
		return fmt.Errorf("Result.SetJSON: only valid for JSON or struct scalar return types, got %s", returnTypeName(w.def))
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("Result.SetJSON: marshal: %w", err)
	}
	w.value = string(b)
	return nil
}

// SetJSONValue is the most flexible JSON setter. It accepts:
//   - string: stored as-is (assumed valid JSON)
//   - []byte: stored as string(b) (assumed valid JSON)
//   - any other value: marshaled via json.Marshal
//
// Returns an error if the function's return type is not JSON-compatible
// (including when called on a table function).
func (w *Result) SetJSONValue(v any) error {
	if w.def != nil && !w.def.returnsJSON() {
		return fmt.Errorf("Result.SetJSONValue: only valid for JSON or struct scalar return types, got %s", returnTypeName(w.def))
	}
	switch val := v.(type) {
	case string:
		w.value = val
	case []byte:
		w.value = string(val)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("Result.SetJSONValue: marshal: %w", err)
		}
		w.value = string(b)
	}
	return nil
}

// Append writes a row to a table function result.
// Values must match the column order from Col() options.
func (w *Result) Append(values ...any) error {
	w.rows = append(w.rows, values)
	return nil
}

// --- HandleFunc (scalar) ---

// registerScalarFunc builds a handlerScalarFunc and registers it on the mux.
func registerScalarFunc(m *CatalogMux, schema, name string, handler HandlerFunc, opts []Option) error {
	def := &funcDef{}
	for _, o := range opts {
		o(def)
	}
	if def.invalidReturn != "" {
		return fmt.Errorf("HandleFunc %s.%s: %s", schema, name, def.invalidReturn)
	}
	if def.retType == nil {
		return fmt.Errorf("HandleFunc %s.%s: Return() option is required", schema, name)
	}
	if err := validateArgs(schema, name, def.args); err != nil {
		return err
	}
	if err := m.registerStructTypes(def); err != nil {
		return fmt.Errorf("HandleFunc %s.%s: %w", schema, name, err)
	}

	sf := &handlerScalarFunc{
		funcName: strings.ToUpper(name),
		def:      def,
		handler:  handler,
	}
	sf.sdl = generateScalarFuncSDL(schema, name, def)
	m.ScalarFunc(schema, sf)
	return nil
}

// registerTableFunc builds a handlerTableFunc and registers it on the mux.
func registerTableFunc(m *CatalogMux, schema, name string, handler HandlerFunc, opts []Option) error {
	def := &funcDef{}
	for _, o := range opts {
		o(def)
	}
	if len(def.cols) == 0 {
		return fmt.Errorf("HandleTableFunc %s.%s: at least one Col() option is required", schema, name)
	}
	if def.isMutation {
		return fmt.Errorf("HandleTableFunc %s.%s: Mutation() is only valid for scalar functions", schema, name)
	}
	if err := validateArgs(schema, name, def.args); err != nil {
		return err
	}
	if err := m.registerStructTypes(def); err != nil {
		return fmt.Errorf("HandleTableFunc %s.%s: %w", schema, name, err)
	}

	tf := &handlerTableFunc{
		funcName: strings.ToUpper(name),
		def:      def,
		handler:  handler,
	}
	tf.sdl = generateTableFuncSDL(schema, name, def)
	m.TableFunc(schema, tf)
	return nil
}

// validateArgs checks the function's argument list for duplicate names and
// validates ArgFromContext placeholders against the known whitelist.
func validateArgs(schema, name string, args []argDef) error {
	seen := make(map[string]bool, len(args))
	for _, a := range args {
		if seen[a.name] {
			return fmt.Errorf("%s.%s: duplicate argument name %q", schema, name, a.name)
		}
		seen[a.name] = true
		if a.contextValue != "" && !IsKnownArgPlaceholder(a.contextValue) {
			return fmt.Errorf("%s.%s: ArgFromContext placeholder %q is not a known context variable", schema, name, string(a.contextValue))
		}
	}
	return nil
}

// --- scalar function adapter ---

type handlerScalarFunc struct {
	funcName string
	def      *funcDef
	handler  HandlerFunc
	sdl      string
}

func (f *handlerScalarFunc) Name() string    { return f.funcName }
func (f *handlerScalarFunc) Comment() string { return f.def.description }
func (f *handlerScalarFunc) SDL() string     { return f.sdl }

func (f *handlerScalarFunc) Signature() catalog.FunctionSignature {
	params := make([]arrow.DataType, len(f.def.args))
	for i, a := range f.def.args {
		params[i] = a.typ.dt
	}
	return catalog.FunctionSignature{
		Parameters: params,
		ReturnType: f.def.retType.dt,
	}
}

func (f *handlerScalarFunc) Execute(ctx context.Context, input arrow.RecordBatch) (arrow.Array, error) {
	nRows := int(input.NumRows())
	mem := memory.DefaultAllocator
	bldr := array.NewBuilder(mem, f.def.retType.dt)
	defer bldr.Release()

	for row := 0; row < nRows; row++ {
		req := &Request{ctx: ctx, args: make(map[string]any, len(f.def.args))}
		for i, a := range f.def.args {
			v, err := arrowValueAt(input.Column(i), row)
			if err != nil {
				return nil, fmt.Errorf("row %d, arg %q: %w", row, a.name, err)
			}
			req.args[a.name] = v
		}

		res := &Result{def: f.def}
		if err := f.handler(res, req); err != nil {
			return nil, fmt.Errorf("row %d: %w", row, err)
		}
		if err := appendValue(bldr, res.value); err != nil {
			return nil, fmt.Errorf("row %d: %w", row, err)
		}
	}
	return bldr.NewArray(), nil
}

// --- table function adapter ---

type handlerTableFunc struct {
	funcName string
	def      *funcDef
	handler  HandlerFunc
	sdl      string
}

func (f *handlerTableFunc) Name() string    { return f.funcName }
func (f *handlerTableFunc) Comment() string { return f.def.description }
func (f *handlerTableFunc) SDL() string     { return f.sdl }

func (f *handlerTableFunc) Signature() catalog.FunctionSignature {
	params := make([]arrow.DataType, len(f.def.args))
	for i, a := range f.def.args {
		params[i] = a.typ.dt
	}
	return catalog.FunctionSignature{Parameters: params}
}

func (f *handlerTableFunc) resultSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(f.def.cols))
	for i, c := range f.def.cols {
		fields[i] = arrow.Field{Name: c.name, Type: c.typ.dt, Nullable: c.nullable}
	}
	return arrow.NewSchema(fields, nil)
}

func (f *handlerTableFunc) SchemaForParameters(ctx context.Context, params []any) (*arrow.Schema, error) {
	return f.resultSchema(), nil
}

func (f *handlerTableFunc) Execute(ctx context.Context, params []any, opts *catalog.ScanOptions) (array.RecordReader, error) {
	req := &Request{ctx: ctx, args: make(map[string]any, len(f.def.args))}
	for i, a := range f.def.args {
		if i < len(params) {
			req.args[a.name] = params[i]
		}
	}

	res := &Result{def: f.def}
	if err := f.handler(res, req); err != nil {
		return nil, err
	}

	schema := f.resultSchema()
	nCols := len(f.def.cols)
	nRows := len(res.rows)
	mem := memory.DefaultAllocator

	builders := make([]array.Builder, nCols)
	for i, c := range f.def.cols {
		builders[i] = array.NewBuilder(mem, c.typ.dt)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	for _, row := range res.rows {
		for col := 0; col < nCols; col++ {
			var v any
			if col < len(row) {
				v = row[col]
			}
			if err := appendValue(builders[col], v); err != nil {
				return nil, fmt.Errorf("col %d: %w", col, err)
			}
		}
	}

	arrays := make([]arrow.Array, nCols)
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()

	rec := array.NewRecordBatch(schema, arrays, int64(nRows))
	return array.NewRecordReader(schema, []arrow.RecordBatch{rec})
}

// --- Arrow helpers ---

func arrowValueAt(arr arrow.Array, idx int) (any, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx), nil
	case *array.Int8:
		return a.Value(idx), nil
	case *array.Int16:
		return a.Value(idx), nil
	case *array.Int32:
		return a.Value(idx), nil
	case *array.Int64:
		return a.Value(idx), nil
	case *array.Uint8:
		return a.Value(idx), nil
	case *array.Uint16:
		return a.Value(idx), nil
	case *array.Uint32:
		return a.Value(idx), nil
	case *array.Uint64:
		return a.Value(idx), nil
	case *array.Float32:
		return a.Value(idx), nil
	case *array.Float64:
		return a.Value(idx), nil
	case *array.String:
		return a.Value(idx), nil
	case *array.Binary:
		return a.Value(idx), nil
	case *catalog.GeometryArray:
		return a.Value(idx)
	default:
		// Try extension arrays (e.g., geometry stored as binary)
		if ext, ok := arr.(array.ExtensionArray); ok {
			return arrowValueAt(ext.Storage(), idx)
		}
		return nil, fmt.Errorf("unsupported array type: %T", arr)
	}
}

func appendValue(bldr array.Builder, v any) error {
	if v == nil {
		bldr.AppendNull()
		return nil
	}
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int8Builder:
		b.Append(v.(int8))
	case *array.Int16Builder:
		b.Append(v.(int16))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		switch val := v.(type) {
		case int64:
			b.Append(val)
		case int:
			b.Append(int64(val))
		default:
			return fmt.Errorf("expected int64, got %T", v)
		}
	case *array.Uint8Builder:
		b.Append(v.(uint8))
	case *array.Uint16Builder:
		b.Append(v.(uint16))
	case *array.Uint32Builder:
		b.Append(v.(uint32))
	case *array.Uint64Builder:
		switch val := v.(type) {
		case uint64:
			b.Append(val)
		case uint:
			b.Append(uint64(val))
		default:
			return fmt.Errorf("expected uint64, got %T", v)
		}
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		switch val := v.(type) {
		case float64:
			b.Append(val)
		case float32:
			b.Append(float64(val))
		case int:
			b.Append(float64(val))
		default:
			return fmt.Errorf("expected float64, got %T", v)
		}
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.BinaryBuilder:
		switch val := v.(type) {
		case []byte:
			b.Append(val)
		case orb.Geometry:
			data, err := wkb.Marshal(val)
			if err != nil {
				return fmt.Errorf("geometry encode: %w", err)
			}
			b.Append(data)
		default:
			return fmt.Errorf("expected []byte or orb.Geometry, got %T", v)
		}
	case *array.ListBuilder:
		// Native Arrow LIST builder for ReturnList(scalar) — accepts a Go slice
		// of the underlying scalar type. Append each element to the value builder.
		b.Append(true)
		valueBldr := b.ValueBuilder()
		return appendListValues(valueBldr, v)
	default:
		// Extension type builders (geometry uses binary builder underneath)
		if eb, ok := bldr.(*array.ExtensionBuilder); ok {
			return appendValue(eb.StorageBuilder(), v)
		}
		return fmt.Errorf("unsupported builder type: %T", bldr)
	}
	return nil
}

// appendListValues iterates over a Go slice and appends each element to the
// list builder's value builder. Supports common scalar slice types directly
// and falls back to []any with per-element appendValue. For slice element
// types not listed here, wrap the values in []any or convert to one of the
// supported slice types.
func appendListValues(valueBldr array.Builder, v any) error {
	switch slice := v.(type) {
	case []string:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []bool:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []int:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []int8:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []int16:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []int32:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []int64:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []uint8:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []uint16:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []uint32:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []uint64:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []float32:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []float64:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	case []any:
		for _, x := range slice {
			if err := appendValue(valueBldr, x); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported list value type: %T (expected a scalar slice like []string, []int64, []float64, or []any)", v)
	}
	return nil
}

// generateScalarSDL and generateTableFuncSDL are in sdl_gen.go
