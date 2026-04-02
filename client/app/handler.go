package app

import (
	"context"
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
type Type struct {
	dt      arrow.DataType
	graphql string
}

// Arrow type references — use these in Arg(), Return(), Col() options.
var (
	Boolean   = Type{arrow.FixedWidthTypes.Boolean, "Boolean"}
	Int8      = Type{arrow.PrimitiveTypes.Int8, "Int"}
	Int16     = Type{arrow.PrimitiveTypes.Int16, "Int"}
	Int32     = Type{arrow.PrimitiveTypes.Int32, "Int"}
	Int64     = Type{arrow.PrimitiveTypes.Int64, "BigInt"}
	Uint8     = Type{arrow.PrimitiveTypes.Uint8, "UInt"}
	Uint16    = Type{arrow.PrimitiveTypes.Uint16, "UInt"}
	Uint32    = Type{arrow.PrimitiveTypes.Uint32, "UInt"}
	Uint64    = Type{arrow.PrimitiveTypes.Uint64, "BigUInt"}
	Float32   = Type{arrow.PrimitiveTypes.Float32, "Float"}
	Float64   = Type{arrow.PrimitiveTypes.Float64, "Float"}
	String    = Type{arrow.BinaryTypes.String, "String"}
	Binary    = Type{arrow.BinaryTypes.Binary, "Base64"}
	Timestamp = Type{arrow.FixedWidthTypes.Timestamp_us, "DateTime"}
	Date      = Type{arrow.FixedWidthTypes.Date32, "Date"}
	Geometry  = Type{catalog.NewGeometryExtensionType(), "Geometry"}
)

// HandlerFunc is the function signature for scalar and table function handlers.
type HandlerFunc func(w *Result, r *Request) error

// Option configures function registration.
type Option func(*funcDef)

type argDef struct {
	name        string
	typ         Type
	description string
}

type colDef struct {
	name        string
	typ         Type
	nullable    bool
	pk          bool
	description string
}

type funcDef struct {
	description string   // function description
	args        []argDef
	cols        []colDef // for table functions: result columns
	retType     *Type    // for scalar functions: return type
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

// Result writes function output.
// For scalar functions, use Set() to write the return value.
// For table functions, use Append() to write rows.
type Result struct {
	// scalar mode
	value any

	// table mode
	rows [][]any
}

// Set writes a scalar function return value.
func (w *Result) Set(v any) error {
	w.value = v
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
	if def.retType == nil {
		return fmt.Errorf("HandleFunc %s.%s: Return() option is required", schema, name)
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

	tf := &handlerTableFunc{
		funcName: strings.ToUpper(name),
		def:      def,
		handler:  handler,
	}
	tf.sdl = generateTableFuncSDL(schema, name, def)
	m.TableFunc(schema, tf)
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

		res := &Result{}
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

	res := &Result{}
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

	rec := array.NewRecord(schema, arrays, int64(nRows))
	return array.NewRecordReader(schema, []arrow.Record{rec})
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
	default:
		// Extension type builders (geometry uses binary builder underneath)
		if eb, ok := bldr.(*array.ExtensionBuilder); ok {
			return appendValue(eb.StorageBuilder(), v)
		}
		return fmt.Errorf("unsupported builder type: %T", bldr)
	}
	return nil
}

// generateScalarSDL and generateTableFuncSDL are in sdl_gen.go
