package app

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/hugr-lab/airport-go/catalog"
)

// CatalogMux is a goroutine-safe catalog multiplexer.
// Like http.ServeMux routes URLs to handlers, CatalogMux routes schema+name
// pairs to functions, tables, and table references.
//
// The catalog is static once built — SDL does not change at runtime.
// For dynamic multi-catalog scenarios, see MultiCatalogProvider in client/apps.
//
// Thread safety: read methods (Schemas, Schema, SDL) and the direct
// registration methods (ScalarFunc, TableFunc, Table, TableRef, ...) are
// safe for concurrent use. The higher-level registration helpers HandleFunc
// and HandleTableFunc additionally update the shared struct-type registry
// and are **not** goroutine-safe among themselves: build the catalog from a
// single goroutine (typically Application.Catalog), then publish it. After
// the catalog is handed to the runtime, all subsequent reads are safe for
// any number of goroutines.
type CatalogMux struct {
	mu      sync.RWMutex
	schemas map[string]*muxSchema
	rawSDL  string // user-provided SDL override; if set, SDL() returns this

	// structTypes tracks registered struct type definitions by name for
	// deduplication. The same struct used in multiple functions is emitted
	// once in SDL; conflicting definitions (same name, different fields)
	// fail at registration time.
	structTypes map[string]*StructType
}

// New creates a new empty CatalogMux.
func New() *CatalogMux {
	return &CatalogMux{
		schemas:     make(map[string]*muxSchema),
		structTypes: make(map[string]*StructType),
	}
}

// registerStructTypes walks a function definition and registers each struct
// type it references (return type and arguments). For each struct:
//   - if not yet registered, store it
//   - if already registered with identical fields, reuse silently
//   - if already registered with different fields, return an error
//
// Not goroutine-safe: see the thread-safety note on CatalogMux. Call only
// during single-goroutine catalog construction (before publishing to the
// runtime).
func (m *CatalogMux) registerStructTypes(def *funcDef) error {
	if def == nil {
		return nil
	}
	collect := func(t *Type) error {
		if t == nil || t.structDef == nil {
			return nil
		}
		s := t.structDef
		existing, ok := m.structTypes[s.name]
		if !ok {
			m.structTypes[s.name] = s
			return nil
		}
		if !equalStructFields(existing, s) {
			return fmt.Errorf("struct type %q already registered with different fields", s.name)
		}
		return nil
	}
	if err := collect(def.retType); err != nil {
		return err
	}
	for i := range def.args {
		if err := collect(&def.args[i].typ); err != nil {
			return err
		}
	}
	return nil
}

// HandleFunc registers a Go function as a scalar function.
// The handler is called once per row with typed arguments via Request.
// Options declare arguments and return type: Arg(), Return().
//
//	mux.HandleFunc("math", "add", func(w *app.Result, r *app.Request) error {
//	    return w.Set(r.Int64("a") + r.Int64("b"))
//	}, app.Arg("a", app.Int64), app.Arg("b", app.Int64), app.Return(app.Int64))
func (m *CatalogMux) HandleFunc(schema, name string, handler HandlerFunc, opts ...Option) error {
	return registerScalarFunc(m, schema, name, handler, opts)
}

// HandleTableFunc registers a Go function as a table function.
// The handler is called once and writes rows via Result.Append().
// Options declare arguments and result columns: Arg(), Col(), ColPK(), ColNullable().
//
//	mux.HandleTableFunc("users", "search", func(w *app.Result, r *app.Request) error {
//	    w.Append(int64(1), "Alice")
//	    return nil
//	}, app.Arg("query", app.String), app.ColPK("id", app.Int64), app.Col("name", app.String))
func (m *CatalogMux) HandleTableFunc(schema, name string, handler HandlerFunc, opts ...Option) error {
	return registerTableFunc(m, schema, name, handler, opts)
}

// ScalarFunc registers a catalog.ScalarFunction directly.
func (m *CatalogMux) ScalarFunc(schema string, fn catalog.ScalarFunction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateSchema(schema)
	s.scalarFuncs[fn.Name()] = fn
}

// TableFunc registers a catalog.TableFunction directly.
func (m *CatalogMux) TableFunc(schema string, fn catalog.TableFunction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateSchema(schema)
	s.tableFuncs[fn.Name()] = fn
}

// TableFuncInOut registers a catalog.TableFunctionInOut directly.
func (m *CatalogMux) TableFuncInOut(schema string, fn catalog.TableFunctionInOut) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateSchema(schema)
	s.tableFuncsInOut[fn.Name()] = fn
}

// Table registers a catalog.Table directly.
// If the table doesn't have SDL attached (via WithSDL), SDL is auto-generated
// from the Arrow schema. Mutable tables → @table, read-only → @view.
// SchemaOptions configure PK, references, field_references, etc.
func (m *CatalogMux) Table(schema string, table catalog.Table, opts ...SchemaOption) {
	if sdlOf(table) == "" {
		if sdl := GenerateTableSDL(schema, table, opts...); sdl != "" {
			table = WithSDL(table, sdl)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateSchema(schema)
	s.tables[table.Name()] = table
}

// TableRef registers a catalog.TableRef in the given schema.
// If the ref doesn't have SDL attached (via WithTableRefSDL), SDL is auto-generated
// from the Arrow schema as a @view.
// SchemaOptions configure PK, references, field_references, etc.
func (m *CatalogMux) TableRef(schema string, ref catalog.TableRef, opts ...SchemaOption) {
	if sdlOf(ref) == "" {
		if sdl := GenerateTableRefSDL(schema, ref, opts...); sdl != "" {
			ref = WithTableRefSDL(ref, sdl)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getOrCreateSchema(schema)
	s.tableRefs[ref.Name()] = ref
}

// --- catalog.Catalog ---

func (m *CatalogMux) Schemas(ctx context.Context) ([]catalog.Schema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]catalog.Schema, 0, len(m.schemas))
	for _, s := range m.schemas {
		result = append(result, s)
	}
	return result, nil
}

func (m *CatalogMux) Schema(ctx context.Context, name string) (catalog.Schema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.schemas[name]
	if !ok {
		return nil, nil
	}
	return s, nil
}

// --- SDL ---

// WithSDL sets a raw SDL string that overrides all auto-generated SDL.
// When set, SDL() returns this string instead of collecting from registered items.
func (m *CatalogMux) WithSDL(sdl string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rawSDL = sdl
}

// SDL returns the full GraphQL SDL for the catalog.
// If WithSDL was called, returns that string. Otherwise collects SDL from all
// registered items across non-system schemas. Objects from the default schema
// have no @module directive; objects from named schemas get @module(name)
// injected per-definition during SDL generation (see schemaModuleName in
// sdl_gen.go). System schemas (_mount, _funcs) are excluded.
func (m *CatalogMux) SDL() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.rawSDL != "" {
		return m.rawSDL
	}
	var b strings.Builder
	for name, s := range m.schemas {
		if ReservedSchemas[name] {
			continue
		}
		s.writeSDL(&b)
	}
	return b.String()
}

// --- internal ---

func (m *CatalogMux) getOrCreateSchema(name string) *muxSchema {
	s, ok := m.schemas[name]
	if !ok {
		s = newMuxSchema(name)
		m.schemas[name] = s
	}
	return s
}

// Compile-time interface check.
var _ catalog.Catalog = (*CatalogMux)(nil)
