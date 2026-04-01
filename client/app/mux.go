package app

import (
	"context"
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
// All methods are safe for concurrent use from multiple goroutines.
type CatalogMux struct {
	mu      sync.RWMutex
	schemas map[string]*muxSchema
	rawSDL  string // user-provided SDL override; if set, SDL() returns this
}

// New creates a new empty CatalogMux.
func New() *CatalogMux {
	return &CatalogMux{
		schemas: make(map[string]*muxSchema),
	}
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
// If WithSDL was called, returns that string. Otherwise collects SDL from all registered items.
func (m *CatalogMux) SDL() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.rawSDL != "" {
		return m.rawSDL
	}
	var b strings.Builder
	for _, s := range m.schemas {
		s.writeSDL(&b)
	}
	return b.String()
}

// SDLWithModules returns the full GraphQL SDL with module directives.
// Objects from defaultSchema have no @module. Other schemas get @module(name: schemaName).
// System schemas (_mount, _funcs) are excluded.
func (m *CatalogMux) SDLWithModules(defaultSchema string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.rawSDL != "" {
		return m.rawSDL
	}
	if defaultSchema == "" {
		defaultSchema = "default"
	}
	var b strings.Builder
	for name, s := range m.schemas {
		switch name {
		case "_mount", "_funcs":
			continue // system schemas excluded
		case defaultSchema:
			s.writeSDL(&b) // no @module
		default:
			s.writeSDLWithModule(&b, name) // adds @module(name)
		}
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
