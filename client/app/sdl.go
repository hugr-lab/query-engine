package app

import (
	"github.com/hugr-lab/airport-go/catalog"
)

// sdlProvider is implemented by catalog items that carry SDL.
// The CatalogMux collects SDL from all registered items via SDL().
type sdlProvider interface {
	SDL() string
}

// sdlOf extracts the SDL string from an item if it implements sdlProvider.
// Returns empty string if the item has no SDL attached.
func sdlOf(item any) string {
	if p, ok := item.(sdlProvider); ok {
		return p.SDL()
	}
	return ""
}

// --- SDL wrappers ---
// These wrap catalog items with an SDL string for hugr schema compilation.
// The wrapped item delegates all interface methods to the inner item.

// sdlTable wraps catalog.Table with SDL.
type sdlTable struct {
	catalog.Table
	sdl string
}

func (t *sdlTable) SDL() string { return t.sdl }

// WithSDL wraps a catalog.Table with its hugr SDL definition.
func WithSDL(table catalog.Table, sdl string) catalog.Table {
	return &sdlTable{Table: table, sdl: sdl}
}

// sdlScalarFunction wraps catalog.ScalarFunction with SDL.
type sdlScalarFunction struct {
	catalog.ScalarFunction
	sdl string
}

func (f *sdlScalarFunction) SDL() string { return f.sdl }

// WithScalarFuncSDL wraps a catalog.ScalarFunction with its hugr SDL definition.
func WithScalarFuncSDL(fn catalog.ScalarFunction, sdl string) catalog.ScalarFunction {
	return &sdlScalarFunction{ScalarFunction: fn, sdl: sdl}
}

// sdlTableFunction wraps catalog.TableFunction with SDL.
type sdlTableFunction struct {
	catalog.TableFunction
	sdl string
}

func (f *sdlTableFunction) SDL() string { return f.sdl }

// WithTableFuncSDL wraps a catalog.TableFunction with its hugr SDL definition.
func WithTableFuncSDL(fn catalog.TableFunction, sdl string) catalog.TableFunction {
	return &sdlTableFunction{TableFunction: fn, sdl: sdl}
}

// sdlTableFunctionInOut wraps catalog.TableFunctionInOut with SDL.
type sdlTableFunctionInOut struct {
	catalog.TableFunctionInOut
	sdl string
}

func (f *sdlTableFunctionInOut) SDL() string { return f.sdl }

// WithTableFuncInOutSDL wraps a catalog.TableFunctionInOut with its hugr SDL definition.
func WithTableFuncInOutSDL(fn catalog.TableFunctionInOut, sdl string) catalog.TableFunctionInOut {
	return &sdlTableFunctionInOut{TableFunctionInOut: fn, sdl: sdl}
}

// sdlTableRef wraps catalog.TableRef with SDL.
type sdlTableRef struct {
	catalog.TableRef
	sdl string
}

func (r *sdlTableRef) SDL() string { return r.sdl }

// WithTableRefSDL wraps a catalog.TableRef with its hugr SDL definition.
func WithTableRefSDL(ref catalog.TableRef, sdl string) catalog.TableRef {
	return &sdlTableRef{TableRef: ref, sdl: sdl}
}
