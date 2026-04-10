package app

import (
	"context"
	"strings"

	"github.com/hugr-lab/airport-go/catalog"
)

// muxSchema is a schema within the CatalogMux.
// Implements catalog.Schema and catalog.SchemaWithTableRefs.
type muxSchema struct {
	name            string
	comment         string
	tables          map[string]catalog.Table
	tableRefs       map[string]catalog.TableRef
	scalarFuncs     map[string]catalog.ScalarFunction
	tableFuncs      map[string]catalog.TableFunction
	tableFuncsInOut map[string]catalog.TableFunctionInOut
}

func newMuxSchema(name string) *muxSchema {
	return &muxSchema{
		name:            name,
		tables:          make(map[string]catalog.Table),
		tableRefs:       make(map[string]catalog.TableRef),
		scalarFuncs:     make(map[string]catalog.ScalarFunction),
		tableFuncs:      make(map[string]catalog.TableFunction),
		tableFuncsInOut: make(map[string]catalog.TableFunctionInOut),
	}
}

func (s *muxSchema) Name() string    { return s.name }
func (s *muxSchema) Comment() string { return s.comment }

func (s *muxSchema) Tables(ctx context.Context) ([]catalog.Table, error) {
	result := make([]catalog.Table, 0, len(s.tables))
	for _, t := range s.tables {
		result = append(result, t)
	}
	return result, nil
}

func (s *muxSchema) Table(ctx context.Context, name string) (catalog.Table, error) {
	t, ok := s.tables[name]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (s *muxSchema) ScalarFunctions(ctx context.Context) ([]catalog.ScalarFunction, error) {
	result := make([]catalog.ScalarFunction, 0, len(s.scalarFuncs))
	for _, fn := range s.scalarFuncs {
		result = append(result, fn)
	}
	return result, nil
}

func (s *muxSchema) TableFunctions(ctx context.Context) ([]catalog.TableFunction, error) {
	result := make([]catalog.TableFunction, 0, len(s.tableFuncs))
	for _, fn := range s.tableFuncs {
		result = append(result, fn)
	}
	return result, nil
}

func (s *muxSchema) TableFunctionsInOut(ctx context.Context) ([]catalog.TableFunctionInOut, error) {
	result := make([]catalog.TableFunctionInOut, 0, len(s.tableFuncsInOut))
	for _, fn := range s.tableFuncsInOut {
		result = append(result, fn)
	}
	return result, nil
}

// --- catalog.SchemaWithTableRefs ---

func (s *muxSchema) TableRefs(ctx context.Context) ([]catalog.TableRef, error) {
	result := make([]catalog.TableRef, 0, len(s.tableRefs))
	for _, ref := range s.tableRefs {
		result = append(result, ref)
	}
	return result, nil
}

func (s *muxSchema) TableRef(ctx context.Context, name string) (catalog.TableRef, error) {
	ref, ok := s.tableRefs[name]
	if !ok {
		return nil, nil
	}
	return ref, nil
}

// writeSDL writes SDL for all items that implement sdlProvider.
func (s *muxSchema) writeSDL(b *strings.Builder) {
	for _, t := range s.tables {
		if sdl := sdlOf(t); sdl != "" {
			b.WriteString(sdl)
			b.WriteString("\n")
		}
	}
	for _, ref := range s.tableRefs {
		if sdl := sdlOf(ref); sdl != "" {
			b.WriteString(sdl)
			b.WriteString("\n")
		}
	}
	for _, fn := range s.scalarFuncs {
		if sdl := sdlOf(fn); sdl != "" {
			b.WriteString(sdl)
			b.WriteString("\n")
		}
	}
	for _, fn := range s.tableFuncs {
		if sdl := sdlOf(fn); sdl != "" {
			b.WriteString(sdl)
			b.WriteString("\n")
		}
	}
	for _, fn := range s.tableFuncsInOut {
		if sdl := sdlOf(fn); sdl != "" {
			b.WriteString(sdl)
			b.WriteString("\n")
		}
	}
}

// Compile-time interface checks.
var (
	_ catalog.Schema              = (*muxSchema)(nil)
	_ catalog.SchemaWithTableRefs = (*muxSchema)(nil)
)
