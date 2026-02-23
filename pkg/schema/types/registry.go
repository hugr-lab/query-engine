package types

import (
	"fmt"
	"iter"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
)

var registry = make(map[string]ScalarType)

// Register adds a scalar type to the global registry.
// If a scalar with the same name already exists, it is overwritten (last-write-wins).
func Register(scalar ScalarType) {
	registry[scalar.Name()] = scalar
}

// Build validates that all registered scalars have non-empty names and SDL.
// Full SDL parsing validation happens at compile time when all sources are combined.
func Build() error {
	for _, s := range registry {
		if s.Name() == "" {
			return fmt.Errorf("scalar with empty name registered")
		}
		if s.SDL() == "" {
			return fmt.Errorf("scalar %s: empty SDL", s.Name())
		}
	}
	return nil
}

// Lookup returns the scalar type with the given name, or nil if not found.
func Lookup(name string) ScalarType {
	return registry[name]
}

// IsScalar returns true if a scalar with the given name is registered.
func IsScalar(name string) bool {
	_, ok := registry[name]
	return ok
}

// Scalars returns an iterator over all registered scalar types.
func Scalars() iter.Seq[ScalarType] {
	return func(yield func(ScalarType) bool) {
		for _, s := range registry {
			if !yield(s) {
				return
			}
		}
	}
}

// Sources returns all scalar SDL merged into ast.Source entries.
func Sources() []*ast.Source {
	var b strings.Builder
	for _, s := range registry {
		sdl := s.SDL()
		if sdl != "" {
			b.WriteString(sdl)
			b.WriteByte('\n')
		}
		if f, ok := s.(Filterable); ok {
			b.WriteString(f.FilterSDL())
			b.WriteByte('\n')
		}
		if lf, ok := s.(ListFilterable); ok {
			b.WriteString(lf.ListFilterSDL())
			b.WriteByte('\n')
		}
		if a, ok := s.(Aggregatable); ok {
			b.WriteString(a.AggregationSDL())
			b.WriteByte('\n')
		}
		if ma, ok := s.(MeasurementAggregatable); ok {
			b.WriteString(ma.MeasurementAggregationSDL())
			b.WriteByte('\n')
		}
	}
	if b.Len() == 0 {
		return nil
	}
	return []*ast.Source{
		{Name: "scalar_types.graphql", Input: b.String()},
	}
}
