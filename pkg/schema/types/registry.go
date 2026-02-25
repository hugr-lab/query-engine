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

// SubAggregationTypeName returns the sub-aggregation type name for a given aggregation type.
// E.g., "FloatAggregation" → "FloatSubAggregation". Returns "" if not found.
func SubAggregationTypeName(aggTypeName string) string {
	for _, s := range registry {
		agg, ok := s.(Aggregatable)
		if !ok || agg.AggregationTypeName() != aggTypeName {
			continue
		}
		sub, ok := s.(SubAggregatable)
		if !ok {
			return ""
		}
		return sub.SubAggregationTypeName()
	}
	return ""
}

// AggregationTypeFromSub returns the parent aggregation type name for a sub-aggregation type.
// E.g., "FloatSubAggregation" → "FloatAggregation". Returns "" if not found.
func AggregationTypeFromSub(subAggTypeName string) string {
	for _, s := range registry {
		sub, ok := s.(SubAggregatable)
		if !ok || sub.SubAggregationTypeName() != subAggTypeName {
			continue
		}
		agg, ok := s.(Aggregatable)
		if !ok {
			return ""
		}
		return agg.AggregationTypeName()
	}
	return ""
}

// IsSubAggregationType returns true if the type name is a scalar sub-aggregation type.
func IsSubAggregationType(typeName string) bool {
	return AggregationTypeFromSub(typeName) != ""
}

// JSONTypeHint returns the JSON extraction type hint for a type name.
// Checks scalars, their aggregation types, and their sub-aggregation types.
// Returns "" if not a known type or no hint available.
func JSONTypeHint(typeName string) string {
	// Direct scalar lookup
	if s := registry[typeName]; s != nil {
		if h, ok := s.(JSONTypeHintProvider); ok {
			return h.JSONTypeHint()
		}
		return ""
	}
	// Check aggregation and sub-aggregation types
	for _, s := range registry {
		h, hasHint := s.(JSONTypeHintProvider)
		if agg, ok := s.(Aggregatable); ok {
			if agg.AggregationTypeName() == typeName {
				if hasHint {
					return h.JSONTypeHint()
				}
				return ""
			}
		}
		if sub, ok := s.(SubAggregatable); ok {
			if sub.SubAggregationTypeName() == typeName {
				// Sub-aggregation types have no JSON hint
				return ""
			}
		}
		_ = hasHint
	}
	return ""
}

// IsKnownJSONType returns true if the type name has a known JSON type mapping
// (scalar, aggregation, or sub-aggregation type).
func IsKnownJSONType(typeName string) bool {
	if registry[typeName] != nil {
		return true
	}
	for _, s := range registry {
		if agg, ok := s.(Aggregatable); ok && agg.AggregationTypeName() == typeName {
			return true
		}
		if sub, ok := s.(SubAggregatable); ok && sub.SubAggregationTypeName() == typeName {
			return true
		}
	}
	return false
}

// JSONTypeHintWithOk returns the JSON extraction type hint and whether the type is known.
func JSONTypeHintWithOk(typeName string) (string, bool) {
	if !IsKnownJSONType(typeName) {
		return "", false
	}
	return JSONTypeHint(typeName), true
}

// ParseValue dispatches value parsing to the scalar type's ValueParser interface.
func ParseValue(typeName string, v any) (any, error) {
	s := Lookup(typeName)
	if s == nil {
		return v, nil
	}
	if p, ok := s.(ValueParser); ok {
		return p.ParseValue(v)
	}
	return v, nil
}

// ParseArray dispatches array parsing to the scalar type's ArrayParser interface.
func ParseArray(typeName string, v any) (any, error) {
	s := Lookup(typeName)
	if s == nil {
		return nil, fmt.Errorf("unsupported array type [%s]", typeName)
	}
	if p, ok := s.(ArrayParser); ok {
		return p.ParseArray(v)
	}
	return nil, fmt.Errorf("unsupported array type [%s]", typeName)
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
	}
	if b.Len() == 0 {
		return nil
	}
	return []*ast.Source{
		{Name: "scalar_types.graphql", Input: b.String()},
	}
}
