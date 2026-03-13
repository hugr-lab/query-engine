package types

import (
	"fmt"

	"github.com/uber/h3-go/v4"
)

// Compile-time interface assertions.
var (
	_ ScalarType           = (*h3CellScalar)(nil)
	_ ValueParser          = (*h3CellScalar)(nil)
	_ ArrayParser          = (*h3CellScalar)(nil)
	_ JSONTypeHintProvider = (*h3CellScalar)(nil)
	_ SQLOutputTransformer = (*h3CellScalar)(nil)
)

type h3CellScalar struct{}

func (s *h3CellScalar) Name() string { return "H3Cell" }

func (s *h3CellScalar) SDL() string {
	return `"""
The ` + "`H3Cell`" + ` scalar type represents an H3 hexagonal hierarchical geospatial index cell identifier.
"""
scalar H3Cell`
}

func (s *h3CellScalar) JSONTypeHint() string { return "h3string" }

func (s *h3CellScalar) ParseValue(v any) (any, error) {
	return parseH3Cell(v)
}

func (s *h3CellScalar) ParseArray(v any) (any, error) {
	vv, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expected array of H3 cells, got %T", v)
	}
	out := make([]any, len(vv))
	var err error
	for i, val := range vv {
		if val == nil {
			continue
		}
		out[i], err = parseH3Cell(val)
		if err != nil {
			return nil, fmt.Errorf("invalid H3 cell value at index %d: %w", i, err)
		}
	}
	return out, nil
}

func parseH3Cell(val any) (any, error) {
	if val == nil {
		return h3.Cell(0), nil
	}
	switch v := val.(type) {
	case h3.Cell:
		return v, nil
	case string:
		return h3.IndexFromString(v), nil
	case int:
		return h3.Cell(v), nil
	case int64:
		return h3.Cell(v), nil
	case uint64:
		return h3.Cell(v), nil
	case float64:
		return h3.Cell(uint64(v)), nil
	default:
		return h3.Cell(0), fmt.Errorf("invalid H3 cell value: %v (%[1]T)", v)
	}
}

func (s *h3CellScalar) ToOutputSQL(sql string, _ bool) string {
	return "h3_h3_to_string(" + sql + ")"
}

func (s *h3CellScalar) ToStructFieldSQL(sql string) string {
	return "h3_h3_to_string(" + sql + ")"
}
