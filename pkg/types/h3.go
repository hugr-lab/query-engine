package types

import (
	"fmt"

	"github.com/uber/h3-go/v4"
)

func ParseH3Cell(val any) (any, error) {
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
