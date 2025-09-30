package types

import (
	"fmt"
	"strconv"
	"strings"
)

type Vector []float64

func (v Vector) Len() int {
	return len(v)
}

func (v Vector) MarshalJSON() ([]byte, error) {
	out := "\"["
	for i, item := range v {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%f", item)
	}
	out += "]\""
	return []byte(out), nil
}

func (v *Vector) UnmarshalJSON(data []byte) error {
	val := string(data)
	val = strings.Trim(val, "\"")
	vec, err := ParseVector(val)
	if err != nil {
		return err
	}
	*v = vec
	return nil
}

func ParseVector(data any) (Vector, error) {
	if data == nil {
		return nil, nil
	}
	switch v := data.(type) {
	case []float64:
		return Vector(v), nil
	case []int:
		out := make(Vector, len(v))
		for i, item := range v {
			out[i] = float64(item)
		}
		return out, nil
	case []int64:
		out := make(Vector, len(v))
		for i, item := range v {
			out[i] = float64(item)
		}
		return out, nil
	case []any:
		out := make(Vector, len(v))
		for i, item := range v {
			switch item := item.(type) {
			case float64:
				out[i] = item
			case int:
				out[i] = float64(item)
			case int64:
				out[i] = float64(item)
			default:
				return nil, fmt.Errorf("invalid vector element at index %d: %T", i, item)
			}
		}
		return out, nil
	case string:
		out := make(Vector, 0, len(v)/2+1)
		var curr string
		var lc rune
		for _, lc = range []rune(v) {
			switch lc {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '-', 'e', 'E':
				curr += string(lc)
			case ',':
				if curr == "" {
					curr = "0"
				}
				val, err := strconv.ParseFloat(curr, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid vector element: %s", curr)
				}
				out = append(out, val)
				curr = ""
			case ' ', '"', '[', ']':
				lc = ' '
			default:
				return nil, fmt.Errorf("invalid vector element: %c", lc)
			}
		}
		if curr != "" {
			val, err := strconv.ParseFloat(curr, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid vector element: %s", curr)
			}
			out = append(out, val)
		}
		if lc == ',' {
			out = append(out, 0)
		}
		if len(out) == 0 {
			return out, nil
		}
		return out, nil
	case Vector:
		return v, nil
	default:
		return nil, fmt.Errorf("invalid vector data: %T", data)
	}
}
