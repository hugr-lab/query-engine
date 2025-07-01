package types

import "fmt"

func ParseScalarArray[T ScalarTypes](v any) ([]T, error) {
	if v == nil {
		return nil, nil
	}
	switch v := v.(type) {
	case []T:
		return v, nil
	case []interface{}:
		a := make([]T, len(v))
		for i, e := range v {
			t, ok := e.(T)
			if !ok {
				return nil, fmt.Errorf("invalid %T array value: %v(%[2]T)", *new(T), e)
			}
			a[i] = t
		}
		return a, nil
	default:
		return nil, fmt.Errorf("invalid %T array value: %v", *new(T), v)
	}
}
