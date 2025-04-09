package types

import (
	"encoding/json"
	"fmt"
)

func ParseJsonValue(v any) (map[string]interface{}, error) {
	if v == nil {
		return nil, nil
	}
	switch v := v.(type) {
	case map[string]interface{}:
		return v, nil
	case string:
		var m map[string]interface{}
		err := json.Unmarshal([]byte(v), &m)
		return m, err
	default:
		return nil, fmt.Errorf("invalid json value: %v", v)
	}
}
