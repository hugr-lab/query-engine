package types

import (
	"encoding/json"
	"fmt"
)

// ParseJsonValue parses any value supplied as a GraphQL JSON scalar input.
// Accepts:
//   - nil
//   - strings: parsed as JSON literals (objects, arrays, numbers, strings, bools, null)
//   - any already-decoded Go value (map, slice, number, string, bool) — passed through as-is
//
// The historical contract returned only map[string]interface{} (forcing JSON inputs
// to be objects). It now returns `any` so coalesce / eq / arguments can carry full
// JSON values, matching the JSON spec rather than the legacy object-only restriction.
func ParseJsonValue(v any) (any, error) {
	if v == nil {
		return nil, nil
	}
	if s, ok := v.(string); ok {
		var out any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, fmt.Errorf("invalid json value: %w", err)
		}
		return out, nil
	}
	return v, nil
}
