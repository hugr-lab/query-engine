package engines

import (
	"fmt"
	"strings"
)

// JSONFieldFilterSQL generates SQL for a single JSONFieldFilter entry.
// It extracts path, coalesce, and comparison operators from the filter map.
func JSONFieldFilterSQL(e Engine, sqlName string, filter map[string]any, params []any) (string, []any, error) {
	pathVal, ok := filter["path"]
	if !ok {
		return "", nil, fmt.Errorf("field filter requires 'path'")
	}
	path, ok := pathVal.(string)
	if !ok || path == "" {
		return "", nil, fmt.Errorf("field filter 'path' must be a non-empty string")
	}

	// Check for coalesce — if present, wrap the extracted value
	coalesceVal, hasCoalesce := filter["coalesce"]

	// Build the base SQL name with path applied
	var baseSQLName string
	if hasCoalesce && coalesceVal != nil {
		extracted := e.FieldValueByPath(sqlName, path)
		coalesceSQLVal, err := e.SQLValue(coalesceVal)
		if err != nil {
			return "", nil, fmt.Errorf("invalid coalesce value: %w", err)
		}
		baseSQLName = fmt.Sprintf("COALESCE(%s, %s)", extracted, coalesceSQLVal)
	} else {
		baseSQLName = sqlName
	}

	var filters []string
	for op, v := range filter {
		if op == "path" || op == "coalesce" || v == nil {
			continue
		}
		switch op {
		case "in":
			// in: [JSON!] — value IN list, expand to OR of eq
			arr, ok := v.([]any)
			if !ok {
				return "", nil, fmt.Errorf("in filter value must be an array")
			}
			var orParts []string
			for _, item := range arr {
				var q string
				var p []any
				var err error
				if hasCoalesce && coalesceVal != nil {
					q, p, err = e.FilterOperationSQLValue(baseSQLName, "", "eq", item, params)
				} else {
					q, p, err = e.FilterOperationSQLValue(baseSQLName, path, "eq", item, params)
				}
				if err != nil {
					return "", nil, err
				}
				orParts = append(orParts, "("+q+")")
				params = p
			}
			if len(orParts) > 0 {
				filters = append(filters, "("+strings.Join(orParts, " OR ")+")")
			}
		case "not_in":
			// not_in: [JSON!] — value NOT IN list, expand to AND of NOT eq
			arr, ok := v.([]any)
			if !ok {
				return "", nil, fmt.Errorf("not_in filter value must be an array")
			}
			var andParts []string
			for _, item := range arr {
				var q string
				var p []any
				var err error
				if hasCoalesce && coalesceVal != nil {
					q, p, err = e.FilterOperationSQLValue(baseSQLName, "", "eq", item, params)
				} else {
					q, p, err = e.FilterOperationSQLValue(baseSQLName, path, "eq", item, params)
				}
				if err != nil {
					return "", nil, err
				}
				andParts = append(andParts, "NOT("+q+")")
				params = p
			}
			if len(andParts) > 0 {
				filters = append(filters, "("+strings.Join(andParts, " AND ")+")")
			}
		default:
			var q string
			var p []any
			var err error
			if hasCoalesce && coalesceVal != nil {
				// With coalesce: use the wrapped baseSQLName (no path, already applied)
				q, p, err = e.FilterOperationSQLValue(baseSQLName, "", op, v, params)
			} else {
				q, p, err = e.FilterOperationSQLValue(sqlName, path, op, v, params)
			}
			if err != nil {
				return "", nil, err
			}
			filters = append(filters, "("+q+")")
			params = p
		}
	}
	if len(filters) == 0 {
		return "TRUE", params, nil
	}
	if len(filters) == 1 {
		return strings.TrimPrefix(strings.TrimSuffix(filters[0], ")"), "("), params, nil
	}
	return strings.Join(filters, " AND "), params, nil
}
