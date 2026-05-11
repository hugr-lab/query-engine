package engines

import (
	"errors"
	"fmt"
)

// JSONFieldFilterSubTypes maps GraphQL JSONFieldFilter sub-filter names to
// the engine-native SQL type names that ExtractJSONTypedValue accepts.
// The slice order doubles as a deterministic iteration order so multiple
// engines pick the same single sub-filter when parsing the input.
//
// Range types (intRange, bigIntRange, timestampRange) are intentionally absent:
// reconstructing a range from a JSON object would require dialect-specific
// composition that isn't implemented yet.
var JSONFieldFilterSubTypes = []struct {
	Name    string
	SQLType string
}{
	{"int", "INTEGER"},
	{"bigInt", "BIGINT"},
	{"float", "DOUBLE PRECISION"},
	{"string", "VARCHAR"},
	{"bool", "BOOLEAN"},
	{"date", "DATE"},
	{"time", "TIME"},
	{"dateTime", "TIMESTAMP"},
	{"timestamp", "TIMESTAMPTZ"},
	{"interval", "INTERVAL"},
	{"geometry", "GEOMETRY"},
}

// JSONFieldFilterShape is the parsed contents of a JSONFieldFilter input map:
// the resolved dot-path, optional isNull, optional coalesce default, and at
// most one typed sub-filter (selected from JSONFieldFilterSubTypes). Engines
// build SQL from this struct, applying dialect-specific value coercion and
// per-operator emission.
type JSONFieldFilterShape struct {
	JSONPath    string
	SubName     string
	SubValue    map[string]any
	SubType     string
	IsNullVal   bool
	HasIsNull   bool
	CoalesceVal any
	HasCoalesce bool
}

// ParseJSONFieldFilterShape validates the JSONFieldFilter input and resolves
// its dot-path under basePath. Errors are returned for the shape-level
// invariants documented in JSONFieldFilter's SDL:
//   - path is required and non-empty;
//   - at most one typed sub-filter may be present;
//   - if no typed sub-filter is given, isNull must be present.
func ParseJSONFieldFilterShape(fv map[string]any, basePath string) (*JSONFieldFilterShape, error) {
	rawPath, ok := fv["path"].(string)
	if !ok || rawPath == "" {
		return nil, errors.New("JSONFieldFilter.path is required")
	}
	jsonPath := rawPath
	if basePath != "" {
		jsonPath = basePath + "." + rawPath
	}

	out := &JSONFieldFilterShape{JSONPath: jsonPath}

	for _, st := range JSONFieldFilterSubTypes {
		v, present := fv[st.Name]
		if !present || v == nil {
			continue
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("JSONFieldFilter.%s must be an object", st.Name)
		}
		if len(m) == 0 {
			continue
		}
		if out.SubName != "" {
			return nil, fmt.Errorf("JSONFieldFilter accepts at most one typed sub-filter, got both %s and %s", out.SubName, st.Name)
		}
		out.SubName = st.Name
		out.SubValue = m
		out.SubType = st.SQLType
	}

	if v, present := fv["isNull"]; present {
		out.HasIsNull = true
		out.IsNullVal, _ = v.(bool)
	}
	if v, present := fv["coalesce"]; present {
		out.HasCoalesce = true
		out.CoalesceVal = v
	}

	if out.SubName == "" && !out.HasIsNull {
		return nil, errors.New("JSONFieldFilter must specify isNull or one typed sub-filter")
	}

	return out, nil
}

// JSONFieldCompareOp maps a JSONFieldFilter comparison operator (as used in
// IntFilter, DateFilter, etc.) to the SQL infix operator. Returns ok=false
// for operators that are not pure comparisons (like, ilike, regex, has).
func JSONFieldCompareOp(op string) (string, bool) {
	switch op {
	case "eq":
		return "=", true
	case "gt":
		return ">", true
	case "gte":
		return ">=", true
	case "lt":
		return "<", true
	case "lte":
		return "<=", true
	}
	return "", false
}
