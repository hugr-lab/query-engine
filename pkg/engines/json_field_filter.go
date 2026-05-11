package engines

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// SQL type names that flow through ExtractJSONTypedValue, CoerceJSONFieldFilterValue
// and JSONFieldFilterParamCast. Engines switch on these as strings; centralising
// them as constants turns a typo into a compile error and makes the set of
// supported types greppable from one place.
const (
	SQLTypeInteger     = "INTEGER"
	SQLTypeBigInt      = "BIGINT"
	SQLTypeDoublePrec  = "DOUBLE PRECISION"
	SQLTypeVarchar     = "VARCHAR"
	SQLTypeBoolean     = "BOOLEAN"
	SQLTypeDate        = "DATE"
	SQLTypeTime        = "TIME"
	SQLTypeTimestamp   = "TIMESTAMP"
	SQLTypeTimestampTZ = "TIMESTAMPTZ"
	SQLTypeInterval    = "INTERVAL"
	SQLTypeGeometry    = "GEOMETRY"
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
	{"int", SQLTypeInteger},
	{"bigInt", SQLTypeBigInt},
	{"float", SQLTypeDoublePrec},
	{"string", SQLTypeVarchar},
	{"bool", SQLTypeBoolean},
	{"date", SQLTypeDate},
	{"time", SQLTypeTime},
	{"dateTime", SQLTypeTimestamp},
	{"timestamp", SQLTypeTimestampTZ},
	{"interval", SQLTypeInterval},
	{"geometry", SQLTypeGeometry},
}

// JSONFieldFilterShape is the parsed contents of a JSONFieldFilter input map.
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
// its dot-path under basePath. Errors are returned for shape-level invariants:
// path required, at most one typed sub-filter, isNull required when no
// typed sub-filter is present.
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

// sqlParamRegExp matches a `$N` SQL parameter placeholder. Used by
// castParamRefsAs to rewrite a subset of placeholders after the engine's
// FilterOperationSQLValue has returned its SQL fragment.
var sqlParamRegExp = regexp.MustCompile(`\$(\d+)`)

// CompileJSONFieldFilterSQL is the dialect-agnostic orchestration for a
// GraphQL JSONFieldFilter input. It parses the shape, handles isNull and
// coalesce uniformly, then iterates the typed sub-filter's operators and
// delegates each one to the engine's FilterOperationSQLValue — with two
// engine-specific hooks: CoerceJSONFieldFilterValue (run before binding) and
// JSONFieldFilterParamCast (used to wrap freshly-bound `$N` placeholders with
// a typed CAST so the comparison matches the JSON-extraction side).
//
// Engines stay small: they expose typed JSON extraction, value coercion, and
// the cast decision. The bookkeeping — params indexing, sorted op iteration,
// AND-folding — lives here.
func CompileJSONFieldFilterSQL(e Engine, sqlName, basePath string, fv map[string]any, params []any) (string, []any, error) {
	shape, err := ParseJSONFieldFilterShape(fv, basePath)
	if err != nil {
		return "", nil, err
	}

	var conds []string
	if shape.HasIsNull {
		conds = append(conds, e.JSONPathIsNull(sqlName, shape.JSONPath, shape.IsNullVal))
	}

	if shape.SubName != "" {
		extracted := e.ExtractJSONTypedValue(sqlName, shape.JSONPath, shape.SubType)
		if shape.HasCoalesce && shape.CoalesceVal != nil {
			params = append(params, shape.CoalesceVal)
			val := "$" + strconv.Itoa(len(params))
			extracted = fmt.Sprintf("COALESCE(%s, %s)", extracted, e.ExtractJSONTypedValue(val, "", shape.SubType))
		}

		ops := make([]string, 0, len(shape.SubValue))
		for op := range shape.SubValue {
			ops = append(ops, op)
		}
		sort.Strings(ops)

		paramCast := e.JSONFieldFilterParamCast(shape.SubType)
		var subFilters []string
		for _, op := range ops {
			v := shape.SubValue[op]
			if v == nil {
				continue
			}
			v = e.CoerceJSONFieldFilterValue(v, shape.SubType)
			paramsBefore := len(params)
			s, p, err := e.FilterOperationSQLValue(extracted, "", op, v, params)
			if err != nil {
				return "", nil, fmt.Errorf("JSONFieldFilter.%s.%s: %w", shape.SubName, op, err)
			}
			params = p
			if paramCast != "" {
				s = castParamRefsAs(s, paramCast, paramsBefore)
			}
			subFilters = append(subFilters, "("+s+")")
		}
		if len(subFilters) > 0 {
			conds = append(conds, strings.Join(subFilters, " AND "))
		}
	}

	switch len(conds) {
	case 0:
		return "TRUE", params, nil
	case 1:
		return conds[0], params, nil
	}
	return strings.Join(conds, " AND "), params, nil
}

// castParamRefsAs rewrites every `$N` placeholder in sql where N > skipBelow
// to `CAST($N AS sqlType)`. Pre-existing placeholders (e.g. the COALESCE
// default that was bound before the FilterOperationSQLValue call) are left
// untouched. The orchestration uses this to reconcile a freshly-bound
// parameter with the typed extraction on the JSON side.
func castParamRefsAs(sql, sqlType string, skipBelow int) string {
	return sqlParamRegExp.ReplaceAllStringFunc(sql, func(m string) string {
		idx, err := strconv.Atoi(m[1:])
		if err != nil || idx <= skipBelow {
			return m
		}
		return "CAST(" + m + " AS " + sqlType + ")"
	})
}
