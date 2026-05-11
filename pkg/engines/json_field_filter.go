package engines

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// sqlParamRegExp matches a `$N` SQL parameter placeholder.
var sqlParamRegExp = regexp.MustCompile(`\$(\d+)`)

// castParamRefs rewrites every `$N` placeholder in sql whose N is strictly
// greater than skipBelow to `CAST($N AS sqlType)`. Placeholders bound before
// the JSONFieldFilter pipeline started (e.g. a COALESCE default that was
// appended into params earlier) are left untouched. Engines use this from
// the shared JSONFieldFilter compiler to reconcile a stringified parameter
// (after value coercion) with the typed JSON-extraction side, e.g. for
// DATE / TIME / TIMESTAMP / INTERVAL.
func castParamRefs(sql, sqlType string, skipBelow int) string {
	if sqlType == "" {
		return sql
	}
	return sqlParamRegExp.ReplaceAllStringFunc(sql, func(m string) string {
		idx, err := strconv.Atoi(m[1:])
		if err != nil || idx <= skipBelow {
			return m
		}
		return "CAST(" + m + " AS " + sqlType + ")"
	})
}

// jsonFieldFilterEngine bundles the dialect-specific hooks consumed by
// compileJSONFieldFilter together with the engine's recursive filter
// compiler. Implemented by *DuckDB and *Postgres.
type jsonFieldFilterEngine interface {
	// FilterOperationSQLValue is invoked recursively for each operator inside
	// the typed sub-filter, with sqlName already rewritten to the typed
	// JSON-extraction fragment.
	FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error)
	// extractJSONFilterValue returns a SQL fragment that extracts a JSON path
	// (path != "") or wraps a parameter placeholder (path == "") as the
	// target gqlType.
	extractJSONFilterValue(sqlName, path, gqlType string) string
	// jsonFieldCoerceCastType returns the SQL type used both for sub-filter
	// value coercion and for the `CAST($N AS T)` wrap around freshly-bound
	// placeholders. Empty when the type rides the driver's native binding.
	jsonFieldCoerceCastType(gqlType string) string
	// jsonFieldCoerce converts a Go sub-filter value into the form the driver
	// must bind. sqlType is the result of jsonFieldCoerceCastType.
	jsonFieldCoerce(v any, sqlType string) any
	// jsonPathIsNull renders an isNull guard for the given JSON path.
	jsonPathIsNull(sqlName, path string, isNull bool) string
}

// jsonFieldFilterTypes lists every GraphQL type accepted as a typed sub-filter
// of JSONFieldFilter. intRange / bigIntRange / timestampRange are intentionally
// excluded.
var jsonFieldFilterTypes = map[string]struct{}{
	"int": {}, "bigInt": {}, "float": {}, "string": {}, "bool": {},
	"date": {}, "time": {}, "dateTime": {}, "timestamp": {},
	"interval": {}, "geometry": {},
}

// compileJSONFieldFilter implements the JSONFieldFilter contract:
//
//	{path: "<dot.path>", isNull?: Bool, coalesce?: JSON,
//	 <type>: { <op>: <value>, ... }}
//
// The orchestration (path/isNull/coalesce/typed-subfilter parsing, validation,
// per-operator iteration, AND-fold) is dialect-agnostic and lives here; the
// engine plugs in its dialect-specific hooks through jsonFieldFilterEngine.
func compileJSONFieldFilter(
	e jsonFieldFilterEngine,
	sqlName string,
	value map[string]any,
	params []any,
) (string, []any, error) {
	pp, _ := value["path"].(string)
	if pp == "" {
		return "", nil, fmt.Errorf("JSONFieldFilter.path is required")
	}
	isNullVal, hasIsNull := false, false
	if raw, ok := value["isNull"]; ok {
		isNullVal, _ = raw.(bool)
		hasIsNull = true
	}
	coalesceVal, hasCoalesce := value["coalesce"]
	hasCoalesce = hasCoalesce && coalesceVal != nil

	var filterType string
	var filterValueMap map[string]any
	for k, v := range value {
		if k == "path" || k == "isNull" || k == "coalesce" {
			continue
		}
		if v == nil {
			continue
		}
		m, ok := v.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("JSONFieldFilter.%s must be an object", k)
		}
		if len(m) == 0 {
			continue
		}
		if filterType != "" {
			return "", nil, fmt.Errorf("JSONFieldFilter accepts at most one typed sub-filter, got both %s and %s", filterType, k)
		}
		filterType = k
		filterValueMap = m
	}
	if filterType == "" && !hasIsNull {
		return "", nil, fmt.Errorf("JSONFieldFilter must specify isNull or one typed sub-filter")
	}

	var conds []string
	if hasIsNull {
		conds = append(conds, e.jsonPathIsNull(sqlName, pp, isNullVal))
	}
	if filterType != "" {
		if _, ok := jsonFieldFilterTypes[filterType]; !ok {
			return "", nil, fmt.Errorf("unsupported JSONFieldFilter type: %s", filterType)
		}
		coerceCastType := e.jsonFieldCoerceCastType(filterType)
		jsonField := e.extractJSONFilterValue(sqlName, pp, filterType)
		if hasCoalesce {
			params = append(params, coalesceVal)
			ph := "$" + strconv.Itoa(len(params))
			jsonField = fmt.Sprintf("COALESCE(%s, %s)", jsonField, e.extractJSONFilterValue(ph, "", filterType))
		}
		ops := make([]string, 0, len(filterValueMap))
		for k := range filterValueMap {
			ops = append(ops, k)
		}
		sort.Strings(ops)
		var ff []string
		for _, sop := range ops {
			sv := filterValueMap[sop]
			if sv == nil {
				continue
			}
			sv = e.jsonFieldCoerce(sv, coerceCastType)
			paramsBefore := len(params)
			f, p, err := e.FilterOperationSQLValue(jsonField, "", sop, sv, params)
			if err != nil {
				return "", nil, fmt.Errorf("JSONFieldFilter.%s.%s: %w", filterType, sop, err)
			}
			params = p
			f = castParamRefs(f, coerceCastType, paramsBefore)
			ff = append(ff, "("+f+")")
		}
		if len(ff) > 0 {
			conds = append(conds, strings.Join(ff, " AND "))
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
