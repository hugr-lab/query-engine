package engines

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	_ Engine             = &Postgres{}
	_ EngineQueryScanner = &Postgres{}
	_ EngineTypeCaster   = &Postgres{}
	_ EngineAggregator   = &Postgres{}
)

type Postgres struct {
}

func NewPostgres() *Postgres {
	return &Postgres{}
}

func (e *Postgres) Type() Type {
	return TypePostgres
}

func (e *Postgres) FieldValueByPath(sqlName, path string) string {
	if path == "" {
		return sqlName
	}
	return sqlName + extractPGJsonFieldByPath(path, false)
}

func (e *Postgres) SQLValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch v := v.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", v), nil
	case []bool:
		return SQLValueArrayFormatter(e, v)
	case []int:
		return SQLValueArrayFormatter(e, v)
	case []int64:
		return SQLValueArrayFormatter(e, v)
	case []float64:
		return SQLValueArrayFormatter(e, v)
	case string:
		v = strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", v), nil
	case []string:
		return SQLValueArrayFormatter(e, v)
	case orb.Geometry:
		b := wkt.Marshal(v)
		return fmt.Sprintf("ST_GeomFromText('%s')", b), nil
	case time.Time:
		return fmt.Sprintf("'%s'::TIMESTAMP", v.Format(time.RFC3339)), nil
	case []time.Time:
		return SQLValueArrayFormatter(e, v)
	case time.Duration:
		return types.IntervalToSQLValue(v)
	case []time.Duration:
		return SQLValueArrayFormatter(e, v)
	case types.Int32Range:
		str, err := pgRangeValueToSQLValue(v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s::INT4RANGE", str), nil
	case []types.Int32Range:
		return SQLValueArrayFormatter(e, v)
	case types.Int64Range:
		str, err := pgRangeValueToSQLValue(v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s::INT8RANGE", str), nil
	case []types.Int64Range:
		return SQLValueArrayFormatter(e, v)
	case types.TimeRange:
		str, err := pgRangeValueToSQLValue(v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s::TSTZRANGE", str), nil
	case types.BaseRange:
		return pgRangeValueToSQLValue(v)
	case []types.BaseRange:
		return SQLValueArrayFormatter(e, v)
	case map[string]any, []map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("'%s'::JSONB", b), nil
	case []any:
		var valueStrings []string
		for _, v := range v {
			s, err := e.SQLValue(v)
			if err != nil {
				return "", err
			}
			valueStrings = append(valueStrings, s)
		}
		return fmt.Sprintf("ARRAY[%s]", strings.Join(valueStrings, ",")), nil
	case types.Vector:
		if v == nil {
			return "NULL", nil
		}
		var sql string
		for i, v := range v {
			s, err := e.SQLValue(v)
			if err != nil {
				return "", err
			}
			if i > 0 {
				sql += ","
			}
			sql += s
		}
		return fmt.Sprintf("'[%s]'", sql), nil
	}

	return "", fmt.Errorf("unsupported value type: %T", v)
}

func (e *Postgres) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	var args []string
	for _, v := range positional {
		s, err := e.SQLValue(v)
		if err != nil {
			return "", err
		}
		args = append(args, s)
	}
	for k, v := range named {
		s, err := e.SQLValue(v)
		if err != nil {
			return "", err
		}
		args = append(args, fmt.Sprintf("%s=>%s", k, s))
	}
	return name + "(" + strings.Join(args, ",") + ")", nil
}

var jsonPathOpMap = map[string]string{
	"eq":              "==",
	"gt":              ">",
	"gte":             ">=",
	"lt":              "<",
	"lte":             "<=",
	"regex":           "like_regex",
	"like":            "like",
	"ilike":           "ilike",
	"has":             "has",
	"has_all":         "has_all",
	"contains":        "@>",
	"intersects":      "&&",
	"includes":        "@>",
	"upper":           "upper",
	"lower":           "lower",
	"upper_inclusive": "upper_inc",
	"lower_inclusive": "lower_inc",
	"upper_inf":       "upper_inf",
	"lower_inf":       "lower_inf",
	"in":              "in",
}

func typedArrayToAnyArray[T string | bool | ~int | ~int8 | ~int32 | ~int64 | ~float64 | ~float32 | time.Time](v []T) []any {
	var r []any
	for _, vv := range v {
		r = append(r, vv)
	}
	return r
}

func arrayToAnyArray(v any) []any {
	switch v := v.(type) {
	case []bool:
		return typedArrayToAnyArray(v)
	case []int:
		return typedArrayToAnyArray(v)
	case []int8:
		return typedArrayToAnyArray(v)
	case []int32:
		return typedArrayToAnyArray(v)
	case []int64:
		return typedArrayToAnyArray(v)
	case []float64:
		return typedArrayToAnyArray(v)
	case []string:
		return typedArrayToAnyArray(v)
	case []time.Time:
		return typedArrayToAnyArray(v)
	case []time.Duration:
		return typedArrayToAnyArray(v)
	default:
		return nil
	}
}
func escapeJsonPathString(s string) string {
	s = strings.ReplaceAll(s, `"`, `\"`)
	return strings.ReplaceAll(s, "'", "''")
}

// TODO add compiler options to enable/disable type of operations and types support
func (e *Postgres) FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error) {
	if jOp, ok := jsonPathOpMap[op]; ok && path != "" { // apply json path to jsonb field
		jsonPathTemplate := "COALESCE(" + sqlName + " @@ '$." + path + " " + jOp + " %v', false)"
		switch value := value.(type) {
		case string:
			if op == "has" { // json check if path exists
				return sqlName + " @? '$." + path + "'", params, nil
			}
			if op == "like" || op == "ilike" {
				params = append(params, value)
				sqlName += extractPGJsonFieldByPath(path, true)
				return fmt.Sprintf("%s %s %s", sqlName, strings.ToUpper(op), "$"+strconv.Itoa(len(params))), params, nil
			}
			return fmt.Sprintf(jsonPathTemplate, "\""+escapeJsonPathString(value)+"\""), params, nil
		case []bool, []int, []int8, []int16, []int32, []int64, []uint, []uint8, []uint16, []uint32, []uint64, []float32, []float64, []string, []time.Time:
			switch op {
			case "has_all":
				params = append(params, value)
				sqlName += extractPGJsonFieldByPath(path, true)
				return fmt.Sprintf("%s |& %s", sqlName, "$"+strconv.Itoa(len(params))), params, nil
			case "in":
				var values []string
				for _, v := range arrayToAnyArray(value) {
					q, p, err := e.FilterOperationSQLValue(sqlName, path, "eq", v, params)
					if err != nil {
						return "", nil, err
					}
					params = p
					values = append(values, "("+q+")")
				}
				return strings.Join(values, " OR "), params, nil
			case "contains", "intersects":
				var values []string
				for _, v := range arrayToAnyArray(value) {
					q, p, err := e.FilterOperationSQLValue(sqlName, path+"[*]", "eq", v, params)
					if err != nil {
						return "", nil, err
					}
					params = p
					values = append(values, "("+q+")")
				}
				if op == "contains" {
					return strings.Join(values, " AND "), params, nil
				}
				return strings.Join(values, " OR "), params, nil
			}
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
			sqlName += extractPGJsonFieldByPath(path, true)
			switch op {
			case "upper", "lower":
				return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
			case "upper_inclusive", "lower_inclusive", "upper_inf", "lower_inf":
				return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
			}
			return fmt.Sprintf(jsonPathTemplate, value), params, nil
		case time.Time:
			return fmt.Sprintf(jsonPathTemplate, "\""+value.Format(time.RFC3339)+"\""), params, nil
		case time.Duration:
			params = append(params, value)
			sqlName += extractPGJsonFieldByPath(path, true)
			return fmt.Sprintf("(%s)::interval %s %s", sqlName, op, "$"+strconv.Itoa(len(params))), params, nil
		case []time.Duration:
			return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
		case types.Int32Range, types.Int64Range, types.TimeRange, []types.Int32Range, []types.Int64Range, []types.TimeRange:
			return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
		}
	}
	if path != "" {
		sqlName += extractPGJsonFieldByPath(path, false)
	}
	if op == "is_null" {
		if value.(bool) {
			return fmt.Sprintf("%s IS NULL", sqlName), params, nil
		}
		return fmt.Sprintf("%s IS NOT NULL", sqlName), params, nil
	}
	switch value := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool,
		time.Time, time.Duration:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "gt":
			return fmt.Sprintf("%s > %s", sqlName, val), params, nil
		case "gte":
			return fmt.Sprintf("%s >= %s", sqlName, val), params, nil
		case "lt":
			return fmt.Sprintf("%s < %s", sqlName, val), params, nil
		case "lte":
			return fmt.Sprintf("%s <= %s", sqlName, val), params, nil
		// range ops
		case "contains":
			return fmt.Sprintf("%s @> %s", sqlName, val), params, nil
		case "upper":
			return fmt.Sprintf("upper(%s) = %s", sqlName, val), params, nil
		case "lower":
			return fmt.Sprintf("lower(%s) = %s", sqlName, val), params, nil
		case "upper_inclusive":
			return fmt.Sprintf("upper_inc(%s) = %s", sqlName, val), params, nil
		case "lower_inclusive":
			return fmt.Sprintf("lower_inc(%s) = %s", sqlName, val), params, nil
		case "upper_inf":
			return fmt.Sprintf("upper_inf(%s) = %s", sqlName, val), params, nil
		case "lower_inf":
			return fmt.Sprintf("lower_inf(%s) = %s", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case string:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "like":
			return fmt.Sprintf("%s LIKE %s", sqlName, val), params, nil
		case "ilike":
			return fmt.Sprintf("%s ILIKE %s", sqlName, val), params, nil
		case "regex":
			return fmt.Sprintf("%s ~ %s", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case []bool, []int64, []int, []float64, []string, []time.Time, []time.Duration, []any:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "contains":
			if path != "" {
				return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
			}
			return fmt.Sprintf("%s @> %s", sqlName, val), params, nil
		case "intersects":
			if path != "" {
				return "", nil, fmt.Errorf("unsupported filter operator for json type: %s", op)
			}
			return fmt.Sprintf("%s && %s", sqlName, val), params, nil
		case "in":
			return fmt.Sprintf("%s = ANY(%s)", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case orb.Geometry:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		if path != "" {
			sqlName = fmt.Sprintf("ST_GeomFromGeoJSON((%s)::text)", sqlName)
		}
		switch op {
		case "eq":
			return fmt.Sprintf("ST_Equals(%s,%s)", sqlName, val), params, nil
		case "intersects":
			return fmt.Sprintf("ST_Intersects(%s,%s)", sqlName, val), params, nil
		case "contains":
			return fmt.Sprintf("ST_Contains(%s,%s)", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case types.Int32Range, types.Int64Range, types.TimeRange:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "intersects":
			return fmt.Sprintf("%s && %s", sqlName, val), params, nil
		case "includes":
			return fmt.Sprintf("%s @> %s", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case map[string]any: // json
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "contains":
			return fmt.Sprintf("%s @> %s", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	default:
		return "", nil, fmt.Errorf("unsupported filter value type: %T", value)
	}
}

func (e *Postgres) RepackObject(sql string, field *ast.Field) string {
	if len(field.SelectionSet) == 0 {
		return sql
	}
	return repackPGJsonRecursive(sql, field, "")
}

func (e *Postgres) UnpackObjectToFieldList(sql string, field *ast.Field) string {
	var fields []string
	for _, f := range SelectedFields(field.SelectionSet) {
		extractValue := sql + extractPGJsonFieldByPath(f.Field.Name, false)
		switch {
		case len(f.Field.SelectionSet) == 0:
			fields = append(fields, extractValue+" AS "+Ident(f.Field.Alias))
		case f.Field.Definition.Type.NamedType != "":
			children := repackPGJsonRecursive(sql, f.Field, f.Field.Name)
			fields = append(fields, children+" AS "+Ident(f.Field.Alias))
		default:
			children := repackPGJsonRecursive("_values", f.Field, "")
			if children == "_value" {
				fields = append(fields, extractValue+" AS "+Ident(f.Field.Alias))
			}
			fields = append(fields,
				"(SELECT array_agg("+children+") "+
					"FROM jsonb_array_elements("+extractValue+") AS _value)"+
					" AS "+Ident(f.Field.Alias),
			)
		}
	}

	return strings.Join(fields, ",")
}

func (e Postgres) PackFieldsToObject(prefix string, field *ast.Field) string {
	var fields []string
	if prefix != "" {
		prefix += "."
	}
	for _, f := range SelectedFields(field.SelectionSet) {
		if f.Field.Definition.Type.NamedType == compiler.GeometryTypeName {
			fields = append(fields, "'"+Ident(f.Field.Alias)+"',ST_AsGeoJSON("+prefix+Ident(f.Field.Alias)+")::JSON")
			continue
		}
		fields = append(fields, "'"+f.Field.Alias+"',"+prefix+Ident(f.Field.Alias))
	}
	return "jsonb_build_object(" + strings.Join(fields, ",") + ")"
}

func (e Postgres) MakeObject(fields map[string]string) string {
	var res []string
	for k, v := range fields {
		res = append(res, "'"+k+"',"+v)
	}
	return "jsonb_build_object(" + strings.Join(res, ",") + ")"
}

func (e *Postgres) AddObjectFields(sqlName string, fields map[string]string) string {
	if len(fields) == 0 {
		return sqlName
	}
	var res []string
	for k, v := range fields {
		res = append(res, "'"+k+"',"+v)
	}
	return sqlName + " || jsonb_build_object(" + strings.Join(res, ",") + ")"
}

func (e *Postgres) WarpScann(db, query string) string {
	query = strings.ReplaceAll(query, "'", "''")
	return fmt.Sprintf("postgres_query(%s,' %s ')", Ident(db), query)
}

func (e *Postgres) WrapExec(db, query string) string {
	query = strings.ReplaceAll(query, "'", "''")
	return fmt.Sprintf("postgres_execute(%s,' %s ')", Ident(db), query)
}

func (e *Postgres) ToIntermediateType(f *ast.Field) (string, error) {
	return Ident(f.Alias), nil
}

func (e *Postgres) CastFromIntermediateType(f *ast.Field, toJSON bool) (string, error) {
	// only for geometry and non scalar objects types, other types are converted automatically
	// interval type will be converted to TEXT representation
	if f.Definition.Type.NamedType == compiler.GeometryTypeName {
		out := "ST_GeomFromHEXWKB(%s)"
		if toJSON {
			out = "ST_AsGeoJson(" + out + ")::JSON"
		}
		return fmt.Sprintf(out, Ident(f.Alias)), nil
	}

	if !compiler.IsScalarType(f.Definition.Type.Name()) {
		if toJSON {
			if f.Definition.Type.NamedType == "" {
				return Ident(f.Alias) + "::JSON[]", nil
			}
			return Ident(f.Alias) + "::JSON", nil
		}
		if f.Definition.Type.NamedType == "" && f.Directives.ForName(base.UnnestDirectiveName) == nil {
			return "list_transform(" + Ident(f.Alias) + "," + Ident(f.Alias) + "->" + JsonToStruct(f, "", false, false) + ")", nil
		}
		return JsonToStruct(f, "", false, false), nil
	}

	if f.Definition.Type.Name() == compiler.JSONTypeName {
		if f.Definition.Type.NamedType == "" {
			return fmt.Sprintf("(%s)::JSON[]", Ident(f.Alias)), nil
		}
		return fmt.Sprintf("(%s)::JSON", Ident(f.Alias)), nil
	}

	return Ident(f.Alias), nil
}

func pgRangeValueToSQLValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	var lower, upper string
	var detail types.RangeDetail
	switch v := v.(type) {
	case types.Int32Range:
		if !v.Detail.IsLowerInfinity() {
			lower = strconv.Itoa(int(v.Lower))
		}
		if !v.Detail.IsUpperInfinity() {
			upper = strconv.Itoa(int(v.Upper))
		}
		detail = v.Detail
	case types.Int64Range:
		if !v.Detail.IsLowerInfinity() {
			lower = strconv.Itoa(int(v.Lower))
		}
		if !v.Detail.IsUpperInfinity() {
			upper = strconv.Itoa(int(v.Upper))
		}
		detail = v.Detail
	case types.TimeRange:
		if !v.Detail.IsLowerInfinity() {
			lower = v.Lower.Format(time.RFC3339)
		}
		if !v.Detail.IsUpperInfinity() {
			upper = v.Upper.Format(time.RFC3339)
		}
		detail = v.Detail
	case types.BaseRange:
		detail = v.Detail
		switch v.Type {
		case types.RangeTypeInt32, types.RangeTypeInt64:
			if !v.Detail.IsLowerInfinity() {
				lower = strconv.Itoa(v.Lower.(int))
			}
			if !v.Detail.IsUpperInfinity() {
				upper = strconv.Itoa(v.Upper.(int))
			}
		case types.RangeTypeTimestamp:
			if !v.Detail.IsLowerInfinity() {
				lower = v.Lower.(time.Time).Format(time.RFC3339)
			}
			if !v.Detail.IsUpperInfinity() {
				upper = v.Upper.(time.Time).Format(time.RFC3339)
			}
		default:
			return "", fmt.Errorf("invalid range value")
		}
	default:
		return "", fmt.Errorf("invalid range value")
	}
	if detail.IsEmpty() {
		return "'empty'", nil
	}
	rightBracket, leftBracket := ")", "("
	if detail.IsLowerInclusive() {
		leftBracket = "["
	}
	if detail.IsUpperInclusive() {
		rightBracket = "]"
	}
	return fmt.Sprintf("'%s%s,%s%s'", leftBracket, lower, upper, rightBracket), nil
}

const (
	intExtractJSONTemplate    = "(CASE WHEN jsonb_typeof(%s) = 'number' THEN (%[1]s)::INTEGER ELSE NULL END)"
	bigIntExtractJSONTemplate = "(CASE WHEN jsonb_typeof(%s) = 'number' THEN (%[1]s)::BIGINT ELSE NULL END)"
	floatExtractJSONTemplate  = "(CASE WHEN jsonb_typeof(%s) = 'number' THEN (%[1]s)::float ELSE NULL END)"
	stringExtractJSONTemplate = `(CASE WHEN jsonb_typeof(%s) = 'string' THEN trim(both '"' from (%[1]s)::TEXT) ELSE NULL END)`
	boolExtractJSONTemplate   = "(CASE WHEN jsonb_typeof(%s) = 'boolean' THEN (%[1]s)::BOOL ELSE NULL END)"
	timeExtractJSONTemplate   = "jsonb_path_query_first(%s, '$.datetime()', silent=>true)::TEXT"
)

func (e Postgres) ExtractJSONStruct(sql string, jsonStruct map[string]any) string {
	var fields []string
	for k, v := range jsonStruct {
		switch v := v.(type) {
		case string:
			// scalar value
			field := sql + "->'" + k + "'"
			fields = append(fields, "'"+k+"',"+e.extractJsonTypedValue(field, v))
		case map[string]any:
			fields = append(fields, "'"+k+"',(SELECT "+e.ExtractJSONStruct("_value", v)+" FROM (SELECT "+sql+"->'"+k+"' AS _value) AS _value)")
		case []any:
			if len(v) == 0 {
				fields = append(fields, "'"+k+"',NULL")
				continue
			}
			switch v := v[0].(type) {
			case map[string]any:
				fields = append(fields,
					"'"+k+"',"+
						"(CASE WHEN jsonb_typeof("+sql+"->'"+k+"') = 'array' THEN "+
						"(SELECT array_agg(_value) "+
						"FROM (SELECT "+e.ExtractJSONStruct("_value", v)+" AS _value "+
						"FROM (SELECT jsonb_array_elements("+sql+"->'"+k+"') AS _value) AS _value) "+
						"WHERE _value IS NOT NULL AND _value != '{}'::JSONB)"+
						" ELSE NULL END)",
				)
			case string:
				fields = append(fields,
					"'"+k+"',"+
						"(CASE WHEN jsonb_typeof("+sql+"->'"+k+"') = 'array' THEN "+
						"(SELECT array_agg("+e.extractJsonTypedValue("_value", v)+") "+
						"FROM (SELECT jsonb_array_elements("+sql+"->'"+k+"') AS _value) AS _value)"+
						" ELSE NULL END)",
				)
			default:
				fields = append(fields, "'"+k+"',NULL")
			}
		}
	}
	slices.Sort(fields)
	return "jsonb_build_object(" + strings.Join(fields, ",") + ")"
}

func (e Postgres) ApplyFieldTransforms(ctx context.Context, qe types.Querier, sql string, field *ast.Field, args compiler.FieldQueryArguments, params []any) (string, []any, error) {
	switch compiler.TransformBaseFieldType(field.Definition) {
	case compiler.GeometryTypeName:
		return e.GeometryTransform(sql, field, args), params, nil
	case compiler.JSONTypeName:
		sa := args.ForName("struct")
		if sa == nil {
			return sql, params, nil
		}
		s, ok := sa.Value.(map[string]any)
		if !ok {
			return sql, params, nil
		}
		return e.ExtractJSONStruct(sql, s), params, nil
	case compiler.TimestampTypeName:
		return e.TimestampTransform(sql, field, args), params, nil
	case base.VectorTypeName:
		return e.VectorTransform(ctx, qe, sql, field, args, params)
	}
	return sql, params, nil
}

func (e Postgres) GeometryTransform(sql string, field *ast.Field, args compiler.FieldQueryArguments) string {
	if compiler.IsExtraField(field.Definition) {
		if a := args.ForName("Transform"); a != nil && a.Value != nil && a.Value.(bool) {
			from := args.ForName("from")
			to := args.ForName("to")
			if from == nil || to == nil {
				return "NULL"
			}
			sql = fmt.Sprintf("ST_Transform(%s,%v)", sql, to.Value)
		}
		mt := args.ForName("type")
		if mt == nil || mt.Value == nil {
			return sql
		}
		t, ok := mt.Value.(string)
		if !ok {
			return "NULL"
		}
		switch t {
		case "Area":
			sql = fmt.Sprintf("ST_Area(%s)", sql)
		case "AreaSpheroid":
			sql = fmt.Sprintf("ST_Area((%s)::geography, true)", sql)
		case "Length":
			sql = fmt.Sprintf("ST_Length(%s)", sql)
		case "LengthSpheroid":
			sql = fmt.Sprintf("ST_LengthSpheroid(%s, 'SPHEROID[\"GRS_1980\",6378137,298.257222101]')", sql)
		case "Perimeter":
			sql = fmt.Sprintf("ST_Perimeter(%s)", sql)
		case "PerimeterSpheroid":
			sql = fmt.Sprintf("ST_LengthSpheroid(%s, 'SPHEROID[\"GRS_1980\",6378137,298.257222101]')", sql)
		}
	}

	v := args.ForName("transforms")
	if v == nil || v.Value == nil {
		return sql
	}
	tt, ok := v.Value.([]any)
	if !ok {
		t, ok := v.Value.(string)
		if !ok {
			return "NULL"
		}
		tt = []any{t}
	}
	currentSrid := 4326
	if d := field.Definition.Directives.ForName("geometry_info"); d != nil {
		if srid := d.Arguments.ForName("srid"); srid != nil {
			currentSrid, _ = strconv.Atoi(srid.Value.Raw)
		}
	}
	for _, v := range tt {
		t, ok := v.(string)
		if !ok {
			return "NULL"
		}
		switch t {
		case "Transform":
			from := args.ForName("from")
			to := args.ForName("to")
			if from == nil || to == nil {
				return "NULL"
			}
			sql = fmt.Sprintf("ST_Transform(%s,%v)", sql, to.Value)
			currentSrid = int(to.Value.(int64))
		case "Buffer":
			buffer := args.ForName("buffer")
			if buffer == nil {
				return "NULL"
			}
			v := buffer.Value.(float64)
			if currentSrid == 4326 {
				v = v / 111111
			}
			sql = fmt.Sprintf("ST_Buffer(%s,%v)", sql, v)
		case "Centroid":
			sql = fmt.Sprintf("ST_Centroid(%s)", sql)
		case "Simplify":
			factor := args.ForName("simplify_factor")
			if factor == nil {
				return "NULL"
			}
			v := factor.Value.(float64)
			sql = fmt.Sprintf("ST_Simplify(%s,%v)", sql, v)
		case "SimplifyTopology":
			factor := args.ForName("simplify_factor")
			if factor == nil {
				return "NULL"
			}
			v := factor.Value.(float64)
			sql = fmt.Sprintf("ST_SimplifyPreserveTopology(%s,%v)", sql, v)
		case "StartPoint":
			sql = fmt.Sprintf("ST_StartPoint(%s)", sql)
		case "EndPoint":
			sql = fmt.Sprintf("ST_EndPoint(%s)", sql)
		case "Reverse":
			sql = fmt.Sprintf("ST_Reverse(%s)", sql)
		case "FlipCoordinates":
			sql = fmt.Sprintf("ST_FlipCoordinates(%s)", sql)
		case "ConvexHull":
			sql = fmt.Sprintf("ST_ConvexHull(%s)", sql)
		case "Envelope":
			sql = fmt.Sprintf("ST_Envelope(%s)", sql)
		default:
			return "NULL"
		}
	}
	return sql
}

func (e Postgres) TimestampTransform(sql string, field *ast.Field, args compiler.FieldQueryArguments) string {
	if len(args) == 0 {
		return sql
	}
	if compiler.IsTimescaleKey(field.Definition) {
		bf := "time_bucket"
		if bucket := args.ForName("bucket"); bucket != nil {
			return fmt.Sprintf("date_trunc('%s', %s)", bucket.Value, sql)
		}
		if gapFill := args.ForName("gapfill"); gapFill != nil {
			if v, ok := gapFill.Value.(bool); v && ok {
				bf = "time_bucket_gapfill"
			}
		}
		if interval := args.ForName("bucket_interval"); interval != nil {
			iSQL, err := types.IntervalToSQLValue(interval.Value)
			if err != nil {
				return "NULL"
			}
			return fmt.Sprintf("%s(%s, %s)", bf, iSQL, sql)
		}
	}
	if bucket := args.ForName("bucket"); bucket != nil {
		return fmt.Sprintf("date_trunc('%s', %s)", bucket.Value, sql)
	}
	if interval := args.ForName("bucket_interval"); interval != nil {
		iSQL, err := types.IntervalToSQLValue(interval.Value)
		if err != nil {
			return "NULL"
		}
		return fmt.Sprintf("to_timestamp((extract(epoch from %s)::BIGINT / extract(epoch from %s)::BIGINT) * extract(epoch from %[2]s)::BIGINT)", sql, iSQL)
	}
	if extract := args.ForName("extract"); extract != nil {
		part := extract.Value.(string)
		switch part {
		case "iso_dow":
			part = "isodow"
		case "iso_year":
			part = "isoyear"
		}
		sql := fmt.Sprintf("EXTRACT(%s FROM %s)", part, sql)
		if div := args.ForName("extract_divide"); div != nil {
			sql = fmt.Sprintf("(%s::BIGINT / %v)", sql, div.Value)
		}
		return sql
	}
	return "NULL"
}

func (e Postgres) ExtractNestedTypedValue(sql, path, t string) string {
	if t == "string" && path != "" {
		return sql + extractPGJsonFieldByPath(path, true)
	}
	if path != "" {
		sql = e.FieldValueByPath(sql, path)
	}
	switch t {
	case "number":
		return fmt.Sprintf("(%s)::FLOAT", e.extractJsonTypedValue(sql, "float"))
	case "string":
		return fmt.Sprintf("(%s)::TEXT", e.extractJsonTypedValue(sql, "string"))
	case "bool":
		return fmt.Sprintf("(%s)::BOOL", e.extractJsonTypedValue(sql, "bool"))
	case "timestamp":
		return fmt.Sprintf("(%s)::TIMESTAMP", e.extractJsonTypedValue(sql, "timestamp"))
	case "":
		return sql
	default:
		return "NULL"
	}
}

func (e *Postgres) extractJsonTypedValue(field, typeName string) string {
	switch strings.ToLower(typeName) {
	case "int":
		return fmt.Sprintf(intExtractJSONTemplate, field)
	case "bigint":
		return fmt.Sprintf(bigIntExtractJSONTemplate, field)
	case "float":
		return fmt.Sprintf(floatExtractJSONTemplate, field)
	case "string", "h3string":
		return fmt.Sprintf(stringExtractJSONTemplate, field)
	case "bool":
		return fmt.Sprintf(boolExtractJSONTemplate, field)
	case "timestamp":
		return fmt.Sprintf(timeExtractJSONTemplate, field)
	case "json":
		return field
	default:
		return "NULL"
	}
}

func (e Postgres) AggregateFuncSQL(funcName, sql, path, factor string, field *ast.FieldDefinition, isHyperTable bool, args map[string]any, params []any) (string, []any, error) {
	switch funcName {
	case "count":
		if field == nil {
			return "COUNT(*)", params, nil
		}
		if field.Type.Name() == compiler.JSONTypeName && args != nil && args["path"] != nil {
			if path != "" {
				path += "."
			}
			path += args["path"].(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
		}
		return "COUNT(DISTINCT " + sql + ")", params, nil
	case "sum":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "number")
		}
		return "SUM(" + sql + ")", params, nil
	case "avg":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "number")
		}
		return "AVG(" + sql + ")", params, nil
	case "min":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			jt, ok := compiler.FieldJSONTypes[field.Type.Name()]
			if !ok {
				return "", nil, compiler.ErrorPosf(field.Position, "unsupported type for min aggregate function")
			}
			if jt == "" {
				jt = "number"
			}
			sql = e.ExtractNestedTypedValue(sql, path, jt)
		}
		return "MIN(" + sql + ")", params, nil
	case "max":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			jt, ok := compiler.FieldJSONTypes[field.Type.Name()]
			if !ok {
				return "", nil, compiler.ErrorPosf(field.Position, "unsupported type for min aggregate function")
			}
			if jt == "" {
				jt = "number"
			}
			sql = e.ExtractNestedTypedValue(sql, path, jt)
		}
		return "MAX(" + sql + ")", params, nil
	case "list":
		if field.Type.Name() == compiler.JSONTypeName && args != nil && args["path"] != nil {
			if path != "" {
				path += "."
			}
			path += args["path"].(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
		}
		if args != nil && args["distinct"] != nil && args["distinct"].(bool) {
			return "ARRAY_AGG(DISTINCT " + sql + ")", params, nil
		}
		return "ARRAY_AGG(" + sql + ")", params, nil
	case "last":
		if field.Type.Name() == compiler.JSONTypeName && args != nil && args["path"] != nil {
			if path != "" {
				path += "."
			}
			path += args["path"].(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
		}
		// only for hypetable
		if !isHyperTable {
			return "LAST_AGG_VALUE(" + sql + ")", params, nil
		}
		return "LAST(" + sql + ")", params, nil
	case "any":
		if field.Type.Name() == compiler.JSONTypeName && args != nil && args["path"] != nil {
			if path != "" {
				path += "."
			}
			path += args["path"].(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
		}
		return "ANY_VALUE(" + sql + ")", params, nil
	case "bool_and":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "bool")
		}
		return "BOOL_AND(" + sql + ")", params, nil
	case "bool_or":
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "bool")
		}
		return "BOOL_OR(" + sql + ")", params, nil
	case "string_agg":
		sep := args["sep"]
		if sep == nil {
			return "", nil, compiler.ErrorPosf(field.Position, "separator argument is required")
		}
		if field.Type.Name() == compiler.JSONTypeName {
			jp := args["path"]
			if jp == nil {
				return "", nil, compiler.ErrorPosf(field.Position, "path argument is required")
			}
			if path != "" {
				path += "."
			}
			path += jp.(string)
		}
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "string")
		}

		if args["distinct"] != nil && args["distinct"].(bool) {
			return "STRING_AGG(DISTINCT " + sql + ", '" + sep.(string) + "')", params, nil
		}
		return "STRING_AGG(" + sql + ", '" + sep.(string) + "')", params, nil
	case "union":
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
			sql = "ST_GeomFromGeoJSON(" + sql + ")"
		}
		return "ST_UNION(" + sql + ")", params, nil
	case "extent":
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
			sql = "ST_GeomFromGeoJSON(" + sql + ")"
		}
		return "ST_Extent(" + sql + ")::geometry", params, nil
	default:
		return "", nil, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}
}

func (e Postgres) AggregateFuncAny(sql string) string {
	return "ANY_VALUE(" + sql + ")"
}
func (e Postgres) JSONTypeCast(sql string) string {
	return sql + "::JSONB"
}

func (e Postgres) LateralJoin(sql, alias string) string {
	return "LEFT JOIN LATERAL (" + sql + ") AS " + alias + " ON TRUE"
}

func repackPGJsonRecursive(sql string, field *ast.Field, path string) string {
	// if nothing to repack, return the field name
	if len(field.SelectionSet) == 0 || len(path) > 1000 {
		return sql // return parameter to repack function
	}
	var fields []string // fields to repack
	check := map[string]int{}
	for _, f := range SelectedFields(field.SelectionSet) {
		if _, ok := check[f.Field.ObjectDefinition.Name]; !ok {
			check[f.Field.ObjectDefinition.Name] = len(f.Field.ObjectDefinition.Fields)
		}
		if f.Field.Name == "__typename" {
			fields = append(fields, "'"+Ident(f.Field.Alias)+"','"+f.Field.ObjectDefinition.Name+"'")
			continue
		}
		info := compiler.FieldInfo(f.Field)
		extractValue := info.FieldSourceName("", false)
		if extractValue != f.Field.Name || info.IsCalcField() { // need to full repack this level
			check[f.Field.ObjectDefinition.Name]++
		}
		if path != "" {
			extractValue = path + "." + f.Field.Name
		}
		if !info.IsCalcField() {
			extractValue = sql + extractPGJsonFieldByPath(extractValue, false)
		}
		if info.IsCalcField() {
			extractValue = info.SQLFieldFunc("", func(s string) string { return sql + extractPGJsonFieldByPath(s, false) })
		}
		newPath := f.Field.Name
		if path != "" {
			newPath = path + "." + f.Field.Name
		}
		switch {
		case len(f.Field.SelectionSet) == 0:
			fields = append(fields, "'"+f.Field.Alias+"',"+extractValue)
			if f.Field.Name == f.Field.Alias {
				check[f.Field.ObjectDefinition.Name]--
			}
		case f.Field.Definition.Type.NamedType != "":
			children := repackPGJsonRecursive(sql, f.Field, newPath)
			fields = append(fields, "'"+f.Field.Alias+"',"+children)
			if f.Field.Name == f.Field.Alias && children == extractValue {
				check[f.Field.ObjectDefinition.Name]--
			}
		default:
			children := repackPGJsonRecursive("_value", f.Field, "")
			if children == "_value" {
				fields = append(fields, "'"+f.Field.Alias+"',"+extractValue)
				check[f.Field.ObjectDefinition.Name]--
				continue
			}
			fields = append(fields,
				"'"+f.Field.Alias+"', (SELECT array_agg("+children+") "+
					"FROM jsonb_array_elements("+extractValue+") AS _value)",
			)
		}
	}
	sum := 0
	for _, v := range check {
		sum += v
	}
	if sum == 0 {
		if path != "" {
			return path + "." + sql
		}
		return sql
	}

	return "jsonb_build_object(" + strings.Join(fields, ",") + ")"
}

func extractPGJsonFieldByPath(path string, asText bool) string {
	if path == "" {
		return ""
	}
	parts := strings.Split(path, ".")
	for i, p := range parts {
		if strings.HasPrefix(p, "\"") && strings.HasSuffix(p, "\"") {
			p = strings.TrimPrefix(p, "\"")
			p = strings.TrimSuffix(p, "\"")
		}
		parts[i] = fmt.Sprintf("'%s'", p)
	}
	if asText {
		if len(parts) == 1 {
			return fmt.Sprintf("->>%s", parts[0])
		}
		return fmt.Sprintf("->%s->>%s", strings.Join(parts[:len(parts)-1], "->"), parts[len(parts)-1])
	}
	return "->" + strings.Join(parts, "->")
}

var _ EngineVectorDistanceCalculator = (*Postgres)(nil)

func (e *Postgres) VectorDistanceSQL(sql, distMetric string, vector types.Vector, params []any) (string, []any, error) {
	val := "$" + strconv.Itoa(len(params)+1)
	params = append(params, vector)
	switch distMetric {
	case base.VectorSearchDistanceL2:
		return fmt.Sprintf("%s <-> %s", sql, val), params, nil
	case base.VectorSearchDistanceCosine:
		return fmt.Sprintf("%s <=> %s", sql, val), params, nil
	case base.VectorSearchDistanceIP:
		return fmt.Sprintf("%s <#> %s", sql, val), params, nil
	default:
		return "", nil, fmt.Errorf("unsupported distance metric: %s", distMetric)
	}
}

func (e *Postgres) VectorTransform(ctx context.Context, qe types.Querier, sql string, field *ast.Field, args compiler.FieldQueryArguments, params []any) (string, []any, error) {
	return commonVectorTransform(ctx, e, qe, sql, field, args, params)
}
