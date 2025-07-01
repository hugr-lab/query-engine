package engines

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/uber/h3-go/v4"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	_ Engine           = &DuckDB{}
	_ EngineAggregator = &DuckDB{}
)

type DuckDB struct {
}

func NewDuckDB() *DuckDB {
	return &DuckDB{}
}

func (e *DuckDB) Type() Type {
	return TypeDuckDB
}

func (e *DuckDB) FieldValueByPath(sqlName, path string) string {
	if path == "" {
		return sqlName
	}
	return sqlName + extractStructFieldByPath(path)
}

func (e *DuckDB) SQLValue(v any) (string, error) {
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
		return fmt.Sprintf("ST_GeomFromText('%s', true)", string(b)), nil
	case time.Time:
		return fmt.Sprintf("'%s'::TIMESTAMP", v.Format(time.RFC3339)), nil
	case []time.Time:
		return SQLValueArrayFormatter(e, v)
	case time.Duration:
		return types.IntervalToSQLValue(v)
	case []time.Duration:
		return SQLValueArrayFormatter(e, v)
	case []types.BaseRange:
		return SQLValueArrayFormatter(e, v)
	case map[string]any, []map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("'%s'::JSON", b), nil
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
	case h3.Cell:
		// Convert H3 cell to UBIGINT
		return fmt.Sprintf("h3_string_to_h3('%s')", v.String()), nil
	}

	return "", fmt.Errorf("unsupported value type: %T", v)
}

func (e *DuckDB) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
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
		args = append(args, fmt.Sprintf("%s:=%s", k, s))
	}
	return name + "(" + strings.Join(args, ",") + ")", nil
}

func (e *DuckDB) RepackObject(sql string, field *ast.Field) string {
	if len(field.SelectionSet) == 0 {
		return sql
	}
	out := repackStructRecursive(sql, field, "")
	if field.Definition.Type.NamedType != "" {
		return out
	}
	return "list_transform(" + sql + "," + field.Name + "->" + out + ")"
}

// create fields list for the first level of object (struct)
func (e *DuckDB) UnpackObjectToFieldList(sql string, field *ast.Field) string {
	var fields []string
	for _, f := range SelectedFields(field.SelectionSet) {
		extractValue := sql + extractStructFieldByPath(f.Field.Name)
		switch {
		case len(f.Field.SelectionSet) == 0:
			fields = append(fields, extractValue+" AS "+Ident(f.Field.Alias))
		case f.Field.Definition.Type.NamedType != "":
			children := repackStructRecursive(sql, f.Field, f.Field.Name)
			fields = append(fields, children+" AS "+Ident(f.Field.Alias))
		default:
			children := repackStructRecursive("_value", f.Field, "")
			if children == "_value" {
				fields = append(fields, extractValue+" AS "+Ident(f.Field.Alias))
			}
			fields = append(fields,
				"list_transform("+
					extractValue+",_value->"+children+")"+
					" AS "+Ident(f.Field.Alias),
			)
		}
	}

	return strings.Join(fields, ",")
}

func (e DuckDB) PackFieldsToObject(prefix string, field *ast.Field) string {
	var fields []string
	if prefix != "" {
		prefix += "."
	}
	for _, f := range SelectedFields(field.SelectionSet) {
		if st, ok := compiler.ScalarTypes[f.Field.Definition.Type.Name()]; ok && st.ToStructFieldSQL != nil {
			fields = append(fields, Ident(f.Field.Alias)+": "+prefix+st.ToStructFieldSQL(Ident(f.Field.Alias)))
			continue
		}
		/*if f.Field.Definition.Type.NamedType == compiler.GeometryTypeName {
			fields = append(fields, Ident(f.Field.Alias)+":ST_AsGeoJSON("+prefix+Ident(f.Field.Alias)+")")
			continue
		}*/
		fields = append(fields, Ident(f.Field.Alias)+":"+prefix+Ident(f.Field.Alias))
	}
	return "{" + strings.Join(fields, ",") + "}"
}

func (e DuckDB) MakeObject(fields map[string]string) string {
	var res []string
	for k, v := range fields {
		res = append(res, Ident(k)+": "+v)
	}
	return "{" + strings.Join(res, ",") + "}"
}

func (e DuckDB) AddObjectFields(sqlName string, fields map[string]string) string {
	if len(fields) == 0 {
		return sqlName
	}
	var res []string
	for k, v := range fields {
		res = append(res, Ident(k)+":="+v)
	}
	return "struct_insert(" + sqlName + "," + strings.Join(res, ",") + ")"
}

func (e *DuckDB) FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error) {
	if path != "" {
		sqlName += extractStructFieldByPath(path)
	}
	if op == "is_null" {
		if value.(bool) {
			return fmt.Sprintf("%s IS NULL", sqlName), params, nil
		}
		return fmt.Sprintf("%s IS NOT NULL", sqlName), params, nil
	}

	switch value := value.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64,
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
			return fmt.Sprintf("regexp_matches(%s,%s)", sqlName, val), params, nil
		case "has":
			return fmt.Sprintf("json_exists(%s,%s)", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case []int64, []bool, []int, []float64, []string, []time.Time, []time.Duration, []any:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "contains":
			return fmt.Sprintf("list_has_all(%s,%s)", sqlName, val), params, nil
		case "intersects":
			return fmt.Sprintf("list_has_any(%s,%s)", sqlName, val), params, nil
		case "in":
			return fmt.Sprintf("%s IN (SELECT unnest(%s))", sqlName, val), params, nil
		case "has_all":
			return fmt.Sprintf("list_aggregate(list_transform(%[2]s, x -> json_exists(%[1]s, x)), 'bool_and')", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	case orb.Geometry:
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
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
	case map[string]any: // json
		params = append(params, value)
		val := "$" + strconv.Itoa(len(params))
		switch op {
		case "eq":
			return fmt.Sprintf("%s = %s", sqlName, val), params, nil
		case "contains":
			return fmt.Sprintf("json_transform(%[1]s,json_structure(%[2]s)) = json_transform(%[2]s, json_structure(%[2]s)", sqlName, val), params, nil
		default:
			return "", nil, fmt.Errorf("unsupported filter operator: %s", op)
		}
	default:
		return "", nil, fmt.Errorf("unsupported filter value type: %T", value)
	}
}

func (e DuckDB) ExtractJSONStruct(sql string, jsonStruct map[string]any) string {
	// create json structure by map
	str := jsonStructByMap(jsonStruct)
	if str == "" {
		return "NULL"
	}
	// apply json transform to extract json structure
	return "json_transform(" + sql + ",'" + str + "')"
}

func (e DuckDB) ApplyFieldTransforms(sql string, field *ast.Field, args compiler.FieldQueryArguments) string {
	switch compiler.TransformBaseFieldType(field.Definition) {
	case compiler.GeometryTypeName:
		return e.GeometryTransform(sql, field, args)
	case compiler.JSONTypeName:
		sa := args.ForName("struct")
		if sa == nil {
			return sql
		}
		s, ok := sa.Value.(map[string]any)
		if !ok {
			return sql
		}
		return e.ExtractJSONStruct(sql, s)
	case compiler.TimestampTypeName:
		return e.TimestampTransform(sql, field, args)
	}
	return sql
}

func (e DuckDB) GeometryTransform(sql string, field *ast.Field, args compiler.FieldQueryArguments) string {
	if compiler.IsExtraField(field.Definition) {
		if a := args.ForName("Transform"); a != nil && a.Value != nil && a.Value.(bool) {
			from := args.ForName("from")
			to := args.ForName("to")
			if from == nil || to == nil {
				return "NULL"
			}
			sql = fmt.Sprintf("ST_Transform(%s,'EPSG:%v', 'EPSG:%v')", sql, from.Value, to.Value)
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
			sql = fmt.Sprintf("ST_Area_Spheroid(%s)", sql)
		case "Length":
			sql = fmt.Sprintf("ST_Length(%s)", sql)
		case "LengthSpheroid":
			sql = fmt.Sprintf("ST_Length_Spheroid(%s)", sql)
		case "Perimeter":
			sql = fmt.Sprintf("ST_Perimeter(%s)", sql)
		case "PerimeterSpheroid":
			sql = fmt.Sprintf("ST_Perimeter_Spheroid(%s)", sql)
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
			sql = fmt.Sprintf("ST_Transform(%s,'EPSG:%v','EPSG:%v',always_xy:=true)", sql, from.Value, to.Value)
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
		}
	}
	return sql
}

func (e DuckDB) TimestampTransform(sql string, field *ast.Field, args compiler.FieldQueryArguments) string {
	if len(args) == 0 {
		return sql
	}
	if bucket := args.ForName("bucket"); bucket != nil {
		return fmt.Sprintf("time_bucket('%s', %s)", bucket.Value, sql)
	}
	if interval := args.ForName("bucket_interval"); interval != nil {
		iSQL, err := types.IntervalToSQLValue(interval.Value)
		if err != nil {
			return "NULL"
		}
		return fmt.Sprintf("time_bucket(%s, %s)", iSQL, sql)
	}
	if extract := args.ForName("extract"); extract != nil {
		part := extract.Value.(string)
		switch part {
		case "iso_dow":
			part = "isodow"
		case "iso_year":
			part = "isoyear"
		}
		sql := fmt.Sprintf("date_part('%s', %s)", part, sql)
		if div := args.ForName("extract_divide"); div != nil {
			sql = fmt.Sprintf("(%s::INTEGER / %v)", sql, div.Value)
		}
		return sql
	}
	return "NULL"
}

func (e DuckDB) ExtractNestedTypedValue(sql, path, t string) string {
	val := e.FieldValueByPath(sql, path)
	switch t {
	case "number":
		return "try_cast(" + val + " AS FLOAT)"
	case "string":
		return "try_cast(" + val + " AS VARCHAR)"
	case "bool":
		return "try_cast(" + val + " AS BOOLEAN)"
	case "timestamp":
		return "try_cast(" + val + " AS TIMESTAMP)"
	case "h3string":
		return fmt.Sprintf("try_cast(h3_string_to_h3(%s))", val)
	case "":
		return val
	}
	return fmt.Sprintf("try_cast(%s AS %s)", val, t)
}

func (e DuckDB) ExtractJSONTypedValue(sql, path, t string) string {
	if path != "" {
		sql = "json_value(" + sql + "::JSON,'$." + path + "')"
	}
	switch t {
	case "number":
		return "try_cast(" + sql + " AS FLOAT)"
	case "string":
		return "try_cast(" + sql + " AS VARCHAR)"
	case "bool":
		return "try_cast(" + sql + " AS BOOLEAN)"
	case "timestamp":
		return "try_cast(" + sql + " AS TIMESTAMP)"
	case "h3string":
		return fmt.Sprintf("try_cast(h3_string_to_h3(%s))", sql)
	case "":
		return sql
	}
	return fmt.Sprintf("try_cast(%s AS %s)", sql, t)
}

func (e DuckDB) AggregateFuncSQL(funcName, sql, path, factor string, field *ast.FieldDefinition, _ bool, args map[string]any, params []any) (string, []any, error) {
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
		if factor != "" {
			switch field.Type.Name() {
			case compiler.JSONTypeName, "Float":
				return "SUM(" + sql + " * " + factor + ")", params, nil
			case "Int", "BigInt":
				return "SUM(" + sql + " * " + factor + ")::BIGINT", params, nil
			}
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
		if factor != "" {
			switch field.Type.Name() {
			case compiler.JSONTypeName, "Float":
				return "AVG(" + sql + " * " + factor + ")", params, nil
			case "Int", "BigInt":
				return "AVG(" + sql + " * " + factor + ")::BIGINT", params, nil
			}
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
		if field.Type.NamedType == compiler.GeometryAggregationTypeName && path == "" {
			sql = "ST_AsGeoJSON(" + sql + ")"
		}
		if args != nil && args["distinct"] != nil && args["distinct"].(bool) {
			return "ARRAY_AGG(DISTINCT " + sql + ")", params, nil
		}
		return "ARRAY_AGG(" + sql + ")", params, nil
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
		if field.Type.NamedType == compiler.GeometryAggregationTypeName && path == "" {
			return "ST_AsGeoJSON(ANY_VALUE(" + sql + "))", params, nil
		}
		return "ANY_VALUE(" + sql + ")", params, nil
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
		if field.Type.NamedType == compiler.GeometryAggregationTypeName && path == "" {
			return "ST_AsGeoJSON(LAST(" + sql + "))", params, nil
		}
		return "LAST(" + sql + ")", params, nil
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
	case "intersection":
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
			sql = "ST_GeomFromGeoJSON(" + sql + ")"
		}
		return "ST_AsGeoJson(ST_INTERSECTION_AGG(" + sql + "))", params, nil
	case "union":
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
			sql = "ST_GeomFromGeoJSON(" + sql + ")"
		}
		return "ST_AsGeoJson(ST_UNION_AGG(" + sql + "))", params, nil
	case "envelope":
		if path != "" {
			sql = e.ExtractNestedTypedValue(sql, path, "")
			sql = "ST_GeomFromGeoJSON(" + sql + ")"
		}
		return "ST_AsGeoJson(ST_ENVELOPE(" + sql + "))", params, nil
	default:
		return "", nil, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}
}

func (e DuckDB) AggregateFuncAny(sql string) string {
	return "ANY_VALUE(" + sql + ")"
}
func (e DuckDB) JSONTypeCast(sql string) string {
	return "try_cast(" + sql + " AS JSON)"
}

func jsonStructByMap(jsonStruct map[string]any) string {
	var fields []string
	for k, v := range jsonStruct {
		switch v := v.(type) {
		case string:
			fields = append(fields, "\""+k+"\":\""+resolveJsonDuckDBType(v)+"\"")
		case map[string]any:
			fields = append(fields, "\""+k+"\":"+jsonStructByMap(v))
		case []any:
			if len(v) == 0 {
				fields = append(fields, "\""+k+"\":[]")
				continue
			}
			switch v := v[0].(type) {
			case string:
				fields = append(fields, "\""+k+"\":["+resolveJsonDuckDBType(v)+"]")
			case map[string]any:
				fields = append(fields, "\""+k+"\":["+jsonStructByMap(v)+"]")
			}
		}
	}
	return "{" + strings.Join(fields, ",") + "}"
}

func resolveJsonDuckDBType(t string) string {
	switch strings.ToLower(t) {
	case "string":
		return "VARCHAR"
	case "int":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "float":
		return "FLOAT"
	case "bool":
		return "BOOLEAN"
	case "time":
		return "TIMESTAMP"
	case "json":
		return "JSON"
	case "h3string":
		return "VARCHAR"
	}
	return ""
}

func repackStructRecursive(sql string, field *ast.Field, path string) string {
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
			fields = append(fields, Ident(f.Field.Alias)+":'"+f.Field.ObjectDefinition.Name+"'")
			continue
		}
		fi := compiler.FieldInfo(f.Field)
		fieldName := fi.FieldSourceName("", false)
		if fieldName != f.Field.Name { // need to full repack this level
			check[f.Field.ObjectDefinition.Name]++
		}
		if path != "" {
			fieldName = path + "." + fieldName
		}
		extractValue := sql + extractStructFieldByPath(fieldName)
		if fi.IsTransformed() {
			extractValue = fi.TransformSQL(extractValue)
		}
		if st, ok := compiler.ScalarTypes[f.Field.Definition.Type.Name()]; ok && st.ToStructFieldSQL != nil {
			extractValue = st.ToStructFieldSQL(extractValue)
		}
		switch {
		case len(f.Field.SelectionSet) == 0:
			fields = append(fields, Ident(f.Field.Alias)+": "+extractValue)
			if f.Field.Name == f.Field.Alias {
				check[f.Field.ObjectDefinition.Name]--
			}
		case f.Field.Definition.Type.NamedType != "" || f.Field.Directives.ForName(base.UnnestDirectiveName) != nil:
			children := repackStructRecursive(sql, f.Field, fieldName)
			fields = append(fields, Ident(f.Field.Alias)+": "+children)
			if f.Field.Name == f.Field.Alias && children == sql {
				check[f.Field.ObjectDefinition.Name]--
			}
		default:
			children := repackStructRecursive("_value", f.Field, "")
			if children == "_value" {
				fields = append(fields, Ident(f.Field.Alias)+": "+extractValue)
				check[f.Field.ObjectDefinition.Name]--
				continue
			}
			fields = append(fields, Ident(f.Field.Alias)+
				": list_transform("+extractValue+",_value->"+children+")")
		}
	}
	sum := 0
	for _, v := range check {
		sum += v
	}
	if sum == 0 {
		if path != "" {
			return sql + extractStructFieldByPath(path)
		}
		return sql
	}
	return "{" + strings.Join(fields, ",") + "}"
}

func JsonToStruct(field *ast.Field, prefix string, useNativeTypes bool, byFieldFieldSource bool) string {
	fieldName := Ident(field.Alias)
	if prefix != "" {
		fieldName = prefix + "." + fieldName
	}
	structStr := jsonStructRecursive(field, useNativeTypes, byFieldFieldSource)
	return "json_transform(" + fieldName + ", '" + structStr + "')"
}

func (e *DuckDB) LateralJoin(sql, alias string) string {
	return " LEFT JOIN LATERAL (" + sql + ") AS " + alias + " ON TRUE"
}

// нужна функция проверки наличия поля по пути в структуре запроса

func extractStructFieldByPath(path string) string {
	if path == "" {
		return ""
	}
	pathValues := strings.Split(path, ".")
	for i, v := range pathValues {
		if strings.HasPrefix(v, "\"") || strings.HasSuffix(v, "\"") {
			v = strings.Trim(v, "\"")
		}
		pathValues[i] = "['" + v + "']"
	}
	return strings.Join(pathValues, "")
}

func jsonStructRecursive(field *ast.Field, useNativeTypes bool, byFieldSource bool) string {
	var fields []string
	for _, f := range SelectedFields(field.SelectionSet) {
		leftBracket, rightBracket := "", ""
		if f.Field.Definition.Type.NamedType == "" {
			leftBracket, rightBracket = "[", "]"
		}
		fn := f.Field.Alias
		if byFieldSource {
			fi := compiler.FieldInfo(f.Field)
			if fi != nil {
				fn = fi.FieldSourceName("", false)
			} else {
				fn = f.Field.Name
			}
		}
		if t, ok := compiler.ScalarTypes[f.Field.Definition.Type.Name()]; ok {
			tn := t.JSONToStructType
			if useNativeTypes {
				tn = t.JSONNativeType
			}
			fields = append(fields, "\""+fn+"\":"+
				leftBracket+"\""+tn+"\""+rightBracket,
			)
			continue
		}
		fields = append(fields, "\""+fn+"\":"+
			leftBracket+jsonStructRecursive(f.Field, useNativeTypes, byFieldSource)+rightBracket,
		)
	}
	return "{" + strings.Join(fields, ",") + "}"
}
