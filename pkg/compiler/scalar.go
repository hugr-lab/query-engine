package compiler

import (
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
	"golang.org/x/exp/slices"
)

const (
	JSONTypeName                = "JSON"
	TimestampTypeName           = "Timestamp"
	H3CellTypeName              = "H3Cell"
	GeometryTypeName            = "Geometry"
	GeometryAggregationTypeName = "GeometryAggregation"

	GeometryMeasurementExtraFieldName = "Measurement"
	TimestampExtractExtraFieldName    = "Extract"
)

type ScalarType struct {
	Name             string
	Description      string
	Arguments        ast.ArgumentDefinitionList
	ExtraField       func(field *ast.FieldDefinition) *ast.FieldDefinition
	JSONType         string
	JSONToStructType string
	JSONNativeType   string
	// ToOutputType converts SQL string to output type.
	ToOutputTypeSQL  func(sql string, raw bool) string
	ToStructFieldSQL func(sql string) string
	FilterInput      string
	ListFilterInput  string
	ParseValue       func(value any) (any, error)
	ParseArray       func(value any) (any, error)
	AggType          string
	MeasurementAggs  string
	OpenAPISchema    *openapi3.Schema
}

var ScalarTypes = map[string]ScalarType{
	"String": {
		Name:             "String",
		Description:      "String type",
		JSONType:         "string",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "StringFilter",
		ListFilterInput:  "StringListFilter",
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[string](value)
		},
		AggType:         "StringAggregation",
		MeasurementAggs: "StringMeasurementAggregation",
		OpenAPISchema:   openapi3.NewStringSchema(),
	},
	"Int": {
		Name:             "Int",
		Description:      "Int type",
		JSONType:         "number",
		JSONToStructType: "INTEGER",
		JSONNativeType:   "INTEGER",
		FilterInput:      "IntFilter",
		ListFilterInput:  "IntListFilter",
		ParseValue: func(value any) (any, error) {
			switch v := value.(type) {
			case int:
				return int64(v), nil
			case int32:
				return int64(v), nil
			case int64:
				return v, nil
			case float64:
				return int64(v), nil
			}
			return nil, fmt.Errorf("unexpected type %T for BigInt", value)
		},
		ParseArray: func(value any) (any, error) {
			if value == nil {
				return nil, nil
			}
			vv, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array of BigInt values, got %T", value)
			}
			if len(vv) == 0 {
				return []int64{}, nil
			}
			if _, ok := vv[0].(float64); !ok {
				return types.ParseScalarArray[int64](vv)
			}
			dd, err := types.ParseScalarArray[float64](value)
			if err != nil {
				return nil, err
			}
			out := make([]int64, len(dd))
			for i, v := range dd {
				out[i] = int64(v)
			}
			return out, nil
		},
		AggType:         "IntAggregation",
		MeasurementAggs: "IntMeasurementAggregation",
		OpenAPISchema:   openapi3.NewInt32Schema(),
	},
	"BigInt": {
		Name:             "BigInt",
		Description:      "BigInt type",
		JSONType:         "number",
		JSONToStructType: "BIGINT",
		JSONNativeType:   "BIGINT",
		FilterInput:      "BigIntFilter",
		ListFilterInput:  "BigIntListFilter",
		ParseValue: func(value any) (any, error) {
			switch v := value.(type) {
			case int:
				return int64(v), nil
			case int32:
				return int64(v), nil
			case int64:
				return v, nil
			case float64:
				return int64(v), nil
			}
			return nil, fmt.Errorf("unexpected type %T for BigInt", value)
		},
		ParseArray: func(value any) (any, error) {
			if value == nil {
				return nil, nil
			}
			vv, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array of BigInt values, got %T", value)
			}
			if len(vv) == 0 {
				return []int64{}, nil
			}
			if _, ok := vv[0].(float64); !ok {
				return types.ParseScalarArray[int64](vv)
			}
			dd, err := types.ParseScalarArray[float64](value)
			if err != nil {
				return nil, err
			}
			out := make([]int64, len(dd))
			for i, v := range dd {
				out[i] = int64(v)
			}
			return out, nil
		},
		AggType:         "BigIntAggregation",
		MeasurementAggs: "BigIntMeasurementAggregation",
		OpenAPISchema:   openapi3.NewInt64Schema(),
	},
	"Float": {
		Name:             "Float",
		Description:      "Float type",
		JSONType:         "number",
		JSONToStructType: "FLOAT",
		JSONNativeType:   "FLOAT",
		FilterInput:      "FloatFilter",
		ListFilterInput:  "FloatListFilter",
		ParseValue: func(value any) (any, error) {
			switch v := value.(type) {
			case int:
				return float64(v), nil
			case int32:
				return float64(v), nil
			case int64:
				return float64(v), nil
			case float64:
				return v, nil
			}
			return nil, fmt.Errorf("unexpected type %T for Float", value)
		},
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[float64](value)
		},
		AggType:         "FloatAggregation",
		MeasurementAggs: "FloatMeasurementAggregation",
		OpenAPISchema:   openapi3.NewFloat64Schema(),
	},
	"Boolean": {
		Name:             "Boolean",
		Description:      "Boolean type",
		JSONType:         "bool",
		JSONToStructType: "BOOLEAN",
		JSONNativeType:   "BOOLEAN",
		FilterInput:      "BooleanFilter",
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[bool](value)
		},
		AggType:         "BooleanAggregation",
		MeasurementAggs: "BooleanMeasurementAggregation",
		OpenAPISchema:   openapi3.NewBoolSchema(),
	},
	"Date": {
		Name:        "Date",
		Description: "Date type",
		Arguments: ast.ArgumentDefinitionList{
			{
				Name: "bucket",
				Description: "Truncate ti the specified part of the timestamp. " +
					"Possible values: 'year', 'month', 'day'.",
				Type:     ast.NamedType("TimeBucket", compiledPos()),
				Position: compiledPos(),
			},
		},
		ExtraField: func(field *ast.FieldDefinition) *ast.FieldDefinition {
			fieldName := "_" + field.Name + "_part"
			if strings.HasPrefix(field.Name, "_") {
				fieldName = strings.TrimPrefix(field.Name, "_")
			}

			sql := "[" + field.Name + "]"

			return &ast.FieldDefinition{
				Name:        fieldName,
				Description: field.Description + " (extracted part)",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "extract",
						Description: "Extracts the specified part of the date",
						Type:        ast.NonNullNamedType("TimeExtract", compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "extract_divide",
						Description: "Divides the extracted value",
						Type:        ast.NamedType("Int", compiledPos()),
						Position:    compiledPos(),
					},
				},
				Directives: ast.DirectiveList{
					base.SqlFieldDirective(sql),
					base.ExtraFieldDirective(TimestampExtractExtraFieldName, field.Name, TimestampTypeName),
				},
				Type:     ast.NamedType("BigInt", compiledPos()),
				Position: compiledPos(),
			}
		},
		JSONType:         "timestamp",
		JSONToStructType: "DATE",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "DateFilter",
		ListFilterInput:  "DateListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseTimeValue(value)
		},
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[time.Time](value)
		},
		AggType:         "DateAggregation",
		MeasurementAggs: "DateMeasurementAggregation",
		OpenAPISchema:   openapi3.NewStringSchema().WithFormat("date"),
	},
	"Timestamp": {
		Name:        "Timestamp",
		Description: "Timestamp type",
		Arguments: ast.ArgumentDefinitionList{
			{
				Name: "bucket",
				Description: "Truncate ti the specified part of the timestamp. " +
					"Possible values: 'year', 'month', 'day', 'hour', 'minute', 'second'.",
				Type:     ast.NamedType("TimeBucket", compiledPos()),
				Position: compiledPos(),
			},
			{
				Name:        "bucket_interval",
				Description: "Truncate the specified part of the timestamp",
				Type:        ast.NamedType("Interval", compiledPos()),
				Position:    compiledPos(),
			},
		},
		ExtraField: func(field *ast.FieldDefinition) *ast.FieldDefinition {
			fieldName := "_" + field.Name + "_part"
			if strings.HasPrefix(field.Name, "_") {
				fieldName = strings.TrimPrefix(field.Name, "_")
			}

			sql := "[" + field.Name + "]"

			return &ast.FieldDefinition{
				Name:        fieldName,
				Description: field.Description + " (extracted part)",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "extract",
						Description: "Extracts the specified part of the timestamp",
						Type:        ast.NonNullNamedType("TimeExtract", compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "extract_divide",
						Description: "Divides the extracted value",
						Type:        ast.NamedType("Int", compiledPos()),
						Position:    compiledPos(),
					},
				},
				Directives: ast.DirectiveList{
					base.SqlFieldDirective(sql),
					base.ExtraFieldDirective(TimestampExtractExtraFieldName, field.Name, TimestampTypeName),
				},
				Type:     ast.NamedType("BigInt", compiledPos()),
				Position: compiledPos(),
			}
		},
		JSONToStructType: "TIMESTAMP",
		JSONNativeType:   "VARCHAR",
		JSONType:         "timestamp",
		FilterInput:      "TimestampFilter",
		ListFilterInput:  "TimestampListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseTimeValue(value)
		},
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[time.Time](value)
		},
		AggType:         "TimestampAggregation",
		MeasurementAggs: "TimestampMeasurementAggregation",
		OpenAPISchema:   openapi3.NewStringSchema().WithFormat("date-time"),
	},
	"Time": {
		Name:             "Time",
		Description:      "Time type",
		JSONType:         "timestamp",
		JSONToStructType: "TIME",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "TimeFilter",
		ListFilterInput:  "TimeListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseTimeValue(value)
		},
		ParseArray: func(value any) (any, error) {
			return types.ParseScalarArray[time.Time](value)
		},
		AggType:         "TimeAggregation",
		MeasurementAggs: "TimeMeasurementAggregation",
		OpenAPISchema:   openapi3.NewStringSchema().WithFormat("time"),
	},
	"Interval": {
		Name:             "Interval",
		Description:      "Interval type (duration)",
		JSONType:         "string",
		JSONToStructType: "INTERVAL",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "IntervalFilter",
		ListFilterInput:  "IntervalListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseIntervalValue(value)
		},
		ParseArray: func(value any) (any, error) {
			vv, err := types.ParseScalarArray[string](value)
			if err != nil {
				return nil, err
			}
			out := make([]time.Duration, len(vv))
			for i, v := range vv {
				r, err := types.ParseIntervalValue(v)
				if err != nil {
					return nil, err
				}
				out[i] = r
			}
			return out, nil
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("interval"),
	},
	"JSON": {
		Name:        "JSON",
		Description: "JSON type",
		Arguments: ast.ArgumentDefinitionList{
			{
				Name: "struct",
				Description: "Provides json structure to extract partial data from json field." +
					"Structure: {field: \"type\", field2: [\"type2\"], field3: [{field4: \"type4\"}]}.",
				Type:     ast.NamedType("JSON", compiledPos()),
				Position: compiledPos(),
			},
		},
		JSONToStructType: "JSON",
		JSONNativeType:   "JSON",
		FilterInput:      "JSONFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseJsonValue(value)
		},
		AggType:       "JSONAggregation",
		OpenAPISchema: openapi3.NewObjectSchema(),
	},
	"IntRange": {
		Name:             "IntRange",
		Description:      "IntRange type",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "IntRangeFilter",
		ListFilterInput:  "IntRangeListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseRangeValue(types.RangeTypeInt32, value)
		},
		ParseArray: func(value any) (any, error) {
			vv, err := types.ParseScalarArray[string](value)
			if err != nil {
				return nil, err
			}
			out := make([]types.Int32Range, len(vv))
			for i, v := range vv {
				r, err := types.ParseRangeValue(types.RangeTypeInt32, v)
				if err != nil {
					return nil, err
				}
				out[i] = r.(types.Int32Range)
			}
			return out, nil
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("int-range"),
	},
	"BigIntRange": {
		Name:             "Int8Range",
		Description:      "Int8Range type",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "BigIntRangeFilter",
		ListFilterInput:  "BigIntRangeListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseRangeValue(types.RangeTypeInt64, value)
		},
		ParseArray: func(value any) (any, error) {
			vv, err := types.ParseScalarArray[string](value)
			if err != nil {
				return nil, err
			}
			out := make([]types.Int64Range, len(vv))
			for i, v := range vv {
				r, err := types.ParseRangeValue(types.RangeTypeInt64, v)
				if err != nil {
					return nil, err
				}
				out[i] = r.(types.Int64Range)
			}
			return out, nil
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("int-range"),
	},
	"TimestampRange": {
		Name:             "TimestampRange",
		Description:      "TimestampRange type",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "TimestampRangeFilter",
		ListFilterInput:  "TimestampRangeListFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseRangeValue(types.RangeTypeTimestamp, value)
		},
		ParseArray: func(value any) (any, error) {
			vv, err := types.ParseScalarArray[string](value)
			if err != nil {
				return nil, err
			}
			out := make([]types.TimeRange, len(vv))
			for i, v := range vv {
				r, err := types.ParseRangeValue(types.RangeTypeTimestamp, v)
				if err != nil {
					return nil, err
				}
				out[i] = r.(types.TimeRange)
			}
			return out, nil
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("timestamp-range"),
	},
	"Geometry": {
		Name:        "Geometry",
		Description: "Geometry type",
		Arguments: ast.ArgumentDefinitionList{
			{
				Name:        "transforms",
				Description: "Provides function to transform geometry",
				Type:        ast.ListType(ast.NonNullNamedType("GeometryTransform", compiledPos()), compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "from",
				Description: "Converts geometry from the specified SRID",
				Type:        ast.NamedType("Int", compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "to",
				Description: "Converts geometry to the specified SRID",
				Type:        ast.NamedType("Int", compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "buffer",
				Description: "Expands the geometry by the specified distance",
				Type:        ast.NamedType("Float", compiledPos()),
				Position:    compiledPos(),
			},
			{
				Name:        "simplify_factor",
				Description: "Simplifies the geometry by the specified factor",
				Type:        ast.NamedType("Float", compiledPos()),
			},
		},
		ExtraField: func(field *ast.FieldDefinition) *ast.FieldDefinition {
			fieldName := "_" + field.Name + "_measurement"
			if strings.HasPrefix(field.Name, "_") {
				fieldName = strings.TrimPrefix(field.Name, "_")
			}

			sql := "[" + field.Name + "]"

			return &ast.FieldDefinition{
				Name:        fieldName,
				Description: field.Description + " (geometry measurement)",
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "type",
						Description: "Measurement type",
						Type:        ast.NonNullNamedType("GeometryMeasurementTypes", compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "transform",
						Description: "Reproject geometry (parameters from and to are required)",
						Type:        ast.NamedType("Boolean", compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "from",
						Description: "Converts geometry from the specified SRID",
						Type:        ast.NamedType("Int", compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "to",
						Description: "Converts geometry to the specified SRID",
						Type:        ast.NamedType("Int", compiledPos()),
						Position:    compiledPos(),
					},
				},
				Directives: ast.DirectiveList{
					base.SqlFieldDirective(sql),
					base.ExtraFieldDirective(GeometryMeasurementExtraFieldName, field.Name, GeometryTypeName),
				},
				Type:     ast.NamedType("Float", compiledPos()),
				Position: compiledPos(),
			}
		},
		JSONToStructType: "JSON",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "GeometryFilter",
		ParseValue: func(value any) (any, error) {
			return types.ParseGeometryValue(value)
		},
		AggType: "GeometryAggregation",
		ToOutputTypeSQL: func(sql string, raw bool) string {
			if raw {
				return fmt.Sprintf("ST_AsWKB(%s)", sql)
			}
			return fmt.Sprintf("ST_AsGeoJSON(%s)", sql)
		},
		ToStructFieldSQL: func(sql string) string {
			return fmt.Sprintf("ST_AsGeoJSON(%s)", sql)
		},
		OpenAPISchema: openapi3.NewObjectSchema().WithProperty("type", openapi3.NewStringSchema().WithEnum([]string{"Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"})),
	},
	"H3Cell": {
		Name:             "H3Cell",
		Description:      "H3Cell type",
		JSONType:         "h3string",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		ParseValue: func(value any) (any, error) {
			return types.ParseH3Cell(value)
		},
		ParseArray: func(value any) (any, error) {
			vv, ok := value.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array of H3 cells, got %T", value)
			}
			out := make([]any, len(vv))
			var err error
			for i, v := range vv {
				if v == nil {
					continue
				}
				out[i], err = types.ParseH3Cell(v)
				if err != nil {
					return nil, fmt.Errorf("invalid H3 cell value at index %d: %w", i, err)
				}
			}
			return out, nil
		},
		ToOutputTypeSQL: func(sql string, raw bool) string {
			return "h3_h3_to_string(" + sql + ")"
		},
		ToStructFieldSQL: func(sql string) string {
			return "h3_h3_to_string(" + sql + ")"
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("h3string"),
	},
	"Vector": {
		Name:             "Vector",
		Description:      "Vector type (embeddings vector)",
		JSONType:         "VARCHAR",
		JSONToStructType: "VARCHAR",
		JSONNativeType:   "VARCHAR",
		FilterInput:      "VectorFilter",
		ParseValue: func(value any) (any, error) {
			if value == nil {
				return nil, nil
			}
			return types.ParseVector(value)
		},
		ToOutputTypeSQL: func(sql string, raw bool) string {
			return "(" + sql + ")::VARCHAR"
		},
		ToStructFieldSQL: func(sql string) string {
			return "(" + sql + ")::VARCHAR"
		},
		OpenAPISchema: openapi3.NewStringSchema().WithFormat("vector"),
		ExtraField: func(field *ast.FieldDefinition) *ast.FieldDefinition {
			fieldName := field.Name + "_" + base.DistanceFieldNameSuffix
			if !strings.HasPrefix(field.Name, "_") {
				fieldName = "_" + fieldName
			}
			sql := "[" + field.Name + "]"

			return &ast.FieldDefinition{
				Name:        fieldName,
				Description: "Calculate vector distance to the specified vector for field " + field.Name,
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:        "vector",
						Description: "Vector to calculate distance to",
						Type:        ast.NonNullNamedType(base.VectorTypeName, compiledPos()),
						Position:    compiledPos(),
					},
					{
						Name:        "distance",
						Description: "Distance metric to use",
						Type:        ast.NonNullNamedType(base.VectorDistanceTypeEnumName, compiledPos()),
						Position:    compiledPos(),
					},
				},
				Directives: ast.DirectiveList{
					base.SqlFieldDirective(sql),
					base.ExtraFieldDirective(base.VectorDistanceExtraFieldName, field.Name, base.VectorTypeName),
				},
				Type:     ast.NamedType("Float", compiledPos()),
				Position: compiledPos(),
			}
		},
	},
}

var MeasurementAggregations = map[string]string{
	"MIN": "min",
	"MAX": "max",
	"SUM": "sum",
	"AVG": "avg",
	"ANY": "any",
}

var subAggregationTypes = map[string]string{
	"StringAggregation":       "StringSubAggregation",
	"IntAggregation":          "IntSubAggregation",
	"BigIntAggregation":       "BigIntSubAggregation",
	"FloatAggregation":        "FloatSubAggregation",
	"BooleanAggregation":      "BooleanSubAggregation",
	"DateAggregation":         "DateSubAggregation",
	"TimestampAggregation":    "TimestampSubAggregation",
	"TimeAggregation":         "TimeSubAggregation",
	"JSONAggregation":         "JSONSubAggregation",
	"GeometryAggregation":     "GeometrySubAggregation",
	"StringSubAggregation":    "",
	"IntSubAggregation":       "",
	"BigIntSubAggregation":    "",
	"FloatSubAggregation":     "",
	"BooleanSubAggregation":   "",
	"DateSubAggregation":      "",
	"TimestampSubAggregation": "",
	"TimeSubAggregation":      "",
	"JSONSubAggregation":      "",
	"GeometrySubAggregation":  "",
}

var FieldJSONTypes = map[string]string{
	"String":                  "string",
	"Int":                     "number",
	"BigInt":                  "number",
	"Float":                   "number",
	"Boolean":                 "bool",
	"Date":                    "timestamp",
	"Timestamp":               "timestamp",
	"Time":                    "timestamp",
	"StringAggregation":       "string",
	"IntAggregation":          "number",
	"BigIntAggregation":       "number",
	"FloatAggregation":        "number",
	"BooleanAggregation":      "bool",
	"DateAggregation":         "timestamp",
	"TimestampAggregation":    "timestamp",
	"TimeAggregation":         "timestamp",
	"H3Cell":                  "h3string",
	"StringSubAggregation":    "",
	"IntSubAggregation":       "",
	"BigIntSubAggregation":    "",
	"FloatSubAggregation":     "",
	"BooleanSubAggregation":   "",
	"DateSubAggregation":      "",
	"TimestampSubAggregation": "",
	"TimeSubAggregation":      "",
	"JSONSubAggregation":      "",
	"GeometrySubAggregation":  "",
}

func ParseArgumentValue(defs Definitions, arg *ast.ArgumentDefinition, value *ast.Value, vars map[string]any, checkRequired bool) (any, error) {
	if arg.Type.NonNull {
		if value == nil {
			return nil, ErrorPosf(arg.Position, "argument %s is required", arg.Name)
		}
	}
	if value == nil {
		return nil, nil
	}
	t, ok := ScalarTypes[arg.Type.Name()]
	if !ok {
		// not scalar type - can be input object or enum (not supported yet)
		def := defs.ForName(arg.Type.Name())
		if def == nil || (def.Kind != ast.InputObject && def.Kind != ast.Enum) {
			return nil, ErrorPosf(arg.Position, "unsupported argument type %s", arg.Type.Name())
		}
		if value.Kind == ast.Variable {
			return ParseDataAsInputObject(defs, arg.Type, vars[value.Raw], checkRequired)
		}
		if value.Kind == ast.EnumValue {
			return value.Raw, nil
		}
		if value.Kind == ast.ListValue {
			var vv []any
			for _, v := range value.Children {
				v, err := ParseArgumentValue(defs, &ast.ArgumentDefinition{
					Name:     arg.Name,
					Type:     v.Value.ExpectedType,
					Position: v.Position,
				}, v.Value, vars, checkRequired)
				if err != nil {
					return nil, err
				}
				if v != nil {
					vv = append(vv, v)
				}
			}
			return vv, nil
		}
		vv := map[string]any{}
		for _, f := range def.Fields { // convert values to map
			v, err := ParseArgumentValue(defs, &ast.ArgumentDefinition{
				Name:     f.Name,
				Type:     f.Type,
				Position: f.Position,
			}, value.Children.ForName(f.Name), vars, checkRequired)
			if err != nil {
				return nil, err
			}
			if v != nil {
				vv[f.Name] = v
			}
		}
		return vv, nil
	}
	val, err := value.Value(vars)
	if err != nil {
		return nil, err
	}
	if arg.Type.NamedType != "" {
		if t.ParseValue != nil {
			val, err = t.ParseValue(val)
			if err != nil {
				return nil, err
			}
		}
		if err := checkDim(val, arg.Directives.ForName(base.FieldDimDirectiveName)); err != nil {
			return nil, err
		}
		return val, nil
	}
	if t.ParseArray != nil {
		return t.ParseArray(val)
	}
	return nil, ErrorPosf(value.Position, "unsupported argument type [%s]", arg.Type.Name())
}

func ParseDataAsInputObject(defs Definitions, inputType *ast.Type, data any, checkRequired bool) (any, error) {
	if data == nil {
		return nil, nil
	}
	if inputType.NamedType == "" {
		vv, ok := data.([]any)
		if !ok {
			return nil, ErrorPosf(inputType.Position, "expected array of objects")
		}
		var out []any
		for _, v := range vv {
			o, err := ParseDataAsInputObject(defs, inputType.Elem, v, checkRequired)
			if err != nil {
				return nil, err
			}
			out = append(out, o)
		}
		return out, nil
	}
	def := defs.ForName(inputType.Name())
	vv, ok := data.(map[string]any)
	if !ok {
		return nil, ErrorPosf(inputType.Position, "expected object")
	}
	out := map[string]any{}
	for _, f := range def.Fields {
		v, ok := vv[f.Name]
		if f.Type.NonNull && (!ok || v == nil) && checkRequired {
			return nil, ErrorPosf(inputType.Position, "field %s.%s is required", def.Name, f.Name)
		}
		if !ok {
			continue
		}
		if !IsScalarType(f.Type.Name()) {
			if v == nil {
				continue
			}
			val, err := ParseDataAsInputObject(defs, f.Type, v, checkRequired)
			if err != nil {
				return nil, err
			}
			out[f.Name] = val
			continue
		}
		t := ScalarTypes[f.Type.Name()]
		var err error
		parsed := v
		if f.Type.NamedType != "" {
			if t.ParseValue != nil {
				parsed, err = t.ParseValue(parsed)
				if err != nil {
					return nil, err
				}
			}
		}
		if f.Type.NamedType == "" {
			if t.ParseArray != nil {
				parsed, err = t.ParseArray(parsed)
				if err != nil {
					return nil, err
				}
			}
		}
		if err := checkDim(parsed, f.Directives.ForName(base.FieldDimDirectiveName)); err != nil {
			return nil, err
		}
		out[f.Name] = parsed
	}
	return out, nil
}

func IsScalarType(typeName string) bool {
	_, ok := ScalarTypes[typeName]
	return ok
}

func IsScalarAggregationType(typeName string) bool {
	_, ok := subAggregationTypes[typeName]
	return ok
}

func IsJSONType(typeName string) bool {
	return typeName == JSONTypeName
}

func SpecifiedByURL(def *ast.Definition) string {
	for _, d := range def.Directives {
		if d.Name == "specifiedBy" {
			return d.Arguments.ForName("url").Value.Raw
		}
	}
	return ""
}

// scalarFilterInputTypeName returns filter name type for scalar type by its name.
func scalarFilterInputTypeName(typeName string, isList bool) (string, bool) {
	v, ok := ScalarTypes[typeName]
	if isList {
		return v.ListFilterInput, ok
	}
	return v.FilterInput, ok
}

// shrunkScalarFilterType returns a shrunk by exclude directive scalar filter type.
func shrunkScalarFilterType(object, field, typeName string, isList bool, exclude *ast.Directive) *ast.Definition {
	filterName, ok := scalarFilterInputTypeName(typeName, isList)
	if !ok {
		return nil
	}
	inputName := object + "_" + field + "_" + filterName
	operators := exclude.Arguments.ForName("operators")
	var oo []string
	for _, o := range operators.Value.Children {
		oo = append(oo, o.Name)
	}

	filter := &ast.Definition{
		Kind: ast.InputObject,
		Name: inputName,
		Directives: []*ast.Directive{
			{
				Name: "base",
				Arguments: []*ast.Argument{
					{Name: "name", Value: &ast.Value{Raw: typeName, Kind: ast.StringValue}, Position: compiledPos()},
				},
				Position: compiledPos(),
			},
		},
		Position: compiledPos(),
	}

	for _, f := range base.Schema.Types[filterName].Fields {
		if slices.Contains(oo, f.Name) {
			filter.Fields = append(filter.Fields, f)
		}
	}
	if len(filter.Fields) == 0 {
		return nil
	}

	return filter
}

func checkDim(val any, dim *ast.Directive) error {
	if dim == nil {
		return nil
	}
	if dim.Arguments.ForName("len").Value.Raw == "" {
		return ErrorPosf(dim.Position, "missing length argument")
	}
	d, err := dim.Arguments.ForName("len").Value.Value(nil)
	if err != nil {
		return err
	}
	di, ok := d.(int64)
	if !ok || di <= 0 {
		return ErrorPosf(dim.Position, "invalid length argument: %v", d)
	}
	switch v := val.(type) {
	case types.Dimentional:
		if v.Len() != d {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", v.Len(), d)
		}
	case []any:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []float64:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []string:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	case []int64:
		if len(v) != int(di) {
			return ErrorPosf(dim.Position, "invalid vector length: %d, expected: %d", len(v), di)
		}
	default:
	}
	return nil
}
