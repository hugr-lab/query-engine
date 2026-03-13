package types

import (
	"time"

	"github.com/vektah/gqlparser/v2/ast"
)

// ScalarType represents a scalar's compilation-time metadata.
// Capabilities are determined via type assertions on optional interfaces
// (Filterable, ListFilterable, Aggregatable, MeasurementAggregatable, ExtraFieldProvider).
type ScalarType interface {
	Name() string
	SDL() string
}

// Filterable indicates the scalar supports single-value filtering.
type Filterable interface {
	FilterTypeName() string
}

// ListFilterable indicates the scalar supports list-value filtering.
type ListFilterable interface {
	ListFilterTypeName() string
}

// Aggregatable indicates the scalar supports aggregation.
type Aggregatable interface {
	AggregationTypeName() string
}

// MeasurementAggregatable indicates the scalar supports measurement aggregation.
type MeasurementAggregatable interface {
	MeasurementAggregationTypeName() string
}

// FieldArgumentsProvider indicates the scalar has field-level arguments
// (e.g., bucket for Timestamp, transforms for Geometry, struct for JSON).
type FieldArgumentsProvider interface {
	FieldArguments() ast.ArgumentDefinitionList
}

// ExtraFieldProvider indicates the scalar generates extra derived fields.
type ExtraFieldProvider interface {
	ExtraFieldName() string
	GenerateExtraField(fieldName string) *ast.FieldDefinition
}

// ValueParser is implemented by scalar types that need custom value parsing.
type ValueParser interface {
	ParseValue(v any) (any, error)
}

// ArrayParser is implemented by scalar types that support array parsing.
type ArrayParser interface {
	ParseArray(v any) (any, error)
}

// SubAggregatable indicates the scalar has a sub-aggregation type for nested aggregations.
type SubAggregatable interface {
	SubAggregationTypeName() string
}

// JSONTypeHintProvider provides the JSON extraction type hint for engines.
// Hint examples: "string", "number", "bool", "timestamp".
type JSONTypeHintProvider interface {
	JSONTypeHint() string
}

// SQLOutputTransformer is implemented by scalar types that need SQL output
// transformation (e.g., Geometry→ST_AsGeoJSON, H3Cell→h3_h3_to_string).
type SQLOutputTransformer interface {
	ToOutputSQL(sql string, raw bool) string
	ToStructFieldSQL(sql string) string
}

type ScalarTypes interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string | ~bool | Int32Range | Int64Range | TimeRange | BaseRange | time.Time
}

type Dimensional interface {
	Len() int
}
