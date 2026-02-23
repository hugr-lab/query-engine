package types

import "github.com/vektah/gqlparser/v2/ast"

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
	FilterSDL() string
}

// ListFilterable indicates the scalar supports list-value filtering.
type ListFilterable interface {
	ListFilterTypeName() string
	ListFilterSDL() string
}

// Aggregatable indicates the scalar supports aggregation.
type Aggregatable interface {
	AggregationTypeName() string
	AggregationSDL() string
}

// MeasurementAggregatable indicates the scalar supports measurement aggregation.
type MeasurementAggregatable interface {
	MeasurementAggregationTypeName() string
	MeasurementAggregationSDL() string
}

// ExtraFieldProvider indicates the scalar generates extra derived fields.
type ExtraFieldProvider interface {
	ExtraFieldName() string
	GenerateExtraField(fieldName string) *ast.FieldDefinition
}
