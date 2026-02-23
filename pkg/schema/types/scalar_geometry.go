package types

import "github.com/vektah/gqlparser/v2/ast"

// Compile-time interface assertions.
var (
	_ ScalarType             = (*geometryScalar)(nil)
	_ Filterable             = (*geometryScalar)(nil)
	_ Aggregatable           = (*geometryScalar)(nil)
	_ ExtraFieldProvider     = (*geometryScalar)(nil)
	_ FieldArgumentsProvider = (*geometryScalar)(nil)
)

type geometryScalar struct{}

func (s *geometryScalar) Name() string { return "Geometry" }

func (s *geometryScalar) SDL() string {
	return `"""
The ` + "`Geometry`" + ` scalar type represents a spatial geometry value (WKB/WKT encoded).
Filter operators: eq, intersects, contains, is_null
Aggregation functions: count, list, any, last, intersection, union, extent
Extra field: Measurement (calculates area, length, perimeter, etc.)
"""
scalar Geometry`
}

func (s *geometryScalar) FilterTypeName() string { return "GeometryFilter" }

func (s *geometryScalar) FilterSDL() string {
	return `input GeometryFilter @system {
  eq: Geometry
  intersects: Geometry
  contains: Geometry
  is_null: Boolean
}`
}

func (s *geometryScalar) AggregationTypeName() string { return "GeometryAggregation" }

func (s *geometryScalar) AggregationSDL() string {
	return `type GeometryAggregation @system {
  count: BigInt
  list(distinct: Boolean = false): [Geometry!]
  any: Geometry
  last: Geometry
  intersection: Geometry
  union: Geometry
  extent: Geometry
}

type GeometrySubAggregation @system {
  count: BigIntAggregation
  intersection: GeometryAggregation
  union: GeometryAggregation
  extent: GeometryAggregation
}`
}

func (s *geometryScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "transforms", Type: ast.ListType(ast.NonNullNamedType("GeometryTransform", nil), nil)},
		{Name: "from", Type: ast.NamedType("Int", nil)},
		{Name: "to", Type: ast.NamedType("Int", nil)},
		{Name: "buffer", Type: ast.NamedType("Float", nil)},
		{Name: "simplify_factor", Type: ast.NamedType("Float", nil)},
	}
}

func (s *geometryScalar) ExtraFieldName() string { return "Measurement" }

func (s *geometryScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateGeometryExtraField(fieldName)
}
