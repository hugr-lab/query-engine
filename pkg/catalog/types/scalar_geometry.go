package types

import (
	"fmt"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time interface assertions.
var (
	_ ScalarType             = (*geometryScalar)(nil)
	_ Filterable             = (*geometryScalar)(nil)
	_ Aggregatable           = (*geometryScalar)(nil)
	_ SubAggregatable        = (*geometryScalar)(nil)
	_ ExtraFieldProvider     = (*geometryScalar)(nil)
	_ FieldArgumentsProvider = (*geometryScalar)(nil)
	_ ValueParser            = (*geometryScalar)(nil)
	_ SQLOutputTransformer   = (*geometryScalar)(nil)
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
scalar Geometry

input GeometryFilter @system {
  eq: Geometry
  intersects: Geometry
  contains: Geometry
  is_null: Boolean
}

type GeometryAggregation @system {
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
}

enum GeometryTransform @system {
  Transform
  Centroid
  Buffer
  Simplify
  SimplifyTopology
  StartPoint
  EndPoint
  Reverse
  FlipCoordinates
  ConvexHull
  Envelope
}

enum GeometryMeasurementTypes @system {
  Area
  AreaSpheroid
  Length
  LengthSpheroid
  Perimeter
  PerimeterSpheroid
}`
}

func (s *geometryScalar) FilterTypeName() string { return "GeometryFilter" }

func (s *geometryScalar) AggregationTypeName() string { return "GeometryAggregation" }

func (s *geometryScalar) SubAggregationTypeName() string { return "GeometrySubAggregation" }

func (s *geometryScalar) FieldArguments() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "transforms", Description: "Apply geometry transformations", Type: ast.ListType(ast.NonNullNamedType("GeometryTransform", nil), nil)},
		{Name: "from", Description: "Converts geometry from the specified SRID", Type: ast.NamedType("Int", nil)},
		{Name: "to", Description: "Converts geometry to the specified SRID", Type: ast.NamedType("Int", nil)},
		{Name: "buffer", Description: "Buffer geometry by the specified distance", Type: ast.NamedType("Float", nil)},
		{Name: "simplify_factor", Description: "Simplify geometry by the specified factor", Type: ast.NamedType("Float", nil)},
	}
}

func (s *geometryScalar) ExtraFieldName() string { return "Measurement" }

func (s *geometryScalar) GenerateExtraField(fieldName string) *ast.FieldDefinition {
	return generateGeometryExtraField(fieldName)
}

func (s *geometryScalar) ParseValue(v any) (any, error) {
	return ParseGeometryValue(v)
}

func (s *geometryScalar) ToOutputSQL(sql string, raw bool) string {
	if raw {
		return sql
	}
	return fmt.Sprintf("ST_AsGeoJSON(%s)", sql)
}

func (s *geometryScalar) ToStructFieldSQL(sql string) string {
	return fmt.Sprintf("ST_AsGeoJSON(%s)", sql)
}

func ParseGeometryValue(v any) (orb.Geometry, error) {
	if v == nil {
		return nil, nil
	}
	switch v := v.(type) {
	case orb.Geometry:
		return v, nil
	case map[string]interface{}: // GeoJSON
		return parseGeoJSONGeometry(v)
	case string: // WKT or HEX / WKB
		g, err := wkb.Unmarshal([]byte(v))
		if err == nil {
			return g, nil
		}
		return wkt.Unmarshal(v)
	default:
		return nil, fmt.Errorf("invalid geometry value: %v", v)
	}
}

func GeometryToSQLValue(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	g, ok := v.(orb.Geometry)
	if !ok {
		var err error
		g, err = ParseGeometryValue(v)
		if err != nil {
			return nil, err
		}
	}
	return wkb.Marshal(g)
}

func SQLValueToGeometry(v any) (orb.Geometry, error) {
	if v == nil {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("invalid geometry value: %v", v)
	}
	return wkb.Unmarshal([]byte(s))
}

func parseGeoJSONGeometry(v map[string]interface{}) (orb.Geometry, error) {
	t, ok := v["type"]
	if !ok {
		return nil, fmt.Errorf("geometry missing type")
	}
	ts, ok := t.(string)
	if !ok {
		return nil, fmt.Errorf("geometry type is not a string")
	}
	if ts == "GeometryCollection" {
		geometries, ok := v["geometries"]
		if !ok {
			return nil, fmt.Errorf("geometry collection missing geometries")
		}
		if geoms, ok := geometries.([]interface{}); ok {
			return parseGeoJSONGeometryCollection(geoms)
		}
		return nil, fmt.Errorf("geometry collection geometries is not an array")
	}
	coordinates, ok := v["coordinates"]
	if !ok {
		return nil, fmt.Errorf("line string missing coordinates")
	}
	coords, ok := coordinates.([]interface{})
	if !ok {
		return nil, fmt.Errorf("point coordinates is not an array")
	}

	switch ts {
	case "Point":
		return parseGeoJSONPoint(coords)
	case "LineString":
		return parseGeoJSONLineString(coords)
	case "Polygon":
		return parseGeoJSONPolygon(coords)
	case "MultiPoint":
		return parseGeoJSONMultiPoint(coords)
	case "MultiLineString":
		return parseGeoJSONMultiLineString(coords)
	case "MultiPolygon":
		return parseGeoJSONMultiPolygon(coords)
	default:
		return nil, fmt.Errorf("unsupported geometry type: %s", ts)
	}
}

func parseGeoJSONPoint(coords []interface{}) (orb.Point, error) {
	if len(coords) < 2 || len(coords) > 3 {
		return orb.Point{}, fmt.Errorf("point coordinates must have 2 elements")
	}
	x, ok := coords[0].(float64)
	if !ok {
		return orb.Point{}, fmt.Errorf("point x is not a number")
	}
	y, ok := coords[1].(float64)
	if !ok {
		return orb.Point{}, fmt.Errorf("point y is not a number")
	}
	return orb.Point{x, y}, nil
}

func parseGeoJSONLineString(coords []interface{}) (orb.LineString, error) {
	ls := make(orb.LineString, len(coords))
	var err error
	for i, c := range coords {
		cs, ok := c.([]interface{})
		if !ok {
			return nil, fmt.Errorf("line string coordinate is not an array")
		}
		ls[i], err = parseGeoJSONPoint(cs)
		if err != nil {
			return nil, err
		}
	}
	return ls, nil
}

func parseGeoJSONPolygon(coords []interface{}) (orb.Polygon, error) {
	p := make(orb.Polygon, len(coords))
	for i, c := range coords {
		cs, ok := c.([]interface{})
		if !ok {
			return nil, fmt.Errorf("polygon ring is not an array")
		}
		ls, err := parseGeoJSONLineString(cs)
		if err != nil {
			return nil, err
		}
		p[i] = orb.Ring(ls)
	}
	return p, nil
}

func parseGeoJSONMultiPoint(coords []interface{}) (orb.MultiPoint, error) {
	mp := make(orb.MultiPoint, len(coords))
	var err error
	for i, c := range coords {
		cs, ok := c.([]interface{})
		if !ok {
			return nil, fmt.Errorf("multi point coordinate is not an array")
		}
		mp[i], err = parseGeoJSONPoint(cs)
		if err != nil {
			return nil, err
		}
	}
	return mp, nil
}

func parseGeoJSONMultiLineString(coords []interface{}) (orb.MultiLineString, error) {
	mls := make(orb.MultiLineString, len(coords))
	var err error
	for i, c := range coords {
		cs, ok := c.([]interface{})
		if !ok {
			return nil, fmt.Errorf("multi line string coordinate is not an array")
		}
		mls[i], err = parseGeoJSONLineString(cs)
		if err != nil {
			return nil, err
		}
	}
	return mls, nil
}

func parseGeoJSONMultiPolygon(coords []interface{}) (orb.MultiPolygon, error) {
	mp := make(orb.MultiPolygon, len(coords))
	var err error
	for i, c := range coords {
		cs, ok := c.([]interface{})
		if !ok {
			return nil, fmt.Errorf("multi polygon ring is not an array")
		}
		mp[i], err = parseGeoJSONPolygon(cs)
		if err != nil {
			return nil, err
		}
	}
	return mp, nil
}

func parseGeoJSONGeometryCollection(geoms []any) (orb.Collection, error) {
	gc := make(orb.Collection, len(geoms))
	var err error
	for i, c := range geoms {
		cs, ok := c.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("geometry collection geometry is not a map")
		}
		gc[i], err = parseGeoJSONGeometry(cs)
		if err != nil {
			return nil, err
		}
	}

	return gc, nil
}
