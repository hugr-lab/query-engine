//go:build duckdb_arrow

package ingesttest

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/stretchr/testify/require"
)

type Point struct {
	X float64
	Y float64
}

const (
	geoArrowPointXField = iota
	geoArrowPointYField
)

const (
	geoJSONGeometryTypeField = iota
	geoJSONGeometryCoordinatesField
)

func AppendPoint(sb *array.StructBuilder, point Point) {
	sb.Append(true)
	sb.FieldBuilder(geoArrowPointXField).(*array.Float64Builder).Append(point.X)
	sb.FieldBuilder(geoArrowPointYField).(*array.Float64Builder).Append(point.Y)
}

func AppendGeoJSONPolygonStruct(t testing.TB, builder array.Builder, x, y float64) {
	t.Helper()
	AppendGeoJSONPolygonStructFromRings(t, builder, PolygonRings(x, y))
}

func AppendGeoJSONPolygonStructFromRings(t testing.TB, builder array.Builder, rings [][]Point) {
	t.Helper()
	sb, ok := builder.(*array.StructBuilder)
	require.Truef(t, ok, "got %T, want *array.StructBuilder", builder)

	sb.Append(true)
	sb.FieldBuilder(geoJSONGeometryTypeField).(*array.StringBuilder).Append("Polygon")
	AppendGeoJSONPolygonCoordinates(sb.FieldBuilder(geoJSONGeometryCoordinatesField).(*array.ListBuilder), rings)
}

func AppendGeoJSONPolygonCoordinates(lb *array.ListBuilder, rings [][]Point) {
	lb.Append(true)
	ringBuilder := lb.ValueBuilder().(*array.ListBuilder)
	for _, ring := range rings {
		ringBuilder.Append(true)
		pointBuilder := ringBuilder.ValueBuilder().(*array.ListBuilder)
		for _, point := range ring {
			pointBuilder.Append(true)
			pointBuilder.ValueBuilder().(*array.Float64Builder).AppendValues([]float64{point.X, point.Y}, nil)
		}
	}
}

func AppendPointList(lb *array.ListBuilder, points []Point) {
	lb.Append(true)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	for _, point := range points {
		AppendPoint(sb, point)
	}
}

func AppendPointListList(lb *array.ListBuilder, lines [][]Point) {
	lb.Append(true)
	inner := lb.ValueBuilder().(*array.ListBuilder)
	for _, points := range lines {
		AppendPointList(inner, points)
	}
}

func AppendPointListListList(lb *array.ListBuilder, polygons [][][]Point) {
	lb.Append(true)
	inner := lb.ValueBuilder().(*array.ListBuilder)
	for _, rings := range polygons {
		AppendPointListList(inner, rings)
	}
}

func WKBPoint(t testing.TB, point Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orb.Point{point.X, point.Y})
	require.NoError(t, err)
	return data
}

func WKBLineString(t testing.TB, points []Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orbLineString(points))
	require.NoError(t, err)
	return data
}

func WKBPolygon(t testing.TB, rings [][]Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orbPolygon(rings))
	require.NoError(t, err)
	return data
}

func WKBMultiPoint(t testing.TB, points []Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orbMultiPoint(points))
	require.NoError(t, err)
	return data
}

func WKBMultiLineString(t testing.TB, lines [][]Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orbMultiLineString(lines))
	require.NoError(t, err)
	return data
}

func WKBMultiPolygon(t testing.TB, polygons [][][]Point) []byte {
	t.Helper()

	data, err := wkb.Marshal(orbMultiPolygon(polygons))
	require.NoError(t, err)
	return data
}

func orbLineString(points []Point) orb.LineString {
	line := make(orb.LineString, 0, len(points))
	for _, point := range points {
		line = append(line, orb.Point{point.X, point.Y})
	}
	return line
}

func orbPolygon(rings [][]Point) orb.Polygon {
	polygon := make(orb.Polygon, 0, len(rings))
	for _, points := range rings {
		polygon = append(polygon, orb.Ring(orbLineString(points)))
	}
	return polygon
}

func orbMultiPoint(points []Point) orb.MultiPoint {
	multiPoint := make(orb.MultiPoint, 0, len(points))
	for _, point := range points {
		multiPoint = append(multiPoint, orb.Point{point.X, point.Y})
	}
	return multiPoint
}

func orbMultiLineString(lines [][]Point) orb.MultiLineString {
	multiLine := make(orb.MultiLineString, 0, len(lines))
	for _, line := range lines {
		multiLine = append(multiLine, orbLineString(line))
	}
	return multiLine
}

func orbMultiPolygon(polygons [][][]Point) orb.MultiPolygon {
	multiPolygon := make(orb.MultiPolygon, 0, len(polygons))
	for _, polygon := range polygons {
		multiPolygon = append(multiPolygon, orbPolygon(polygon))
	}
	return multiPolygon
}

func LinePoints(x, y float64) []Point {
	return []Point{{X: x, Y: y}, {X: x + 1, Y: y + 1}, {X: x + 2, Y: y + 1}}
}

func PolygonRings(x, y float64) [][]Point {
	return [][]Point{
		{{X: x, Y: y}, {X: x, Y: y + 4}, {X: x + 4, Y: y + 4}, {X: x + 4, Y: y}, {X: x, Y: y}},
		{{X: x + 1, Y: y + 1}, {X: x + 2, Y: y + 1}, {X: x + 2, Y: y + 2}, {X: x + 1, Y: y + 2}, {X: x + 1, Y: y + 1}},
	}
}

func MultiPoints(x, y float64) []Point {
	return []Point{{X: x, Y: y}, {X: x + 1, Y: y + 1}, {X: x + 2, Y: y}}
}

func MultiLines(x, y float64) [][]Point {
	return [][]Point{
		{{X: x, Y: y}, {X: x + 1, Y: y + 1}},
		{{X: x + 2, Y: y + 2}, {X: x + 3, Y: y + 3}},
	}
}

func MultiPolygons(x, y float64) [][][]Point {
	return [][][]Point{
		PolygonRings(x, y),
		{{{X: x + 10, Y: y + 10}, {X: x + 10, Y: y + 12}, {X: x + 12, Y: y + 12}, {X: x + 12, Y: y + 10}, {X: x + 10, Y: y + 10}}},
	}
}

func LineWKT(x, y float64) string {
	return fmt.Sprintf("LINESTRING (%s %s, %s %s, %s %s)",
		Coord(x), Coord(y),
		Coord(x+1), Coord(y+1),
		Coord(x+2), Coord(y+1))
}

func PointWKT(point Point) string {
	return fmt.Sprintf("POINT(%s %s)", Coord(point.X), Coord(point.Y))
}

func PolygonGeoJSON(x, y float64) string {
	return fmt.Sprintf(`{"type":"Polygon","coordinates":[[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]],[[%s,%s],[%s,%s],[%s,%s],[%s,%s],[%s,%s]]]}`,
		Coord(x), Coord(y),
		Coord(x), Coord(y+4),
		Coord(x+4), Coord(y+4),
		Coord(x+4), Coord(y),
		Coord(x), Coord(y),
		Coord(x+1), Coord(y+1),
		Coord(x+2), Coord(y+1),
		Coord(x+2), Coord(y+2),
		Coord(x+1), Coord(y+2),
		Coord(x+1), Coord(y+1))
}

func GeoJSONGeometry(typ string, coordinates any) map[string]any {
	return map[string]any{
		"type":        typ,
		"coordinates": coordinates,
	}
}

func PointCoordinate(point Point) []any {
	return []any{point.X, point.Y}
}

func PointCoordinates(points []Point) []any {
	coords := make([]any, 0, len(points))
	for _, point := range points {
		coords = append(coords, PointCoordinate(point))
	}
	return coords
}

func NestedPointCoordinates(lines [][]Point) []any {
	coords := make([]any, 0, len(lines))
	for _, line := range lines {
		coords = append(coords, PointCoordinates(line))
	}
	return coords
}

func DeepPointCoordinates(polygons [][][]Point) []any {
	coords := make([]any, 0, len(polygons))
	for _, polygon := range polygons {
		coords = append(coords, NestedPointCoordinates(polygon))
	}
	return coords
}

func RealWorldPolygonGeoJSON(rings [][]Point) string {
	return fmt.Sprintf(`{"type":"Polygon","coordinates":%s}`, RealWorldJSONCoordinates(NestedPointCoordinates(rings)))
}

func RealWorldJSONCoordinates(coordinates any) string {
	b, err := json.Marshal(coordinates)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func RealWorldPointWKT(point Point) string {
	return fmt.Sprintf("POINT(%s %s)", Coord(point.X), Coord(point.Y))
}

func RealWorldLineWKT(points []Point) string {
	return "LINESTRING(" + RealWorldPointListWKT(points) + ")"
}

func RealWorldPolygonWKT(rings [][]Point) string {
	return "POLYGON(" + RealWorldRingsWKT(rings) + ")"
}

func RealWorldMultiPointWKT(points []Point) string {
	return "MULTIPOINT(" + RealWorldPointListWKT(points) + ")"
}

func RealWorldMultiLineWKT(lines [][]Point) string {
	parts := make([]string, 0, len(lines))
	for _, line := range lines {
		parts = append(parts, "("+RealWorldPointListWKT(line)+")")
	}
	return "MULTILINESTRING(" + strings.Join(parts, ",") + ")"
}

func RealWorldMultiPolygonWKT(polygons [][][]Point) string {
	parts := make([]string, 0, len(polygons))
	for _, polygon := range polygons {
		parts = append(parts, "("+RealWorldRingsWKT(polygon)+")")
	}
	return "MULTIPOLYGON(" + strings.Join(parts, ",") + ")"
}

func RealWorldRingsWKT(rings [][]Point) string {
	parts := make([]string, 0, len(rings))
	for _, ring := range rings {
		parts = append(parts, "("+RealWorldPointListWKT(ring)+")")
	}
	return strings.Join(parts, ",")
}

func RealWorldPointListWKT(points []Point) string {
	parts := make([]string, 0, len(points))
	for _, point := range points {
		parts = append(parts, Coord(point.X)+" "+Coord(point.Y))
	}
	return strings.Join(parts, ",")
}

func AddCoord(v string, delta float64) string {
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(err)
	}
	return Coord(f + delta)
}

func CompactWKT(s string) string {
	s = strings.ReplaceAll(s, ", ", ",")
	s = strings.ReplaceAll(s, " (", "(")
	if strings.HasPrefix(s, "MULTIPOINT((") && strings.HasSuffix(s, "))") {
		inner := strings.TrimSuffix(strings.TrimPrefix(s, "MULTIPOINT(("), "))")
		s = "MULTIPOINT(" + strings.ReplaceAll(inner, "),(", ",") + ")"
	}
	return s
}

func Coord(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
