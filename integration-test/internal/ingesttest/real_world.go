//go:build duckdb_arrow

package ingesttest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type RealWorldFeatureCollection struct {
	Features []RealWorldFeature `json:"features"`
}

type RealWorldFeature struct {
	Properties RealWorldFeatureProperties `json:"properties"`
	Geometry   RealWorldGeometry          `json:"geometry"`
}

type RealWorldFeatureProperties struct {
	GeometryKind string `json:"geometry_kind"`
	Name         string `json:"name"`
}

type RealWorldGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type RealWorldExpectedCounts struct {
	Features      int
	GeometryKinds map[string]int
	GeometryTypes map[string]int
}

var (
	NaturalEarthGeometryExpectedCounts = realWorldExpectedCounts(map[string]int{
		"point":        7342,
		"line":         1000,
		"multiline":    800,
		"polygon":      500,
		"multipolygon": 300,
		"multipoint":   58,
	})
	NaturalEarthGeometryValuesExpectedCounts = balancedRealWorldExpectedCounts(3)
	OSMGeometryExpectedCounts                = realWorldExpectedCounts(map[string]int{
		"point":        5000,
		"line":         2500,
		"polygon":      1500,
		"multipoint":   500,
		"multiline":    300,
		"multipolygon": 200,
	})
	OSMGeometryValuesExpectedCounts = balancedRealWorldExpectedCounts(3)
)

var geometryTypeByGeometryKind = map[string]string{
	"point":        "Point",
	"line":         "LineString",
	"polygon":      "Polygon",
	"multipoint":   "MultiPoint",
	"multiline":    "MultiLineString",
	"multipolygon": "MultiPolygon",
}

func balancedRealWorldExpectedCounts(perGeometryKind int) RealWorldExpectedCounts {
	geometryKinds := make(map[string]int, len(geometryTypeByGeometryKind))
	for geometryKind := range geometryTypeByGeometryKind {
		geometryKinds[geometryKind] = perGeometryKind
	}
	return realWorldExpectedCounts(geometryKinds)
}

func realWorldExpectedCounts(geometryKinds map[string]int) RealWorldExpectedCounts {
	geometryKindCounts := make(map[string]int, len(geometryKinds))
	geometryTypeCounts := make(map[string]int, len(geometryKinds))
	features := 0
	for geometryKind, count := range geometryKinds {
		geometryType, ok := geometryTypeByGeometryKind[geometryKind]
		if !ok {
			panic(fmt.Sprintf("unsupported real-world geometry kind %q", geometryKind))
		}
		geometryKindCounts[geometryKind] = count
		geometryTypeCounts[geometryType] = count
		features += count
	}
	return RealWorldExpectedCounts{
		Features:      features,
		GeometryKinds: geometryKindCounts,
		GeometryTypes: geometryTypeCounts,
	}
}

func LoadNaturalEarthGeometryFeatures(t testing.TB, filename string, expected RealWorldExpectedCounts) ([]RealWorldFeature, map[string]int) {
	t.Helper()

	path := filepath.Join("..", "ingest", "testdata", "real-world", "natural-earth", filename)
	return LoadRealWorldGeometryFeatures(t, path, expected)
}

func LoadOSMGeometryFeatures(t testing.TB, filename string, expected RealWorldExpectedCounts) ([]RealWorldFeature, map[string]int) {
	t.Helper()

	path := filepath.Join("..", "ingest", "testdata", "real-world", "osm", filename)
	return LoadRealWorldGeometryFeatures(t, path, expected)
}

func LoadRealWorldGeometryFeatures(t testing.TB, path string, expected RealWorldExpectedCounts) ([]RealWorldFeature, map[string]int) {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var fc RealWorldFeatureCollection
	require.NoError(t, json.Unmarshal(data, &fc))
	require.Len(t, fc.Features, expected.Features, "real-world fixture %s should have expected feature count", path)

	geometryKindCounts := make(map[string]int)
	geometryTypeCounts := make(map[string]int)
	for i, feature := range fc.Features {
		require.NotEmptyf(t, feature.Properties.GeometryKind, "feature %d has no geometry_kind", i)
		require.NotEmptyf(t, feature.Properties.Name, "feature %d has no source name", i)
		require.NotEmptyf(t, feature.Geometry.Type, "feature %d has no geometry type", i)
		geometryKindCounts[feature.Properties.GeometryKind]++
		geometryTypeCounts[feature.Geometry.Type]++
	}
	require.Equal(t, expected.GeometryKinds, geometryKindCounts, "fixture data must match expected feature geometry kinds")
	require.Equal(t, expected.GeometryTypes, geometryTypeCounts, "fixture data must match expected geometry types")
	return fc.Features, geometryKindCounts
}

func FirstFeatureWithGeometryKind(t testing.TB, features []RealWorldFeature, geometryKind string) RealWorldFeature {
	t.Helper()

	for _, feature := range features {
		if feature.Properties.GeometryKind == geometryKind {
			return feature
		}
	}
	t.Fatalf("real-world fixture should include geometry kind %q", geometryKind)
	return RealWorldFeature{}
}

func FeaturesByGeometryKind(features []RealWorldFeature) map[string][]RealWorldFeature {
	byGeometryKind := make(map[string][]RealWorldFeature)
	for _, feature := range features {
		byGeometryKind[feature.Properties.GeometryKind] = append(byGeometryKind[feature.Properties.GeometryKind], feature)
	}
	return byGeometryKind
}

func GeometryKindOrder() []string {
	return []string{"point", "line", "polygon", "multipoint", "multiline", "multipolygon"}
}

func ColumnsForGeometryKind(geometryKind string) []string {
	columns := []string{"name", "value", "is_active"}
	return append(columns, GeometryColumnsForGeometryKind(geometryKind)...)
}

func GeometryColumnsForGeometryKind(geometryKind string) []string {
	switch geometryKind {
	case "point":
		return []string{"geom", "geom_wkb", "geom_hexwkb"}
	case "line":
		return []string{"geom_wkt", "geom_line"}
	case "polygon":
		return []string{"geom_geojson", "geom_hugr_geojson", "geom_plain_geojson", "geom_geojson_struct", "geom_polygon_native"}
	case "multipoint":
		return []string{"geom_multipoint"}
	case "multiline":
		return []string{"geom_multiline"}
	case "multipolygon":
		return []string{"geom_multipolygon"}
	default:
		panic(fmt.Sprintf("unsupported real-world geometry kind %q", geometryKind))
	}
}

func ExpectedFeatureCount(geometryKindCounts map[string]int) int {
	total := 0
	for _, count := range geometryKindCounts {
		total += count
	}
	return total
}

func ExpectedDBGeometryTypeCounts(geometryKindCounts map[string]int) map[string]int {
	return map[string]int{
		"POINT":           geometryKindCounts["point"] * 3,
		"LINESTRING":      geometryKindCounts["line"] * 2,
		"POLYGON":         geometryKindCounts["polygon"] * 5,
		"MULTIPOINT":      geometryKindCounts["multipoint"],
		"MULTILINESTRING": geometryKindCounts["multiline"],
		"MULTIPOLYGON":    geometryKindCounts["multipolygon"],
	}
}

func ExpectedColumnCounts(geometryKindCounts map[string]int) map[string]int {
	return map[string]int{
		"geom":                geometryKindCounts["point"],
		"geom_wkb":            geometryKindCounts["point"],
		"geom_hexwkb":         geometryKindCounts["point"],
		"geom_wkt":            geometryKindCounts["line"],
		"geom_line":           geometryKindCounts["line"],
		"geom_geojson":        geometryKindCounts["polygon"],
		"geom_hugr_geojson":   geometryKindCounts["polygon"],
		"geom_plain_geojson":  geometryKindCounts["polygon"],
		"geom_geojson_struct": geometryKindCounts["polygon"],
		"geom_polygon_native": geometryKindCounts["polygon"],
		"geom_multipoint":     geometryKindCounts["multipoint"],
		"geom_multiline":      geometryKindCounts["multiline"],
		"geom_multipolygon":   geometryKindCounts["multipolygon"],
	}
}

func CountedGeometryColumns() []string {
	return []string{
		"geom",
		"geom_wkb",
		"geom_hexwkb",
		"geom_wkt",
		"geom_line",
		"geom_geojson",
		"geom_hugr_geojson",
		"geom_plain_geojson",
		"geom_geojson_struct",
		"geom_polygon_native",
		"geom_multipoint",
		"geom_multiline",
		"geom_multipolygon",
	}
}

type RealWorldSample struct {
	Row     int
	Feature RealWorldFeature
}

func FirstFeatureByGeometryKind(t testing.TB, features []RealWorldFeature) map[string]RealWorldSample {
	t.Helper()

	samples := make(map[string]RealWorldSample)
	geometryKindRows := make(map[string]int)
	for _, feature := range features {
		geometryKind := feature.Properties.GeometryKind
		row := geometryKindRows[geometryKind]
		geometryKindRows[geometryKind]++
		if _, exists := samples[geometryKind]; !exists {
			samples[geometryKind] = RealWorldSample{Row: row, Feature: feature}
		}
	}
	for _, geometryKind := range GeometryKindOrder() {
		require.Contains(t, samples, geometryKind, "real-world fixture should include %s", geometryKind)
	}
	return samples
}

func ExpectedGeometryByColumn(t testing.TB, feature RealWorldFeature) map[string]string {
	t.Helper()

	switch feature.Properties.GeometryKind {
	case "point":
		point := PointFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom":        RealWorldPointWKT(point),
			"geom_wkb":    RealWorldPointWKT(point),
			"geom_hexwkb": RealWorldPointWKT(point),
		}
	case "line":
		line := LineStringFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom_wkt":  RealWorldLineWKT(line),
			"geom_line": RealWorldLineWKT(line),
		}
	case "polygon":
		polygon := PolygonFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom_geojson":        RealWorldPolygonWKT(polygon),
			"geom_hugr_geojson":   RealWorldPolygonWKT(polygon),
			"geom_plain_geojson":  RealWorldPolygonWKT(polygon),
			"geom_geojson_struct": RealWorldPolygonWKT(polygon),
			"geom_polygon_native": RealWorldPolygonWKT(polygon),
		}
	case "multipoint":
		multiPoint := MultiPointFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom_multipoint": RealWorldMultiPointWKT(multiPoint),
		}
	case "multiline":
		multiLine := MultiLineStringFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom_multiline": RealWorldMultiLineWKT(multiLine),
		}
	case "multipolygon":
		multiPolygon := MultiPolygonFromGeometry(t, feature.Geometry)
		return map[string]string{
			"geom_multipolygon": RealWorldMultiPolygonWKT(multiPolygon),
		}
	default:
		t.Fatalf("unsupported real-world geometry kind %q", feature.Properties.GeometryKind)
		return nil
	}
}

func RowName(rowPrefix string, row int, feature RealWorldFeature) string {
	return fmt.Sprintf("%s-%05d-%s", rowPrefix, row, feature.Properties.GeometryKind)
}

func PointFromGeometry(t testing.TB, geom RealWorldGeometry) Point {
	t.Helper()
	require.Equal(t, "Point", geom.Type)
	var coords []float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	return PointFromCoord(t, coords)
}

func MultiPointFromGeometry(t testing.TB, geom RealWorldGeometry) []Point {
	t.Helper()
	require.Equal(t, "MultiPoint", geom.Type)
	var coords [][]float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	return PointsFromCoords(t, coords)
}

func LineStringFromGeometry(t testing.TB, geom RealWorldGeometry) []Point {
	t.Helper()
	require.Equal(t, "LineString", geom.Type)
	var coords [][]float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	return PointsFromCoords(t, coords)
}

func MultiLineStringFromGeometry(t testing.TB, geom RealWorldGeometry) [][]Point {
	t.Helper()
	require.Equal(t, "MultiLineString", geom.Type)
	var coords [][][]float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	lines := make([][]Point, 0, len(coords))
	for _, line := range coords {
		lines = append(lines, PointsFromCoords(t, line))
	}
	return lines
}

func PolygonFromGeometry(t testing.TB, geom RealWorldGeometry) [][]Point {
	t.Helper()
	require.Equal(t, "Polygon", geom.Type)
	var coords [][][]float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	return LinesFromCoords(t, coords)
}

func MultiPolygonFromGeometry(t testing.TB, geom RealWorldGeometry) [][][]Point {
	t.Helper()
	require.Equal(t, "MultiPolygon", geom.Type)
	var coords [][][][]float64
	require.NoError(t, json.Unmarshal(geom.Coordinates, &coords))
	polygons := make([][][]Point, 0, len(coords))
	for _, polygon := range coords {
		polygons = append(polygons, LinesFromCoords(t, polygon))
	}
	return polygons
}

func LinesFromCoords(t testing.TB, coords [][][]float64) [][]Point {
	t.Helper()
	lines := make([][]Point, 0, len(coords))
	for _, line := range coords {
		lines = append(lines, PointsFromCoords(t, line))
	}
	return lines
}

func PointsFromCoords(t testing.TB, coords [][]float64) []Point {
	t.Helper()
	points := make([]Point, 0, len(coords))
	for _, coord := range coords {
		points = append(points, PointFromCoord(t, coord))
	}
	return points
}

func PointFromCoord(t testing.TB, coord []float64) Point {
	t.Helper()
	require.Len(t, coord, 2)
	return Point{X: coord[0], Y: coord[1]}
}
