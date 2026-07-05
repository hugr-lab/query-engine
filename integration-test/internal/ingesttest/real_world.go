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
	Role string `json:"role"`
	Name string `json:"name"`
}

type RealWorldGeometry struct {
	Type        string          `json:"type"`
	Coordinates json.RawMessage `json:"coordinates"`
}

type RealWorldExpectedCounts struct {
	Features      int
	Roles         map[string]int
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

var geometryTypeByRole = map[string]string{
	"point":        "Point",
	"line":         "LineString",
	"polygon":      "Polygon",
	"multipoint":   "MultiPoint",
	"multiline":    "MultiLineString",
	"multipolygon": "MultiPolygon",
}

func balancedRealWorldExpectedCounts(perRole int) RealWorldExpectedCounts {
	roles := make(map[string]int, len(geometryTypeByRole))
	for role := range geometryTypeByRole {
		roles[role] = perRole
	}
	return realWorldExpectedCounts(roles)
}

func realWorldExpectedCounts(roles map[string]int) RealWorldExpectedCounts {
	roleCounts := make(map[string]int, len(roles))
	geometryTypeCounts := make(map[string]int, len(roles))
	features := 0
	for role, count := range roles {
		geometryType, ok := geometryTypeByRole[role]
		if !ok {
			panic(fmt.Sprintf("unsupported real-world geometry role %q", role))
		}
		roleCounts[role] = count
		geometryTypeCounts[geometryType] = count
		features += count
	}
	return RealWorldExpectedCounts{
		Features:      features,
		Roles:         roleCounts,
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

	roleCounts := make(map[string]int)
	geometryTypeCounts := make(map[string]int)
	for i, feature := range fc.Features {
		require.NotEmptyf(t, feature.Properties.Role, "feature %d has no role", i)
		require.NotEmptyf(t, feature.Properties.Name, "feature %d has no source name", i)
		require.NotEmptyf(t, feature.Geometry.Type, "feature %d has no geometry type", i)
		roleCounts[feature.Properties.Role]++
		geometryTypeCounts[feature.Geometry.Type]++
	}
	require.Equal(t, expected.Roles, roleCounts, "fixture data must match expected feature roles")
	require.Equal(t, expected.GeometryTypes, geometryTypeCounts, "fixture data must match expected geometry types")
	return fc.Features, roleCounts
}

func FirstFeatureWithRole(t testing.TB, features []RealWorldFeature, role string) RealWorldFeature {
	t.Helper()

	for _, feature := range features {
		if feature.Properties.Role == role {
			return feature
		}
	}
	t.Fatalf("real-world fixture should include role %q", role)
	return RealWorldFeature{}
}

func FeaturesByRole(features []RealWorldFeature) map[string][]RealWorldFeature {
	byRole := make(map[string][]RealWorldFeature)
	for _, feature := range features {
		byRole[feature.Properties.Role] = append(byRole[feature.Properties.Role], feature)
	}
	return byRole
}

func RoleOrder() []string {
	return []string{"point", "line", "polygon", "multipoint", "multiline", "multipolygon"}
}

func ColumnsForRole(role string) []string {
	columns := []string{"name", "value", "is_active"}
	return append(columns, GeometryColumnsForRole(role)...)
}

func GeometryColumnsForRole(role string) []string {
	switch role {
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
		panic(fmt.Sprintf("unsupported real-world geometry role %q", role))
	}
}

func ExpectedFeatureCount(roleCounts map[string]int) int {
	total := 0
	for _, count := range roleCounts {
		total += count
	}
	return total
}

func ExpectedDBGeometryTypeCounts(roleCounts map[string]int) map[string]int {
	return map[string]int{
		"POINT":           roleCounts["point"] * 3,
		"LINESTRING":      roleCounts["line"] * 2,
		"POLYGON":         roleCounts["polygon"] * 5,
		"MULTIPOINT":      roleCounts["multipoint"],
		"MULTILINESTRING": roleCounts["multiline"],
		"MULTIPOLYGON":    roleCounts["multipolygon"],
	}
}

func ExpectedColumnCounts(roleCounts map[string]int) map[string]int {
	return map[string]int{
		"geom":                roleCounts["point"],
		"geom_wkb":            roleCounts["point"],
		"geom_hexwkb":         roleCounts["point"],
		"geom_wkt":            roleCounts["line"],
		"geom_line":           roleCounts["line"],
		"geom_geojson":        roleCounts["polygon"],
		"geom_hugr_geojson":   roleCounts["polygon"],
		"geom_plain_geojson":  roleCounts["polygon"],
		"geom_geojson_struct": roleCounts["polygon"],
		"geom_polygon_native": roleCounts["polygon"],
		"geom_multipoint":     roleCounts["multipoint"],
		"geom_multiline":      roleCounts["multiline"],
		"geom_multipolygon":   roleCounts["multipolygon"],
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

func FirstFeatureByRole(t testing.TB, features []RealWorldFeature) map[string]RealWorldSample {
	t.Helper()

	samples := make(map[string]RealWorldSample)
	roleRows := make(map[string]int)
	for _, feature := range features {
		role := feature.Properties.Role
		row := roleRows[role]
		roleRows[role]++
		if _, exists := samples[role]; !exists {
			samples[role] = RealWorldSample{Row: row, Feature: feature}
		}
	}
	for _, role := range RoleOrder() {
		require.Contains(t, samples, role, "real-world fixture should include %s", role)
	}
	return samples
}

func ExpectedGeometryByColumn(t testing.TB, feature RealWorldFeature) map[string]string {
	t.Helper()

	switch feature.Properties.Role {
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
		t.Fatalf("unsupported real-world geometry role %q", feature.Properties.Role)
		return nil
	}
}

func RowName(rowPrefix string, row int, feature RealWorldFeature) string {
	return fmt.Sprintf("%s-%05d-%s", rowPrefix, row, feature.Properties.Role)
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
