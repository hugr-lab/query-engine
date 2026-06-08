package engines

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// ArrowIngestStagingBuilder builds SQL fragments evaluated by DuckDB while an
// Arrow reader is registered as a temporary view. Target engines still decide
// how Arrow columns are shaped, but default/auth expression functions must be
// valid in this DuckDB staging SELECT.
type ArrowIngestStagingBuilder struct {
	duckdb DuckDB
}

func NewArrowIngestStagingBuilder() *ArrowIngestStagingBuilder {
	return &ArrowIngestStagingBuilder{}
}

func (b *ArrowIngestStagingBuilder) SQLValue(v any) (string, error) {
	return b.duckdb.SQLValue(v)
}

func (b *ArrowIngestStagingBuilder) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	return b.duckdb.FunctionCall(name, positional, named)
}

func arrowExtensionName(field arrow.Field) string {
	if extType, ok := field.Type.(arrow.ExtensionType); ok {
		return strings.ToLower(extType.ExtensionName())
	}
	if ext, ok := field.Metadata.GetValue("ARROW:extension:name"); ok {
		return strings.ToLower(ext)
	}
	if ext, ok := field.Metadata.GetValue("extension:name"); ok {
		return strings.ToLower(ext)
	}
	return ""
}

func duckDBGeoArrowPointCoords(sql string) string {
	return "format('{} {}', struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))"
}

func duckDBGeoArrowPointWKT(sql string) string {
	return "'POINT (' || " + duckDBGeoArrowPointCoords(sql) + " || ')'"
}

func duckDBGeoArrowPointListCoords(sql string) string {
	return "array_to_string(list_transform(" + sql + ", lambda _p: " + duckDBGeoArrowPointCoords("_p") + "), ', ')"
}

func duckDBGeoArrowLineStringWKT(sql string) string {
	return "'LINESTRING (' || " + duckDBGeoArrowPointListCoords(sql) + " || ')'"
}

func duckDBGeoArrowRingWKT(sql string) string {
	return "'(' || " + duckDBGeoArrowPointListCoords(sql) + " || ')'"
}

func duckDBGeoArrowPolygonWKT(sql string) string {
	return "'POLYGON (' || array_to_string(list_transform(" + sql + ", lambda _r: " +
		duckDBGeoArrowRingWKT("_r") + "), ', ') || ')'"
}

func duckDBGeoArrowMultiPointWKT(sql string) string {
	return "'MULTIPOINT (' || " + duckDBGeoArrowPointListCoords(sql) + " || ')'"
}

func duckDBGeoArrowMultiLineStringWKT(sql string) string {
	return "'MULTILINESTRING (' || array_to_string(list_transform(" + sql + ", lambda _ls: " +
		duckDBGeoArrowRingWKT("_ls") + "), ', ') || ')'"
}

func duckDBGeoArrowMultiPolygonWKT(sql string) string {
	return "'MULTIPOLYGON (' || array_to_string(list_transform(" + sql + ", lambda _poly: '(' || " +
		"array_to_string(list_transform(_poly, lambda _r: " + duckDBGeoArrowRingWKT("_r") +
		"), ', ') || ')'), ', ') || ')'"
}

func duckDBGeoArrowNativeWKT(ext, sql string) (string, error) {
	switch ext {
	case "geoarrow.point":
		return duckDBGeoArrowPointWKT(sql), nil
	case "geoarrow.linestring":
		return duckDBGeoArrowLineStringWKT(sql), nil
	case "geoarrow.polygon":
		return duckDBGeoArrowPolygonWKT(sql), nil
	case "geoarrow.multipoint":
		return duckDBGeoArrowMultiPointWKT(sql), nil
	case "geoarrow.multilinestring":
		return duckDBGeoArrowMultiLineStringWKT(sql), nil
	case "geoarrow.multipolygon":
		return duckDBGeoArrowMultiPolygonWKT(sql), nil
	case "geoarrow.geometry", "geoarrow.geometrycollection":
		return "", fmt.Errorf("%s ingest is not supported from native union storage; send geoarrow.wkb, geoarrow.wkt, geoarrow.geojson, or a concrete GeoArrow coordinate layout", ext)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}
