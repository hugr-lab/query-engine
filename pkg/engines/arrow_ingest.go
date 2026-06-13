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

func duckDBArrowJSONExpr(arrowField arrow.Field, sourceExpr string) string {
	switch arrowField.Type.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW,
		arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return "try_cast(" + sourceExpr + " AS JSON)"
	case arrow.STRUCT, arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST,
		arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW, arrow.MAP:
		return "to_json(" + sourceExpr + ")"
	default:
		return sourceExpr
	}
}

func duckDBArrowGeometryExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	if ext := arrowExtensionName(arrowField); ext != "" {
		return duckDBArrowGeometryExprFromTrustedExtension(ext, sourceExpr)
	}
	return duckDBArrowGeometryExprFromPhysicalType(arrowField, sourceExpr)
}

// duckDBArrowGeometryExprFromTrustedExtension uses GeoArrow/Hugr extension
// metadata as the source of truth for geometry semantics. The physical Arrow
// storage type is intentionally not used as a fallback once extension metadata
// is present; unsupported metadata should fail during planning instead of being
// guessed from Type.ID().
func duckDBArrowGeometryExprFromTrustedExtension(ext, sourceExpr string) (string, error) {
	switch ext {
	case "geoarrow.wkb":
		return sourceExpr, nil
	case "geoarrow.wkt":
		return "ST_GeomFromText(" + sourceExpr + ", true)", nil
	case "hugr.geojson", "geoarrow.geojson", "geojson":
		return "ST_GeomFromGeoJSON(" + sourceExpr + ")", nil
	case "geoarrow.linestring", "geoarrow.polygon",
		"geoarrow.multipoint", "geoarrow.multilinestring", "geoarrow.multipolygon",
		"geoarrow.point", "geoarrow.geometry", "geoarrow.geometrycollection":
		return duckDBGeoArrowNativeGeometryExpr(ext, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}

// duckDBArrowGeometryExprFromPhysicalType is the best-effort path for
// unannotated Arrow columns. Without extension metadata we infer common
// geometry encodings from physical Arrow storage.
func duckDBArrowGeometryExprFromPhysicalType(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowField.Type.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "ST_GeomFromWKB(" + sourceExpr + ")", nil
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "CASE WHEN starts_with(trim(" + sourceExpr + "), '{') THEN ST_GeomFromGeoJSON(" + sourceExpr + ") ELSE ST_GeomFromText(" + sourceExpr + ", true) END", nil
	case arrow.STRUCT, arrow.MAP:
		return "ST_GeomFromGeoJSON(to_json(" + sourceExpr + ")::VARCHAR)", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot be ingested as Geometry without geoarrow/hugr metadata", arrowField.Name, arrowField.Type)
	}
}

func duckDBArrowGeometryWKTWireExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	if ext := arrowExtensionName(arrowField); ext != "" {
		return duckDBArrowGeometryWKTWireExprFromTrustedExtension(ext, sourceExpr)
	}
	return duckDBArrowGeometryWKTWireExprFromPhysicalType(arrowField, sourceExpr)
}

// duckDBArrowGeometryWKTWireExprFromTrustedExtension builds a DuckDB expression
// returning WKT text for engines whose insert path cannot accept DuckDB
// GEOMETRY/WKB values directly. This is currently needed for Postgres attached
// tables: DuckDB's postgres extension COPY path accepts WKT/EWKT text for
// PostGIS geometry columns, but rejects WKB_BLOB/HEXWKB expressions.
func duckDBArrowGeometryWKTWireExprFromTrustedExtension(ext, sourceExpr string) (string, error) {
	switch ext {
	case "geoarrow.wkb":
		return "ST_AsText(" + sourceExpr + ")", nil
	case "geoarrow.wkt":
		return sourceExpr, nil
	case "hugr.geojson", "geoarrow.geojson", "geojson":
		return "ST_AsText(ST_GeomFromGeoJSON(" + sourceExpr + "))", nil
	case "geoarrow.linestring", "geoarrow.polygon",
		"geoarrow.multipoint", "geoarrow.multilinestring", "geoarrow.multipolygon",
		"geoarrow.point", "geoarrow.geometry", "geoarrow.geometrycollection":
		return duckDBGeoArrowNativeWKT(ext, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}

func duckDBArrowGeometryWKTWireExprFromPhysicalType(arrowField arrow.Field, sourceExpr string) (string, error) {
	switch arrowField.Type.ID() {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW, arrow.FIXED_SIZE_BINARY:
		return "ST_AsText(ST_GeomFromWKB(" + sourceExpr + "))", nil
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "CASE WHEN starts_with(trim(" + sourceExpr + "), '{') THEN ST_AsText(ST_GeomFromGeoJSON(" + sourceExpr + ")) ELSE " + sourceExpr + " END", nil
	case arrow.STRUCT, arrow.MAP:
		return "ST_AsText(ST_GeomFromGeoJSON(to_json(" + sourceExpr + ")::VARCHAR))", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot be ingested as Geometry without geoarrow/hugr metadata", arrowField.Name, arrowField.Type)
	}
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

func duckDBGeoArrowPointGeometryExpr(sql string) string {
	return "ST_Point(struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))"
}

func duckDBGeoArrowPointWKT(sql string) string {
	return "'POINT (' || " + duckDBGeoArrowPointCoords(sql) + " || ')'"
}

func duckDBGeoArrowLineStringGeometryExpr(sql string) string {
	return "ST_MakeLine(list_transform(" + sql + ", lambda _p: " + duckDBGeoArrowPointGeometryExpr("_p") + "))"
}

func duckDBGeoArrowPolygonGeometryExpr(sql string) string {
	shell := duckDBGeoArrowLineStringGeometryExpr(sql + "[1]")
	holes := "list_transform(" + sql + "[2:], lambda _r: " + duckDBGeoArrowLineStringGeometryExpr("_r") + ")"
	return "ST_MakePolygon(" + shell + ", " + holes + ")"
}

func duckDBGeoArrowMultiPointGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _p: " + duckDBGeoArrowPointGeometryExpr("_p") + ")))"
}

func duckDBGeoArrowMultiLineStringGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _ls: " + duckDBGeoArrowLineStringGeometryExpr("_ls") + ")))"
}

func duckDBGeoArrowMultiPolygonGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _poly: " + duckDBGeoArrowPolygonGeometryExpr("_poly") + ")))"
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

func duckDBGeoArrowNativeGeometryExpr(ext, sql string) (string, error) {
	switch ext {
	case "geoarrow.point":
		return duckDBGeoArrowPointGeometryExpr(sql), nil
	case "geoarrow.linestring":
		return duckDBGeoArrowLineStringGeometryExpr(sql), nil
	case "geoarrow.polygon":
		return duckDBGeoArrowPolygonGeometryExpr(sql), nil
	case "geoarrow.multipoint":
		return duckDBGeoArrowMultiPointGeometryExpr(sql), nil
	case "geoarrow.multilinestring":
		return duckDBGeoArrowMultiLineStringGeometryExpr(sql), nil
	case "geoarrow.multipolygon":
		return duckDBGeoArrowMultiPolygonGeometryExpr(sql), nil
	case "geoarrow.geometry", "geoarrow.geometrycollection":
		return "", fmt.Errorf("%s ingest is not supported from native union storage; send geoarrow.wkb, geoarrow.wkt, geoarrow.geojson, or a concrete GeoArrow coordinate layout", ext)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}
