package engines

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// ArrowIngestStagingBuilder owns every SQL expression evaluated by DuckDB while
// an Arrow reader is registered as a view. Target-specific conversion, when a
// target needs one, is applied separately through EngineIngestValueAdapter.
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

// SelectExpr converts an Arrow-view column to its canonical DuckDB staging
// representation for the target GraphQL field.
func (b *ArrowIngestStagingBuilder) SelectExpr(field *ast.Field, arrowField arrow.Field, sourceExpr string) (string, error) {
	if field == nil || field.Definition == nil {
		return sourceExpr, nil
	}
	switch field.Definition.Type.Name() {
	case base.JSONTypeName:
		return arrowIngestJSONStagingExpr(arrowField, sourceExpr)
	case base.GeometryTypeName:
		return arrowIngestGeometryStagingExpr(arrowField, sourceExpr)
	default:
		return sourceExpr, nil
	}
}

// LiteralExpr converts a non-Arrow value, such as permission data, to a
// canonical DuckDB staging expression.
func (b *ArrowIngestStagingBuilder) LiteralExpr(field *ast.Field, value any) (string, error) {
	if value == nil {
		return "NULL", nil
	}
	if field != nil && field.Definition != nil && field.Definition.Type.Name() == base.GeometryTypeName {
		geom, err := ctypes.ParseGeometryValue(value)
		if err != nil {
			return "", err
		}
		if geom == nil {
			return "NULL", nil
		}
		wkbValue, err := ctypes.GeometryToSQLValue(geom)
		if err != nil {
			return "", err
		}
		return "ST_GeomFromWKB(from_hex('" + strings.ToUpper(hex.EncodeToString(wkbValue)) + "'))", nil
	}
	return b.duckdb.SQLValue(value)
}

const (
	arrowJSONExtension = "arrow.json"

	hugrGeoJSONExtension     = "hugr.geojson"
	geoArrowGeoJSONExtension = "geoarrow.geojson"
	plainGeoJSONExtension    = "geojson"
	hugrHexWKBExtension      = "hugr.hexwkb"
	geoArrowHexWKBExtension  = "geoarrow.hexwkb"
	plainHexWKBExtension     = "hexwkb"

	geoArrowWKBExtension                = "geoarrow.wkb"
	geoArrowWKTExtension                = "geoarrow.wkt"
	geoArrowPointExtension              = "geoarrow.point"
	geoArrowLineStringExtension         = "geoarrow.linestring"
	geoArrowPolygonExtension            = "geoarrow.polygon"
	geoArrowMultiPointExtension         = "geoarrow.multipoint"
	geoArrowMultiLineStringExtension    = "geoarrow.multilinestring"
	geoArrowMultiPolygonExtension       = "geoarrow.multipolygon"
	geoArrowGeometryExtension           = "geoarrow.geometry"
	geoArrowGeometryCollectionExtension = "geoarrow.geometrycollection"
)

func arrowIngestJSONStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	ext := arrowExtensionNameFromTypeOrMetadata(arrowField)
	switch {
	case ext == "":
		return jsonExprFromPlainArrow(arrowField, sourceExpr), nil
	case ext == arrowJSONExtension:
		return jsonExprFromArrowJSONExtension(arrowField, sourceExpr)
	case isGeoJSONExtension(ext):
		return jsonExprFromGeoJSONExtension(arrowField, sourceExpr)
	case needsGeometryToJSON(ext):
		geomExpr, err := geometryExprFromExtension(ext, arrowField, sourceExpr)
		if err != nil {
			return "", err
		}
		return jsonExprFromGeometryExpr(geomExpr), nil
	default:
		return "", fmt.Errorf("unsupported Arrow extension %q for JSON ingest", ext)
	}
}

func jsonExprFromPlainArrow(arrowField arrow.Field, sourceExpr string) string {
	if expr, ok := jsonExprFromSerializedStorage(arrowField, sourceExpr); ok {
		return expr
	}
	return duckDBToJSON(sourceExpr)
}

func jsonExprFromArrowJSONExtension(arrowField arrow.Field, sourceExpr string) (string, error) {
	if expr, ok := jsonExprFromSerializedStorage(arrowField, sourceExpr); ok {
		return expr, nil
	}
	return "", storageError(arrowField, arrowJSONExtension)
}

func jsonExprFromGeoJSONExtension(arrowField arrow.Field, sourceExpr string) (string, error) {
	if expr, ok := jsonExprFromSerializedStorage(arrowField, sourceExpr); ok {
		return expr, nil
	}
	if isArrowObjectStorage(arrowStorageTypeID(arrowField.Type)) {
		return duckDBToJSON(sourceExpr), nil
	}
	return "", storageError(arrowField, "GeoJSON")
}

func jsonExprFromSerializedStorage(arrowField arrow.Field, sourceExpr string) (string, bool) {
	storage := arrowStorageTypeID(arrowField.Type)
	switch {
	case isArrowStringStorage(storage):
		return "CAST(" + sourceExpr + " AS JSON)", true
	case isArrowBinaryStorage(storage):
		return "CAST(decode(" + sourceExpr + ") AS JSON)", true
	default:
		return "", false
	}
}

func jsonExprFromGeometryExpr(geometryExpr string) string {
	return "CAST(ST_AsGeoJSON(" + geometryExpr + ") AS JSON)"
}

func duckDBToJSON(sql string) string {
	return "to_json(" + sql + ")"
}

func arrowIngestGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	ext := arrowExtensionNameFromTypeOrMetadata(arrowField)
	if ext == "" {
		return geometryExprFromPlainArrow(arrowField, sourceExpr)
	}
	return geometryExprFromExtension(ext, arrowField, sourceExpr)
}

// geometryExprFromExtension uses GeoArrow/Hugr extension metadata as the source
// of truth. The physical Arrow storage type is only validated inside the
// selected extension handler; unsupported metadata never falls back to guessing.
func geometryExprFromExtension(ext string, arrowField arrow.Field, sourceExpr string) (string, error) {
	switch {
	case ext == geoArrowWKBExtension:
		return geometryExprFromGeoArrowWKB(arrowField, sourceExpr)
	case isHexWKBExtension(ext):
		return geometryExprFromHexWKB(arrowField, sourceExpr)
	case ext == geoArrowWKTExtension:
		return geometryExprFromWKT(arrowField, sourceExpr)
	case isGeoJSONExtension(ext):
		return geometryExprFromGeoJSON(arrowField, sourceExpr)
	case ext == arrowJSONExtension:
		return geometryExprFromArrowJSON(arrowField, sourceExpr)
	case isGeoArrowCoordinateExtension(ext):
		return geometryExprFromGeoArrowCoordinates(ext, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}

func geometryExprFromPlainArrow(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	switch {
	case isArrowBinaryStorage(storage) || storage == arrow.FIXED_SIZE_BINARY:
		return "ST_GeomFromWKB(" + sourceExpr + ")", nil
	case isArrowStringStorage(storage):
		return "ST_GeomFromText(" + sourceExpr + ", true)", nil
	case isArrowObjectStorage(storage):
		return "ST_GeomFromGeoJSON(" + duckDBJSONAsVarchar(sourceExpr) + ")", nil
	default:
		return "", fmt.Errorf("arrow column %q with type %s cannot be ingested as Geometry without geoarrow/hugr metadata", arrowField.Name, arrowField.Type)
	}
}

func geometryExprFromGeoArrowWKB(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	if isArrowBinaryStorage(storage) || storage == arrow.FIXED_SIZE_BINARY {
		return sourceExpr, nil
	}
	return "", storageError(arrowField, geoArrowWKBExtension)
}

func geometryExprFromHexWKB(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	if isArrowStringStorage(storage) {
		return "ST_GeomFromWKB(from_hex(" + sourceExpr + "))", nil
	}
	return "", storageError(arrowField, "hexwkb")
}

func geometryExprFromWKT(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	if isArrowStringStorage(storage) {
		return "ST_GeomFromText(" + sourceExpr + ", true)", nil
	}
	return "", storageError(arrowField, geoArrowWKTExtension)
}

func geometryExprFromGeoJSON(arrowField arrow.Field, sourceExpr string) (string, error) {
	textExpr, err := geoJSONTextExpr(arrowField, sourceExpr)
	if err != nil {
		return "", err
	}
	return "ST_GeomFromGeoJSON(" + textExpr + ")", nil
}

func geometryExprFromArrowJSON(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	if isArrowStringStorage(storage) {
		return "ST_GeomFromGeoJSON(CAST(" + sourceExpr + " AS VARCHAR))", nil
	}
	return "", storageError(arrowField, arrowJSONExtension)
}

func geoJSONTextExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	storage := arrowStorageTypeID(arrowField.Type)
	switch {
	case isArrowStringStorage(storage):
		return sourceExpr, nil
	case isArrowBinaryStorage(storage):
		return "CAST(decode(" + sourceExpr + ") AS VARCHAR)", nil
	case isArrowObjectStorage(storage):
		return duckDBJSONAsVarchar(sourceExpr), nil
	default:
		return "", storageError(arrowField, "GeoJSON")
	}
}

func duckDBJSONAsVarchar(sql string) string {
	return duckDBToJSON(sql) + "::VARCHAR"
}

func arrowExtensionNameFromTypeOrMetadata(field arrow.Field) string {
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

func arrowStorageTypeID(dt arrow.DataType) arrow.Type {
	if extType, ok := dt.(arrow.ExtensionType); ok {
		return extType.StorageType().ID()
	}
	return dt.ID()
}

func isArrowStringStorage(storage arrow.Type) bool {
	switch storage {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return true
	default:
		return false
	}
}

func isArrowBinaryStorage(storage arrow.Type) bool {
	switch storage {
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return true
	default:
		return false
	}
}

func isArrowObjectStorage(storage arrow.Type) bool {
	switch storage {
	case arrow.STRUCT, arrow.MAP:
		return true
	default:
		return false
	}
}

func isGeoJSONExtension(ext string) bool {
	switch ext {
	case hugrGeoJSONExtension, geoArrowGeoJSONExtension, plainGeoJSONExtension:
		return true
	default:
		return false
	}
}

func isHexWKBExtension(ext string) bool {
	switch ext {
	case hugrHexWKBExtension, geoArrowHexWKBExtension, plainHexWKBExtension:
		return true
	default:
		return false
	}
}

func needsGeometryToJSON(ext string) bool {
	return ext == geoArrowWKBExtension ||
		ext == geoArrowWKTExtension ||
		isHexWKBExtension(ext) ||
		isGeoArrowCoordinateExtension(ext)
}

func isGeoArrowCoordinateExtension(ext string) bool {
	switch ext {
	case geoArrowPointExtension,
		geoArrowLineStringExtension,
		geoArrowPolygonExtension,
		geoArrowMultiPointExtension,
		geoArrowMultiLineStringExtension,
		geoArrowMultiPolygonExtension,
		geoArrowGeometryExtension,
		geoArrowGeometryCollectionExtension:
		return true
	default:
		return false
	}
}

func storageError(arrowField arrow.Field, format string) error {
	return fmt.Errorf("arrow column %q with type %s cannot use %s storage", arrowField.Name, arrowField.Type, format)
}

func geoArrowPointGeometryExpr(sql string) string {
	return "ST_Point(struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))"
}

func geoArrowLineStringGeometryExpr(sql string) string {
	return "ST_MakeLine(list_transform(" + sql + ", lambda _p: " + geoArrowPointGeometryExpr("_p") + "))"
}

func geoArrowPolygonGeometryExpr(sql string) string {
	shell := geoArrowLineStringGeometryExpr(sql + "[1]")
	holes := "list_transform(" + sql + "[2:], lambda _r: " + geoArrowLineStringGeometryExpr("_r") + ")"
	return "ST_MakePolygon(" + shell + ", " + holes + ")"
}

func geoArrowMultiPointGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _p: " + geoArrowPointGeometryExpr("_p") + ")))"
}

func geoArrowMultiLineStringGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _ls: " + geoArrowLineStringGeometryExpr("_ls") + ")))"
}

func geoArrowMultiPolygonGeometryExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _poly: " + geoArrowPolygonGeometryExpr("_poly") + ")))"
}

func geometryExprFromGeoArrowCoordinates(ext, sql string) (string, error) {
	switch ext {
	case geoArrowPointExtension:
		return geoArrowPointGeometryExpr(sql), nil
	case geoArrowLineStringExtension:
		return geoArrowLineStringGeometryExpr(sql), nil
	case geoArrowPolygonExtension:
		return geoArrowPolygonGeometryExpr(sql), nil
	case geoArrowMultiPointExtension:
		return geoArrowMultiPointGeometryExpr(sql), nil
	case geoArrowMultiLineStringExtension:
		return geoArrowMultiLineStringGeometryExpr(sql), nil
	case geoArrowMultiPolygonExtension:
		return geoArrowMultiPolygonGeometryExpr(sql), nil
	case geoArrowGeometryExtension, geoArrowGeometryCollectionExtension:
		return "", fmt.Errorf("%s ingest is not supported from native union storage; send geoarrow.wkb, geoarrow.wkt, geoarrow.geojson, or a concrete GeoArrow coordinate layout", ext)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}
