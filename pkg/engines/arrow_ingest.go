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
		return arrowIngestJSONStagingExpr(arrowField, sourceExpr), nil
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

func arrowIngestJSONStagingExpr(arrowField arrow.Field, sourceExpr string) string {
	switch arrowField.Type.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "CAST(" + sourceExpr + " AS JSON)"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
		return "CAST(decode(" + sourceExpr + ") AS JSON)"
	case arrow.STRUCT, arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST,
		arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW, arrow.MAP:
		return "to_json(" + sourceExpr + ")"
	default:
		return "to_json(" + sourceExpr + ")"
	}
}

func arrowIngestGeometryStagingExpr(arrowField arrow.Field, sourceExpr string) (string, error) {
	if ext := arrowExtensionName(arrowField); ext != "" {
		return arrowIngestGeometryStagingExprFromTrustedExtension(ext, sourceExpr)
	}
	return arrowIngestGeometryStagingExprFromPhysicalType(arrowField, sourceExpr)
}

// arrowIngestGeometryStagingExprFromTrustedExtension uses GeoArrow/Hugr extension
// metadata as the source of truth for geometry semantics. The physical Arrow
// storage type is intentionally not used as a fallback once extension metadata
// is present; unsupported metadata should fail during planning instead of being
// guessed from Type.ID().
func arrowIngestGeometryStagingExprFromTrustedExtension(ext, sourceExpr string) (string, error) {
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
		return arrowIngestGeoArrowNativeGeometryStagingExpr(ext, sourceExpr)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}

// arrowIngestGeometryStagingExprFromPhysicalType is the best-effort path for
// unannotated Arrow columns. Without extension metadata we infer common
// geometry encodings from physical Arrow storage.
func arrowIngestGeometryStagingExprFromPhysicalType(arrowField arrow.Field, sourceExpr string) (string, error) {
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

func arrowIngestGeoArrowPointGeometryStagingExpr(sql string) string {
	return "ST_Point(struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))"
}

func arrowIngestGeoArrowLineStringGeometryStagingExpr(sql string) string {
	return "ST_MakeLine(list_transform(" + sql + ", lambda _p: " + arrowIngestGeoArrowPointGeometryStagingExpr("_p") + "))"
}

func arrowIngestGeoArrowPolygonGeometryStagingExpr(sql string) string {
	shell := arrowIngestGeoArrowLineStringGeometryStagingExpr(sql + "[1]")
	holes := "list_transform(" + sql + "[2:], lambda _r: " + arrowIngestGeoArrowLineStringGeometryStagingExpr("_r") + ")"
	return "ST_MakePolygon(" + shell + ", " + holes + ")"
}

func arrowIngestGeoArrowMultiPointGeometryStagingExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _p: " + arrowIngestGeoArrowPointGeometryStagingExpr("_p") + ")))"
}

func arrowIngestGeoArrowMultiLineStringGeometryStagingExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _ls: " + arrowIngestGeoArrowLineStringGeometryStagingExpr("_ls") + ")))"
}

func arrowIngestGeoArrowMultiPolygonGeometryStagingExpr(sql string) string {
	return "ST_Multi(ST_Collect(list_transform(" + sql + ", lambda _poly: " + arrowIngestGeoArrowPolygonGeometryStagingExpr("_poly") + ")))"
}

func arrowIngestGeoArrowNativeGeometryStagingExpr(ext, sql string) (string, error) {
	switch ext {
	case "geoarrow.point":
		return arrowIngestGeoArrowPointGeometryStagingExpr(sql), nil
	case "geoarrow.linestring":
		return arrowIngestGeoArrowLineStringGeometryStagingExpr(sql), nil
	case "geoarrow.polygon":
		return arrowIngestGeoArrowPolygonGeometryStagingExpr(sql), nil
	case "geoarrow.multipoint":
		return arrowIngestGeoArrowMultiPointGeometryStagingExpr(sql), nil
	case "geoarrow.multilinestring":
		return arrowIngestGeoArrowMultiLineStringGeometryStagingExpr(sql), nil
	case "geoarrow.multipolygon":
		return arrowIngestGeoArrowMultiPolygonGeometryStagingExpr(sql), nil
	case "geoarrow.geometry", "geoarrow.geometrycollection":
		return "", fmt.Errorf("%s ingest is not supported from native union storage; send geoarrow.wkb, geoarrow.wkt, geoarrow.geojson, or a concrete GeoArrow coordinate layout", ext)
	default:
		return "", fmt.Errorf("unsupported GeoArrow extension %q", ext)
	}
}
