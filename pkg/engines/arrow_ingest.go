package engines

import (
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

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

func arrowFieldIsExtensionType(field arrow.Field) bool {
	_, ok := field.Type.(arrow.ExtensionType)
	return ok
}

func duckDBGeoArrowPoint(sql string) string {
	return "ST_Point(struct_extract(" + sql + ", 'x'), struct_extract(" + sql + ", 'y'))"
}
