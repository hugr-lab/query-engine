//go:build duckdb_arrow

package ingesttest

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/require"
)

type nativeGeometryLayout uint8

const (
	nativeGeometryStructLayout nativeGeometryLayout = iota
	nativeGeometryFixedSizeListLayout
)

// WithFixedSizeListNativeGeometryFields replaces only native GeoArrow
// coordinate storage. Serialized WKB/WKT/GeoJSON fields remain unchanged.
func WithFixedSizeListNativeGeometryFields(fields []arrow.Field) []arrow.Field {
	result := append([]arrow.Field(nil), fields...)
	pointType := arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float64)
	lineType := arrow.ListOf(pointType)
	polygonType := arrow.ListOf(lineType)

	for i := range result {
		extension, _ := result[i].Metadata.GetValue("ARROW:extension:name")
		switch extension {
		case "geoarrow.point":
			result[i].Type = pointType
		case "geoarrow.linestring", "geoarrow.multipoint":
			result[i].Type = lineType
		case "geoarrow.polygon", "geoarrow.multilinestring":
			result[i].Type = polygonType
		case "geoarrow.multipolygon":
			result[i].Type = arrow.ListOf(polygonType)
		}
	}
	return result
}

func appendNativePoint(t testing.TB, builder array.Builder, layout nativeGeometryLayout, point Point) {
	t.Helper()
	switch layout {
	case nativeGeometryStructLayout:
		AppendPoint(builder.(*array.StructBuilder), point)
	case nativeGeometryFixedSizeListLayout:
		list := builder.(*array.FixedSizeListBuilder)
		list.Append(true)
		list.ValueBuilder().(*array.Float64Builder).AppendValues([]float64{point.X, point.Y}, nil)
	default:
		t.Fatalf("unsupported native geometry layout %d", layout)
	}
}

func appendNativePointList(t testing.TB, builder array.Builder, layout nativeGeometryLayout, points []Point) {
	t.Helper()
	list := builder.(*array.ListBuilder)
	list.Append(true)
	for _, point := range points {
		appendNativePoint(t, list.ValueBuilder(), layout, point)
	}
}

func appendNativePointListList(t testing.TB, builder array.Builder, layout nativeGeometryLayout, lines [][]Point) {
	t.Helper()
	list := builder.(*array.ListBuilder)
	list.Append(true)
	for _, line := range lines {
		appendNativePointList(t, list.ValueBuilder(), layout, line)
	}
}

func appendNativePointListListList(t testing.TB, builder array.Builder, layout nativeGeometryLayout, polygons [][][]Point) {
	t.Helper()
	list := builder.(*array.ListBuilder)
	list.Append(true)
	for _, polygon := range polygons {
		appendNativePointListList(t, list.ValueBuilder(), layout, polygon)
	}
}

func nativeGeometryBuilderLayout(t testing.TB, builder array.Builder, listDepth int) nativeGeometryLayout {
	t.Helper()
	coordinateBuilder := builder
	for depth := 0; depth < listDepth; depth++ {
		list, ok := coordinateBuilder.(*array.ListBuilder)
		require.Truef(t, ok, "got %T at list depth %d, want *array.ListBuilder", coordinateBuilder, depth+1)
		coordinateBuilder = list.ValueBuilder()
	}
	switch coordinateBuilder.(type) {
	case *array.StructBuilder:
		return nativeGeometryStructLayout
	case *array.FixedSizeListBuilder:
		return nativeGeometryFixedSizeListLayout
	default:
		t.Fatalf("unsupported native coordinate builder %T", coordinateBuilder)
		return 0
	}
}
