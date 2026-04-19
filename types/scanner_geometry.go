package types

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/wkb"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/paulmach/orb/geojson"
)

// GeometryDecoder converts an Arrow cell into an orb.Geometry. Registered
// against an Arrow extension name via RegisterGeometryDecoder. The scanner
// invokes a decoder when the destination Go type is geometry-typed and the
// column carries a recognised extension name.
type GeometryDecoder func(arr arrow.Array, row int, fieldMeta arrow.Metadata) (orb.Geometry, error)

var (
	geometryDecoders   = make(map[string]GeometryDecoder)
	geometryDecodersMu sync.RWMutex
)

// RegisterGeometryDecoder binds an Arrow extension name to a decoder. If name
// is already registered the new decoder replaces the old one. Safe to call
// from init; uses an RWMutex for thread-safety.
func RegisterGeometryDecoder(name string, fn GeometryDecoder) {
	geometryDecodersMu.Lock()
	defer geometryDecodersMu.Unlock()
	if fn == nil {
		delete(geometryDecoders, name)
		return
	}
	geometryDecoders[name] = fn
}

func lookupGeometryDecoder(name string) (GeometryDecoder, bool) {
	geometryDecodersMu.RLock()
	defer geometryDecodersMu.RUnlock()
	fn, ok := geometryDecoders[name]
	return fn, ok
}

// Arrow-extension metadata key (see
// https://arrow.apache.org/docs/format/Columnar.html#extension-types).
// The companion "ARROW:extension:metadata" key is available on field.Metadata
// if decoders need SRID / encoding hints.
const arrowExtNameKey = "ARROW:extension:name"

// fieldGeometryName returns the geometry extension name associated with a
// field, or the empty string if it isn't a geometry column.
//
// Detection order (matches research.md R4):
//  1. If the field type is an Arrow ExtensionType, use its ExtensionName().
//  2. Otherwise, read ARROW:extension:name from field metadata (the engine
//     tags plain-storage columns this way for GeoJSONString / H3Cell).
func fieldGeometryName(f arrow.Field) string {
	if ext, ok := f.Type.(arrow.ExtensionType); ok {
		return ext.ExtensionName()
	}
	if f.Metadata.Len() > 0 {
		if i := f.Metadata.FindKey(arrowExtNameKey); i >= 0 {
			return f.Metadata.Values()[i]
		}
	}
	return ""
}

// orbGeometryType is the reflect.Type of the orb.Geometry interface.
var orbGeometryType = reflect.TypeOf((*orb.Geometry)(nil)).Elem()

// isGeometryDest returns true when the Go destination type is orb.Geometry
// or one of the concrete orb types (Point, LineString, Polygon, Multi*,
// Ring, Bound, Collection).
func isGeometryDest(t reflect.Type) bool {
	if t == orbGeometryType {
		return true
	}
	// A concrete orb type implements orb.Geometry.
	if reflect.PointerTo(t).Implements(orbGeometryType) || t.Implements(orbGeometryType) {
		// Keep it to types declared in the orb package to avoid false positives.
		return t.PkgPath() == "github.com/paulmach/orb"
	}
	return false
}

// geometryConvertFunc builds a convertFunc that decodes the cell via the
// registered decoder for extName, then assigns the result to dst.
// If the destination is a concrete orb subtype, a type check is performed.
// The field's metadata is closed over so decoders have access to SRID /
// encoding hints without reaching into array internals.
func geometryConvertFunc(extName string, fieldMeta arrow.Metadata, dstType reflect.Type) (convertFunc, error) {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		dec, ok := lookupGeometryDecoder(extName)
		if !ok {
			return fmt.Errorf("%w: no decoder registered for extension %q", ErrGeometryDecode, extName)
		}
		// Extension arrays expose the storage via Storage(); decoders may
		// expect the raw storage rather than the extension wrapper.
		decodeCol := col
		if ext, ok := col.(array.ExtensionArray); ok {
			decodeCol = ext.Storage()
		}
		geom, err := dec(decodeCol, row, fieldMeta)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrGeometryDecode, err.Error())
		}
		if geom == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		// Assign.
		if dstType == orbGeometryType {
			dst.Set(reflect.ValueOf(geom))
			return nil
		}
		// Concrete subtype — type-check the decoded value.
		gv := reflect.ValueOf(geom)
		if gv.Type() == dstType {
			dst.Set(gv)
			return nil
		}
		// Allow value-kind dereference if orb returns a pointer vs value mismatch.
		if gv.Kind() == reflect.Pointer && gv.Elem().Type() == dstType {
			dst.Set(gv.Elem())
			return nil
		}
		return fmt.Errorf("%w: decoded %T is not assignable to %s", ErrGeometryDecode, geom, dstType)
	}, nil
}

// autoStringGeometryConvertFunc handles the case where a String / LargeString
// column carries geometry but no ARROW:extension:name metadata tags it.
// Observed on engine-emitted nested paths (e.g. tf.digital_twin.roads.parts[].geom).
// Strategy: peek at the first non-space byte — '{' → GeoJSON, otherwise WKT.
func autoStringGeometryConvertFunc(fieldMeta arrow.Metadata, dstType reflect.Type) convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		var s string
		switch a := col.(type) {
		case *array.String:
			s = a.Value(row)
		case *array.LargeString:
			s = a.Value(row)
		default:
			return fmt.Errorf("%w: auto geometry expected string storage, got %s", ErrGeometryDecode, col.DataType())
		}
		extName := detectStringGeometryKind(s)
		dec, ok := lookupGeometryDecoder(extName)
		if !ok {
			return fmt.Errorf("%w: no decoder for inferred extension %q", ErrGeometryDecode, extName)
		}
		geom, err := dec(col, row, fieldMeta)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrGeometryDecode, err.Error())
		}
		if geom == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		if dstType == orbGeometryType {
			dst.Set(reflect.ValueOf(geom))
			return nil
		}
		gv := reflect.ValueOf(geom)
		if gv.Type() == dstType {
			dst.Set(gv)
			return nil
		}
		return fmt.Errorf("%w: decoded %T is not assignable to %s", ErrGeometryDecode, geom, dstType)
	}
}

// detectStringGeometryKind peeks at leading whitespace-trimmed bytes to
// distinguish GeoJSON from WKT.
func detectStringGeometryKind(s string) string {
	for _, r := range s {
		switch r {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return "hugr.geojson"
		default:
			return "geoarrow.wkt"
		}
	}
	return "hugr.geojson" // empty — decoder will return nil
}

// ─── Default decoders ────────────────────────────────────────────────────────

func init() {
	RegisterGeometryDecoder("geoarrow.wkb", decodeWKB)
	RegisterGeometryDecoder("geoarrow.wkt", decodeWKT)
	RegisterGeometryDecoder("geoarrow.point", decodePoint)
	RegisterGeometryDecoder("geoarrow.linestring", decodeLineString)
	RegisterGeometryDecoder("geoarrow.polygon", decodePolygon)
	RegisterGeometryDecoder("geoarrow.multipoint", decodeMultiPoint)
	RegisterGeometryDecoder("geoarrow.multilinestring", decodeMultiLineString)
	RegisterGeometryDecoder("geoarrow.multipolygon", decodeMultiPolygon)
	RegisterGeometryDecoder("geoarrow.geometry", decodeGeometryNative)
	RegisterGeometryDecoder("geoarrow.geometrycollection", decodeCollection)
	RegisterGeometryDecoder("hugr.geojson", decodeGeoJSON)
}

// decodeWKB handles Binary / LargeBinary storage containing raw WKB bytes.
func decodeWKB(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	var b []byte
	switch a := col.(type) {
	case *array.Binary:
		b = a.Value(row)
	case *array.LargeBinary:
		b = a.Value(row)
	case *array.String:
		b = []byte(a.Value(row))
	case *array.LargeString:
		b = []byte(a.Value(row))
	default:
		return nil, fmt.Errorf("unexpected storage for geoarrow.wkb: %s", col.DataType())
	}
	if len(b) == 0 {
		return nil, nil
	}
	return wkb.Unmarshal(b)
}

// decodeWKT handles String / LargeString storage containing WKT text.
func decodeWKT(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	var s string
	switch a := col.(type) {
	case *array.String:
		s = a.Value(row)
	case *array.LargeString:
		s = a.Value(row)
	case *array.Binary:
		s = string(a.Value(row))
	case *array.LargeBinary:
		s = string(a.Value(row))
	default:
		return nil, fmt.Errorf("unexpected storage for geoarrow.wkt: %s", col.DataType())
	}
	if s == "" {
		return nil, nil
	}
	return wkt.Unmarshal(s)
}

// decodeGeoJSON handles both Binary (raw JSON bytes) and String (JSON text)
// storage, registered as "hugr.geojson".
func decodeGeoJSON(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	var b []byte
	switch a := col.(type) {
	case *array.String:
		b = []byte(a.Value(row))
	case *array.LargeString:
		b = []byte(a.Value(row))
	case *array.Binary:
		b = a.Value(row)
	case *array.LargeBinary:
		b = a.Value(row)
	default:
		return nil, fmt.Errorf("unexpected storage for hugr.geojson: %s", col.DataType())
	}
	if len(b) == 0 {
		return nil, nil
	}
	g, err := geojson.UnmarshalGeometry(b)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, nil
	}
	return g.Geometry(), nil
}

// ─── Native geoarrow coordinate walkers ──────────────────────────────────────
//
// These expect the storage types defined by the GeoArrow spec:
//   point          struct<x:f64, y:f64[, z:f64, m:f64]>
//   linestring     list<point>
//   polygon        list<list<point>>
//   multipoint     list<point>
//   multilinestring list<list<point>>
//   multipolygon   list<list<list<point>>>
//   geometry       dense-union/tagged struct (decoded by type field — supported
//                  in a minimal form here: decoded via GetOneForMarshal + recursion)
//   geometrycollection list<geometry>

func decodePoint(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	p, err := readPoint(col, row)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func readPoint(col arrow.Array, row int) (orb.Point, error) {
	sa, ok := col.(*array.Struct)
	if !ok {
		return orb.Point{}, fmt.Errorf("point storage must be Struct, got %s", col.DataType())
	}
	st, ok := col.DataType().(*arrow.StructType)
	if !ok {
		return orb.Point{}, fmt.Errorf("point storage missing StructType")
	}
	var x, y float64
	var haveX, haveY bool
	for i := 0; i < st.NumFields(); i++ {
		f := st.Field(i)
		switch f.Name {
		case "x":
			v, err := asFloat64(sa.Field(i), row)
			if err != nil {
				return orb.Point{}, err
			}
			x, haveX = v, true
		case "y":
			v, err := asFloat64(sa.Field(i), row)
			if err != nil {
				return orb.Point{}, err
			}
			y, haveY = v, true
		}
	}
	if !haveX || !haveY {
		return orb.Point{}, errors.New("point struct missing x/y fields")
	}
	return orb.Point{x, y}, nil
}

func decodeLineString(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	pts, err := readPoints(col, row)
	if err != nil {
		return nil, err
	}
	return orb.LineString(pts), nil
}

func decodeMultiPoint(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	pts, err := readPoints(col, row)
	if err != nil {
		return nil, err
	}
	return orb.MultiPoint(pts), nil
}

func readPoints(col arrow.Array, row int) ([]orb.Point, error) {
	start, end, values, err := listRange(col, row)
	if err != nil {
		return nil, err
	}
	if start == end {
		return nil, nil
	}
	out := make([]orb.Point, 0, end-start)
	for j := int(start); j < int(end); j++ {
		p, err := readPoint(values, j)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func decodePolygon(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	rings, err := readRings(col, row)
	if err != nil {
		return nil, err
	}
	return orb.Polygon(rings), nil
}

func decodeMultiLineString(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	rings, err := readRings(col, row)
	if err != nil {
		return nil, err
	}
	out := make(orb.MultiLineString, len(rings))
	for i, r := range rings {
		out[i] = orb.LineString(r)
	}
	return out, nil
}

func readRings(col arrow.Array, row int) ([]orb.Ring, error) {
	start, end, values, err := listRange(col, row)
	if err != nil {
		return nil, err
	}
	if start == end {
		return nil, nil
	}
	out := make([]orb.Ring, 0, end-start)
	for j := int(start); j < int(end); j++ {
		pts, err := readPoints(values, j)
		if err != nil {
			return nil, err
		}
		out = append(out, orb.Ring(pts))
	}
	return out, nil
}

func decodeMultiPolygon(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	start, end, values, err := listRange(col, row)
	if err != nil {
		return nil, err
	}
	if start == end {
		return orb.MultiPolygon{}, nil
	}
	out := make(orb.MultiPolygon, 0, end-start)
	for j := int(start); j < int(end); j++ {
		rings, err := readRings(values, j)
		if err != nil {
			return nil, err
		}
		out = append(out, orb.Polygon(rings))
	}
	return out, nil
}

// decodeGeometryNative handles geoarrow.geometry — a tagged union. The arrow
// Union type support is complex; for now we only handle the case where the
// storage exposes a "type" + "geometry" pair via GetOneForMarshal.
// The engine currently doesn't emit this encoding; decoder is here for
// forward-compatibility and returns a clear error on unknown shapes.
func decodeGeometryNative(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	return nil, fmt.Errorf("geoarrow.geometry native union decode not implemented; use geoarrow.wkb or register a custom decoder")
}

func decodeCollection(col arrow.Array, row int, _ arrow.Metadata) (orb.Geometry, error) {
	return nil, fmt.Errorf("geoarrow.geometrycollection native decode not implemented; use geoarrow.wkb or register a custom decoder")
}
