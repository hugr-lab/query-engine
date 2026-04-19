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

// GeometryDecoder converts an Arrow cell into an orb.Geometry. It is
// registered against an Arrow extension name via RegisterGeometryDecoder.
// The scanner invokes a decoder when the destination Go type is
// orb.Geometry (or a concrete subtype) and the column carries that
// extension name — either via an ExtensionArray's ExtensionType().ExtensionName()
// or via an ARROW:extension:name entry in the column's field metadata.
//
// Built-in decoders are registered for: geoarrow.wkb, geoarrow.wkt,
// geoarrow.point, geoarrow.linestring, geoarrow.polygon, geoarrow.multipoint,
// geoarrow.multilinestring, geoarrow.multipolygon, hugr.geojson. The
// native union forms geoarrow.geometry and geoarrow.geometrycollection are
// registered as stub decoders that error clearly — override them if your
// data actually carries those encodings.
//
// Return (nil, nil) for a cell that is logically null; the scanner zeroes
// the destination in that case.
type GeometryDecoder func(arr arrow.Array, row int, fieldMeta arrow.Metadata) (orb.Geometry, error)

var (
	geometryDecoders   = make(map[string]GeometryDecoder)
	geometryDecodersMu sync.RWMutex
)

// RegisterGeometryDecoder binds an Arrow extension name to a decoder. If the
// name is already registered, the new decoder replaces the old one. Passing
// a nil fn deletes the entry. Safe to call from init or at runtime;
// thread-safe via an internal RWMutex.
//
// Example — register a decoder for a custom WKT+EWKT encoding:
//
//	func init() {
//	    types.RegisterGeometryDecoder("myorg.geo.ewkt", func(
//	        arr arrow.Array, row int, meta arrow.Metadata,
//	    ) (orb.Geometry, error) {
//	        s := arr.(*array.String).Value(row)
//	        return myparser.ParseEWKT(s)
//	    })
//	}
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

// Reflect-type constants for geometry destination matching. Precomputed to
// avoid per-plan-build reflect.TypeOf calls and to make the recognised-type
// list explicit (vs. string-comparing PkgPath, which would break silently if
// orb were renamed or vendored).
var (
	orbGeometryType     = reflect.TypeOf((*orb.Geometry)(nil)).Elem()
	geojsonGeometryType = reflect.TypeOf(geojson.Geometry{})

	orbPointType           = reflect.TypeOf(orb.Point{})
	orbMultiPointType      = reflect.TypeOf(orb.MultiPoint{})
	orbLineStringType      = reflect.TypeOf(orb.LineString{})
	orbMultiLineStringType = reflect.TypeOf(orb.MultiLineString{})
	orbRingType            = reflect.TypeOf(orb.Ring{})
	orbPolygonType         = reflect.TypeOf(orb.Polygon{})
	orbMultiPolygonType    = reflect.TypeOf(orb.MultiPolygon{})
	orbCollectionType      = reflect.TypeOf(orb.Collection{})
	orbBoundType           = reflect.TypeOf(orb.Bound{})
)

// isGeometryDest returns true when the Go destination type is orb.Geometry,
// a concrete orb subtype (Point, LineString, Polygon, Multi*, Ring, Bound,
// Collection), or geojson.Geometry. Called with the already-dereferenced
// type (pointer destinations are unwrapped in buildConvertFunc), so both
// `geojson.Geometry` and `*geojson.Geometry` fields are covered.
func isGeometryDest(t reflect.Type) bool {
	switch t {
	case orbGeometryType, geojsonGeometryType,
		orbPointType, orbMultiPointType,
		orbLineStringType, orbMultiLineStringType,
		orbRingType, orbPolygonType, orbMultiPolygonType,
		orbCollectionType, orbBoundType:
		return true
	}
	return false
}

// geometryConvertFunc builds a convertFunc that decodes the cell via the
// decoder registered for extName, then assigns the result to dst. The
// decoder is looked up once at plan-build time so the hot path is lock-free;
// callers who re-register decoders after building a plan get the snapshotted
// behaviour for existing cursors. If the destination is a concrete orb
// subtype, a type check is performed. The field's metadata is closed over
// so decoders have access to SRID / encoding hints.
func geometryConvertFunc(extName string, fieldMeta arrow.Metadata, dstType reflect.Type) (convertFunc, error) {
	dec, ok := lookupGeometryDecoder(extName)
	if !ok {
		return nil, fmt.Errorf("%w: no decoder registered for extension %q", ErrGeometryDecode, extName)
	}
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
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
		return assignGeometry(geom, dstType, dst)
	}, nil
}

// assignGeometry writes a decoded orb.Geometry into dst, with the type
// adaptation rules the scanner supports. dst is the already-dereferenced
// reflect.Value (pointer unwrapping done in buildConvertFunc).
//
// Supported destination types:
//   - orb.Geometry interface              → direct assignment.
//   - concrete orb subtype (orb.Point…)   → type-check the decoded value.
//   - geojson.Geometry                    → wrap via geojson.NewGeometry so
//                                            the same struct definition works
//                                            for ScanTable and ScanObject.
func assignGeometry(geom orb.Geometry, dstType reflect.Type, dst reflect.Value) error {
	if dstType == orbGeometryType {
		dst.Set(reflect.ValueOf(geom))
		return nil
	}
	if dstType == geojsonGeometryType {
		if wrapped := geojson.NewGeometry(geom); wrapped != nil {
			dst.Set(reflect.ValueOf(*wrapped))
		}
		return nil
	}
	// Concrete orb subtype — type-check the decoded value.
	geomValue := reflect.ValueOf(geom)
	if geomValue.Type() == dstType {
		dst.Set(geomValue)
		return nil
	}
	if geomValue.Kind() == reflect.Pointer && geomValue.Elem().Type() == dstType {
		dst.Set(geomValue.Elem())
		return nil
	}
	return fmt.Errorf("%w: decoded %T is not assignable to %s", ErrGeometryDecode, geom, dstType)
}

// autoStringGeometryConvertFunc handles the case where a String / LargeString
// column carries geometry but no ARROW:extension:name metadata tags it.
// Observed on engine-emitted nested paths (e.g. tf.digital_twin.roads.parts[].geom).
// Strategy: peek at the first non-space byte of each row — '{' → GeoJSON,
// otherwise WKT. Both decoders are snapshotted at plan-build time so the
// hot path is lock-free.
func autoStringGeometryConvertFunc(fieldMeta arrow.Metadata, dstType reflect.Type) convertFunc {
	geojsonDec, _ := lookupGeometryDecoder("hugr.geojson")
	wktDec, _ := lookupGeometryDecoder("geoarrow.wkt")
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
		var dec GeometryDecoder
		if looksLikeGeoJSON(s) {
			dec = geojsonDec
		} else {
			dec = wktDec
		}
		if dec == nil {
			return fmt.Errorf("%w: no decoder available for auto-detected geometry string", ErrGeometryDecode)
		}
		geom, err := dec(col, row, fieldMeta)
		if err != nil {
			return fmt.Errorf("%w: %s", ErrGeometryDecode, err.Error())
		}
		if geom == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		return assignGeometry(geom, dstType, dst)
	}
}

// looksLikeGeoJSON returns true if s starts (after whitespace) with '{' — the
// only JSON-object opener. Empty strings count as GeoJSON too: the GeoJSON
// decoder returns (nil, nil) on empty bytes, which the caller treats as a
// null geometry — the same behaviour a blank WKT string would get.
func looksLikeGeoJSON(s string) bool {
	for _, r := range s {
		switch r {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return true
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
