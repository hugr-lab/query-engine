package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// convertFunc writes the value at (col, row) into dst.
type convertFunc func(col arrow.Array, row int, dst reflect.Value) error

// fieldMapping binds a destination struct field to an Arrow column.
type fieldMapping struct {
	colIdx    int
	fieldPath []int       // reflect.FieldByIndex
	convert   convertFunc // chosen based on (Arrow column type, Go field type) at plan-build time
}

type structPlan struct {
	fields []fieldMapping
}

// buildStructPlan walks the struct type, resolves `json` tags, matches
// Arrow schema columns, and builds per-field convert closures.
// Missing columns → field skipped (matches encoding/json).
// Unexported / untagged fields → skipped (same as encoding/json when tag is not present;
// we require a json tag to avoid ambiguity with non-GraphQL fields).
func buildStructPlan(t reflect.Type, schema *arrow.Schema) (*structPlan, error) {
	if schema == nil {
		return nil, errors.New("types: nil schema")
	}
	var fields []fieldMapping
	if err := collectFields(t, nil, schema, &fields); err != nil {
		return nil, err
	}
	return &structPlan{fields: fields}, nil
}

func collectFields(t reflect.Type, prefix []int, schema *arrow.Schema, out *[]fieldMapping) error {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		path := append(append([]int{}, prefix...), i)
		name := jsonFieldName(f)
		if name == "-" {
			continue
		}
		// Embedded anonymous struct without json tag: flatten its fields.
		if f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct {
			if err := collectFields(f.Type, path, schema, out); err != nil {
				return err
			}
			continue
		}
		if name == "" {
			// No json tag: skip (explicit opt-in policy, matches our call-site convention).
			continue
		}
		colIdx := -1
		fieldList := schema.Fields()
		for j, af := range fieldList {
			if af.Name == name {
				colIdx = j
				break
			}
		}
		if colIdx < 0 {
			continue
		}
		conv, err := buildConvertFunc(fieldList[colIdx], f.Type)
		if err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
		*out = append(*out, fieldMapping{
			colIdx:    colIdx,
			fieldPath: path,
			convert:   conv,
		})
	}
	return nil
}

// jsonFieldName returns the json tag name or empty string if absent / "-".
func jsonFieldName(f reflect.StructField) string {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return ""
	}
	if tag == "-" {
		return "-"
	}
	if idx := strings.IndexByte(tag, ','); idx >= 0 {
		return tag[:idx]
	}
	return tag
}

// buildConvertFunc picks the right convert closure for the given Arrow field
// and Go destination type. Geometry dispatch is routed here as well.
func buildConvertFunc(afield arrow.Field, dstType reflect.Type) (convertFunc, error) {
	// Dereference a *T destination once; converters accept the pointed-to reflect.Value.
	if dstType.Kind() == reflect.Pointer {
		inner := dstType.Elem()
		innerConv, err := buildConvertFunc(afield, inner)
		if err != nil {
			return nil, err
		}
		return func(col arrow.Array, row int, dst reflect.Value) error {
			if col.IsNull(row) {
				dst.Set(reflect.Zero(dst.Type()))
				return nil
			}
			if dst.IsNil() {
				dst.Set(reflect.New(inner))
			}
			return innerConv(col, row, dst.Elem())
		}, nil
	}

	// Geometry dispatch: if destination type wants orb.Geometry (interface or
	// concrete subtype), route through the geometry decoder registry.
	if isGeometryDest(dstType) {
		extName := fieldGeometryName(afield)
		if extName != "" {
			return geometryConvertFunc(extName, afield.Metadata, dstType)
		}
		// No extension name on this column — fall through to default converter
		// so the field receives whatever the raw Arrow value is.
	}

	return buildPlainConvertFunc(afield, dstType)
}

func buildPlainConvertFunc(afield arrow.Field, dstType reflect.Type) (convertFunc, error) {
	// Special destination-typed cases first (more specific than kind-based matching).
	switch dstType {
	case reflect.TypeOf(time.Time{}):
		return timeConvertFunc(afield), nil
	case reflect.TypeOf(DateTime{}):
		return dateTimeConvertFunc(afield), nil
	case reflect.TypeOf(time.Duration(0)):
		return durationConvertFunc(afield), nil
	case reflect.TypeOf([]byte(nil)):
		return bytesConvertFunc(), nil
	}

	switch dstType.Kind() {
	case reflect.Bool:
		return boolConvertFunc(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intConvertFunc(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uintConvertFunc(), nil
	case reflect.Float32, reflect.Float64:
		return floatConvertFunc(), nil
	case reflect.String:
		return stringConvertFunc(), nil
	case reflect.Slice:
		return sliceConvertFunc(afield, dstType)
	case reflect.Map:
		return mapConvertFunc(afield, dstType)
	case reflect.Struct:
		return structConvertFunc(afield, dstType)
	case reflect.Interface:
		// any
		if dstType.NumMethod() == 0 {
			return anyConvertFunc(afield), nil
		}
	}
	return nil, fmt.Errorf("unsupported dest type %s for Arrow %s", dstType, afield.Type)
}

// convertIntoValue is the public entry for one-shot conversion (no plan cache).
// Used by scanIntoMap when the map element type is non-interface.
func convertIntoValue(col arrow.Array, row int, afield arrow.Field, dst reflect.Value) error {
	if col.IsNull(row) {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	conv, err := buildConvertFunc(afield, dst.Type())
	if err != nil {
		return err
	}
	return conv(col, row, dst)
}

// ─── Primitive converters ────────────────────────────────────────────────────

func boolConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		a, ok := col.(*array.Boolean)
		if !ok {
			return fmt.Errorf("cannot scan Arrow %s into bool", col.DataType())
		}
		dst.SetBool(a.Value(row))
		return nil
	}
}

func intConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		v, err := asInt64(col, row)
		if err != nil {
			return err
		}
		if dst.OverflowInt(v) {
			return fmt.Errorf("value %d overflows %s", v, dst.Type())
		}
		dst.SetInt(v)
		return nil
	}
}

func uintConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		v, err := asUint64(col, row)
		if err != nil {
			return err
		}
		if dst.OverflowUint(v) {
			return fmt.Errorf("value %d overflows %s", v, dst.Type())
		}
		dst.SetUint(v)
		return nil
	}
}

func floatConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		v, err := asFloat64(col, row)
		if err != nil {
			return err
		}
		dst.SetFloat(v)
		return nil
	}
}

func stringConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.SetString("")
			return nil
		}
		switch a := col.(type) {
		case *array.String:
			dst.SetString(a.Value(row))
		case *array.LargeString:
			dst.SetString(a.Value(row))
		case *array.Binary:
			dst.SetString(string(a.Value(row)))
		case *array.LargeBinary:
			dst.SetString(string(a.Value(row)))
		case array.ExtensionArray:
			return stringConvertFunc()(a.Storage(), row, dst)
		default:
			return fmt.Errorf("cannot scan Arrow %s into string", col.DataType())
		}
		return nil
	}
}

func bytesConvertFunc() convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.SetBytes(nil)
			return nil
		}
		switch a := col.(type) {
		case *array.Binary:
			v := a.Value(row)
			cp := make([]byte, len(v))
			copy(cp, v)
			dst.SetBytes(cp)
		case *array.LargeBinary:
			v := a.Value(row)
			cp := make([]byte, len(v))
			copy(cp, v)
			dst.SetBytes(cp)
		case *array.String:
			dst.SetBytes([]byte(a.Value(row)))
		case *array.LargeString:
			dst.SetBytes([]byte(a.Value(row)))
		case array.ExtensionArray:
			return bytesConvertFunc()(a.Storage(), row, dst)
		default:
			return fmt.Errorf("cannot scan Arrow %s into []byte", col.DataType())
		}
		return nil
	}
}

// ─── Temporal converters ─────────────────────────────────────────────────────

func timeConvertFunc(afield arrow.Field) convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		t, err := arrowToTime(col, row, afield.Type)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(t))
		return nil
	}
}

func dateTimeConvertFunc(afield arrow.Field) convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		t, err := arrowToTimeNaive(col, row, afield.Type)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(DateTime(t)))
		return nil
	}
}

func durationConvertFunc(afield arrow.Field) convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		d, err := arrowToDuration(col, row, afield.Type)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(d))
		return nil
	}
}

// arrowToTime honours TimestampType.Unit + TimeZone; defaults to UTC when tz is empty.
func arrowToTime(col arrow.Array, row int, dt arrow.DataType) (time.Time, error) {
	switch a := col.(type) {
	case *array.Timestamp:
		tt := dt.(*arrow.TimestampType)
		ns := timestampToNanos(int64(a.Value(row)), tt.Unit)
		loc := time.UTC
		if tt.TimeZone != "" {
			if l, err := time.LoadLocation(tt.TimeZone); err == nil {
				loc = l
			}
			// unknown tz → silently fall back to UTC (per spec edge case)
		}
		return time.Unix(0, ns).In(loc), nil
	case *array.Date32:
		// Date32 = days since Unix epoch
		return time.Unix(int64(a.Value(row))*86400, 0).UTC(), nil
	case *array.Date64:
		// Date64 = milliseconds since epoch, midnight UTC
		return time.Unix(0, int64(a.Value(row))*int64(time.Millisecond)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot scan Arrow %s into time.Time", dt)
}

// arrowToTimeNaive ignores timezone metadata (used for DateTime destination).
func arrowToTimeNaive(col arrow.Array, row int, dt arrow.DataType) (time.Time, error) {
	switch a := col.(type) {
	case *array.Timestamp:
		tt := dt.(*arrow.TimestampType)
		ns := timestampToNanos(int64(a.Value(row)), tt.Unit)
		return time.Unix(0, ns).UTC(), nil
	case *array.Date32:
		return time.Unix(int64(a.Value(row))*86400, 0).UTC(), nil
	case *array.Date64:
		return time.Unix(0, int64(a.Value(row))*int64(time.Millisecond)).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("cannot scan Arrow %s into naive time", dt)
}

func timestampToNanos(raw int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Nanosecond:
		return raw
	case arrow.Microsecond:
		return raw * 1_000
	case arrow.Millisecond:
		return raw * 1_000_000
	case arrow.Second:
		return raw * 1_000_000_000
	}
	return raw
}

func arrowToDuration(col arrow.Array, row int, dt arrow.DataType) (time.Duration, error) {
	switch a := col.(type) {
	case *array.Time32:
		tt := dt.(*arrow.Time32Type)
		return time.Duration(timeUnitToNanos(int64(a.Value(row)), tt.Unit)), nil
	case *array.Time64:
		tt := dt.(*arrow.Time64Type)
		return time.Duration(timeUnitToNanos(int64(a.Value(row)), tt.Unit)), nil
	case *array.MonthInterval:
		return 0, fmt.Errorf("MonthInterval cannot be scanned into time.Duration")
	}
	// arrow-go's Duration array: fall back to int64 scaled by unit if available via reflection.
	if a, ok := col.(interface {
		Value(int) arrow.Duration
	}); ok {
		if dur, ok2 := dt.(*arrow.DurationType); ok2 {
			return time.Duration(timeUnitToNanos(int64(a.Value(row)), dur.Unit)), nil
		}
	}
	return 0, fmt.Errorf("cannot scan Arrow %s into time.Duration", dt)
}

func timeUnitToNanos(raw int64, unit arrow.TimeUnit) int64 {
	return timestampToNanos(raw, unit)
}

// ─── Numeric helpers ─────────────────────────────────────────────────────────

func asInt64(col arrow.Array, row int) (int64, error) {
	switch a := col.(type) {
	case *array.Int8:
		return int64(a.Value(row)), nil
	case *array.Int16:
		return int64(a.Value(row)), nil
	case *array.Int32:
		return int64(a.Value(row)), nil
	case *array.Int64:
		return a.Value(row), nil
	case *array.Uint8:
		return int64(a.Value(row)), nil
	case *array.Uint16:
		return int64(a.Value(row)), nil
	case *array.Uint32:
		return int64(a.Value(row)), nil
	case *array.Uint64:
		return int64(a.Value(row)), nil
	}
	return 0, fmt.Errorf("cannot scan Arrow %s into int", col.DataType())
}

func asUint64(col arrow.Array, row int) (uint64, error) {
	switch a := col.(type) {
	case *array.Uint8:
		return uint64(a.Value(row)), nil
	case *array.Uint16:
		return uint64(a.Value(row)), nil
	case *array.Uint32:
		return uint64(a.Value(row)), nil
	case *array.Uint64:
		return a.Value(row), nil
	case *array.Int8:
		return uint64(a.Value(row)), nil
	case *array.Int16:
		return uint64(a.Value(row)), nil
	case *array.Int32:
		return uint64(a.Value(row)), nil
	case *array.Int64:
		return uint64(a.Value(row)), nil
	}
	return 0, fmt.Errorf("cannot scan Arrow %s into uint", col.DataType())
}

func asFloat64(col arrow.Array, row int) (float64, error) {
	switch a := col.(type) {
	case *array.Float32:
		return float64(a.Value(row)), nil
	case *array.Float64:
		return a.Value(row), nil
	}
	if v, err := asInt64(col, row); err == nil {
		return float64(v), nil
	}
	return 0, fmt.Errorf("cannot scan Arrow %s into float", col.DataType())
}

// ─── Complex converters ──────────────────────────────────────────────────────

func sliceConvertFunc(afield arrow.Field, dstType reflect.Type) (convertFunc, error) {
	elem := dstType.Elem()
	// Build element convertFunc based on the list's value type.
	var valueField arrow.Field
	switch lt := afield.Type.(type) {
	case *arrow.ListType:
		valueField = arrow.Field{Name: "item", Type: lt.ElemField().Type, Nullable: lt.ElemField().Nullable}
	case *arrow.LargeListType:
		valueField = arrow.Field{Name: "item", Type: lt.ElemField().Type, Nullable: lt.ElemField().Nullable}
	case *arrow.FixedSizeListType:
		valueField = arrow.Field{Name: "item", Type: lt.ElemField().Type, Nullable: lt.ElemField().Nullable}
	default:
		// not a list — e.g. []byte handled elsewhere, []any default via raw
		return anyConvertFunc(afield), nil
	}
	innerConv, err := buildConvertFunc(valueField, elem)
	if err != nil {
		return nil, err
	}
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		start, end, values, err := listRange(col, row)
		if err != nil {
			return err
		}
		out := reflect.MakeSlice(dst.Type(), int(end-start), int(end-start))
		for j := int(start); j < int(end); j++ {
			if err := innerConv(values, j, out.Index(j-int(start))); err != nil {
				return err
			}
		}
		dst.Set(out)
		return nil
	}, nil
}

func listRange(col arrow.Array, row int) (int64, int64, arrow.Array, error) {
	switch v := col.(type) {
	case *array.List:
		s, e := v.ValueOffsets(row)
		return s, e, v.ListValues(), nil
	case *array.LargeList:
		s, e := v.ValueOffsets(row)
		return s, e, v.ListValues(), nil
	case *array.FixedSizeList:
		s, e := v.ValueOffsets(row)
		return s, e, v.ListValues(), nil
	}
	return 0, 0, nil, fmt.Errorf("not a list array: %s", col.DataType())
}

func mapConvertFunc(afield arrow.Field, dstType reflect.Type) (convertFunc, error) {
	if dstType.Key().Kind() != reflect.String {
		return nil, fmt.Errorf("map key must be string for Arrow map scan, got %s", dstType.Key().Kind())
	}
	mt, ok := afield.Type.(*arrow.MapType)
	if !ok {
		return anyConvertFunc(afield), nil
	}
	keyField := arrow.Field{Name: "key", Type: mt.KeyField().Type}
	valField := arrow.Field{Name: "value", Type: mt.ItemField().Type, Nullable: mt.ItemField().Nullable}
	keyConv, err := buildConvertFunc(keyField, reflect.TypeOf(""))
	if err != nil {
		return nil, err
	}
	valConv, err := buildConvertFunc(valField, dstType.Elem())
	if err != nil {
		return nil, err
	}

	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		ma, ok := col.(*array.Map)
		if !ok {
			return fmt.Errorf("expected map array, got %s", col.DataType())
		}
		s, e := ma.ValueOffsets(row)
		out := reflect.MakeMapWithSize(dst.Type(), int(e-s))
		keys := ma.Keys()
		items := ma.Items()
		for j := int(s); j < int(e); j++ {
			k := reflect.New(reflect.TypeOf("")).Elem()
			if err := keyConv(keys, j, k); err != nil {
				return err
			}
			v := reflect.New(dst.Type().Elem()).Elem()
			if err := valConv(items, j, v); err != nil {
				return err
			}
			out.SetMapIndex(k, v)
		}
		dst.Set(out)
		return nil
	}, nil
}

func structConvertFunc(afield arrow.Field, dstType reflect.Type) (convertFunc, error) {
	st, ok := afield.Type.(*arrow.StructType)
	if !ok {
		return nil, fmt.Errorf("cannot scan Arrow %s into struct %s", afield.Type, dstType)
	}
	// Build a mini-plan via a shared helper. Anonymous embedded struct fields
	// are flattened — same rule as collectFields uses at the top level, so
	// behaviour is consistent whether the destination struct is the cursor's
	// top-level dest or a list element / nested struct field.
	var mappings []structMapping
	if err := collectStructMappings(dstType, nil, st, &mappings); err != nil {
		return nil, err
	}

	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		sa, ok := col.(*array.Struct)
		if !ok {
			return fmt.Errorf("expected struct array, got %s", col.DataType())
		}
		for _, m := range mappings {
			if err := m.convert(sa.Field(m.childIdx), row, dst.FieldByIndex(m.fieldPath)); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

type structMapping struct {
	childIdx  int
	fieldPath []int
	convert   convertFunc
}

// collectStructMappings walks a Go struct type against an Arrow StructType,
// recursing into anonymous embedded Go struct fields so their children are
// matched against the same Arrow parent's children. Mirrors collectFields's
// flatten rule but for the nested (Arrow Struct → Go struct) conversion path.
func collectStructMappings(dstType reflect.Type, prefix []int, st *arrow.StructType, out *[]structMapping) error {
	for i := 0; i < dstType.NumField(); i++ {
		f := dstType.Field(i)
		if !f.IsExported() {
			continue
		}
		path := append(append([]int{}, prefix...), i)
		name := jsonFieldName(f)
		if name == "-" {
			continue
		}
		// Anonymous embedded struct without json tag: flatten.
		if f.Anonymous && name == "" && f.Type.Kind() == reflect.Struct {
			if err := collectStructMappings(f.Type, path, st, out); err != nil {
				return err
			}
			continue
		}
		if name == "" {
			continue
		}
		childIdx := -1
		for j := 0; j < st.NumFields(); j++ {
			if st.Field(j).Name == name {
				childIdx = j
				break
			}
		}
		if childIdx < 0 {
			continue
		}
		conv, err := buildConvertFunc(st.Field(childIdx), f.Type)
		if err != nil {
			return fmt.Errorf("field %s: %w", name, err)
		}
		*out = append(*out, structMapping{
			childIdx:  childIdx,
			fieldPath: path,
			convert:   conv,
		})
	}
	return nil
}

// anyConvertFunc writes whatever defaultConvertValue produces into an interface{}
// (or interface{}-compatible) destination.
func anyConvertFunc(afield arrow.Field) convertFunc {
	return func(col arrow.Array, row int, dst reflect.Value) error {
		if col.IsNull(row) {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		val := defaultConvertValue(col, row, afield)
		if val == nil {
			dst.Set(reflect.Zero(dst.Type()))
			return nil
		}
		rv := reflect.ValueOf(val)
		if !rv.Type().AssignableTo(dst.Type()) {
			return fmt.Errorf("cannot assign %s to %s", rv.Type(), dst.Type())
		}
		dst.Set(rv)
		return nil
	}
}

// ─── Raw-default converter (used for any/map[string]any/[]any) ───────────────

// defaultConvertValue returns the "as-is" Go value for an Arrow cell.
// Never decodes geometry — raw storage value is returned. Callers who want
// decoded geometry use a geometry-typed struct field.
func defaultConvertValue(col arrow.Array, row int, afield arrow.Field) any {
	if col.IsNull(row) {
		return nil
	}
	// Unwrap extension arrays to their storage unless they carry specific typed handling.
	// For the default (raw) path we want the underlying storage value regardless of
	// extension name (per FR-012).
	if ext, ok := col.(array.ExtensionArray); ok {
		return defaultConvertValue(ext.Storage(), row, afield)
	}
	switch a := col.(type) {
	case *array.Boolean:
		return a.Value(row)
	case *array.Int8:
		return a.Value(row)
	case *array.Int16:
		return a.Value(row)
	case *array.Int32:
		return a.Value(row)
	case *array.Int64:
		return a.Value(row)
	case *array.Uint8:
		return a.Value(row)
	case *array.Uint16:
		return a.Value(row)
	case *array.Uint32:
		return a.Value(row)
	case *array.Uint64:
		return a.Value(row)
	case *array.Float32:
		return a.Value(row)
	case *array.Float64:
		return a.Value(row)
	case *array.String:
		return a.Value(row)
	case *array.LargeString:
		return a.Value(row)
	case *array.Binary:
		v := a.Value(row)
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp
	case *array.LargeBinary:
		v := a.Value(row)
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp
	case *array.Date32:
		return time.Unix(int64(a.Value(row))*86400, 0).UTC()
	case *array.Date64:
		return time.Unix(0, int64(a.Value(row))*int64(time.Millisecond)).UTC()
	case *array.Time32:
		tt := afield.Type.(*arrow.Time32Type)
		return time.Duration(timeUnitToNanos(int64(a.Value(row)), tt.Unit))
	case *array.Time64:
		tt := afield.Type.(*arrow.Time64Type)
		return time.Duration(timeUnitToNanos(int64(a.Value(row)), tt.Unit))
	case *array.Timestamp:
		tt := afield.Type.(*arrow.TimestampType)
		ns := timestampToNanos(int64(a.Value(row)), tt.Unit)
		loc := time.UTC
		if tt.TimeZone != "" {
			if l, err := time.LoadLocation(tt.TimeZone); err == nil {
				loc = l
			}
		}
		return time.Unix(0, ns).In(loc)
	case *array.List:
		s, e := a.ValueOffsets(row)
		if s == e {
			return []any{}
		}
		vals := a.ListValues()
		child := arrow.Field{Name: "item", Type: vals.DataType()}
		out := make([]any, e-s)
		for j := int(s); j < int(e); j++ {
			out[j-int(s)] = defaultConvertValue(vals, j, child)
		}
		return out
	case *array.LargeList:
		s, e := a.ValueOffsets(row)
		if s == e {
			return []any{}
		}
		vals := a.ListValues()
		child := arrow.Field{Name: "item", Type: vals.DataType()}
		out := make([]any, e-s)
		for j := int(s); j < int(e); j++ {
			out[j-int(s)] = defaultConvertValue(vals, j, child)
		}
		return out
	case *array.FixedSizeList:
		s, e := a.ValueOffsets(row)
		if s == e {
			return []any{}
		}
		vals := a.ListValues()
		child := arrow.Field{Name: "item", Type: vals.DataType()}
		out := make([]any, e-s)
		for j := int(s); j < int(e); j++ {
			out[j-int(s)] = defaultConvertValue(vals, j, child)
		}
		return out
	case *array.Map:
		s, e := a.ValueOffsets(row)
		m := make(map[string]any, e-s)
		keys := a.Keys()
		items := a.Items()
		keyField := arrow.Field{Name: "key", Type: keys.DataType()}
		valField := arrow.Field{Name: "value", Type: items.DataType()}
		for j := int(s); j < int(e); j++ {
			k := defaultConvertValue(keys, j, keyField)
			v := defaultConvertValue(items, j, valField)
			ks, _ := k.(string)
			if ks == "" {
				ks = fmt.Sprintf("%v", k)
			}
			m[ks] = v
		}
		return m
	case *array.Struct:
		st := afield.Type.(*arrow.StructType)
		m := make(map[string]any, st.NumFields())
		for j := 0; j < st.NumFields(); j++ {
			child := st.Field(j)
			m[child.Name] = defaultConvertValue(a.Field(j), row, child)
		}
		return m
	case *array.Null:
		return nil
	case *array.Dictionary:
		dict := a.Dictionary()
		idx := a.GetValueIndex(row)
		return defaultConvertValue(dict, idx, afield)
	}
	// Unknown type: fallback to JSON marshal via GetOneForMarshal and decode.
	raw := col.GetOneForMarshal(row)
	if raw == nil {
		return nil
	}
	if b, err := json.Marshal(raw); err == nil {
		var out any
		if json.Unmarshal(b, &out) == nil {
			return out
		}
	}
	return raw
}
