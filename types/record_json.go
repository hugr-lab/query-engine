package types

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/paulmach/orb/geojson"
)

// emitCell writes the JSON representation of col[row] to w. The output
// shape is compatible with what DuckDB's (_data::JSON)::TEXT produces
// today, with these intentional improvements:
//
//   - Timestamps are emitted as RFC3339Nano regardless of the source
//     unit — more precise and natively parseable by stdlib JSON.
//   - Geometry extension columns (geoarrow.wkb etc.) decode to orb.Geometry
//     and emit as inline GeoJSON objects (DuckDB used to do the same trick
//     via the wrap SQL).
//   - Nested utf8 columns annotated in geomInfo as "GeoJSONString" have
//     their raw GeoJSON text embedded unquoted (as an object).
//
// path is the dotted field path of the current cell within its parent
// record batch ("" at the top level, "objects.object.geom" three struct
// levels deep). geomInfo is the per-field metadata attached to the table
// by the planner; nil means no metadata and we fall back to default
// rendering.
func emitCell(w io.Writer, col arrow.Array, row int, path string, geomInfo map[string]GeometryInfo) error {
	if col == nil {
		_, err := w.Write(literalNull)
		return err
	}
	if col.IsNull(row) {
		_, err := w.Write(literalNull)
		return err
	}

	// GeometryInfo lookup first — it's the authoritative source for
	// per-field format hints (populated by the planner from the GraphQL
	// AST). If this field is flagged as WKB, decode the column via the
	// geoarrow.wkb decoder regardless of whether the Arrow extension tag
	// is present (DuckDB's postgres scanner, for example, doesn't always
	// tag geometry columns natively).
	if gi, ok := geomInfo[path]; ok && gi.Format == "WKB" {
		if dec, ok := lookupGeometryDecoder("geoarrow.wkb"); ok {
			return emitGeometryViaDecoder(w, dec, col, row)
		}
	}

	// Arrow extension arrays: next line of defence — decode via the
	// shared registry based on the declared extension name.
	if ext, ok := col.(array.ExtensionArray); ok {
		if dec, ok := lookupGeometryDecoder(ext.ExtensionType().ExtensionName()); ok {
			return emitGeometryViaDecoder(w, dec, ext.Storage(), row)
		}
		// Non-geometry extension → fall through to storage kind.
		return emitCell(w, ext.Storage(), row, path, geomInfo)
	}

	switch a := col.(type) {
	case *array.Timestamp:
		return emitTimestamp(w, a, row)
	case *array.Date32:
		return emitDate(w, int64(a.Value(row))*millisPerDay)
	case *array.Date64:
		return emitDate(w, int64(a.Value(row)))
	case *array.Time32:
		return emitTime(w, int64(a.Value(row)), a.DataType().(*arrow.Time32Type).Unit)
	case *array.Time64:
		return emitTime(w, int64(a.Value(row)), a.DataType().(*arrow.Time64Type).Unit)
	case *array.MonthInterval:
		return emitMonthInterval(w, int32(a.Value(row)))
	case *array.DayTimeInterval:
		v := a.Value(row)
		return emitDayTimeInterval(w, v.Days, v.Milliseconds)
	case *array.MonthDayNanoInterval:
		v := a.Value(row)
		return emitMonthDayNanoInterval(w, v.Months, v.Days, v.Nanoseconds)
	case *array.String:
		return emitString(w, a.Value(row), path, geomInfo)
	case *array.LargeString:
		return emitString(w, a.Value(row), path, geomInfo)
	case *array.Struct:
		return emitStruct(w, a, row, path, geomInfo)
	case *array.List:
		return emitList(w, a, row, path, geomInfo)
	case *array.LargeList:
		return emitLargeList(w, a, row, path, geomInfo)
	case *array.FixedSizeList:
		return emitFixedSizeList(w, a, row, path, geomInfo)
	case *array.Map:
		// Maps are rare in response shapes; fall through to stdlib.
		return json.NewEncoder(w).Encode(a.GetOneForMarshal(row))
	case *array.Decimal128:
		return emitDecimal128(w, a, row)
	case *array.Decimal256:
		return emitDecimal256(w, a, row)
	case *array.Float32:
		return emitFloat32(w, a.Value(row))
	case *array.Float64:
		return emitFloat(w, a.Value(row))
	}

	// Default — everything else (integers, floats, bool, binary) via Arrow's
	// own GetOneForMarshal. json.Encoder adds a trailing newline; strip it.
	v := col.GetOneForMarshal(row)
	return writeJSON(w, v)
}

var literalNull = []byte("null")

// emitGeometryViaDecoder decodes one cell via a pre-looked-up decoder and
// writes its GeoJSON-object representation to w.
func emitGeometryViaDecoder(w io.Writer, dec GeometryDecoder, col arrow.Array, row int) error {
	if col.IsNull(row) {
		_, err := w.Write(literalNull)
		return err
	}
	geom, err := dec(col, row, arrow.Metadata{})
	if err != nil {
		return fmt.Errorf("decode geometry: %w", err)
	}
	if geom == nil {
		_, err := w.Write(literalNull)
		return err
	}
	g := geojson.NewGeometry(geom)
	b, err := g.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal geometry: %w", err)
	}
	_, err = w.Write(b)
	return err
}

// writeJSON marshals v with stdlib and writes to w without the trailing
// newline that json.Encoder.Encode adds.
func writeJSON(w io.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// emitString handles plain utf8 columns. If geomInfo says this field is
// a GeoJSONString or JSONString, embed the value unquoted (the wrapped
// SQL path did the same via DuckDB's ::JSON cast). If the value doesn't
// look like a JSON value, fall back to emitting as a regular JSON
// string to avoid producing invalid output.
func emitString(w io.Writer, s string, path string, geomInfo map[string]GeometryInfo) error {
	if gi, ok := geomInfo[path]; ok && (gi.Format == "GeoJSONString" || gi.Format == "JSONString") {
		trimmed := strings.TrimSpace(s)
		if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed == "null") {
			_, err := io.WriteString(w, trimmed)
			return err
		}
		// Malformed — emit as regular JSON string.
	}
	return writeJSON(w, s)
}

// emitStruct writes a struct column as a JSON object. Each struct field
// contributes a key named after the field, with the cell value emitted
// recursively. `path` is extended per field so geomInfo lookups resolve
// for nested geometry.
func emitStruct(w io.Writer, a *array.Struct, row int, path string, geomInfo map[string]GeometryInfo) error {
	st := a.DataType().(*arrow.StructType)
	if _, err := w.Write([]byte("{")); err != nil {
		return err
	}
	for i := 0; i < a.NumField(); i++ {
		if i > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		name := st.Field(i).Name
		if err := writeJSON(w, name); err != nil {
			return err
		}
		if _, err := w.Write([]byte(":")); err != nil {
			return err
		}
		sub := joinGeomPath(path, name)
		if err := emitCell(w, a.Field(i), row, sub, geomInfo); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte("}"))
	return err
}

// emitList writes a list column as a JSON array. List nesting does not
// add a path segment — "objects.geom" means "geom inside each element
// of objects".
func emitList(w io.Writer, a *array.List, row int, path string, geomInfo map[string]GeometryInfo) error {
	start, end := a.ValueOffsets(row)
	return emitArrayRange(w, a.ListValues(), int(start), int(end), path, geomInfo)
}

func emitLargeList(w io.Writer, a *array.LargeList, row int, path string, geomInfo map[string]GeometryInfo) error {
	start, end := a.ValueOffsets(row)
	return emitArrayRange(w, a.ListValues(), int(start), int(end), path, geomInfo)
}

func emitFixedSizeList(w io.Writer, a *array.FixedSizeList, row int, path string, geomInfo map[string]GeometryInfo) error {
	n := int(a.DataType().(*arrow.FixedSizeListType).Len())
	start := row * n
	end := start + n
	return emitArrayRange(w, a.ListValues(), start, end, path, geomInfo)
}

func emitArrayRange(w io.Writer, values arrow.Array, start, end int, path string, geomInfo map[string]GeometryInfo) error {
	if _, err := w.Write([]byte("[")); err != nil {
		return err
	}
	for i := start; i < end; i++ {
		if i > start {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if err := emitCell(w, values, i, path, geomInfo); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte("]"))
	return err
}

// emitTimestamp formats an Arrow timestamp cell as RFC 3339 Nano — the
// same shape the wrapped-JSON object path produces via DuckDB's
// ToOutputSQL, so server and client emit byte-identical strings:
//
//   - TIMESTAMPTZ (TimeZone set, UTC) → "2024-03-15T12:30:45.123456789Z"
//   - TIMESTAMPTZ (non-UTC)           → "…+03:00"
//   - TIMESTAMP / DateTime (no tz)    → "2024-03-15T12:30:45.123456789Z"
//
// Naive timestamps (TimeZone empty) are treated as UTC and emitted with
// a "Z" suffix so the output is parseable by Go's time.Time.UnmarshalJSON
// and compatible with strict JSON consumers. Trailing fractional zeros
// are trimmed (Go's RFC3339Nano layout uses "9"s which elide zeros);
// DuckDB's emit path mirrors this via rtrim(..., '0') + rtrim('.').
func emitTimestamp(w io.Writer, a *array.Timestamp, row int) error {
	dt := a.DataType().(*arrow.TimestampType)
	v := int64(a.Value(row))
	nanos := v * unitNanos(dt.Unit)
	t := time.Unix(0, nanos).UTC()
	if dt.TimeZone != "" {
		loc, err := time.LoadLocation(dt.TimeZone)
		if err != nil {
			return fmt.Errorf("timestamp tz %q: %w", dt.TimeZone, err)
		}
		t = t.In(loc)
	}
	// Canonicalise UTC-equivalent locations ("UTC", "Etc/UTC", "Zulu",
	// explicit "+00:00") to Go's time.UTC so time.RFC3339Nano emits "Z"
	// instead of "+00:00" — matches the DuckDB side which collapses
	// extract(timezone) == 0 to "Z".
	if _, off := t.Zone(); off == 0 {
		t = t.UTC()
	}
	return writeJSON(w, t.Format(time.RFC3339Nano))
}

func unitNanos(u arrow.TimeUnit) int64 {
	switch u {
	case arrow.Second:
		return int64(time.Second)
	case arrow.Millisecond:
		return int64(time.Millisecond)
	case arrow.Microsecond:
		return int64(time.Microsecond)
	case arrow.Nanosecond:
		return 1
	}
	return 1
}

const millisPerDay = int64(24 * 60 * 60 * 1000)

func emitDate(w io.Writer, millis int64) error {
	t := time.Unix(0, millis*int64(time.Millisecond)).UTC()
	return writeJSON(w, t.Format("2006-01-02"))
}

func emitTime(w io.Writer, v int64, unit arrow.TimeUnit) error {
	nanos := v * unitNanos(unit)
	// Treat the value as a time-of-day in UTC (Arrow time doesn't carry a
	// date or tz). Format with nanosecond precision.
	t := time.Unix(0, nanos).UTC()
	return writeJSON(w, t.Format("15:04:05.000000000"))
}

// emitMonthInterval emits ISO 8601 duration like "P2M", or "PT0S" for
// a zero-month interval (ISO requires at least one component).
func emitMonthInterval(w io.Writer, months int32) error {
	if months == 0 {
		return writeJSON(w, "PT0S")
	}
	return writeJSON(w, fmt.Sprintf("P%dM", months))
}

// emitDayTimeInterval emits ISO 8601 duration, skipping zero components.
// Arrow's DayTime interval carries days and milliseconds independently.
func emitDayTimeInterval(w io.Writer, days, millis int32) error {
	return writeJSON(w, composeISODuration(0, days, int64(millis)*int64(time.Millisecond)))
}

// emitMonthDayNanoInterval emits ISO 8601 duration with months, days,
// and sub-second nanoseconds, skipping zero components.
func emitMonthDayNanoInterval(w io.Writer, months, days int32, nanos int64) error {
	return writeJSON(w, composeISODuration(months, days, nanos))
}

// composeISODuration builds an ISO 8601 duration string from months,
// days, and a nanosecond-granularity time component. Valid ISO 8601:
// zero-overall returns "PT0S", otherwise components are emitted only
// when non-zero.
//
// Within the T (time) portion, seconds are normalised into hours and
// minutes where they divide evenly — "PT2H" instead of "PT7200S",
// "PT1H30M" instead of "PT5400S". ISO 8601 treats these forms as
// equivalent (any conforming parser — luxon, isodate, java.time
// Duration — round-trips them as the same duration), but the
// normalised form is much more readable.
//
// Days and months stay as-is: month length varies so we can't collapse
// 30 days into 1 month, and cross-boundary normalisation would change
// semantics.
//
// Sub-second nanoseconds attach as a fractional seconds component with
// trailing zeros trimmed (mirrors Go's time.RFC3339Nano semantics).
func composeISODuration(months, days int32, nanos int64) string {
	if months == 0 && days == 0 && nanos == 0 {
		return "PT0S"
	}
	var b strings.Builder
	b.WriteByte('P')
	if months != 0 {
		fmt.Fprintf(&b, "%dM", months)
	}
	if days != 0 {
		fmt.Fprintf(&b, "%dD", days)
	}
	if nanos != 0 {
		b.WriteByte('T')
		writeISOTimePart(&b, nanos)
	}
	return b.String()
}

// writeISOTimePart emits the T-portion of an ISO 8601 duration with
// hour / minute / second normalisation. Precondition: nanos != 0.
func writeISOTimePart(b *strings.Builder, nanos int64) {
	const nsPerSec = int64(time.Second)
	const nsPerMin = 60 * nsPerSec
	const nsPerHour = 60 * nsPerMin

	neg := nanos < 0
	if neg {
		nanos = -nanos
		b.WriteByte('-') // rare but valid: DuckDB allows negative intervals.
	}

	hours := nanos / nsPerHour
	nanos %= nsPerHour
	minutes := nanos / nsPerMin
	nanos %= nsPerMin
	secs := nanos / nsPerSec
	frac := nanos % nsPerSec

	if hours != 0 {
		fmt.Fprintf(b, "%dH", hours)
	}
	if minutes != 0 {
		fmt.Fprintf(b, "%dM", minutes)
	}
	if secs != 0 || frac != 0 {
		if frac == 0 {
			fmt.Fprintf(b, "%dS", secs)
			return
		}
		fs := strings.TrimRight(fmt.Sprintf("%09d", frac), "0")
		fmt.Fprintf(b, "%d.%sS", secs, fs)
	}
}

// emitDecimal128 / emitDecimal256 — emit as a JSON number literal,
// preserving the value's full precision. Arrow stores decimals as a
// big.Int + scale; the Decimal128 / Decimal256 arrays already carry the
// scale via the type. We use their BigInt() representation then insert
// the decimal point ourselves rather than going through float64.
func emitDecimal128(w io.Writer, a *array.Decimal128, row int) error {
	dt := a.DataType().(*arrow.Decimal128Type)
	v := a.Value(row)
	bi := v.BigInt()
	return writeDecimal(w, bi.String(), int(dt.Scale))
}

func emitDecimal256(w io.Writer, a *array.Decimal256, row int) error {
	dt := a.DataType().(*arrow.Decimal256Type)
	v := a.Value(row)
	bi := v.BigInt()
	return writeDecimal(w, bi.String(), int(dt.Scale))
}

// emitFloat writes a float64 as a JSON number. Whole-valued floats emit
// as integers (`10` not `10.0`) — stdlib json.Marshal behaviour.
// NaN / +Inf / -Inf are not valid JSON numbers and emit as `null`
// (DuckDB's JSON cast does the same).
func emitFloat(w io.Writer, v float64) error {
	return emitFloatN(w, v, 64)
}

// emitFloat32 formats a float32 with 32-bit precision so the shortest
// representation matches what a consumer would produce from the same
// single-precision source — prevents the float32→float64 promotion from
// adding spurious trailing digits (e.g. float32 2499.99 →
// float64 2499.9899902… — we want `2499.99`, not `2499.98999`).
func emitFloat32(w io.Writer, v float32) error {
	return emitFloatN(w, float64(v), 32)
}

func emitFloatN(w io.Writer, v float64, bitSize int) error {
	// NaN / ±Inf aren't valid JSON numbers. DuckDB's JSON cast emits
	// them as null too — match.
	if math.IsNaN(v) || math.IsInf(v, 0) {
		_, err := w.Write(literalNull)
		return err
	}
	s := strconv.FormatFloat(v, 'g', -1, bitSize)
	_, err := io.WriteString(w, s)
	return err
}

// writeDecimal inserts a decimal point at `scale` digits from the right
// of the BigInt's string representation.
func writeDecimal(w io.Writer, digits string, scale int) error {
	negative := false
	if strings.HasPrefix(digits, "-") {
		negative = true
		digits = digits[1:]
	}
	if scale <= 0 {
		// Negative scale or zero — no fractional part. Append trailing zeros
		// for negative scale (i.e. scale=-3 means value * 1000).
		if scale < 0 {
			digits = digits + strings.Repeat("0", -scale)
		}
		if negative {
			digits = "-" + digits
		}
		_, err := io.WriteString(w, digits)
		return err
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	intPart := digits[:len(digits)-scale]
	fracPart := digits[len(digits)-scale:]
	out := intPart + "." + fracPart
	if negative {
		out = "-" + out
	}
	_, err := io.WriteString(w, out)
	return err
}

// joinGeomPath concatenates a dotted field path.
func joinGeomPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

