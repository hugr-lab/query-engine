package types

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/vmihailenco/msgpack/v5"
)

// GeometryInfo describes the wire format of a single geometry field inside an
// Arrow table or response. Same vocabulary as the IPC X-Hugr-Geometry-Fields
// header: Format is one of "WKB", "GeoJSON", or "GeoJSONString".
//
// Keys in map[string]GeometryInfo are dotted field paths resolvable against
// the Arrow schema; empty string means the whole row / value.
type GeometryInfo struct {
	SRID   string `json:"srid" msgpack:"srid"`
	Format string `json:"format" msgpack:"format"`
}

type ArrowTable interface {
	SetInfo(info string)
	Info() string
	// SetGeometryInfo attaches per-field geometry metadata to this table.
	// Populated by the query planner (engine side) or the IPC client reader
	// (client side). Idempotent — last call wins. Not safe for concurrent
	// set + read; the contract is "populate once before handing to consumers".
	SetGeometryInfo(info map[string]GeometryInfo)
	// GeometryInfo returns the attached per-field geometry metadata, or an
	// empty non-nil map if none was set. Consumers use it to decide how to
	// render / decode nested utf8 geometry columns on the JSON path, and
	// (optionally) to dispatch the scanner without a byte-peek heuristic.
	GeometryInfo() map[string]GeometryInfo
	Retain()
	Release()
	MarshalJSON() ([]byte, error)
	DecodeMsgpack(dec *msgpack.Decoder) error
	EncodeMsgpack(enc *msgpack.Encoder) error
	Records() ([]arrow.RecordBatch, error)
	Reader(retain bool) (array.RecordReader, error)
	// Rows returns a cursor over this table. The cursor retains the underlying
	// Arrow resources; callers MUST call Close on the returned Rows.
	Rows() (Rows, error)
}

var _ ArrowTable = (*ArrowTableChunked)(nil)

type ArrowTableChunked struct {
	chunks   []arrow.RecordBatch
	wrapped  bool
	asArray  bool
	geomInfo map[string]GeometryInfo
}

func NewArrowTable() *ArrowTableChunked {
	return &ArrowTableChunked{}
}

func NewArrowTableFromReader(reader array.RecordReader) (*ArrowTableChunked, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}
	t := &ArrowTableChunked{}
	defer reader.Release()

	for reader.Next() {
		if reader.Err() != nil {
			reader.Release()
			return nil, reader.Err()
		}
		rec := reader.RecordBatch()
		t.Append(rec)
	}
	return t, nil
}

func (t *ArrowTableChunked) SetInfo(info string) {
	t.wrapped = strings.Contains(info, "wrapped")
	t.asArray = strings.Contains(info, "asArray")
}

func (t *ArrowTableChunked) Info() string {
	var info []string
	if t.wrapped {
		info = append(info, "wrapped")
	}
	if t.asArray {
		info = append(info, "asArray")
	}
	return strings.Join(info, ",")
}

func (t *ArrowTableChunked) SetGeometryInfo(info map[string]GeometryInfo) {
	t.geomInfo = info
}

func (t *ArrowTableChunked) GeometryInfo() map[string]GeometryInfo {
	if t.geomInfo == nil {
		return map[string]GeometryInfo{}
	}
	return t.geomInfo
}

func (t *ArrowTableChunked) Append(rec arrow.RecordBatch) {
	rec.Retain()
	t.chunks = append(t.chunks, rec)
}

func (t *ArrowTableChunked) Retain() {
	for _, rec := range t.chunks {
		rec.Retain()
	}
}

func (t *ArrowTableChunked) Release() {
	for _, rec := range t.chunks {
		rec.Release()
	}
}

func (t *ArrowTableChunked) Records() ([]arrow.RecordBatch, error) {
	if len(t.chunks) == 0 {
		return nil, nil
	}
	records := make([]arrow.RecordBatch, len(t.chunks))
	for i, rec := range t.chunks {
		rec.Retain()
		records[i] = rec
	}
	return records, nil
}

func (t *ArrowTableChunked) Reader(retain bool) (array.RecordReader, error) {
	if len(t.chunks) == 0 {
		return nil, nil
	}
	reader, err := array.NewRecordReader(t.chunks[0].Schema(), t.chunks)
	if err != nil {
		for _, rec := range t.chunks {
			rec.Release()
		}
		t.chunks = nil
		return nil, err
	}
	if retain {
		t.Retain()
	}
	return reader, nil
}

// Rows returns a cursor-style scanner over this table. The cursor takes a
// retained reference to the underlying record reader; callers MUST call
// Close on the returned Rows (or let it reach end-of-stream).
func (t *ArrowTableChunked) Rows() (Rows, error) {
	reader, err := t.Reader(true)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return emptyRows{}, nil
	}
	return newRowScanner(reader)
}

func (t *ArrowTableChunked) RowData(i int) (map[string]any, bool) {
	if i < 0 || i >= t.NumRows() {
		return nil, false
	}
	for _, rec := range t.chunks {
		if i >= int(rec.NumRows()) {
			i -= int(rec.NumRows())
			continue
		}
		row := make(map[string]any)
		for j, col := range rec.Columns() {
			row[rec.Schema().Field(j).Name] = col.GetOneForMarshal(i)
		}
		return row, true
	}

	return nil, false
}

func (t *ArrowTableChunked) NumChunks() int {
	return len(t.chunks)
}

func (t *ArrowTableChunked) Chunk(i int) arrow.RecordBatch {
	return t.chunks[i]
}

func (t *ArrowTableChunked) NumRows() int {
	var numRows int64
	for _, rec := range t.chunks {
		numRows += rec.NumRows()
	}
	return int(numRows)
}

func (t *ArrowTableChunked) NumCols() int {
	if len(t.chunks) == 0 {
		return 0
	}
	return int(t.chunks[0].NumCols())
}

func (t *ArrowTableChunked) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	if len(t.chunks) == 0 {
		return []byte("[]"), nil
	}
	w := bytes.NewBuffer(nil)
	w.WriteByte('[')
	for i, rec := range t.chunks {
		if i > 0 {
			w.WriteByte(',')
		}
		if !t.wrapped {
			if err := RecordToJSON(rec, t.asArray, w); err != nil {
				return nil, err
			}
			continue
		}
		col := colVal{a: rec.Column(0)}
		for i := 0; i < int(rec.NumRows()); i++ {
			if i > 0 {
				w.WriteByte(',')
			}
			val := col.Value(i)
			if val == nil {
				w.WriteString("null")
				continue
			}
			var err error
			switch val := val.(type) {
			case string:
				_, err = w.WriteString(val)
			case []byte:
				_, err = w.Write(val)
			default:
				err = json.NewEncoder(w).Encode(val)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	err := w.WriteByte(']')
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func RecordToJSON(rec arrow.RecordBatch, asArray bool, w io.Writer) error {
	enc := json.NewEncoder(w)

	fields := rec.Schema().Fields()

	cols := make(map[string]any)
	for i := 0; int64(i) < rec.NumRows(); i++ {
		if i > 0 {
			_, _ = w.Write([]byte(","))
		}
		outArr := make([]any, len(fields))
		for j, c := range rec.Columns() {
			if asArray {
				outArr[j] = c.GetOneForMarshal(i)
				continue
			}
			cols[fields[j].Name] = c.GetOneForMarshal(i)
		}
		var out any = cols
		if asArray {
			out = outArr
		}
		if asArray && len(outArr) == 1 {
			out = outArr[0]
		}
		if err := enc.Encode(out); err != nil {
			return err
		}
	}
	return nil
}

type colVal struct {
	a       arrow.Array
	valFunc func(a arrow.Array, i int) any
}

func (c *colVal) Value(i int) any {
	if c.valFunc == nil {
		switch v := c.a.(type) {
		case *array.Time32:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Time64:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Date32:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Date64:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Timestamp:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Int8:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Int16:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Int32:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Int64:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Uint8:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Uint16:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Uint32:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Uint64:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Float32:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Float64:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.String:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.Binary:
			c.valFunc = func(a arrow.Array, i int) any { return v.Value(i) }
		case *array.List:
			c.valFunc = func(a arrow.Array, i int) any {
				start, end := v.ValueOffsets(i)
				if start == end {
					return []any{}
				}
				l := make([]any, end-start)
				for j := int(start); j < int(end); j++ {
					l[j-int(start)] = ColumnValue(v.ListValues(), j)
				}
				return l
			}
		case *array.Map:
			c.valFunc = func(a arrow.Array, i int) any {
				m := make(map[any]any)
				keys := v.Keys()
				items := v.Items()
				start, end := v.ValueOffsets(i)
				for j := int(start); j < int(end); j++ {
					key := ColumnValue(keys, j)
					value := ColumnValue(items, j)
					m[key] = value
				}
				return m
			}
		case *array.Struct:
			c.valFunc = func(a arrow.Array, i int) any {
				s := make(map[string]any)
				for j := 0; j < v.NumField(); j++ {
					field := v.Field(j)
					s[field.String()] = ColumnValue(field, i)
				}
				return s
			}
		case *array.Null:
			return nil
		}
	}
	return c.valFunc(c.a, i)
}

func ColumnValue(a arrow.Array, i int) any {
	switch v := a.(type) {
	case *array.Time32:
		return v.Value(i)
	case *array.Time64:
		return v.Value(i)
	case *array.Date32:
		return v.Value(i)
	case *array.Date64:
		return v.Value(i)
	case *array.Timestamp:
		return v.Value(i)
	case *array.Int8:
		return v.Value(i)
	case *array.Int16:
		return v.Value(i)
	case *array.Int32:
		return v.Value(i)
	case *array.Int64:
		return v.Value(i)
	case *array.Uint8:
		return v.Value(i)
	case *array.Uint16:
		return v.Value(i)
	case *array.Uint32:
		return v.Value(i)
	case *array.Uint64:
		return v.Value(i)
	case *array.Float32:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.String:
		return v.Value(i)
	case *array.Binary:
		return v.Value(i)
	case *array.List:
		start, end := v.ValueOffsets(i)
		if start == end {
			return []any{}
		}
		l := make([]any, end-start)
		for j := int(start); j < int(end); j++ {
			l[j-int(start)] = ColumnValue(v.ListValues(), j)
		}
		return l
	case *array.Map:
		m := make(map[any]any)
		keys := v.Keys()
		items := v.Items()
		start, end := v.ValueOffsets(i)
		for j := int(start); j < int(end); j++ {
			key := ColumnValue(keys, j)
			value := ColumnValue(items, j)
			m[key] = value
		}
		return m
	case *array.Struct:
		return v.GetOneForMarshal(i)
	case *array.Null:
		return nil
	}
	return nil
}

// msgpack custom decoder
var _ msgpack.CustomDecoder = (*ArrowTableChunked)(nil)

func (t *ArrowTableChunked) DecodeMsgpack(dec *msgpack.Decoder) error {
	err := dec.DecodeMulti(&t.wrapped, &t.asArray)
	if err != nil {
		return err
	}
	var encoded []byte
	err = dec.Decode(&encoded)
	if err != nil {
		return err
	}
	// Optional trailing field: geometry info map. Absent on legacy payloads
	// produced before the field was introduced — tolerate io.EOF and leave
	// geomInfo empty.
	if err := dec.Decode(&t.geomInfo); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(encoded) == 0 {
		return nil
	}
	t.chunks, err = decodeRecordsFromIPC(encoded)
	return err
}

func decodeRecordsFromIPC(b []byte) ([]arrow.RecordBatch, error) {
	buf := bytes.NewReader(b)
	fr, err := ipc.NewFileReader(buf)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	rr := make([]arrow.RecordBatch, fr.NumRecords())
	for i := 0; i < fr.NumRecords(); i++ {
		rr[i], err = fr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			for j, rec := range rr {
				if j >= i {
					break
				}
				rec.Release()
			}
			return nil, err
		}
		rr[i].Retain()
	}
	return rr, nil
}

// msgpack custom encoder
var _ msgpack.CustomEncoder = (*ArrowTableChunked)(nil)

func (t *ArrowTableChunked) EncodeMsgpack(enc *msgpack.Encoder) error {
	if t == nil {
		return enc.EncodeNil()
	}

	err := enc.EncodeMulti(t.wrapped, t.asArray)
	if err != nil {
		return err
	}
	// encode each chunk as []string ([][]byte)
	encoded, err := encodeRecordsToIPC(t.chunks)
	if err != nil {
		return err
	}
	if err := enc.Encode(encoded); err != nil {
		return err
	}
	return enc.Encode(t.geomInfo)
}

func encodeRecordsToIPC(rr []arrow.RecordBatch) ([]byte, error) {
	if len(rr) == 0 {
		return nil, nil
	}
	buf := bytes.NewBuffer(nil)
	fw, err := ipc.NewFileWriter(buf, ipc.WithSchema(rr[0].Schema()))
	if err != nil {
		return nil, err
	}
	defer fw.Close()

	for _, rec := range rr {
		if err := fw.Write(rec); err != nil {
			return nil, err
		}
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type JsonValue string

func (v *JsonValue) MarshalJSON() ([]byte, error) {
	return []byte(*v), nil
}

var _ ArrowTable = (*ArrowTableStream)(nil)

type ArrowTableStream struct {
	reader   array.RecordReader
	wrapped  bool
	asArray  bool
	geomInfo map[string]GeometryInfo
}

func NewArrowTableStream(reader array.RecordReader) *ArrowTableStream {
	return &ArrowTableStream{
		reader: reader,
	}
}

func (t *ArrowTableStream) Info() string {
	var info []string
	if t.wrapped {
		info = append(info, "wrapped")
	}
	if t.asArray {
		info = append(info, "asArray")
	}
	return strings.Join(info, ",")
}

func (t *ArrowTableStream) SetInfo(info string) {
	t.wrapped = strings.Contains(info, "wrapped")
	t.asArray = strings.Contains(info, "asArray")
}

func (t *ArrowTableStream) SetGeometryInfo(info map[string]GeometryInfo) {
	t.geomInfo = info
}

func (t *ArrowTableStream) GeometryInfo() map[string]GeometryInfo {
	if t.geomInfo == nil {
		return map[string]GeometryInfo{}
	}
	return t.geomInfo
}

func (t *ArrowTableStream) Release() {
	t.reader.Release()
}

func (t *ArrowTableStream) Retain() {
	t.reader.Retain()
}

func (t *ArrowTableStream) Records() ([]arrow.RecordBatch, error) {
	if t.reader == nil {
		return nil, nil
	}
	rr, err := t.readAll()
	if err != nil {
		t.reader.Release()
		for _, rec := range rr {
			rec.Release()
		}
		return nil, err
	}
	if len(rr) == 0 {
		return nil, nil
	}
	// create a new reader for the records
	reader, err := array.NewRecordReader(rr[0].Schema(), rr)
	if err != nil {
		for _, rec := range rr {
			rec.Release()
		}
		return nil, err
	}
	t.reader.Release()
	t.reader = reader
	return rr, nil
}

func (t *ArrowTableStream) Reader(retain bool) (array.RecordReader, error) {
	if !retain || t.reader == nil {
		return t.reader, nil
	}
	rr, err := t.readAll()
	if err != nil {
		return nil, err
	}
	if len(rr) == 0 {
		return t.reader, nil
	}

	reader, err := array.NewRecordReader(rr[0].Schema(), rr)
	if err != nil {
		for _, rec := range rr {
			rec.Release()
		}
		return nil, err
	}
	t.reader.Release()
	t.reader = reader
	return array.NewRecordReader(rr[0].Schema(), rr)
}

// Rows returns a cursor-style scanner over this streaming table. The cursor
// consumes the underlying reader; callers MUST call Close on the returned
// Rows (or let it reach end-of-stream).
func (t *ArrowTableStream) Rows() (Rows, error) {
	if t.reader == nil {
		return emptyRows{}, nil
	}
	// Retain ownership: the scanner assumes it can Release the reader.
	t.reader.Retain()
	return newRowScanner(t.reader)
}

func (t *ArrowTableStream) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	rr, err := t.readAll()
	if err != nil {
		return nil, err
	}
	if len(rr) == 0 {
		return []byte("[]"), nil
	}
	w := bytes.NewBuffer(nil)
	w.WriteByte('[')
	for i, rec := range rr {
		if i > 0 {
			w.WriteByte(',')
		}
		if !t.wrapped {
			if err := RecordToJSON(rec, t.asArray, w); err != nil {
				return nil, err
			}
			continue
		}
		col := colVal{a: rec.Column(0)}
		for i := 0; i < int(rec.NumRows()); i++ {
			if i > 0 {
				w.WriteByte(',')
			}
			val := col.Value(i)
			if val == nil {
				w.WriteString("null")
				continue
			}
			var err error
			switch val := val.(type) {
			case string:
				_, err = w.WriteString(val)
			case []byte:
				_, err = w.Write(val)
			default:
				err = json.NewEncoder(w).Encode(val)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	err = w.WriteByte(']')
	if err != nil {
		return nil, err
	}
	reader, err := array.NewRecordReader(rr[0].Schema(), rr)
	if err != nil {
		for _, rec := range rr {
			rec.Release()
		}
		return nil, err
	}
	t.reader.Release()
	t.reader = reader

	return w.Bytes(), nil
}

func (t *ArrowTableStream) readAll() ([]arrow.RecordBatch, error) {
	if t.reader == nil {
		return nil, nil
	}
	var rr []arrow.RecordBatch
	for t.reader.Next() {
		if t.reader.Err() != nil {
			t.reader.Release()
			for _, r := range rr {
				r.Release()
			}
			return nil, t.reader.Err()
		}
		rec := t.reader.RecordBatch()
		rr = append(rr, rec)
		rec.Retain()
	}
	return rr, nil
}

// msgpack custom decoder
var _ msgpack.CustomDecoder = (*ArrowTableStream)(nil)

func (t *ArrowTableStream) DecodeMsgpack(dec *msgpack.Decoder) error {
	err := dec.DecodeMulti(&t.wrapped, &t.asArray)
	if err != nil {
		return err
	}
	var encoded []byte
	err = dec.Decode(&encoded)
	if err != nil {
		return err
	}
	// Optional trailing field: geometry info map. Tolerate io.EOF for
	// legacy payloads.
	if err := dec.Decode(&t.geomInfo); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(encoded) == 0 {
		return nil
	}
	rr, err := decodeRecordsFromIPC(encoded)
	if err != nil {
		return err
	}
	if len(rr) == 0 {
		return nil
	}
	t.reader, err = array.NewRecordReader(rr[0].Schema(), rr)
	if err != nil {
		for _, rec := range rr {
			rec.Release()
		}
		return err
	}

	return nil
}

// msgpack custom encoder
var _ msgpack.CustomEncoder = (*ArrowTableStream)(nil)

func (t *ArrowTableStream) EncodeMsgpack(enc *msgpack.Encoder) error {
	if t == nil {
		return enc.EncodeNil()
	}
	err := enc.EncodeMulti(t.wrapped, t.asArray)
	if err != nil {
		return err
	}
	rr, err := t.readAll()
	if err != nil {
		return err
	}
	if len(rr) == 0 {
		return errors.New("no records to encode")
	}
	// create a new reader for the records
	reader, err := array.NewRecordReader(rr[0].Schema(), rr)
	if err != nil {
		for _, rec := range rr {
			rec.Release()
		}
		return err
	}
	t.reader.Release()
	t.reader = reader
	// encode each chunk as []string ([][]byte)
	encoded, err := encodeRecordsToIPC(rr)
	if err != nil {
		return err
	}
	if err := enc.Encode(encoded); err != nil {
		return err
	}
	return enc.Encode(t.geomInfo)
}

func RecordsColNums(rr []arrow.RecordBatch) int64 {
	if len(rr) == 0 {
		return 0
	}
	return rr[0].NumCols()
}

func RecordsRowNums(rr []arrow.RecordBatch) int64 {
	if len(rr) == 0 {
		return 0
	}
	var numRows int64
	for _, rec := range rr {
		numRows += rec.NumRows()
	}
	return numRows
}

func ReleaseRecords(rr []arrow.RecordBatch) {
	for _, rec := range rr {
		rec.Release()
	}
}

func RetainRecords(rr []arrow.RecordBatch) {
	for _, rec := range rr {
		rec.Retain()
	}
}
