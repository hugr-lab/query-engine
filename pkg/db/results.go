package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/marcboeker/go-duckdb/v2"
	"github.com/vmihailenco/msgpack/v5"
)

type ArrowTable struct {
	chunks              []arrow.Record
	wrapped             bool
	asArray             bool
	releaseAfterMarshal bool
}

func NewDBJsonTable(releaseAfterMarshal bool) *ArrowTable {
	return &ArrowTable{
		releaseAfterMarshal: releaseAfterMarshal,
	}
}

func (t *ArrowTable) Append(rec arrow.Record) {
	rec.Retain()
	t.chunks = append(t.chunks, rec)
}

func (t *ArrowTable) Release() {
	for _, rec := range t.chunks {
		rec.Release()
	}
}

func (t *ArrowTable) RowData(i int) (map[string]any, bool) {
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

func (t *ArrowTable) NumChunks() int {
	return len(t.chunks)
}

func (t *ArrowTable) Chunk(i int) arrow.Record {
	return t.chunks[i]
}

func (t *ArrowTable) NumRows() int {
	var numRows int64
	for _, rec := range t.chunks {
		numRows += rec.NumRows()
	}
	return int(numRows)
}

func (t *ArrowTable) NumCols() int {
	if len(t.chunks) == 0 {
		return 0
	}
	return int(t.chunks[0].NumCols())
}

func (t *ArrowTable) SetAutoRelease(release bool) {
	t.releaseAfterMarshal = release
}

func (t *ArrowTable) MarshalJSON() ([]byte, error) {
	if t.releaseAfterMarshal {
		defer t.Release()
	}
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
			RecordToJSON(rec, t.asArray, w)
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
				w.Write(val)
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

func RecordToJSON(rec arrow.Record, asArray bool, w io.Writer) error {
	enc := json.NewEncoder(w)

	fields := rec.Schema().Fields()

	cols := make(map[string]interface{})
	for i := 0; int64(i) < rec.NumRows(); i++ {
		if i > 0 {
			w.Write([]byte(","))
		}
		outArr := make([]interface{}, len(fields))
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
				m := make(duckdb.Map)
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
				for j := 0; j < v.Len(); j++ {
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
		m := make(duckdb.Map)
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
		s := make(map[string]any)
		for j := 0; j < v.Len(); j++ {
			field := v.Field(j)
			s[field.String()] = ColumnValue(field, i)
		}
		return s
	case *array.Null:
		return nil
	}
	return nil
}

// msgpack custom decoder
var _ msgpack.CustomDecoder = (*ArrowTable)(nil)

func (t *ArrowTable) DecodeMsgpack(dec *msgpack.Decoder) error {
	err := dec.DecodeMulti(&t.releaseAfterMarshal, &t.wrapped, &t.asArray)
	if err != nil {
		return err
	}
	var encoded []byte
	err = dec.Decode(&encoded)
	if err != nil {
		return err
	}
	if len(encoded) == 0 {
		return nil
	}
	t.chunks, err = decodeRecordFromIPC(encoded)
	return err
}

func decodeRecordFromIPC(b []byte) ([]arrow.Record, error) {
	buf := bytes.NewReader(b)
	fr, err := ipc.NewFileReader(buf)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	rr := make([]arrow.Record, fr.NumRecords())
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
var _ msgpack.CustomEncoder = (*ArrowTable)(nil)

func (t *ArrowTable) EncodeMsgpack(enc *msgpack.Encoder) error {
	if t == nil {
		enc.EncodeNil()
	}

	err := enc.EncodeMulti(t.releaseAfterMarshal, t.wrapped, t.asArray)
	if err != nil {
		return err
	}
	// encode each chunk as []string ([][]byte)
	encoded, err := encodeRecordToIPC(t.chunks)
	if err != nil {
		return err
	}
	return enc.Encode(encoded)
}

func encodeRecordToIPC(rr []arrow.Record) ([]byte, error) {
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
