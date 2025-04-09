package db

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDBJsonTable_MarshalJSON(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int32},
		{Name: "col2", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"test"}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"test"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	table := NewDBJsonTable(true)
	table.Append(rec)

	data, err := json.Marshal(table)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	expected := `[{"col1":1,"col2":"test"},{"col1":2,"col2":"test"}]`
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("MarshalJSON() = %s, want %s", data, expected)
	}
}

func TestDBJsonTableOne_MarshalJSON(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues([]string{"{\"test\":\"val\", \"val\":21}"}, nil)
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"{\"test\":\"val\", \"val\":22}"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	table := NewDBJsonTable(true)
	table.wrapped = true
	table.Append(rec)

	data, err := json.Marshal(table)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	expected := `[{"test":"val","val":21},{"test":"val","val":22}]`
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("MarshalJSON() = %s, want %s", data, expected)
	}
}

func TestDBJsonTable_EncodeMsgpack(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int32},
		{Name: "col2", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"test1", "test2"}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	table := NewDBJsonTable(false)
	table.Append(rec)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	err := table.EncodeMsgpack(enc)
	if err != nil {
		t.Fatalf("EncodeMsgpack() error = %v", err)
	}

	decodedTable := new(DBJsonTable)
	dec := msgpack.NewDecoder(&buf)

	err = decodedTable.DecodeMsgpack(dec)
	if err != nil {
		t.Fatalf("DecodeMsgpack() error = %v", err)
	}

	if decodedTable.NumRows() != table.NumRows() {
		t.Errorf("NumRows() = %d, want %d", decodedTable.NumRows(), table.NumRows())
	}

	if decodedTable.NumCols() != table.NumCols() {
		t.Errorf("NumCols() = %d, want %d", decodedTable.NumCols(), table.NumCols())
	}

	row1, ok := decodedTable.RowData(0)
	expectedRow1 := map[string]any{"col1": int32(1), "col2": "test1"}
	if !equalMaps(row1, expectedRow1) || !ok {
		t.Errorf("RowData(0) = %v, want %v", row1, expectedRow1)
	}

	row2, ok := decodedTable.RowData(1)
	expectedRow2 := map[string]any{"col1": int32(2), "col2": "test2"}
	if !ok || !equalMaps(row2, expectedRow2) {
		t.Errorf("RowData(1) = %v, want %v", row2, expectedRow2)
	}
}

func TestDBJsonTable_DecodeMsgpack_Empty(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	table := NewDBJsonTable(false)
	err := table.EncodeMsgpack(enc)
	if err != nil {
		t.Fatalf("EncodeMsgpack() error = %v", err)
	}

	decodedTable := NewDBJsonTable(false)
	dec := msgpack.NewDecoder(&buf)

	err = decodedTable.DecodeMsgpack(dec)
	if err != nil {
		t.Fatalf("DecodeMsgpack() error = %v", err)
	}

	if decodedTable.NumRows() != 0 {
		t.Errorf("NumRows() = %d, want 0", decodedTable.NumRows())
	}

	if decodedTable.NumCols() != 0 {
		t.Errorf("NumCols() = %d, want 0", decodedTable.NumCols())
	}
}

func equalMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
