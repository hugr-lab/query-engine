package types

import (
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Regression for the discovery-field_values `{}` bug: a utf8 string column
// scanned into an `any` destination must round-trip plain (non-JSON) text —
// including non-ASCII — instead of failing json.Unmarshal. The bucket
// aggregation key { value: <string> } scanned into `any` used to error on
// the first non-ASCII byte (e.g. "Гатчина" → invalid character 'Ð'), and the
// caller swallowed the error and returned an empty {}.

type bucketKeyRow struct {
	Key struct {
		Value any `json:"value"`
	} `json:"key"`
}

// buildKeyTable builds a one-row table shaped like a bucket aggregation: a
// struct column `key` with a single `value` field of the given Arrow type.
func buildKeyTable(keyType arrow.DataType, appendVal func(*array.StructBuilder)) ArrowTable {
	keyStruct := arrow.StructOf(arrow.Field{Name: "value", Type: keyType})
	schema := arrow.NewSchema([]arrow.Field{{Name: "key", Type: keyStruct}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	kb := b.Field(0).(*array.StructBuilder)
	kb.Append(true)
	appendVal(kb)
	rec := b.NewRecordBatch()
	tbl := NewArrowTable()
	tbl.Append(rec)
	rec.Release()
	return tbl
}

func TestScanData_BucketKeyIntoAny_StringRoundTrips(t *testing.T) {
	// String value (Cyrillic) — the field_values name case that used to {}.
	strTbl := buildKeyTable(arrow.BinaryTypes.String, func(b *array.StructBuilder) {
		b.FieldBuilder(0).(*array.StringBuilder).Append("Гатчина")
	})
	defer strTbl.Release()
	var sr []bucketKeyRow
	if err := (&Response{Data: map[string]any{"values": strTbl}}).ScanData("values", &sr); err != nil {
		t.Fatalf("string key: ScanData error: %v", err)
	}
	if len(sr) != 1 || sr[0].Key.Value != "Гатчина" {
		t.Fatalf("string key: got %+v, want value=Гатчина", sr)
	}

	// Int value — the type_id path must still work.
	intTbl := buildKeyTable(arrow.PrimitiveTypes.Int64, func(b *array.StructBuilder) {
		b.FieldBuilder(0).(*array.Int64Builder).Append(3)
	})
	defer intTbl.Release()
	var ir []bucketKeyRow
	if err := (&Response{Data: map[string]any{"values": intTbl}}).ScanData("values", &ir); err != nil {
		t.Fatalf("int key: ScanData error: %v", err)
	}
	if len(ir) != 1 || ir[0].Key.Value == nil {
		t.Fatalf("int key: got %+v, want a non-nil value", ir)
	}
}

// buildStringColTable builds a one-row table with a single utf8 column `v`.
func buildStringColTable(val string) ArrowTable {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.String}}, nil)
	b := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).Append(val)
	rec := b.NewRecordBatch()
	tbl := NewArrowTable()
	tbl.Append(rec)
	rec.Release()
	return tbl
}

func TestScanData_StringColumnIntoAny(t *testing.T) {
	type row struct {
		V any `json:"v"`
	}
	cases := []struct {
		name string
		val  string
		want any
	}{
		{"plain_ascii", "hello", "hello"},
		{"plain_cyrillic", "Гатчина", "Гатчина"},
		{"text_leading_brace", "{not json}", "{not json}"},   // looks-like-JSON sniff true, decode fails → raw string
		{"text_leading_letter_t", "true story", "true story"}, // sniff true (t), decode fails → raw string
		{"json_object", `{"a":1}`, map[string]any{"a": float64(1)}},
		{"json_array", `[1,2]`, []any{float64(1), float64(2)}},
		{"json_number", "42", float64(42)}, // bare JSON scalar must still parse
		// Leading whitespace must not defeat the sniff — json.Unmarshal
		// tolerates it, so these must parse, not fall back to a raw string.
		{"json_object_leading_spaces", "  {\"a\":1}", map[string]any{"a": float64(1)}},
		{"json_array_leading_tab", "\t[1,2]", []any{float64(1), float64(2)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tbl := buildStringColTable(c.val)
			defer tbl.Release()
			var r []row
			if err := (&Response{Data: map[string]any{"rows": tbl}}).ScanData("rows", &r); err != nil {
				t.Fatalf("ScanData error: %v", err)
			}
			if len(r) != 1 {
				t.Fatalf("len=%d, want 1", len(r))
			}
			if !reflect.DeepEqual(r[0].V, c.want) {
				t.Fatalf("V=%#v (%T), want %#v (%T)", r[0].V, r[0].V, c.want, c.want)
			}
		})
	}
}
