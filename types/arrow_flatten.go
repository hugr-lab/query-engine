// Arrow record batch flattener transforms complex types unsupported by
// Perspective (Struct, List, Map, Union) into simple columns.
// Structs are recursively flattened with dot-separated names;
// List, Map, and Union values are serialized to JSON strings.
package types

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// NeedsFlatten returns true if the schema contains any complex types
// (Struct, List, Map, Union) that need transformation.
func NeedsFlatten(schema *arrow.Schema) bool {
	for _, f := range schema.Fields() {
		if isComplexType(f.Type) {
			return true
		}
	}
	return false
}

// FlattenRecord transforms a record batch by recursively flattening Struct
// fields and converting List/Map/Union fields to JSON strings.
// If the record has no complex types, it is returned as-is (with Retain).
// The caller must Release the returned record.
func FlattenRecord(rec arrow.RecordBatch, mem memory.Allocator) arrow.RecordBatch {
	if !NeedsFlatten(rec.Schema()) {
		rec.Retain()
		return rec
	}

	srcSchema := rec.Schema()
	fields, cols := flattenColumns(srcSchema.Fields(), recordColumns(rec), "", mem)
	md := srcSchema.Metadata()
	schema := arrow.NewSchema(fields, &md)
	out := array.NewRecordBatch(schema, cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return out
}

// FlattenSchema returns the flattened schema without processing data.
func FlattenSchema(schema *arrow.Schema) *arrow.Schema {
	if !NeedsFlatten(schema) {
		return schema
	}
	fields, _ := flattenFields(schema.Fields(), "")
	md := schema.Metadata()
	return arrow.NewSchema(fields, &md)
}

func recordColumns(rec arrow.RecordBatch) []arrow.Array {
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		cols[i] = rec.Column(i)
	}
	return cols
}

func isComplexType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRUCT, arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST,
		arrow.MAP, arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return true
	}
	return false
}

// flattenFields returns the flattened field list (no data processing).
func flattenFields(fields []arrow.Field, prefix string) ([]arrow.Field, int) {
	var out []arrow.Field
	count := 0
	for _, f := range fields {
		name := flattenPrefixName(prefix, f.Name)
		if st, ok := f.Type.(*arrow.StructType); ok {
			sub, n := flattenFields(st.Fields(), name)
			out = append(out, sub...)
			count += n
		} else if isComplexType(f.Type) {
			out = append(out, arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: f.Metadata})
			count++
		} else {
			out = append(out, arrow.Field{Name: name, Type: f.Type, Nullable: f.Nullable, Metadata: f.Metadata})
			count++
		}
	}
	return out, count
}

// flattenColumns recursively flattens struct columns and converts complex types to JSON.
func flattenColumns(fields []arrow.Field, cols []arrow.Array, prefix string, mem memory.Allocator) ([]arrow.Field, []arrow.Array) {
	var outFields []arrow.Field
	var outCols []arrow.Array

	for i, f := range fields {
		col := cols[i]
		name := flattenPrefixName(prefix, f.Name)

		switch f.Type.ID() {
		case arrow.STRUCT:
			st := col.(*array.Struct)
			stType := f.Type.(*arrow.StructType)
			childFields := stType.Fields()
			childCols := make([]arrow.Array, len(childFields))
			for j := range childFields {
				childCols[j] = propagateStructNulls(st, st.Field(j), mem)
			}
			subFields, subCols := flattenColumns(childFields, childCols, name, mem)
			outFields = append(outFields, subFields...)
			outCols = append(outCols, subCols...)
			// Release intermediate null-propagated columns
			for _, c := range childCols {
				c.Release()
			}

		case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST,
			arrow.MAP, arrow.DENSE_UNION, arrow.SPARSE_UNION:
			jsonCol := complexToJSON(col, mem)
			outFields = append(outFields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true, Metadata: f.Metadata})
			outCols = append(outCols, jsonCol)

		default:
			col.Retain()
			outFields = append(outFields, arrow.Field{Name: name, Type: f.Type, Nullable: f.Nullable, Metadata: f.Metadata})
			outCols = append(outCols, col)
		}
	}

	return outFields, outCols
}

// propagateStructNulls creates a child array that also has nulls wherever the
// parent struct is null. If there are no parent nulls, the child is returned as-is.
func propagateStructNulls(parent *array.Struct, child arrow.Array, mem memory.Allocator) arrow.Array {
	if parent.NullN() == 0 {
		child.Retain()
		return child
	}

	// Rebuild the child with combined null bitmap
	n := parent.Len()
	data := child.Data()

	buffers := data.Buffers()
	newBufs := make([]*memory.Buffer, len(buffers))
	copy(newBufs, buffers)

	// Create new validity buffer combining parent and child nulls
	validBuf := memory.NewResizableBuffer(mem)
	validBuf.Resize(int(bitutil.BytesForBits(int64(n))))
	validBytes := validBuf.Bytes()
	for j := range n {
		if parent.IsNull(j) || child.IsNull(j) {
			bitutil.ClearBit(validBytes, j)
		} else {
			bitutil.SetBit(validBytes, j)
		}
	}
	newBufs[0] = validBuf

	nullCount := 0
	for j := range n {
		if !bitutil.BitIsSet(validBytes, j) {
			nullCount++
		}
	}

	childData := data.Children()
	newData := array.NewData(child.DataType(), n, newBufs, childData, nullCount, data.Offset())
	result := array.MakeFromData(newData)
	newData.Release()
	validBuf.Release()
	return result
}

// complexToJSON converts a complex-typed column to a string column
// where each value is a JSON representation using Arrow's GetOneForMarshal.
func complexToJSON(col arrow.Array, mem memory.Allocator) arrow.Array {
	b := array.NewStringBuilder(mem)
	defer b.Release()
	b.Reserve(col.Len())

	for i := range col.Len() {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		v := col.GetOneForMarshal(i)
		data, err := json.Marshal(v)
		if err != nil {
			b.Append(fmt.Sprintf("%v", v))
		} else {
			b.Append(string(data))
		}
	}
	return b.NewArray()
}

func flattenPrefixName(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}
