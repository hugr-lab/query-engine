package types

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// newMultiBatchTable returns a table with n batches of batchRows rows each.
// Counter column "n" increments from 0 across all rows.
func newMultiBatchTable(t *testing.T, batches, batchRows int) *ArrowTableChunked {
	t.Helper()
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	tbl := NewArrowTable()
	counter := int32(0)
	for i := 0; i < batches; i++ {
		b := array.NewRecordBuilder(mem, schema)
		for j := 0; j < batchRows; j++ {
			b.Field(0).(*array.Int32Builder).Append(counter)
			counter++
		}
		rec := b.NewRecordBatch()
		tbl.Append(rec)
		rec.Release()
		b.Release()
	}
	return tbl
}

// newTestTable builds an ArrowTableChunked with the given schema and one record
// populated from columnar data. Used as a scaffolding helper across tests.
func newTestTable(t *testing.T, schema *arrow.Schema, populate func(b *array.RecordBuilder)) *ArrowTableChunked {
	t.Helper()
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	populate(b)
	rec := b.NewRecordBatch()
	defer rec.Release()
	tbl := NewArrowTable()
	tbl.Append(rec)
	return tbl
}

func TestRowScanner_Lifecycle(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(1)
		b.Field(0).(*array.Int32Builder).Append(2)
	})
	defer tbl.Release()

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}

	// Scan before Next → ErrScanBeforeNext.
	var dst struct {
		N int32 `json:"n"`
	}
	if err := rows.Scan(&dst); !errors.Is(err, ErrScanBeforeNext) {
		t.Fatalf("Scan before Next: want ErrScanBeforeNext, got %v", err)
	}

	// Iterate.
	var count int
	for rows.Next() {
		if err := rows.Scan(&dst); err != nil {
			t.Fatalf("Scan row %d: %v", count, err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("want 2 rows, got %d", count)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err after iteration: %v", err)
	}

	// Scan after end → ErrScanAfterEnd.
	if err := rows.Scan(&dst); !errors.Is(err, ErrScanAfterEnd) {
		t.Fatalf("Scan after end: want ErrScanAfterEnd, got %v", err)
	}

	// Close twice is idempotent.
	if err := rows.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close 2 (idempotent): %v", err)
	}
}

func TestRowScanner_Columns(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int32},
		{Name: "b", Type: arrow.BinaryTypes.String},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(1)
		b.Field(1).(*array.StringBuilder).Append("x")
	})
	defer tbl.Release()

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()
	got := rows.Columns()
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("Columns: want [a b], got %v", got)
	}
}

func TestRowScanner_EmptyTable(t *testing.T) {
	tbl := NewArrowTable()
	defer tbl.Release()
	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows on empty table: %v", err)
	}
	if rows.Next() {
		t.Fatal("Next on empty table should be false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err on empty table: %v", err)
	}
	_ = rows.Close()
}

// ─── Response navigation tests (US4) ─────────────────────────────────────────

// buildMixedResponse returns a Response with tables + objects at nested paths.
// Data shape:
//
//	core:
//	  users  -> ArrowTable
//	  info   -> map[string]any{"version": "1.2.3"}
//	function:
//	  core:
//	    load_ds -> map[string]any{"success": true}
//	rows -> ArrowTable
func buildMixedResponse(t *testing.T) *Response {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	usersTbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(1)
	})
	rowsTbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(10)
		b.Field(0).(*array.Int32Builder).Append(20)
	})
	return &Response{
		Data: map[string]any{
			"core": map[string]any{
				"users": usersTbl,
				"info":  map[string]any{"version": "1.2.3"},
			},
			"function": map[string]any{
				"core": map[string]any{
					"load_ds": map[string]any{"success": true},
				},
			},
			"rows": rowsTbl,
		},
	}
}

func TestResponse_TablesAndObjects(t *testing.T) {
	r := buildMixedResponse(t)
	defer r.Close()

	tables := r.Tables()
	objects := r.Objects()

	// Tables should contain "core.users" and "rows"; Objects should contain
	// "core.info" and "function.core.load_ds". Each path in exactly one list.
	wantTables := map[string]bool{"core.users": true, "rows": true}
	wantObjects := map[string]bool{"core.info": true, "function.core.load_ds": true}

	seen := map[string]int{}
	for _, p := range tables {
		if !wantTables[p] {
			t.Errorf("unexpected table path %q", p)
		}
		seen[p]++
	}
	for _, p := range objects {
		if !wantObjects[p] {
			t.Errorf("unexpected object path %q", p)
		}
		seen[p]++
	}
	for p := range wantTables {
		if seen[p] != 1 {
			t.Errorf("table %q seen %d times", p, seen[p])
		}
	}
	for p := range wantObjects {
		if seen[p] != 1 {
			t.Errorf("object %q seen %d times", p, seen[p])
		}
	}
}

func TestResponse_Table(t *testing.T) {
	r := buildMixedResponse(t)
	defer r.Close()

	tbl, err := r.Table("core.users")
	if err != nil {
		t.Fatalf("Table core.users: %v", err)
	}
	if tbl == nil {
		t.Fatal("nil ArrowTable returned")
	}

	if _, err := r.Table("core.info"); !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("Table on object path: want ErrWrongDataPath, got %v", err)
	}
	if _, err := r.Table("missing.path"); !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("Table missing path: want ErrWrongDataPath, got %v", err)
	}
}

func TestResponse_ScanObject(t *testing.T) {
	r := buildMixedResponse(t)
	defer r.Close()

	var info struct {
		Version string `json:"version"`
	}
	if err := r.ScanObject("core.info", &info); err != nil {
		t.Fatalf("ScanObject: %v", err)
	}
	if info.Version != "1.2.3" {
		t.Fatalf("version: %q", info.Version)
	}

	// ScanObject on a table path must return ErrWrongDataPath.
	var dst any
	if err := r.ScanObject("core.users", &dst); !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("ScanObject on table path: want ErrWrongDataPath, got %v", err)
	}
}

func TestResponse_ScanTable(t *testing.T) {
	r := buildMixedResponse(t)
	defer r.Close()

	type row struct {
		N int32 `json:"n"`
	}
	var rows []row
	if err := r.ScanTable("rows", &rows); err != nil {
		t.Fatalf("ScanTable: %v", err)
	}
	if len(rows) != 2 || rows[0].N != 10 || rows[1].N != 20 {
		t.Fatalf("rows: %+v", rows)
	}

	// Into []map[string]any.
	var maps []map[string]any
	if err := r.ScanTable("rows", &maps); err != nil {
		t.Fatalf("ScanTable into maps: %v", err)
	}
	if len(maps) != 2 || maps[0]["n"].(int32) != 10 {
		t.Fatalf("maps: %+v", maps)
	}

	// Error cases.
	if err := r.ScanTable("core.info", &rows); !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("ScanTable on object path: want ErrWrongDataPath, got %v", err)
	}
	if err := r.ScanTable("missing", &rows); !errors.Is(err, ErrWrongDataPath) {
		t.Fatalf("ScanTable missing: want ErrWrongDataPath, got %v", err)
	}
}

func TestRowScanner_NonPointerDest(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
	}, nil)
	tbl := newTestTable(t, schema, func(b *array.RecordBuilder) {
		b.Field(0).(*array.Int32Builder).Append(42)
	})
	defer tbl.Release()

	rows, _ := tbl.Rows()
	defer rows.Close()
	rows.Next()

	var s struct {
		N int32 `json:"n"`
	}
	if err := rows.Scan(s); !errors.Is(err, ErrScanNotPointer) {
		t.Fatalf("non-pointer dest: want ErrScanNotPointer, got %v", err)
	}
	if err := rows.Scan(nil); !errors.Is(err, ErrScanNilDest) {
		t.Fatalf("nil dest: want ErrScanNilDest, got %v", err)
	}
}

// ─── US3 cursor lifecycle tests ──────────────────────────────────────────────

func TestRows_MultiBatch(t *testing.T) {
	// 3 batches × 5 rows = 15 rows total, counter 0..14.
	tbl := newMultiBatchTable(t, 3, 5)
	defer tbl.Release()

	rows, err := tbl.Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()

	type row struct {
		N int32 `json:"n"`
	}
	var got []int32
	for rows.Next() {
		var r row
		if err := rows.Scan(&r); err != nil {
			t.Fatalf("Scan row %d: %v", len(got), err)
		}
		got = append(got, r.N)
	}
	if len(got) != 15 {
		t.Fatalf("want 15 rows, got %d", len(got))
	}
	for i, v := range got {
		if int32(i) != v {
			t.Fatalf("row %d: want %d, got %d", i, i, v)
		}
	}
}

func TestRows_CloseMidStream(t *testing.T) {
	tbl := newMultiBatchTable(t, 2, 3)
	defer tbl.Release()

	rows, _ := tbl.Rows()
	// Iterate once then close early.
	if !rows.Next() {
		t.Fatal("expected first Next() true")
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Next() after Close must return false.
	if rows.Next() {
		t.Fatal("Next() after Close must be false")
	}
	// Scan after Close must error with ErrScanClosed.
	var dst struct {
		N int32 `json:"n"`
	}
	if err := rows.Scan(&dst); !errors.Is(err, ErrScanClosed) {
		t.Fatalf("Scan after Close: want ErrScanClosed, got %v", err)
	}
	// Close again is idempotent.
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
