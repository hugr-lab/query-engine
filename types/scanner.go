// Arrow-native scanner — see scanner.go, scanner_convert.go,
// scanner_geometry.go, scanner_response.go. Use Response.ScanTable /
// Response.Rows / ArrowTable.Rows to read Arrow results directly into Go
// structs without a JSON round-trip. Struct fields are resolved by the
// standard `json` tag. Timestamps preserve unit + timezone metadata;
// geometry columns decode to orb.Geometry for every encoding the engine
// emits. Callers can plug in custom geometry encodings via
// RegisterGeometryDecoder.

package types

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Rows is a cursor over the rows of an ArrowTable. The shape follows
// database/sql.Rows. A Rows is NOT safe for concurrent use across goroutines.
//
// Typical use:
//
//	tbl, _ := resp.Table("core.users")
//	rows, _ := tbl.Rows()
//	defer rows.Close()
//	for rows.Next() {
//	    var u User
//	    if err := rows.Scan(&u); err != nil { return err }
//	    // ...
//	}
//	if err := rows.Err(); err != nil { return err }
type Rows interface {
	// Next advances the cursor to the next row. Returns false at end-of-stream
	// or on error; inspect Err() to distinguish. Calling Next after it returned
	// false is safe and returns false.
	Next() bool

	// Scan copies the current row into dest. dest MUST be a non-nil pointer to
	// a struct, a map[string]any, a slice []any, or interface{}.
	// Struct fields are resolved by `json` tag (matching Response.ScanData).
	Scan(dest any) error

	// Err returns the first error encountered during iteration, if any.
	Err() error

	// Close releases any Arrow resources held by the cursor. Idempotent.
	// Reaching end-of-stream via Next auto-releases the last retained batch;
	// explicit Close is required only for early exit from the iteration.
	Close() error

	// Columns returns the Arrow schema column names in positional order
	// (matches the []any Scan destination order).
	Columns() []string
}

var _ Rows = (*rowScanner)(nil)

// Scanner state errors. All are wrapped by Rows.Scan when the call site
// violates the cursor contract; use errors.Is to match.
var (
	// ErrScanBeforeNext is returned when Scan is called before the first Next.
	ErrScanBeforeNext = errors.New("types: Scan called before Next")
	// ErrScanAfterEnd is returned when Scan is called after Next returned false.
	ErrScanAfterEnd = errors.New("types: Scan called after end of stream")
	// ErrScanClosed is returned when any operation is attempted on a closed Rows.
	ErrScanClosed = errors.New("types: Rows is closed")
	// ErrScanNilDest is returned when Scan is passed a nil destination.
	ErrScanNilDest = errors.New("types: Scan dest is nil")
	// ErrScanNotPointer is returned when Scan's dest is not a non-nil pointer.
	ErrScanNotPointer = errors.New("types: Scan dest must be a non-nil pointer")
)

// rowScanner is the concrete cursor implementation.
// Lifecycle:
//   - created by ArrowTable.Rows(). reader is retained.
//   - Next() advances to next row; auto-loads next batch when current batch is
//     exhausted. When the reader returns false, rec is released.
//   - Close() releases any retained batch. Idempotent.
type rowScanner struct {
	reader array.RecordReader

	rec    arrow.RecordBatch // current batch, retained; nil before first Next()
	nrows  int64             // cached rec.NumRows()
	row    int               // index of the current row (row that Scan will read); -1 before first Next
	schema *arrow.Schema     // schema of the most recent batch (stable across batches in practice)
	cols   []string          // column names in schema order
	colIdx map[string]int    // name → index

	plans map[reflect.Type]*structPlan // per-destination-struct-type plan cache

	err       error
	closed    bool
	exhausted bool // true after reader.Next() returned false
}

func newRowScanner(r array.RecordReader) (*rowScanner, error) {
	if r == nil {
		return nil, errors.New("types: nil RecordReader")
	}
	s := &rowScanner{
		reader: r,
		row:    -1,
		plans:  make(map[reflect.Type]*structPlan),
	}
	// Initialise column metadata lazily once the first batch is read;
	// the reader's schema may be known upfront though, so use it when present.
	if sch := r.Schema(); sch != nil {
		s.setSchema(sch)
	}
	return s, nil
}

func (s *rowScanner) setSchema(sch *arrow.Schema) {
	if s.schema == sch {
		return
	}
	s.schema = sch
	fields := sch.Fields()
	s.cols = make([]string, len(fields))
	s.colIdx = make(map[string]int, len(fields))
	for i, f := range fields {
		s.cols[i] = f.Name
		s.colIdx[f.Name] = i
	}
	// Schema changed: invalidate plan cache because column positions may differ.
	s.plans = make(map[reflect.Type]*structPlan)
}

func (s *rowScanner) Columns() []string {
	// Return a copy to keep callers from mutating our internal state.
	if len(s.cols) == 0 {
		return nil
	}
	out := make([]string, len(s.cols))
	copy(out, s.cols)
	return out
}

func (s *rowScanner) Err() error { return s.err }

func (s *rowScanner) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.rec != nil {
		s.rec.Release()
		s.rec = nil
	}
	if s.reader != nil {
		s.reader.Release()
		s.reader = nil
	}
	return nil
}

func (s *rowScanner) Next() bool {
	if s.closed || s.err != nil || s.exhausted {
		return false
	}
	// Still rows in current batch?
	if s.rec != nil && int64(s.row+1) < s.nrows {
		s.row++
		return true
	}
	// Need next batch.
	if s.rec != nil {
		s.rec.Release()
		s.rec = nil
	}
	for s.reader.Next() {
		if e := s.reader.Err(); e != nil {
			s.err = e
			return false
		}
		rec := s.reader.RecordBatch()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}
		rec.Retain()
		s.rec = rec
		s.nrows = rec.NumRows()
		s.setSchema(rec.Schema())
		s.row = 0
		return true
	}
	if e := s.reader.Err(); e != nil {
		s.err = e
	}
	s.exhausted = true
	// Auto-release reader on end-of-stream per research.md R3.
	if s.reader != nil {
		s.reader.Release()
		s.reader = nil
	}
	return false
}

func (s *rowScanner) Scan(dest any) error {
	if s.closed {
		return ErrScanClosed
	}
	if s.err != nil {
		return s.err
	}
	if s.rec == nil {
		if s.exhausted {
			return ErrScanAfterEnd
		}
		return ErrScanBeforeNext
	}
	if dest == nil {
		return ErrScanNilDest
	}

	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return ErrScanNotPointer
	}
	target := v.Elem()

	switch target.Kind() {
	case reflect.Struct:
		return s.scanIntoStruct(target)
	case reflect.Map:
		return s.scanIntoMap(target)
	case reflect.Slice:
		return s.scanIntoSlice(target)
	case reflect.Interface:
		return s.scanIntoInterface(target)
	default:
		return fmt.Errorf("types: unsupported Scan dest kind %s", target.Kind())
	}
}

// scanIntoStruct uses the per-destination-type plan cache.
func (s *rowScanner) scanIntoStruct(target reflect.Value) error {
	t := target.Type()
	plan, ok := s.plans[t]
	if !ok {
		p, err := buildStructPlan(t, s.schema)
		if err != nil {
			return err
		}
		plan = p
		s.plans[t] = plan
	}
	for _, fm := range plan.fields {
		dst := target.FieldByIndex(fm.fieldPath)
		col := s.rec.Column(fm.colIdx)
		if err := fm.convert(col, s.row, dst); err != nil {
			name := s.cols[fm.colIdx]
			return fmt.Errorf("scan column %q row %d: %w", name, s.row, err)
		}
	}
	return nil
}

// scanIntoMap populates map[string]any with raw-default conversion per column.
func (s *rowScanner) scanIntoMap(target reflect.Value) error {
	if target.Type().Key().Kind() != reflect.String {
		return fmt.Errorf("types: Scan map key must be string, got %s", target.Type().Key().Kind())
	}
	if target.IsNil() {
		target.Set(reflect.MakeMapWithSize(target.Type(), len(s.cols)))
	}
	elemType := target.Type().Elem()
	wantsAny := elemType.Kind() == reflect.Interface && elemType.NumMethod() == 0

	for i, name := range s.cols {
		col := s.rec.Column(i)
		if col.IsNull(s.row) {
			if wantsAny {
				target.SetMapIndex(reflect.ValueOf(name), reflect.Zero(elemType))
			} else {
				target.SetMapIndex(reflect.ValueOf(name), reflect.Zero(elemType))
			}
			continue
		}
		val := defaultConvertValue(col, s.row, s.schema.Field(i))
		if wantsAny {
			if val == nil {
				target.SetMapIndex(reflect.ValueOf(name), reflect.Zero(elemType))
			} else {
				target.SetMapIndex(reflect.ValueOf(name), reflect.ValueOf(val))
			}
			continue
		}
		// typed map element — try best-effort conversion via reflect
		dst := reflect.New(elemType).Elem()
		if err := convertIntoValue(col, s.row, s.schema.Field(i), dst); err != nil {
			return fmt.Errorf("scan column %q row %d: %w", name, s.row, err)
		}
		target.SetMapIndex(reflect.ValueOf(name), dst)
	}
	return nil
}

// scanIntoSlice populates []any positionally from each column.
func (s *rowScanner) scanIntoSlice(target reflect.Value) error {
	elemType := target.Type().Elem()
	wantsAny := elemType.Kind() == reflect.Interface && elemType.NumMethod() == 0
	if !wantsAny {
		return fmt.Errorf("types: Scan into slice requires []any, got []%s", elemType)
	}
	out := reflect.MakeSlice(target.Type(), len(s.cols), len(s.cols))
	for i := range s.cols {
		col := s.rec.Column(i)
		if col.IsNull(s.row) {
			continue
		}
		val := defaultConvertValue(col, s.row, s.schema.Field(i))
		if val != nil {
			out.Index(i).Set(reflect.ValueOf(val))
		}
	}
	target.Set(out)
	return nil
}

// emptyRows is returned when ArrowTable.Rows() is called on a table with no
// records. Next() always returns false; Scan errors out.
type emptyRows struct{}

func (emptyRows) Next() bool           { return false }
func (emptyRows) Scan(dest any) error  { return ErrScanAfterEnd }
func (emptyRows) Err() error           { return nil }
func (emptyRows) Close() error         { return nil }
func (emptyRows) Columns() []string    { return nil }

// scanIntoInterface fills *any with a map[string]any of the row.
func (s *rowScanner) scanIntoInterface(target reflect.Value) error {
	m := make(map[string]any, len(s.cols))
	for i, name := range s.cols {
		col := s.rec.Column(i)
		if col.IsNull(s.row) {
			m[name] = nil
			continue
		}
		m[name] = defaultConvertValue(col, s.row, s.schema.Field(i))
	}
	target.Set(reflect.ValueOf(m))
	return nil
}
