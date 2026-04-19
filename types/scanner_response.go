package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// Tables returns every dotted path in Response.Data that points to an
// ArrowTable value. Paths use the same dot syntax as ScanData. Order is
// non-deterministic (depends on Go map iteration).
func (r *Response) Tables() []string {
	tables, _ := classifyPaths(r)
	return tables
}

// Objects returns every dotted path in Response.Data that points to a leaf
// value that is NOT an ArrowTable — most commonly a *types.JsonValue emitted
// for object-shaped GraphQL selections (by_pk results, function calls,
// etc.), but also raw scalars or slices. map[string]any values are treated
// as namespaces and descended into. Together with Tables this covers every
// meaningful leaf in the response tree.
func (r *Response) Objects() []string {
	_, objects := classifyPaths(r)
	return objects
}

func classifyPaths(r *Response) (tables, objects []string) {
	if r == nil || r.Data == nil {
		return nil, nil
	}
	walkForPaths(r.Data, "", &tables, &objects)
	return tables, objects
}

// walkForPaths walks the response tree under the simplest rule that matches
// how the engine actually shapes responses:
//
//   - map[string]any is always a namespace — recurse into it.
//   - ArrowTable child → add to tables.
//   - Anything else (*JsonValue, scalars, slices, other types) → add to objects.
//
// The engine wraps non-table, non-namespace results in *types.JsonValue
// (see writeJsonValueToIPC / the IPC client decoder), so this catches them
// without needing a special case. If a caller builds a response with a
// map-valued leaf in hand, they should scan the inner scalars one level
// deeper; use ScanObject on any path whose leaf you want to materialise.
func walkForPaths(v any, prefix string, tables, objects *[]string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	for k, val := range m {
		path := joinPath(prefix, k)
		if _, isTable := val.(ArrowTable); isTable {
			*tables = append(*tables, path)
			continue
		}
		if _, isMap := val.(map[string]any); isMap {
			walkForPaths(val, path, tables, objects)
			continue
		}
		*objects = append(*objects, path)
	}
}

func joinPath(prefix, k string) string {
	if prefix == "" {
		return k
	}
	return prefix + "." + k
}

// Table returns the ArrowTable at path, or ErrWrongDataPath if the path is
// missing or the leaf is not an ArrowTable. The returned table is still owned
// by the response; callers must not Release it.
func (r *Response) Table(path string) (ArrowTable, error) {
	if r == nil || r.Data == nil {
		return nil, ErrNoData
	}
	v := ExtractResponseData(path, r.Data)
	if v == nil {
		return nil, ErrWrongDataPath
	}
	t, ok := v.(ArrowTable)
	if !ok {
		return nil, ErrWrongDataPath
	}
	return t, nil
}

// Scan is a generic version of ScanObject and ScanTable. If T is a slice type,
// it behaves like ScanTable; otherwise, it behaves like ScanObject.
// See ScanObject and ScanTable for details and error conditions.
func Scan[T any](resp *Response, path string) (T, error) {
	var zero T

	if resp == nil {
		return zero, errors.New("response is nil")
	}

	// If T is slice use ScanTable, otherwise use ScanObject.
	if t := reflect.TypeOf(any(zero)); t != nil && t.Kind() == reflect.Slice {
		var dest T
		err := resp.ScanTable(path, &dest)
		if err != nil {
			return zero, err
		}
		return dest, nil
	}

	var dest T
	err := resp.ScanObject(path, &dest)
	if err != nil {
		return zero, err
	}

	return dest, nil
}

// ScanObject behaves like ScanData for non-table paths; returns
// ErrWrongDataPath if the path refers to an ArrowTable.
func (r *Response) ScanObject(path string, dest any) error {
	if r == nil || r.Data == nil {
		return ErrNoData
	}
	v := ExtractResponseData(path, r.Data)
	if v == nil {
		return ErrWrongDataPath
	}
	if _, isTable := v.(ArrowTable); isTable {
		return ErrWrongDataPath
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dest)
}

// ScanTable reads every row of the ArrowTable at path into dest. dest MUST be
// a non-nil pointer to a slice (struct elements or map[string]any elements).
// On mid-stream error the slice is left truncated to the prefix of
// successfully-scanned rows and the error is returned.
func (r *Response) ScanTable(path string, dest any) error {
	tbl, err := r.Table(path)
	if err != nil {
		return err
	}
	return scanTableInto(tbl, dest)
}

func scanTableInto(tbl ArrowTable, dest any) error {
	if dest == nil {
		return ErrScanNilDest
	}
	pv := reflect.ValueOf(dest)
	if pv.Kind() != reflect.Pointer || pv.IsNil() {
		return ErrScanNotPointer
	}
	sv := pv.Elem()
	if sv.Kind() != reflect.Slice {
		return fmt.Errorf("types: ScanTable dest must be a pointer to a slice, got pointer to %s", sv.Kind())
	}
	elemType := sv.Type().Elem()

	rows, err := tbl.Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	// Truncate the slice to zero before filling (matches user expectations).
	sv.SetLen(0)
	for rows.Next() {
		// Allocate a new element, Scan into it, then append.
		elemPtr := reflect.New(elemType)
		if err := rows.Scan(elemPtr.Interface()); err != nil {
			return err
		}
		sv.Set(reflect.Append(sv, elemPtr.Elem()))
	}
	return rows.Err()
}
