package types

import (
	"encoding/json"
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

// Objects returns every dotted path in Response.Data that points to a
// "leaf object" — a map that carries data fields (at least one non-map,
// non-ArrowTable direct child) rather than pure namespacing. Together with
// Tables this covers every meaningful leaf in the response.
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

// walkForPaths walks the response tree. A map is treated as a "namespace"
// when every direct child is itself a map or an ArrowTable — in that case we
// recurse into child maps and emit child tables. Any other map is a "leaf
// object" and is emitted via the objects list; tables nested inside that
// object (if any) are still recorded as table paths so they can be scanned.
func walkForPaths(v any, prefix string, tables, objects *[]string) {
	m, ok := v.(map[string]any)
	if !ok {
		return
	}
	if isNamespaceMap(m) {
		for k, val := range m {
			path := joinPath(prefix, k)
			if _, isTable := val.(ArrowTable); isTable {
				*tables = append(*tables, path)
				continue
			}
			walkForPaths(val, path, tables, objects)
		}
		return
	}
	// Leaf object: emit this path, then record table children (one level).
	if prefix != "" {
		*objects = append(*objects, prefix)
	}
	for k, val := range m {
		if _, isTable := val.(ArrowTable); isTable {
			*tables = append(*tables, joinPath(prefix, k))
		}
	}
}

func isNamespaceMap(m map[string]any) bool {
	if len(m) == 0 {
		return false
	}
	for _, v := range m {
		if _, isMap := v.(map[string]any); isMap {
			continue
		}
		if _, isTable := v.(ArrowTable); isTable {
			continue
		}
		return false
	}
	return true
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
