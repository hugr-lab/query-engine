package types

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/paulmach/orb/geojson"
)

// jsonUnmarshalerType is used to detect destinations that supply their own
// JSON decoding (time.Time, types.DateTime, geojson.Geometry via its
// UnmarshalJSON, etc.) — these are leaves from the plan's perspective,
// even when their kind is Struct.
var jsonUnmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()

// scanObject decodes `raw` (the JSON bytes of a Response leaf) into `dest`.
// The plan cache picks between two strategies:
//
//   - If no reachable field of `dest` needs special handling (i.e. no
//     geometry interface types), fall through to stdlib json.Unmarshal.
//     This is the fast path — no behaviour change vs. the pre-refactor
//     ScanObject for plain structs.
//
//   - Otherwise, walk the precomputed plan and decode each geometry-
//     bearing field via geojson.UnmarshalGeometry + assignGeometry, and
//     delegate every non-geometry subtree to json.Unmarshal.
//
// dest must be a non-nil pointer (the caller's ScanObject / Scan[T]
// enforces this at the entry point; scanObject re-validates defensively).
func scanObject(raw []byte, dest any) error {
	if dest == nil {
		return ErrScanNilDest
	}
	pv := reflect.ValueOf(dest)
	if pv.Kind() != reflect.Pointer || pv.IsNil() {
		return ErrScanNotPointer
	}
	plan := planFor(pv.Elem().Type())
	if !plan.needsCustomDecode {
		return json.Unmarshal(raw, dest)
	}
	return decodePlan(raw, pv.Elem(), plan)
}

// objectPlan is an immutable, cached reflection plan for one destination
// type. It records whether the type's reachable tree contains any
// geometry field (needsCustomDecode) and, when it does, how to walk the
// target alongside the incoming JSON.
type objectPlan struct {
	// needsCustomDecode is true when at least one reachable field requires
	// geometry-aware handling; false types use the stdlib fast path.
	needsCustomDecode bool
	kind              planKind
	fields            []planField  // populated when kind == pkStruct
	elem              *objectPlan  // populated for pkSlice / pkArray / pkMap / pkPointer
}

type planKind uint8

const (
	pkLeaf planKind = iota
	pkGeometry
	pkStruct
	pkSlice
	pkArray
	pkMap
	pkPointer
)

type planField struct {
	// index is the reflect field index path; supports embedded anonymous
	// fields that get json-tag-promoted into the parent struct.
	index []int
	// name is the json-tag name (or the field name if no tag was set).
	name string
	plan *objectPlan
}

var objectPlanCache sync.Map // reflect.Type → *objectPlan

// planFor returns the (possibly cached) plan for t. Safe for concurrent
// use: plan construction may race for the same key but is deterministic,
// so worst case the losing racer discards its newly-built plan.
func planFor(t reflect.Type) *objectPlan {
	if p, ok := objectPlanCache.Load(t); ok {
		return p.(*objectPlan)
	}
	built := buildPlan(t, map[reflect.Type]*objectPlan{})
	actual, _ := objectPlanCache.LoadOrStore(t, built)
	return actual.(*objectPlan)
}

// buildPlan recurses into t's type tree and produces the plan.
// `seen` breaks cycles on self-referential struct types (e.g. linked
// list nodes) — when we hit the same type again we return the
// previously-built plan.
func buildPlan(t reflect.Type, seen map[reflect.Type]*objectPlan) *objectPlan {
	if existing, ok := seen[t]; ok {
		return existing
	}

	// Geometry leaf — the whole type is a geometry target.
	if isGeometryDest(t) {
		return &objectPlan{kind: pkGeometry, needsCustomDecode: true}
	}

	// Types that implement json.Unmarshaler handle their own decoding;
	// treat as an opaque leaf so stdlib stays in charge (time.Time,
	// types.DateTime, custom types, etc.).
	if t.Implements(jsonUnmarshalerType) || reflect.PointerTo(t).Implements(jsonUnmarshalerType) {
		return &objectPlan{kind: pkLeaf}
	}

	switch t.Kind() {
	case reflect.Pointer:
		elem := buildPlan(t.Elem(), seen)
		return &objectPlan{
			kind:              pkPointer,
			elem:              elem,
			needsCustomDecode: elem.needsCustomDecode,
		}

	case reflect.Slice, reflect.Array:
		// []byte → treat as leaf (stdlib handles base64 decoding).
		if t.Elem().Kind() == reflect.Uint8 {
			return &objectPlan{kind: pkLeaf}
		}
		elem := buildPlan(t.Elem(), seen)
		kind := pkSlice
		if t.Kind() == reflect.Array {
			kind = pkArray
		}
		return &objectPlan{
			kind:              kind,
			elem:              elem,
			needsCustomDecode: elem.needsCustomDecode,
		}

	case reflect.Map:
		// Only map[string]T — anything else falls back to stdlib json.
		if t.Key().Kind() != reflect.String {
			return &objectPlan{kind: pkLeaf}
		}
		elem := buildPlan(t.Elem(), seen)
		return &objectPlan{
			kind:              pkMap,
			elem:              elem,
			needsCustomDecode: elem.needsCustomDecode,
		}

	case reflect.Struct:
		p := &objectPlan{kind: pkStruct}
		seen[t] = p
		collectStructFields(t, nil, p, seen)
		return p

	default:
		return &objectPlan{kind: pkLeaf}
	}
}

// collectStructFields walks a struct type (including anonymous embedded
// structs whose json-tag-promoted fields appear flat in the JSON) and
// populates plan.fields. Anonymous fields without a json tag are
// flattened; anonymous fields with a tag are treated as regular named
// fields. Non-exported fields are skipped (matching encoding/json).
func collectStructFields(t reflect.Type, parentIndex []int, plan *objectPlan, seen map[reflect.Type]*objectPlan) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		idx := make([]int, len(parentIndex)+1)
		copy(idx, parentIndex)
		idx[len(parentIndex)] = i

		// Anonymous embedded structs — always recurse to pick up promoted
		// exported fields (even when the embedded type itself has a
		// lowercase name). Non-anonymous unexported fields are skipped.
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			if jsonFieldName(f) == "" {
				collectStructFields(f.Type, idx, plan, seen)
				continue
			}
			// Embedded struct with an explicit json tag — treat as a
			// regular named field below.
		}
		if !f.IsExported() {
			continue
		}

		name := jsonFieldName(f)
		if name == "-" {
			continue
		}
		if name == "" {
			name = f.Name
		}
		sub := buildPlan(f.Type, seen)
		plan.fields = append(plan.fields, planField{
			index: idx,
			name:  name,
			plan:  sub,
		})
		if sub.needsCustomDecode {
			plan.needsCustomDecode = true
		}
	}
}

// decodePlan walks `raw` alongside dst/plan, dispatching per-plan-kind.
// Any error is wrapped with context so the caller can localise the
// failing field.
func decodePlan(raw []byte, dst reflect.Value, plan *objectPlan) error {
	switch plan.kind {
	case pkLeaf:
		return json.Unmarshal(raw, dst.Addr().Interface())

	case pkGeometry:
		return decodeGeometry(raw, dst)

	case pkPointer:
		return decodePlanPointer(raw, dst, plan)

	case pkSlice, pkArray:
		return decodePlanSliceArray(raw, dst, plan)

	case pkMap:
		return decodePlanMap(raw, dst, plan)

	case pkStruct:
		return decodePlanStruct(raw, dst, plan)
	}
	return json.Unmarshal(raw, dst.Addr().Interface())
}

// decodeGeometry handles the three wire shapes for a geometry value:
//   - "null"                            → leave dst at its zero value
//   - "{...}"                           → a GeoJSON object; decode directly
//   - "\"{...}\""                       → a quoted GeoJSON string; decode inner
func decodeGeometry(raw []byte, dst reflect.Value) error {
	// Trim JSON whitespace to find the first meaningful byte.
	i := 0
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
		i++
	}
	if i == len(raw) {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	// Null → zero value.
	if raw[i] == 'n' {
		// stdlib json.Unmarshal on "null" leaves the destination as-is,
		// matching our intent.
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}

	var src []byte
	if raw[i] == '"' {
		// Quoted JSON string — unquote to inner GeoJSON bytes.
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("%w: unquoting GeoJSON string: %s", ErrGeometryDecode, err.Error())
		}
		src = []byte(s)
	} else {
		src = raw
	}
	g, err := geojson.UnmarshalGeometry(src)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrGeometryDecode, err.Error())
	}
	if g == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	return assignGeometry(g.Geometry(), dst.Type(), dst)
}

func decodePlanPointer(raw []byte, dst reflect.Value, plan *objectPlan) error {
	// "null" → nil pointer.
	i := 0
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
		i++
	}
	if i < len(raw) && raw[i] == 'n' {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	if dst.IsNil() {
		dst.Set(reflect.New(dst.Type().Elem()))
	}
	return decodePlan(raw, dst.Elem(), plan.elem)
}

func decodePlanSliceArray(raw []byte, dst reflect.Value, plan *objectPlan) error {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return fmt.Errorf("slice/array: %w", err)
	}
	if plan.kind == pkSlice {
		// Grow target slice to len(parts).
		dst.Set(reflect.MakeSlice(dst.Type(), len(parts), len(parts)))
	}
	for i, p := range parts {
		if i >= dst.Len() {
			// Fixed-size array overflow — silently drop extras, matching
			// encoding/json behaviour.
			break
		}
		if err := decodePlan(p, dst.Index(i), plan.elem); err != nil {
			return fmt.Errorf("index %d: %w", i, err)
		}
	}
	return nil
}

func decodePlanMap(raw []byte, dst reflect.Value, plan *objectPlan) error {
	var parts map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return fmt.Errorf("map: %w", err)
	}
	if dst.IsNil() {
		dst.Set(reflect.MakeMapWithSize(dst.Type(), len(parts)))
	}
	elemType := dst.Type().Elem()
	for k, v := range parts {
		entry := reflect.New(elemType).Elem()
		if err := decodePlan(v, entry, plan.elem); err != nil {
			return fmt.Errorf("key %q: %w", k, err)
		}
		dst.SetMapIndex(reflect.ValueOf(k), entry)
	}
	return nil
}

func decodePlanStruct(raw []byte, dst reflect.Value, plan *objectPlan) error {
	// "null" top-level → zero the destination.
	i := 0
	for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
		i++
	}
	if i < len(raw) && raw[i] == 'n' {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	var parts map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return fmt.Errorf("struct: %w", err)
	}
	for _, f := range plan.fields {
		sub, ok := parts[f.name]
		if !ok {
			// Absent field → leave zero. Matches encoding/json.
			continue
		}
		if err := decodePlan(sub, dst.FieldByIndex(f.index), f.plan); err != nil {
			return fmt.Errorf("field %q: %w", f.name, err)
		}
	}
	return nil
}
