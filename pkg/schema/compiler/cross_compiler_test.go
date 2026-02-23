package compiler_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	oldcompiler "github.com/hugr-lab/query-engine/pkg/compiler"
	newcompiler "github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/rules"
	_ "github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// complexTestSchema covers all compilation variants:
// - @table with @pk (single and composite)
// - @table with @references (foreign keys)
// - @table with @unique constraints
// - @view (read-only)
// - @table with is_m2m (many-to-many join table)
// - @module (module hierarchy)
// - Field types: Int, String, Float, Boolean, BigInt, Timestamp, Date, JSON, Geometry
// - @field_references (field-level references)
// - @sql computed fields
// - @default directives
const complexTestSchema = `
"""Airport database - comprehensive test schema"""
type Airport
  @table(name: "airports") {
  """Airport IATA code"""
  iata_code: String! @pk
  """Airport name"""
  name: String!
  """City name"""
  city: String!
  """Country ISO code"""
  country: String!
  """Geographic location"""
  geom: Geometry
  """Latitude"""
  latitude: Float
  """Longitude"""
  longitude: Float
  """Elevation in feet"""
  elevation: Int
  """Timezone"""
  timezone: String
  """Last updated"""
  updated_at: Timestamp
}

"""Flight routes between airports"""
type Route
  @table(name: "routes")
  @unique(fields: ["src_airport", "dst_airport", "airline"], query_suffix: "route") {
  """Route ID"""
  id: Int! @pk
  """Airline code"""
  airline: String!
  """Source airport IATA"""
  src_airport: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "source_airport"
    references_query: "departing_routes"
  )
  """Destination airport IATA"""
  dst_airport: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "destination_airport"
    references_query: "arriving_routes"
  )
  """Number of stops"""
  stops: Int!
  """Active flag"""
  is_active: Boolean @default(value: true)
}

"""Airline companies"""
type Airline
  @table(name: "airlines") {
  """ICAO code"""
  icao: String! @pk
  """Airline name"""
  name: String!
  """Country of registration"""
  country: String
  """Active flag"""
  active: Boolean @default(value: true)
  """Founded date"""
  founded: Date
  """Fleet size"""
  fleet_size: BigInt
  """Extra data"""
  metadata: JSON
}

"""Passenger records"""
type Passenger
  @table(name: "passengers")
  @unique(fields: ["email"], query_suffix: "email") {
  id: Int! @pk
  first_name: String!
  last_name: String!
  email: String!
  booking_time: Timestamp
}

"""Flight schedule — read-only view"""
type FlightSchedule
  @view(name: "flight_schedule_view") {
  id: Int! @pk
  flight_number: String!
  departure_time: Timestamp
  arrival_time: Timestamp
  airline: String
  status: String
}

"""Airport statistics — read-only view with aggregation-friendly types"""
type AirportStats
  @view(name: "airport_stats_view") {
  airport_code: String! @pk
  total_departures: BigInt
  total_arrivals: BigInt
  avg_delay: Float
  last_updated: Timestamp
}

"""Booking ↔ Passenger join table (M2M)"""
type BookingPassenger
  @table(name: "booking_passengers", is_m2m: true)
  @references(
    name: "booking_airline"
    references_name: "Airline"
    source_fields: ["airline_icao"]
    references_fields: ["icao"]
    query: "airline"
    references_query: "booking_passengers"
  )
  @references(
    name: "booking_passenger"
    references_name: "Passenger"
    source_fields: ["passenger_id"]
    references_fields: ["id"]
    query: "passenger"
    references_query: "bookings"
  ) {
  airline_icao: String! @pk
  passenger_id: Int! @pk
}
`

// parseSourceSchemaDocument parses SDL into a SchemaDocument for the OLD compiler.
func parseSourceSchemaDocument(t *testing.T, sdl string) *ast.SchemaDocument {
	t.Helper()
	sd, err := parser.ParseSchema(&ast.Source{Name: "test-schema.graphql", Input: sdl})
	if err != nil {
		t.Fatalf("failed to parse schema: %v", err)
	}
	return sd
}

// extractSourceDefs extracts user definitions from a SchemaDocument for the NEW compiler.
func extractSourceDefs(sd *ast.SchemaDocument) *testSource {
	var defs []*ast.Definition
	for _, def := range sd.Definitions {
		// Skip schema definition types, only take user objects
		if def.Kind == ast.Object || def.Kind == ast.InputObject || def.Kind == ast.Enum {
			defs = append(defs, def)
		}
	}
	return &testSource{defs: defs}
}

// --- AST serialization helpers ---

// typeString serializes an ast.Type to a human-readable string like "String!", "[Int!]!", etc.
func typeString(t *ast.Type) string {
	if t == nil {
		return "<nil>"
	}
	if t.Elem != nil {
		s := "[" + typeString(t.Elem) + "]"
		if t.NonNull {
			s += "!"
		}
		return s
	}
	s := t.NamedType
	if t.NonNull {
		s += "!"
	}
	return s
}

// valueString serializes an ast.Value (including list values) to a string.
func valueString(v *ast.Value) string {
	if v == nil {
		return ""
	}
	if len(v.Children) > 0 {
		var parts []string
		for _, child := range v.Children {
			if child.Value != nil {
				parts = append(parts, child.Value.Raw)
			}
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	return v.Raw
}

// directiveSignature returns a comparable string for a single directive, e.g. "table(name=airports)".
func directiveSignature(d *ast.Directive) string {
	var args []string
	for _, a := range d.Arguments {
		args = append(args, a.Name+"="+valueString(a.Value))
	}
	sort.Strings(args)
	if len(args) > 0 {
		return d.Name + "(" + strings.Join(args, ", ") + ")"
	}
	return d.Name
}

// directiveSignatures returns sorted comparable strings for a directive list.
func directiveSignatures(dirs ast.DirectiveList) []string {
	var sigs []string
	for _, d := range dirs {
		sigs = append(sigs, directiveSignature(d))
	}
	sort.Strings(sigs)
	return sigs
}

// argSignature returns a comparable string for a field argument, e.g. "filter: Airport_filter".
func argSignature(a *ast.ArgumentDefinition) string {
	s := a.Name + ": " + typeString(a.Type)
	if a.DefaultValue != nil {
		s += " = " + a.DefaultValue.Raw
	}
	return s
}

// argsSignatures returns sorted comparable strings for argument definitions.
func argsSignatures(args ast.ArgumentDefinitionList) []string {
	var sigs []string
	for _, a := range args {
		sigs = append(sigs, argSignature(a))
	}
	sort.Strings(sigs)
	return sigs
}

// fieldSignature returns a full comparable string for a field definition, e.g.:
// "Airport(filter: Airport_filter, limit: Int): [Airport!]! @query(name=Airport, type=SELECT)"
func fieldSignature(f *ast.FieldDefinition) string {
	var sb strings.Builder
	sb.WriteString(f.Name)
	if len(f.Arguments) > 0 {
		args := argsSignatures(f.Arguments)
		sb.WriteString("(")
		sb.WriteString(strings.Join(args, ", "))
		sb.WriteString(")")
	}
	sb.WriteString(": ")
	sb.WriteString(typeString(f.Type))
	dirs := directiveSignatures(f.Directives)
	for _, d := range dirs {
		sb.WriteString(" @")
		sb.WriteString(d)
	}
	return sb.String()
}

// --- compileResult ---

// compileResult holds comparison data for one compiler output.
type compileResult struct {
	name string
	// definitions by name (for NEW: extensions merged into definitions)
	defs map[string]*ast.Definition
	// query fields by name with full FieldDefinition
	queryFieldDefs map[string]*ast.FieldDefinition
	// mutation fields by name with full FieldDefinition
	mutFieldDefs map[string]*ast.FieldDefinition
}

func (r *compileResult) queryFieldNames() []string {
	names := make([]string, 0, len(r.queryFieldDefs))
	for n := range r.queryFieldDefs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (r *compileResult) mutationFieldNames() []string {
	names := make([]string, 0, len(r.mutFieldDefs))
	for n := range r.mutFieldDefs {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (r *compileResult) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "=== %s ===\n", r.name)

	defNames := sortedKeys(r.defs)
	fmt.Fprintf(&sb, "Definitions (%d):\n", len(defNames))
	for _, n := range defNames {
		d := r.defs[n]
		dirs := directiveSignatures(d.Directives)
		fmt.Fprintf(&sb, "  %s [%s]", n, d.Kind)
		if len(dirs) > 0 {
			fmt.Fprintf(&sb, " @%s", strings.Join(dirs, " @"))
		}
		fmt.Fprintf(&sb, "\n")
		for _, f := range d.Fields {
			fmt.Fprintf(&sb, "    %s\n", fieldSignature(f))
		}
	}

	qNames := r.queryFieldNames()
	fmt.Fprintf(&sb, "Query fields (%d):\n", len(qNames))
	for _, n := range qNames {
		fmt.Fprintf(&sb, "  %s\n", fieldSignature(r.queryFieldDefs[n]))
	}

	mNames := r.mutationFieldNames()
	fmt.Fprintf(&sb, "Mutation fields (%d):\n", len(mNames))
	for _, n := range mNames {
		fmt.Fprintf(&sb, "  %s\n", fieldSignature(r.mutFieldDefs[n]))
	}

	return sb.String()
}

// --- Builders ---

func isSkipQueryField(name string) bool {
	return name == "_stub" || name == "jq" || name == "__type" || name == "__schema" || name == "function"
}

func isSkipMutField(name string) bool {
	return name == "_stub" || name == "function"
}

// buildOldCompilerResult compiles with old compiler and extracts comparison data.
func buildOldCompilerResult(t *testing.T, sdl string, opts oldcompiler.Options) *compileResult {
	t.Helper()
	sd := parseSourceSchemaDocument(t, sdl)
	schema, err := oldcompiler.Compile(sd, opts)
	if err != nil {
		t.Fatalf("old compiler failed: %v", err)
	}

	result := &compileResult{
		name:           "OLD",
		defs:           make(map[string]*ast.Definition),
		queryFieldDefs: make(map[string]*ast.FieldDefinition),
		mutFieldDefs:   make(map[string]*ast.FieldDefinition),
	}

	// Collect all non-system types
	for name, def := range schema.Types {
		if isBaseSystemType(name) {
			continue
		}
		result.defs[name] = def
	}

	// Extract Query fields
	if schema.Query != nil {
		for _, f := range schema.Query.Fields {
			if isSkipQueryField(f.Name) {
				continue
			}
			result.queryFieldDefs[f.Name] = f
		}
	}

	// Extract Mutation fields
	if schema.Mutation != nil {
		for _, f := range schema.Mutation.Fields {
			if isSkipMutField(f.Name) {
				continue
			}
			result.mutFieldDefs[f.Name] = f
		}
	}

	return result
}

// buildNewCompilerResult compiles with new compiler and extracts comparison data.
// Extensions are merged into definitions for fair comparison with the old compiler.
func buildNewCompilerResult(t *testing.T, sdl string, opts base.Options) *compileResult {
	t.Helper()
	sd := parseSourceSchemaDocument(t, sdl)
	source := extractSourceDefs(sd)
	ctx := context.Background()

	c := newcompiler.New(rules.RegisterAll()...)
	catalog, err := c.Compile(ctx, nil, source, opts)
	if err != nil {
		t.Fatalf("new compiler failed: %v", err)
	}

	result := &compileResult{
		name:           "NEW",
		defs:           make(map[string]*ast.Definition),
		queryFieldDefs: make(map[string]*ast.FieldDefinition),
		mutFieldDefs:   make(map[string]*ast.FieldDefinition),
	}

	// Collect definitions
	for def := range catalog.Definitions(ctx) {
		if def.Name == "Mutation" {
			// Mutation is a placeholder with _stub
			continue
		}
		result.defs[def.Name] = def
	}

	// Merge extensions into definitions; extract query/mutation fields
	for ext := range catalog.Extensions(ctx) {
		if ext.Name == "Query" {
			for _, f := range ext.Fields {
				result.queryFieldDefs[f.Name] = f
			}
			continue
		}
		if ext.Name == "Mutation" {
			for _, f := range ext.Fields {
				result.mutFieldDefs[f.Name] = f
			}
			continue
		}
		// Merge extension fields into the definition
		if def, ok := result.defs[ext.Name]; ok {
			def.Fields = append(def.Fields, ext.Fields...)
		}
	}

	return result
}

// isBaseSystemType returns true for types from base SDL (prelude, system_types, etc.)
func isBaseSystemType(name string) bool {
	// GraphQL built-in scalars
	builtins := map[string]bool{
		"String": true, "Int": true, "Float": true, "Boolean": true, "ID": true,
		"__Schema": true, "__Type": true, "__Field": true, "__EnumValue": true,
		"__Directive": true, "__InputValue": true, "__DirectiveLocation": true, "__TypeKind": true,
	}
	if builtins[name] {
		return true
	}
	// Our system scalars and types
	sysTypes := map[string]bool{
		// Scalars
		"BigInt": true, "Timestamp": true, "Date": true, "Time": true,
		"Interval": true, "JSON": true, "Geometry": true,
		"IntRange": true, "BigIntRange": true, "TimestampRange": true,
		"H3Cell": true, "Vector": true,
		// System types
		"Query": true, "Mutation": true, "Subscription": true,
		"OrderByField": true, "OrderDirection": true,
		"OperationResult": true, "ModuleObjectType": true,
		"QueryType": true, "MutationType": true,
		"GeometryType": true, "ExtraFieldBaseType": true,
		"Function": true, "FunctionMutation": true,
		// Filter/Aggregation input types for scalars
		"IntFilter": true, "IntListFilter": true, "IntAggregation": true, "IntMeasurementAggregation": true,
		"FloatFilter": true, "FloatListFilter": true, "FloatAggregation": true, "FloatMeasurementAggregation": true,
		"StringFilter": true, "StringListFilter": true, "StringAggregation": true, "StringMeasurementAggregation": true,
		"BooleanFilter": true, "BooleanAggregation": true, "BooleanMeasurementAggregation": true,
		"BigIntFilter": true, "BigIntListFilter": true, "BigIntAggregation": true, "BigIntMeasurementAggregation": true,
		"TimestampFilter": true, "TimestampListFilter": true, "TimestampAggregation": true, "TimestampMeasurementAggregation": true,
		"DateFilter": true, "DateListFilter": true, "DateAggregation": true, "DateMeasurementAggregation": true,
		"TimeFilter": true, "TimeListFilter": true, "TimeAggregation": true, "TimeMeasurementAggregation": true,
		"IntervalFilter": true, "IntervalListFilter": true,
		"JSONFilter": true, "JSONAggregation": true,
		"GeometryFilter": true, "GeometryAggregation": true, "GeometryMeasurementAggregation": true,
		"IntRangeFilter": true, "IntRangeListFilter": true,
		"BigIntRangeFilter": true, "BigIntRangeListFilter": true,
		"TimestampRangeFilter": true, "TimestampRangeListFilter": true,
		"VectorFilter": true,
		// GIS types
		"GeometryMeasurementTypes": true, "TimestampPart": true,
		"VectorDistanceMetric": true,
		// Distribution types
		"_distribution_by": true, "_distribution_by_aggregation": true,
		"_distribution_by_bucket": true, "_distribution_by_bucket_aggregation": true,
	}
	return sysTypes[name]
}

// --- CROSS-COMPILER COMPARISON TESTS ---

func TestCrossCompiler_BasicTable(t *testing.T) {
	sdl := complexTestSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

func TestCrossCompiler_ReadOnly(t *testing.T) {
	sdl := complexTestSchema
	oldOpts := oldcompiler.Options{Name: "test", ReadOnly: true, EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", ReadOnly: true, EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	compareSummary(t, oldResult, newResult)

	// Both should have zero mutation fields
	if len(oldResult.mutFieldDefs) != 0 {
		t.Errorf("OLD: expected 0 mutation fields in ReadOnly, got %d", len(oldResult.mutFieldDefs))
	}
	if len(newResult.mutFieldDefs) != 0 {
		t.Errorf("NEW: expected 0 mutation fields in ReadOnly, got %d", len(newResult.mutFieldDefs))
	}
}

func TestCrossCompiler_WithPrefix(t *testing.T) {
	sdl := complexTestSchema
	oldOpts := oldcompiler.Options{Name: "test", Prefix: "air", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", Prefix: "air", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	compareSummary(t, oldResult, newResult)
}

func TestCrossCompiler_AsModule(t *testing.T) {
	sdl := complexTestSchema
	oldOpts := oldcompiler.Options{Name: "aviation", AsModule: true, EngineType: "duckdb"}
	newOpts := base.Options{Name: "aviation", AsModule: true, EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	compareSummary(t, oldResult, newResult)
}

// --- Deep comparison ---

// compareSummary prints a detailed comparison between old and new compiler outputs,
// including field types, arguments, and directives.
func compareSummary(t *testing.T, old, new *compileResult) {
	t.Helper()

	// 1. Compare definition names
	t.Log("=== DEFINITION COMPARISON ===")
	var onlyOld, onlyNew, common []string

	for name := range old.defs {
		if _, ok := new.defs[name]; ok {
			common = append(common, name)
		} else {
			onlyOld = append(onlyOld, name)
		}
	}
	for name := range new.defs {
		if _, ok := old.defs[name]; !ok {
			onlyNew = append(onlyNew, name)
		}
	}
	sort.Strings(onlyOld)
	sort.Strings(onlyNew)
	sort.Strings(common)

	t.Logf("Common definitions: %d", len(common))
	if len(onlyOld) > 0 {
		t.Logf("Only in OLD (%d): %v", len(onlyOld), onlyOld)
	}
	if len(onlyNew) > 0 {
		t.Logf("Only in NEW (%d): %v", len(onlyNew), onlyNew)
	}

	// 2. Deep comparison of common definitions
	t.Log("=== DEFINITION DETAIL COMPARISON ===")
	var stats diffStats
	for _, name := range common {
		compareDefinitions(t, name, old.defs[name], new.defs[name], &stats)
	}

	// 3. Compare query fields
	t.Log("=== QUERY FIELD COMPARISON ===")
	oldQNames := old.queryFieldNames()
	newQNames := new.queryFieldNames()

	oldOnlyQ := stringDiff(oldQNames, newQNames)
	newOnlyQ := stringDiff(newQNames, oldQNames)
	commonQ := stringIntersect(oldQNames, newQNames)

	t.Logf("Common query fields: %d", len(commonQ))
	if len(oldOnlyQ) > 0 {
		t.Logf("Only in OLD query (%d): %v", len(oldOnlyQ), oldOnlyQ)
	}
	if len(newOnlyQ) > 0 {
		t.Logf("Only in NEW query (%d): %v", len(newOnlyQ), newOnlyQ)
	}

	// Deep compare common query fields
	for _, qname := range commonQ {
		compareFieldDef(t, "Query."+qname, old.queryFieldDefs[qname], new.queryFieldDefs[qname], &stats)
	}

	// 4. Compare mutation fields
	t.Log("=== MUTATION FIELD COMPARISON ===")
	oldMNames := old.mutationFieldNames()
	newMNames := new.mutationFieldNames()

	oldOnlyM := stringDiff(oldMNames, newMNames)
	newOnlyM := stringDiff(newMNames, oldMNames)
	commonM := stringIntersect(oldMNames, newMNames)

	t.Logf("Common mutation fields: %d", len(commonM))
	if len(oldOnlyM) > 0 {
		t.Logf("Only in OLD mutation (%d): %v", len(oldOnlyM), oldOnlyM)
	}
	if len(newOnlyM) > 0 {
		t.Logf("Only in NEW mutation (%d): %v", len(newOnlyM), newOnlyM)
	}

	// Deep compare common mutation fields
	for _, mname := range commonM {
		compareFieldDef(t, "Mutation."+mname, old.mutFieldDefs[mname], new.mutFieldDefs[mname], &stats)
	}

	// 5. Summary
	t.Log("=== SUMMARY ===")
	t.Logf("Definitions: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(old.defs), len(new.defs), len(common), len(onlyOld), len(onlyNew))
	t.Logf("Query fields: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(oldQNames), len(newQNames), len(commonQ), len(oldOnlyQ), len(newOnlyQ))
	t.Logf("Mutation fields: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(oldMNames), len(newMNames), len(commonM), len(oldOnlyM), len(newOnlyM))
	t.Logf("Detail diffs: field-names=%d field-types=%d field-args=%d field-directives=%d def-kind=%d def-directives=%d",
		stats.fieldNameDiffs, stats.typeDiffs, stats.argDiffs, stats.fieldDirDiffs, stats.kindDiffs, stats.defDirDiffs)
}

// diffStats accumulates comparison counters.
type diffStats struct {
	fieldNameDiffs int
	typeDiffs      int
	argDiffs       int
	fieldDirDiffs  int
	kindDiffs      int
	defDirDiffs    int
}

// compareDefinitions deeply compares two definitions: kind, directives, and per-field details.
func compareDefinitions(t *testing.T, name string, oldDef, newDef *ast.Definition, stats *diffStats) {
	t.Helper()

	// Compare kind
	if oldDef.Kind != newDef.Kind {
		t.Logf("  %s kind: old=%s new=%s", name, oldDef.Kind, newDef.Kind)
		stats.kindDiffs++
	}

	// Compare definition-level directives
	oldDirs := directiveSignatures(oldDef.Directives)
	newDirs := directiveSignatures(newDef.Directives)
	dOld := stringDiff(oldDirs, newDirs)
	dNew := stringDiff(newDirs, oldDirs)
	if len(dOld) > 0 || len(dNew) > 0 {
		t.Logf("  %s directives: old-only=%v new-only=%v", name, dOld, dNew)
		stats.defDirDiffs++
	}

	// Compare field names
	oldFieldNames := fieldNames(oldDef)
	newFieldNames := fieldNames(newDef)
	fOld := stringDiff(oldFieldNames, newFieldNames)
	fNew := stringDiff(newFieldNames, oldFieldNames)
	if len(fOld) > 0 || len(fNew) > 0 {
		t.Logf("  %s fields: old-only=%v new-only=%v", name, fOld, fNew)
		stats.fieldNameDiffs++
	}

	// Deep compare common fields
	commonFields := stringIntersect(oldFieldNames, newFieldNames)
	for _, fname := range commonFields {
		oldF := oldDef.Fields.ForName(fname)
		newF := newDef.Fields.ForName(fname)
		if oldF == nil || newF == nil {
			continue
		}
		compareFieldDef(t, name+"."+fname, oldF, newF, stats)
	}
}

// compareFieldDef deeply compares two field definitions: return type, arguments, directives.
func compareFieldDef(t *testing.T, prefix string, oldF, newF *ast.FieldDefinition, stats *diffStats) {
	t.Helper()

	// Compare return type
	ot := typeString(oldF.Type)
	nt := typeString(newF.Type)
	if ot != nt {
		t.Logf("    %s type: old=%s new=%s", prefix, ot, nt)
		stats.typeDiffs++
	}

	// Compare arguments
	oldArgs := argsSignatures(oldF.Arguments)
	newArgs := argsSignatures(newF.Arguments)
	aOld := stringDiff(oldArgs, newArgs)
	aNew := stringDiff(newArgs, oldArgs)
	if len(aOld) > 0 || len(aNew) > 0 {
		t.Logf("    %s args: old-only=%v new-only=%v", prefix, aOld, aNew)
		stats.argDiffs++
	}

	// Compare field-level directives
	oldDirs := directiveSignatures(oldF.Directives)
	newDirs := directiveSignatures(newF.Directives)
	dOld := stringDiff(oldDirs, newDirs)
	dNew := stringDiff(newDirs, oldDirs)
	if len(dOld) > 0 || len(dNew) > 0 {
		t.Logf("    %s directives: old-only=%v new-only=%v", prefix, dOld, dNew)
		stats.fieldDirDiffs++
	}
}

// --- Helper functions ---

func fieldNames(def *ast.Definition) []string {
	if def == nil {
		return nil
	}
	names := make([]string, 0, len(def.Fields))
	for _, f := range def.Fields {
		names = append(names, f.Name)
	}
	sort.Strings(names)
	return names
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func stringDiff(a, b []string) []string {
	bSet := make(map[string]bool, len(b))
	for _, s := range b {
		bSet[s] = true
	}
	var diff []string
	for _, s := range a {
		if !bSet[s] {
			diff = append(diff, s)
		}
	}
	return diff
}

func stringIntersect(a, b []string) []string {
	bSet := make(map[string]bool, len(b))
	for _, s := range b {
		bSet[s] = true
	}
	var common []string
	for _, s := range a {
		if bSet[s] {
			common = append(common, s)
		}
	}
	return common
}
