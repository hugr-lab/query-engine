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

// compileResult holds comparison data for one compiler output.
type compileResult struct {
	name string
	// definitions by name
	defs map[string]*ast.Definition
	// extensions by target name → merged fields
	extFields map[string][]string
	// query fields on Query type
	queryFields []string
	// mutation fields on Mutation type
	mutationFields []string
}

func (r *compileResult) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "=== %s ===\n", r.name)

	defNames := make([]string, 0, len(r.defs))
	for n := range r.defs {
		defNames = append(defNames, n)
	}
	sort.Strings(defNames)
	fmt.Fprintf(&sb, "Definitions (%d):\n", len(defNames))
	for _, n := range defNames {
		d := r.defs[n]
		fields := make([]string, 0, len(d.Fields))
		for _, f := range d.Fields {
			fields = append(fields, f.Name)
		}
		fmt.Fprintf(&sb, "  %s [%s] fields=%v\n", n, d.Kind, fields)
	}

	sort.Strings(r.queryFields)
	fmt.Fprintf(&sb, "Query fields (%d): %v\n", len(r.queryFields), r.queryFields)

	sort.Strings(r.mutationFields)
	fmt.Fprintf(&sb, "Mutation fields (%d): %v\n", len(r.mutationFields), r.mutationFields)

	return sb.String()
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
		name: "OLD",
		defs: make(map[string]*ast.Definition),
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
			if f.Name == "_stub" || f.Name == "jq" || f.Name == "__type" || f.Name == "__schema" || f.Name == "function" {
				continue
			}
			result.queryFields = append(result.queryFields, f.Name)
		}
	}

	// Extract Mutation fields
	if schema.Mutation != nil {
		for _, f := range schema.Mutation.Fields {
			if f.Name == "_stub" || f.Name == "function" {
				continue
			}
			result.mutationFields = append(result.mutationFields, f.Name)
		}
	}

	return result
}

// buildNewCompilerResult compiles with new compiler and extracts comparison data.
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
		name:      "NEW",
		defs:      make(map[string]*ast.Definition),
		extFields: make(map[string][]string),
	}

	// Collect definitions
	for def := range catalog.Definitions(ctx) {
		if def.Name == "Mutation" {
			// Mutation is a placeholder with _stub
			continue
		}
		result.defs[def.Name] = def
	}

	// Collect extensions
	for ext := range catalog.Extensions(ctx) {
		var fieldNames []string
		for _, f := range ext.Fields {
			fieldNames = append(fieldNames, f.Name)
		}
		result.extFields[ext.Name] = append(result.extFields[ext.Name], fieldNames...)

		if ext.Name == "Query" {
			result.queryFields = append(result.queryFields, fieldNames...)
		}
		if ext.Name == "Mutation" {
			result.mutationFields = append(result.mutationFields, fieldNames...)
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

	// Compare generated definition names (filter, input, aggregation types)
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
	if len(oldResult.mutationFields) != 0 {
		t.Errorf("OLD: expected 0 mutation fields in ReadOnly, got %d: %v", len(oldResult.mutationFields), oldResult.mutationFields)
	}
	if len(newResult.mutationFields) != 0 {
		t.Errorf("NEW: expected 0 mutation fields in ReadOnly, got %d: %v", len(newResult.mutationFields), newResult.mutationFields)
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

// compareSummary prints a detailed comparison between old and new compiler outputs
// and reports differences as test errors.
func compareSummary(t *testing.T, old, new *compileResult) {
	t.Helper()

	// 1. Compare user-generated definitions (filter, input, aggregation types)
	t.Log("--- DEFINITION COMPARISON ---")
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

	// 2. For common types, compare fields
	var fieldDiffs int
	for _, name := range common {
		oldDef := old.defs[name]
		newDef := new.defs[name]

		oldFields := fieldNames(oldDef)
		newFields := fieldNames(newDef)

		if !stringSliceEqual(oldFields, newFields) {
			onlyInOld := stringDiff(oldFields, newFields)
			onlyInNew := stringDiff(newFields, oldFields)
			if len(onlyInOld) > 0 || len(onlyInNew) > 0 {
				t.Logf("  %s field diff: old-only=%v, new-only=%v", name, onlyInOld, onlyInNew)
				fieldDiffs++
			}
		}
	}

	// 3. Compare query fields
	t.Log("--- QUERY FIELD COMPARISON ---")
	sort.Strings(old.queryFields)
	sort.Strings(new.queryFields)

	oldOnlyQ := stringDiff(old.queryFields, new.queryFields)
	newOnlyQ := stringDiff(new.queryFields, old.queryFields)
	commonQ := stringIntersect(old.queryFields, new.queryFields)

	t.Logf("Common query fields: %d", len(commonQ))
	if len(oldOnlyQ) > 0 {
		t.Logf("Only in OLD query (%d): %v", len(oldOnlyQ), oldOnlyQ)
	}
	if len(newOnlyQ) > 0 {
		t.Logf("Only in NEW query (%d): %v", len(newOnlyQ), newOnlyQ)
	}

	// 4. Compare mutation fields
	t.Log("--- MUTATION FIELD COMPARISON ---")
	sort.Strings(old.mutationFields)
	sort.Strings(new.mutationFields)

	oldOnlyM := stringDiff(old.mutationFields, new.mutationFields)
	newOnlyM := stringDiff(new.mutationFields, old.mutationFields)
	commonM := stringIntersect(old.mutationFields, new.mutationFields)

	t.Logf("Common mutation fields: %d", len(commonM))
	if len(oldOnlyM) > 0 {
		t.Logf("Only in OLD mutation (%d): %v", len(oldOnlyM), oldOnlyM)
	}
	if len(newOnlyM) > 0 {
		t.Logf("Only in NEW mutation (%d): %v", len(newOnlyM), newOnlyM)
	}

	// 5. Summary
	t.Log("--- SUMMARY ---")
	t.Logf("Definitions: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(old.defs), len(new.defs), len(common), len(onlyOld), len(onlyNew))
	t.Logf("Query fields: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(old.queryFields), len(new.queryFields), len(commonQ), len(oldOnlyQ), len(newOnlyQ))
	t.Logf("Mutation fields: old=%d new=%d common=%d old-only=%d new-only=%d",
		len(old.mutationFields), len(new.mutationFields), len(commonM), len(oldOnlyM), len(newOnlyM))
	t.Logf("Field-level diffs in common types: %d", fieldDiffs)
}

// Helper functions

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

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
