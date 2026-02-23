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
// It also processes extensions (e.g. "extend type Function { ... }") by merging them
// into definitions or creating new definitions.
func extractSourceDefs(sd *ast.SchemaDocument) *testSource {
	defMap := make(map[string]*ast.Definition)
	var defs []*ast.Definition
	for _, def := range sd.Definitions {
		// Skip schema definition types, only take user objects
		if def.Kind == ast.Object || def.Kind == ast.InputObject || def.Kind == ast.Enum {
			defs = append(defs, def)
			defMap[def.Name] = def
		}
	}
	// Merge extensions into definitions (for "extend type Function { ... }" etc.)
	for _, ext := range sd.Extensions {
		if existing, ok := defMap[ext.Name]; ok {
			existing.Fields = append(existing.Fields, ext.Fields...)
			existing.Directives = append(existing.Directives, ext.Directives...)
		} else {
			// Create a new definition from the extension
			def := &ast.Definition{
				Kind:       ext.Kind,
				Name:       ext.Name,
				Fields:     ext.Fields,
				Directives: ext.Directives,
				Position:   ext.Position,
			}
			defs = append(defs, def)
			defMap[def.Name] = def
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

	// Collect definitions (filter base system types same as old compiler)
	for def := range catalog.Definitions(ctx) {
		if isBaseSystemType(def.Name) {
			continue
		}
		result.defs[def.Name] = def
	}

	// Merge extensions into definitions; extract query/mutation fields
	for ext := range catalog.Extensions(ctx) {
		if ext.Name == "Query" {
			for _, f := range ext.Fields {
				if isSkipQueryField(f.Name) {
					continue
				}
				result.queryFieldDefs[f.Name] = f
			}
			continue
		}
		if ext.Name == "Mutation" {
			for _, f := range ext.Fields {
				if isSkipMutField(f.Name) {
					continue
				}
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
		"GeometryMeasurementTypes": true, "GeometrySpatialQueryType": true,
		"TimestampPart": true, "VectorDistanceMetric": true,
		// Distribution types
		"_distribution_by": true, "_distribution_by_aggregation": true,
		"_distribution_by_bucket": true, "_distribution_by_bucket_aggregation": true,
		// Sub-aggregation types (from scalar SDL)
		"IntSubAggregation": true, "BigIntSubAggregation": true, "FloatSubAggregation": true,
		"StringSubAggregation": true, "BooleanSubAggregation": true,
		"DateSubAggregation": true, "TimestampSubAggregation": true, "TimeSubAggregation": true,
		"JSONSubAggregation": true, "GeometrySubAggregation": true,
		// System enums and types from old scalar_types.graphql
		"FilterOperator": true, "TimeBucket": true, "TimeExtract": true,
		"GeometryTransform": true, "UniqueRuleType": true,
		"VectorDistanceType": true, "VectorSearchInput": true, "SemanticSearchInput": true,
		"any": true,
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

// functionTestSchema tests functions, function calls, and table_function_call_join.
const functionTestSchema = `
"""Airport with function call and table function call join fields"""
type Airport
  @table(name: "airports") {
  iata_code: String! @pk
  name: String!
  city: String!
  country: String!
  geom: Geometry
  status: String @function_call(references_name: "airport_status", args: {code: "iata_code"})
  nearby: [Airport] @table_function_call_join(references_name: "find_nearby", args: {origin: "iata_code"})
}

extend type Function {
  airport_status(code: String!): String
    @function(name: "airport_status")

  search_airports(country: String!, limit: Int): JSON
    @function(name: "search_airports", json_cast: true)

  find_nearby(origin: String!, radius: Float = 100): [Airport]
    @function(name: "find_nearby")
}
`

func TestCrossCompiler_Functions(t *testing.T) {
	sdl := functionTestSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

func TestCrossCompiler_FunctionsAsModule(t *testing.T) {
	sdl := functionTestSchema
	oldOpts := oldcompiler.Options{Name: "aviation", AsModule: true, EngineType: "duckdb"}
	newOpts := base.Options{Name: "aviation", AsModule: true, EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// nestedModuleSchema tests references, joins, and nested module hierarchy.
// Uses inline @module directives for full module paths (no AsModule needed).
// Hierarchy:
//
//	transport (level 1): Vehicle
//	transport.air (level 2): Airport, Flight (with references to Airport)
//	transport.ground (level 2): Station
const nestedModuleSchema = `
"""Generic vehicle"""
type Vehicle
  @table(name: "vehicles")
  @module(name: "transport") {
  id: Int! @pk
  name: String!
  type: String!
}

"""Airport"""
type Airport
  @table(name: "airports")
  @module(name: "transport.air") {
  iata_code: String! @pk
  name: String!
  city: String!
  geom: Geometry
}

"""Flight with references to Airport"""
type Flight
  @table(name: "flights")
  @module(name: "transport.air") {
  id: Int! @pk
  flight_number: String!
  origin: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "origin_airport"
    references_query: "departures"
  )
  arrival: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "dest_airport"
    references_query: "arrivals"
  )
}

"""Train or bus station"""
type Station
  @table(name: "stations")
  @module(name: "transport.ground") {
  id: Int! @pk
  name: String!
  city: String!
}
`

func TestCrossCompiler_NestedModules(t *testing.T) {
	sdl := nestedModuleSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// nestedModuleAsModuleSchema uses relative @module names combined with AsModule.
// With AsModule name="transport":
//
//	Vehicle (no @module) → module "transport" (level 1)
//	Airport (@module "air") → module "transport.air" (level 2)
//	Flight (@module "air") → module "transport.air" (level 2)
//	Station (@module "ground") → module "transport.ground" (level 2)
const nestedModuleAsModuleSchema = `
"""Generic vehicle at root module level"""
type Vehicle
  @table(name: "vehicles") {
  id: Int! @pk
  name: String!
  type: String!
}

"""Airport in 'air' sub-module"""
type Airport
  @table(name: "airports")
  @module(name: "air") {
  iata_code: String! @pk
  name: String!
  city: String!
  geom: Geometry
}

"""Flight in 'air' sub-module, references Airport"""
type Flight
  @table(name: "flights")
  @module(name: "air") {
  id: Int! @pk
  flight_number: String!
  origin: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "origin_airport"
    references_query: "departures"
  )
  arrival: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "dest_airport"
    references_query: "arrivals"
  )
}

"""Station in 'ground' sub-module"""
type Station
  @table(name: "stations")
  @module(name: "ground") {
  id: Int! @pk
  name: String!
  city: String!
}
`

func TestCrossCompiler_NestedModulesAsModule(t *testing.T) {
	sdl := nestedModuleAsModuleSchema
	oldOpts := oldcompiler.Options{Name: "transport", AsModule: true, EngineType: "duckdb"}
	newOpts := base.Options{Name: "transport", AsModule: true, EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// parameterizedViewSchema tests @view with @args for parameterized views.
const parameterizedViewSchema = `
"""Input arguments for the flight_log view"""
input FlightLogArgs {
  airline_code: String!
  year: Int
}

"""Parameterized view with required args (airline_code is NonNull)"""
type FlightLog
  @view(name: "flight_log_view")
  @args(name: "FlightLogArgs") {
  id: Int! @pk
  flight_number: String!
  departure_time: Timestamp
  status: String
}

"""Regular table to verify coexistence"""
type Airport
  @table(name: "airports") {
  iata_code: String! @pk
  name: String!
  city: String!
}
`

func TestCrossCompiler_ParameterizedView(t *testing.T) {
	sdl := parameterizedViewSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// vectorOnlySchema tests tables with Vector fields but WITHOUT @embeddings.
// This validates that similarity search args are generated for plain vector fields.
const vectorOnlySchema = `
"""Table with a raw vector field (no embeddings)"""
type ImageFeature
  @table(name: "image_features") {
  id: Int! @pk
  image_url: String!
  feature_vector: Vector
  label: String
}

"""Regular table for comparison"""
type Tag
  @table(name: "tags") {
  id: Int! @pk
  name: String!
}
`

func TestCrossCompiler_VectorOnly(t *testing.T) {
	sdl := vectorOnlySchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// embeddingsSchema tests @embeddings directive for vector/semantic search.
const embeddingsSchema = `
"""A document with vector embeddings for semantic search"""
type Document
  @table(name: "documents")
  @embeddings(model: "text-embedding-3-small", vector: "embedding", distance: Cosine) {
  id: Int! @pk
  title: String!
  content: String
  embedding: Vector
}

"""Regular table without embeddings"""
type Category
  @table(name: "categories") {
  id: Int! @pk
  name: String!
}
`

func TestCrossCompiler_Embeddings(t *testing.T) {
	sdl := embeddingsSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// vectorAndEmbeddingsSchema tests both: table with plain Vector AND table with @embeddings.
const vectorAndEmbeddingsSchema = `
"""Table with plain vector field (similarity only, no semantic)"""
type Product
  @table(name: "products") {
  id: Int! @pk
  name: String!
  price: Float
  image_embedding: Vector
}

"""Table with @embeddings (similarity + semantic + summary)"""
type Article
  @table(name: "articles")
  @embeddings(model: "text-embedding-ada-002", vector: "content_vector", distance: L2) {
  id: Int! @pk
  title: String!
  body: String
  content_vector: Vector
}

"""Plain table for baseline comparison"""
type Author
  @table(name: "authors") {
  id: Int! @pk
  name: String!
  email: String
}
`

func TestCrossCompiler_VectorAndEmbeddings(t *testing.T) {
	sdl := vectorAndEmbeddingsSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// geometrySchema tests Geometry fields and _spatial type generation.
const geometrySchema = `
"""Table with Geometry field for spatial queries"""
type Building
  @table(name: "buildings") {
  id: Int! @pk
  name: String!
  location: Geometry
  height: Float
}

"""Table with multiple Geometry fields"""
type Route
  @table(name: "routes") {
  id: Int! @pk
  label: String!
  start_point: Geometry
  end_point: Geometry
}

"""Plain table (no geometry) for comparison"""
type City
  @table(name: "cities") {
  id: Int! @pk
  name: String!
  population: BigInt
}
`

func TestCrossCompiler_Geometry(t *testing.T) {
	sdl := geometrySchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

	compareSummary(t, oldResult, newResult)
}

// vectorGeometryEmbeddingsSchema tests all special field types together.
const vectorGeometryEmbeddingsSchema = `
"""Table with Geometry + Vector + @embeddings"""
type Place
  @table(name: "places")
  @embeddings(model: "text-embedding-3-small", vector: "description_vector", distance: Cosine) {
  id: Int! @pk
  name: String!
  location: Geometry
  description_vector: Vector
}

"""Table with only Geometry (no Vector)"""
type Region
  @table(name: "regions") {
  id: Int! @pk
  name: String!
  boundary: Geometry
}

"""Table with only Vector (no Geometry, no @embeddings)"""
type Embedding
  @table(name: "embeddings") {
  id: Int! @pk
  label: String
  vector_data: Vector
}
`

func TestCrossCompiler_VectorGeometryEmbeddings(t *testing.T) {
	sdl := vectorGeometryEmbeddingsSchema
	oldOpts := oldcompiler.Options{Name: "test", EngineType: "duckdb"}
	newOpts := base.Options{Name: "test", EngineType: "duckdb"}

	oldResult := buildOldCompilerResult(t, sdl, oldOpts)
	newResult := buildNewCompilerResult(t, sdl, newOpts)

	t.Log(oldResult)
	t.Log(newResult)

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

// TestCatalogDirectivePlacement verifies that @catalog(name, engine) is present
// on all expected field categories in the compiled output. Uses the complexTestSchema
// which includes tables, references, geometry, etc. with name="test", engine="duckdb".
func TestCatalogDirectivePlacement(t *testing.T) {
	sdl := complexTestSchema
	opts := base.Options{Name: "test", EngineType: "duckdb"}
	result := buildNewCompilerResult(t, sdl, opts)

	hasCatalog := func(f *ast.FieldDefinition) bool {
		d := f.Directives.ForName("catalog")
		if d == nil {
			return false
		}
		nameArg := d.Arguments.ForName("name")
		engineArg := d.Arguments.ForName("engine")
		return nameArg != nil && nameArg.Value.Raw == "test" &&
			engineArg != nil && engineArg.Value.Raw == "duckdb"
	}

	// 1. Query fields must have @catalog (except system-level fields like "h3")
	for name, f := range result.queryFieldDefs {
		// h3 is a system-level query field, not tied to a catalog
		if f.Directives.ForName("system") != nil {
			continue
		}
		if !hasCatalog(f) {
			t.Errorf("query field %q missing @catalog(name:test, engine:duckdb)", name)
		}
	}

	// 2. Mutation fields must have @catalog
	for name, f := range result.mutFieldDefs {
		if !hasCatalog(f) {
			t.Errorf("mutation field %q missing @catalog(name:test, engine:duckdb)", name)
		}
	}

	// 3. _join fields must have @catalog
	joinDef := result.defs["_join"]
	if joinDef == nil {
		t.Fatal("_join type not found in output")
	}
	for _, f := range joinDef.Fields {
		if !hasCatalog(f) {
			t.Errorf("_join field %q missing @catalog", f.Name)
		}
	}

	// 4. _join_aggregation fields must have @catalog
	joinAggDef := result.defs["_join_aggregation"]
	if joinAggDef == nil {
		t.Fatal("_join_aggregation type not found in output")
	}
	for _, f := range joinAggDef.Fields {
		if !hasCatalog(f) {
			t.Errorf("_join_aggregation field %q missing @catalog", f.Name)
		}
	}

	// 5. _spatial fields must have @catalog (complexTestSchema has Geometry fields)
	spatialDef := result.defs["_spatial"]
	if spatialDef == nil {
		t.Fatal("_spatial type not found in output")
	}
	for _, f := range spatialDef.Fields {
		if !hasCatalog(f) {
			t.Errorf("_spatial field %q missing @catalog", f.Name)
		}
	}

	// 6. _spatial_aggregation fields must have @catalog
	spatialAggDef := result.defs["_spatial_aggregation"]
	if spatialAggDef == nil {
		t.Fatal("_spatial_aggregation type not found in output")
	}
	for _, f := range spatialAggDef.Fields {
		if !hasCatalog(f) {
			t.Errorf("_spatial_aggregation field %q missing @catalog", f.Name)
		}
	}

	// 7. Source object definitions must have @catalog on the definition itself
	for _, objName := range []string{"Airport", "Route", "Airline"} {
		def := result.defs[objName]
		if def == nil {
			t.Errorf("definition %q not found", objName)
			continue
		}
		d := def.Directives.ForName("catalog")
		if d == nil {
			t.Errorf("definition %q missing @catalog directive", objName)
			continue
		}
		nameArg := d.Arguments.ForName("name")
		engineArg := d.Arguments.ForName("engine")
		if nameArg == nil || nameArg.Value.Raw != "test" || engineArg == nil || engineArg.Value.Raw != "duckdb" {
			t.Errorf("definition %q @catalog has wrong args", objName)
		}
	}

	// 8. Reference subquery fields on objects must have @catalog
	// Reference fields have @references_query directive
	refFieldCount := 0
	for objName, def := range result.defs {
		if def.Kind != ast.Object {
			continue
		}
		for _, f := range def.Fields {
			if f.Directives.ForName("references_query") == nil {
				continue
			}
			refFieldCount++
			if !hasCatalog(f) {
				t.Errorf("%s.%s (reference field) missing @catalog", objName, f.Name)
			}
		}
	}
	if refFieldCount == 0 {
		t.Error("no reference fields found in output — expected at least one")
	}

	// 8b. Reference aggregation fields on aggregation types must have @catalog
	refAggCount := 0
	for objName, def := range result.defs {
		if def.Kind != ast.Object {
			continue
		}
		for _, f := range def.Fields {
			if f.Directives.ForName("aggregation_query") == nil {
				continue
			}
			// This is an aggregation query field on a type (e.g., aggregation reference)
			refAggCount++
			if !hasCatalog(f) {
				t.Errorf("%s.%s (aggregation query field) missing @catalog", objName, f.Name)
			}
		}
	}

	// 9. INPUT_OBJECT types must NOT have @catalog
	for name, def := range result.defs {
		if def.Kind == ast.InputObject {
			d := def.Directives.ForName("catalog")
			if d != nil {
				t.Errorf("INPUT_OBJECT %q should NOT have @catalog directive", name)
			}
		}
	}

	// 10. Aggregation types (_*_aggregation) must NOT have @catalog on their definition
	// (only on query fields that return them)
	for name, def := range result.defs {
		if def.Kind == ast.Object && strings.HasSuffix(name, "_aggregation") && !strings.HasPrefix(name, "_join") && !strings.HasPrefix(name, "_spatial") {
			d := def.Directives.ForName("catalog")
			if d != nil {
				t.Errorf("aggregation type %q should NOT have @catalog on definition", name)
			}
		}
	}

	t.Logf("Checked: %d query fields, %d mutation fields, %d _join fields, %d _join_agg fields, %d _spatial fields, %d _spatial_agg fields, %d ref fields, %d agg query fields",
		len(result.queryFieldDefs), len(result.mutFieldDefs),
		len(joinDef.Fields), len(joinAggDef.Fields),
		len(spatialDef.Fields), len(spatialAggDef.Fields),
		refFieldCount, refAggCount)
}

// TestCatalogDirectivePlacement_Modules verifies that module types and module
// root fields on Query/Mutation do NOT have @catalog, since modules can be
// shared across multiple catalogs.
func TestCatalogDirectivePlacement_Modules(t *testing.T) {
	sdl := nestedModuleSchema
	opts := base.Options{Name: "test", EngineType: "duckdb"}
	result := buildNewCompilerResult(t, sdl, opts)

	hasCatalog := func(f *ast.FieldDefinition) bool {
		return f.Directives.ForName("catalog") != nil
	}

	// 1. Module root fields on Query must NOT have @catalog
	for name, f := range result.queryFieldDefs {
		isModule := false
		retType := f.Type.Name()
		if def := result.defs[retType]; def != nil {
			isModule = def.Directives.ForName("module_root") != nil
		}
		if isModule {
			if hasCatalog(f) {
				t.Errorf("query field %q (module root) should NOT have @catalog", name)
			}
		}
	}

	// 2. Module root fields on Mutation must NOT have @catalog
	for name, f := range result.mutFieldDefs {
		isModule := false
		retType := f.Type.Name()
		if def := result.defs[retType]; def != nil {
			isModule = def.Directives.ForName("module_root") != nil
		}
		if isModule {
			if hasCatalog(f) {
				t.Errorf("mutation field %q (module root) should NOT have @catalog", name)
			}
		}
	}

	// 3. Module type definitions (_module_*) must NOT have @catalog
	for name, def := range result.defs {
		if def.Directives.ForName("module_root") != nil {
			if def.Directives.ForName("catalog") != nil {
				t.Errorf("module type %q should NOT have @catalog", name)
			}
		}
	}

	// 4. But individual query fields WITHIN modules must have @catalog
	for name, def := range result.defs {
		if def.Directives.ForName("module_root") == nil {
			continue
		}
		for _, f := range def.Fields {
			// Module navigation fields (pointing to child modules) don't have @catalog
			retType := f.Type.Name()
			if childDef := result.defs[retType]; childDef != nil && childDef.Directives.ForName("module_root") != nil {
				continue
			}
			// Query/mutation fields within a module should have @catalog
			if f.Directives.ForName("query") != nil || f.Directives.ForName("mutation") != nil ||
				f.Directives.ForName("aggregation_query") != nil || f.Directives.ForName("references_query") != nil {
				if !hasCatalog(f) {
					t.Errorf("module type %s field %q (data field) missing @catalog", name, f.Name)
				}
			}
		}
	}

	t.Log("Module @catalog placement checks passed")
}

// TestCatalogAbsence_ForFutureCatalogToggle comprehensively verifies that @catalog
// is correctly absent from types that are shared/structural and should not be tied
// to a specific catalog. This supports future catalog enable/disable functionality.
//
// Rationale: INPUT_OBJECT types (filters, mutation inputs, args inputs) and
// aggregation type definitions are shared infrastructure. Only the query/mutation
// fields that *return* data from a catalog should carry @catalog. The types
// themselves must remain catalog-agnostic so they can be reused across catalogs.
func TestCatalogAbsence_ForFutureCatalogToggle(t *testing.T) {
	// Use parameterized view schema to also cover FlightLogArgs input type
	sdl := parameterizedViewSchema
	opts := base.Options{Name: "test", EngineType: "duckdb"}
	result := buildNewCompilerResult(t, sdl, opts)

	// 1. No INPUT_OBJECT definition should have @catalog
	inputObjectCount := 0
	for name, def := range result.defs {
		if def.Kind != ast.InputObject {
			continue
		}
		inputObjectCount++
		if def.Directives.ForName("catalog") != nil {
			t.Errorf("INPUT_OBJECT %q has @catalog — should not (shared/structural type)", name)
		}
	}
	if inputObjectCount == 0 {
		t.Error("no INPUT_OBJECT definitions found — expected filters and args inputs")
	}

	// 2. FlightLogArgs input type specifically must NOT have @catalog
	flightLogArgs := result.defs["FlightLogArgs"]
	if flightLogArgs == nil {
		t.Fatal("FlightLogArgs not found in output")
	}
	if flightLogArgs.Kind != ast.InputObject {
		t.Errorf("FlightLogArgs kind=%s, expected INPUT_OBJECT", flightLogArgs.Kind)
	}
	if flightLogArgs.Directives.ForName("catalog") != nil {
		t.Error("FlightLogArgs has @catalog — parameterized view args input should not")
	}

	// 3. No aggregation type definition should have @catalog
	// (only query fields that return them should carry @catalog)
	aggTypeCount := 0
	for name, def := range result.defs {
		if def.Kind != ast.Object {
			continue
		}
		if !strings.Contains(name, "_aggregation") {
			continue
		}
		aggTypeCount++
		if def.Directives.ForName("catalog") != nil {
			t.Errorf("aggregation type %q has @catalog on definition — should not", name)
		}
	}
	if aggTypeCount == 0 {
		t.Error("no aggregation type definitions found — expected at least one")
	}

	// 4. Verify the same with the complex schema (tables, refs, m2m)
	complexResult := buildNewCompilerResult(t, complexTestSchema, base.Options{Name: "air", EngineType: "duckdb"})

	complexInputCount := 0
	for name, def := range complexResult.defs {
		if def.Kind != ast.InputObject {
			continue
		}
		complexInputCount++
		if def.Directives.ForName("catalog") != nil {
			t.Errorf("[complex] INPUT_OBJECT %q has @catalog", name)
		}
	}

	complexAggCount := 0
	for name, def := range complexResult.defs {
		if def.Kind != ast.Object {
			continue
		}
		if !strings.Contains(name, "_aggregation") {
			continue
		}
		// Skip _join_aggregation and _spatial_aggregation — those are system-level
		if strings.HasPrefix(name, "_join") || strings.HasPrefix(name, "_spatial") {
			continue
		}
		complexAggCount++
		if def.Directives.ForName("catalog") != nil {
			t.Errorf("[complex] aggregation type %q has @catalog on definition", name)
		}
	}

	t.Logf("Checked: parameterized=%d INPUT_OBJECTs + %d agg types; complex=%d INPUT_OBJECTs + %d agg types",
		inputObjectCount, aggTypeCount, complexInputCount, complexAggCount)
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

// --- SQL parameter validation tests ---

// compileNewOnly compiles a schema with only the new compiler and returns the error (if any).
func compileNewOnly(t *testing.T, sdl string) error {
	t.Helper()
	sd := parseSourceSchemaDocument(t, sdl)
	source := extractSourceDefs(sd)
	ctx := context.Background()
	c := newcompiler.New(rules.RegisterAll()...)
	_, err := c.Compile(ctx, nil, source, base.Options{Name: "test", EngineType: "duckdb"})
	return err
}

func TestValidate_FunctionSQL_ValidArgs(t *testing.T) {
	sdl := `
type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}

extend type Function {
  my_func(arg1: String!, arg2: Int): String
    @function(name: "my_func", sql: "SELECT func([arg1], [arg2])")
}
`
	if err := compileNewOnly(t, sdl); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidate_FunctionSQL_UnknownArg(t *testing.T) {
	sdl := `
type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}

extend type Function {
  my_func(arg1: String!): String
    @function(name: "my_func", sql: "SELECT func([missing_arg])")
}
`
	err := compileNewOnly(t, sdl)
	if err == nil {
		t.Fatal("expected error for unknown SQL argument reference, got nil")
	}
	if !strings.Contains(err.Error(), "missing_arg") {
		t.Fatalf("expected error to mention 'missing_arg', got: %v", err)
	}
}

func TestValidate_FunctionSQL_SystemVar(t *testing.T) {
	sdl := `
type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}

extend type Function {
  my_func(arg1: String!): String
    @function(name: "my_func", sql: "SELECT func([arg1], [$catalog])")
}
`
	if err := compileNewOnly(t, sdl); err != nil {
		t.Fatalf("expected no error for [$catalog] system var, got: %v", err)
	}
}

func TestValidate_ViewArgs_Valid(t *testing.T) {
	sdl := `
input FlightLogArgs {
  airline_code: String!
  year: Int
}

type FlightLog
  @view(name: "flight_log_view")
  @args(name: "FlightLogArgs") {
  id: Int! @pk
  flight_number: String!
}
`
	if err := compileNewOnly(t, sdl); err != nil {
		t.Fatalf("expected no error for valid @args, got: %v", err)
	}
}

func TestValidate_ViewArgs_MissingInputType(t *testing.T) {
	sdl := `
type FlightLog
  @view(name: "flight_log_view")
  @args(name: "NonExistentInput") {
  id: Int! @pk
  flight_number: String!
}
`
	err := compileNewOnly(t, sdl)
	if err == nil {
		t.Fatal("expected error for missing input type, got nil")
	}
	if !strings.Contains(err.Error(), "NonExistentInput") {
		t.Fatalf("expected error to mention 'NonExistentInput', got: %v", err)
	}
}

func TestValidate_ViewArgs_NotInputObject(t *testing.T) {
	sdl := `
type NotAnInput @table(name: "not_input") {
  id: Int! @pk
  name: String!
}

type FlightLog
  @view(name: "flight_log_view")
  @args(name: "NotAnInput") {
  id: Int! @pk
  flight_number: String!
}
`
	err := compileNewOnly(t, sdl)
	if err == nil {
		t.Fatal("expected error for non-input type in @args, got nil")
	}
	if !strings.Contains(err.Error(), "must be an input type") {
		t.Fatalf("expected error about input type, got: %v", err)
	}
}

func TestValidate_ViewArgs_SQLWithValidRefs(t *testing.T) {
	sdl := `
input SearchArgs {
  search_term: String!
}

type SearchResults
  @view(name: "search_results", sql: "SELECT * FROM search WHERE term = [search_term] AND catalog = [$catalog]")
  @args(name: "SearchArgs") {
  id: Int! @pk
  title: String!
}
`
	if err := compileNewOnly(t, sdl); err != nil {
		t.Fatalf("expected no error for valid SQL refs, got: %v", err)
	}
}

func TestValidate_ViewArgs_SQLWithUnknownRef(t *testing.T) {
	sdl := `
input SearchArgs {
  search_term: String!
}

type SearchResults
  @view(name: "search_results", sql: "SELECT * FROM search WHERE x = [unknown_field]")
  @args(name: "SearchArgs") {
  id: Int! @pk
  title: String!
}
`
	err := compileNewOnly(t, sdl)
	if err == nil {
		t.Fatal("expected error for unknown SQL ref in view, got nil")
	}
	if !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("expected error to mention 'unknown_field', got: %v", err)
	}
}
