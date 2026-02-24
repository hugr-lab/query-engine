package comptest

import (
	"context"
	"iter"
	"strings"
	"testing"

	oldbase "github.com/hugr-lab/query-engine/pkg/compiler/base"
	oldcompiler "github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compare"
	newcompiler "github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/rules"
	_ "github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
	"github.com/vektah/gqlparser/v2/validator"
)

// complexTestSchema matches the cross-compiler test schema exactly.
const complexTestSchema = `
"""Airport database - comprehensive test schema"""
type Airport
  @table(name: "airports") {
  iata_code: String! @pk
  name: String!
  city: String!
  country: String!
  geom: Geometry
  latitude: Float
  longitude: Float
  elevation: Int
  timezone: String
  updated_at: Timestamp
}

"""Flight routes between airports"""
type Route
  @table(name: "routes")
  @unique(fields: ["src_airport", "dst_airport", "airline"], query_suffix: "route") {
  id: Int! @pk
  airline: String!
  src_airport: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "source_airport"
    references_query: "departing_routes"
  )
  dst_airport: String! @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "destination_airport"
    references_query: "arriving_routes"
  )
  stops: Int!
  is_active: Boolean @default(value: true)
}

"""Airline companies"""
type Airline
  @table(name: "airlines") {
  icao: String! @pk
  name: String!
  country: String
  active: Boolean @default(value: true)
  founded: Date
  fleet_size: BigInt
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

"""Flight schedule - read-only view"""
type FlightSchedule
  @view(name: "flight_schedule_view") {
  id: Int! @pk
  flight_number: String!
  departure_time: Timestamp
  arrival_time: Timestamp
  airline: String
  status: String
}

"""Airport statistics - read-only view"""
type AirportStats
  @view(name: "airport_stats_view") {
  airport_code: String! @pk
  total_departures: BigInt
  total_arrivals: BigInt
  avg_delay: Float
  last_updated: Timestamp
}

"""Booking-Passenger join table (M2M)"""
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

const functionTestSchema = complexTestSchema + `
type FunctionAirport
  @table(name: "airports") {
  iata_code: String! @pk
  name: String!
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

const nestedModuleSchema = `
type Vehicle
  @table(name: "vehicles")
  @module(name: "transport") {
  id: Int! @pk
  name: String!
  type: String!
}

type Airport
  @table(name: "airports")
  @module(name: "transport.air") {
  iata_code: String! @pk
  name: String!
  city: String!
  geom: Geometry
}

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

type Station
  @table(name: "stations")
  @module(name: "transport.ground") {
  id: Int! @pk
  name: String!
  city: String!
}
`

const nestedModuleAsModuleSchema = `
type Vehicle
  @table(name: "vehicles") {
  id: Int! @pk
  name: String!
  type: String!
}

type Airport
  @table(name: "airports")
  @module(name: "air") {
  iata_code: String! @pk
  name: String!
  city: String!
  geom: Geometry
}

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

type Station
  @table(name: "stations")
  @module(name: "ground") {
  id: Int! @pk
  name: String!
  city: String!
}
`

func TestCompare_BasicTable(t *testing.T) {
	assertCompilersMatch(t, complexTestSchema,
		oldcompiler.Options{Name: "test", EngineType: "duckdb"},
		base.Options{Name: "test", EngineType: "duckdb"},
	)
}

func TestCompare_ReadOnly(t *testing.T) {
	assertCompilersMatch(t, complexTestSchema,
		oldcompiler.Options{Name: "test", EngineType: "duckdb", ReadOnly: true},
		base.Options{Name: "test", EngineType: "duckdb", ReadOnly: true},
	)
}

func TestCompare_WithPrefix(t *testing.T) {
	assertCompilersMatch(t, complexTestSchema,
		oldcompiler.Options{Name: "test", EngineType: "duckdb", Prefix: "pfx"},
		base.Options{Name: "test", EngineType: "duckdb", Prefix: "pfx"},
	)
}

func TestCompare_AsModule(t *testing.T) {
	assertCompilersMatch(t, complexTestSchema,
		oldcompiler.Options{Name: "aviation", EngineType: "duckdb", AsModule: true},
		base.Options{Name: "aviation", EngineType: "duckdb", AsModule: true},
	)
}

func TestCompare_Functions(t *testing.T) {
	assertCompilersMatch(t, functionTestSchema,
		oldcompiler.Options{Name: "test", EngineType: "duckdb"},
		base.Options{Name: "test", EngineType: "duckdb"},
		// Known gap: new compiler adds extra @catalog directive to Function type
		compare.KnownIssues(
			"types.Function.directives.catalog",
		),
	)
}

func TestCompare_FunctionsAsModule(t *testing.T) {
	assertCompilersMatch(t, functionTestSchema,
		oldcompiler.Options{Name: "aviation", EngineType: "duckdb", AsModule: true},
		base.Options{Name: "aviation", EngineType: "duckdb", AsModule: true},
		// Known gap: new compiler adds extra @catalog directive to Function type
		compare.KnownIssues(
			"types.Function.directives.catalog",
		),
	)
}

func TestCompare_NestedModules(t *testing.T) {
	assertCompilersMatch(t, nestedModuleSchema,
		oldcompiler.Options{Name: "test", EngineType: "duckdb"},
		base.Options{Name: "test", EngineType: "duckdb"},
	)
}

func TestCompare_NestedModulesAsModule(t *testing.T) {
	assertCompilersMatch(t, nestedModuleAsModuleSchema,
		oldcompiler.Options{Name: "transport", EngineType: "duckdb", AsModule: true},
		base.Options{Name: "transport", EngineType: "duckdb", AsModule: true},
	)
}

// --- Helpers ---

// assertCompilersMatch compiles with both old and new compilers, builds *ast.Schema
// from each, and uses compare.Compare() to verify structural equivalence.
func assertCompilersMatch(t *testing.T, sdl string, oldOpts oldcompiler.Options, newOpts base.Options, extraOpts ...compare.CompareOption) {
	t.Helper()

	oldSchema := buildOldSchema(t, sdl, oldOpts)
	newSchema := buildNewSchema(t, sdl, newOpts)

	opts := []compare.CompareOption{
		compare.SkipSystemTypes(),
		compare.IgnoreDescriptions(),
		compare.IgnoreDirectiveArgs("if_not_exists"),
		compare.SkipTypes(systemTypesToSkip()...),
	}
	opts = append(opts, extraOpts...)

	result := compare.Compare(oldSchema, newSchema, opts...)

	if !result.Equal() {
		var sb strings.Builder
		sb.WriteString("Schema comparison found diffs:\n")
		for _, d := range result.Diffs {
			sb.WriteString("  ")
			sb.WriteString(d.Kind.String())
			sb.WriteString(" ")
			sb.WriteString(d.Path)
			sb.WriteString(": ")
			sb.WriteString(d.Message)
			sb.WriteByte('\n')
		}
		t.Fatal(sb.String())
	}

	if len(result.KnownIssues) > 0 {
		t.Logf("Known issues: %d", len(result.KnownIssues))
	}
}

func buildOldSchema(t *testing.T, sdl string, opts oldcompiler.Options) *ast.Schema {
	t.Helper()
	sd, err := parser.ParseSchema(&ast.Source{Name: "test.graphql", Input: sdl})
	if err != nil {
		t.Fatalf("parse source SDL: %v", err)
	}
	schema, err := oldcompiler.Compile(sd, opts)
	if err != nil {
		t.Fatalf("old compiler: %v", err)
	}
	return schema
}

func buildNewSchema(t *testing.T, sdl string, opts base.Options) *ast.Schema {
	t.Helper()
	sd, err := parser.ParseSchema(&ast.Source{Name: "test.graphql", Input: sdl})
	if err != nil {
		t.Fatalf("parse source SDL: %v", err)
	}
	source := extractSourceDefs(sd)

	c := newcompiler.New(rules.RegisterAll()...)
	ctx := context.Background()
	catalog, err := c.Compile(ctx, nil, source, opts)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}

	// Build *ast.SchemaDocument from system types (old compiler's Sources includes
	// all base types + scalar_types.graphql with enums like TimeBucket, etc.)
	doc := &ast.SchemaDocument{}
	for _, src := range oldbase.Sources() {
		parsed, err := parser.ParseSchema(src)
		if err != nil {
			t.Fatalf("parse system types: %v", err)
		}
		doc.Merge(parsed)
	}

	// Add if_not_exists directive definition (used by new compiler but not in any SDL)
	doc.Directives = append(doc.Directives, &ast.DirectiveDefinition{
		Name:      "if_not_exists",
		Locations: []ast.DirectiveLocation{ast.LocationObject},
		Position:  &ast.Position{Src: &ast.Source{Name: "comptest"}},
	})

	// Add DDL definitions
	for def := range catalog.Definitions(ctx) {
		doc.Definitions = append(doc.Definitions, def)
	}

	// Merge extensions into base definitions
	for ext := range catalog.Extensions(ctx) {
		if target := doc.Definitions.ForName(ext.Name); target != nil {
			target.Fields = append(target.Fields, ext.Fields...)
		}
	}

	// Ensure schema operation types are set
	if len(doc.Schema) == 0 {
		doc.Schema = append(doc.Schema, &ast.SchemaDefinition{})
	}
	if doc.Schema[0].OperationTypes.ForType("Query") == nil &&
		doc.Definitions.ForName("Query") != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes,
			&ast.OperationTypeDefinition{Operation: ast.Query, Type: "Query"})
	}
	if doc.Schema[0].OperationTypes.ForType("Mutation") == nil &&
		doc.Definitions.ForName("Mutation") != nil {
		doc.Schema[0].OperationTypes = append(doc.Schema[0].OperationTypes,
			&ast.OperationTypeDefinition{Operation: ast.Mutation, Type: "Mutation"})
	}

	// Add _placeholder to empty Object types to pass validation
	// (e.g., Function type when all fields moved to module in AsModule mode)
	pos := &ast.Position{Src: &ast.Source{Name: "comptest"}}
	for _, def := range doc.Definitions {
		if def.Kind == ast.Object && len(def.Fields) == 0 {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:     "_placeholder",
				Type:     ast.NamedType("Boolean", pos),
				Position: pos,
			})
		}
	}

	schema, errs := validator.ValidateSchemaDocument(doc)
	if errs != nil {
		t.Fatalf("validate new schema: %v", errs)
	}
	return schema
}

func extractSourceDefs(sd *ast.SchemaDocument) *testSource {
	defMap := make(map[string]*ast.Definition)
	var defs []*ast.Definition
	for _, def := range sd.Definitions {
		if def.Kind == ast.Object || def.Kind == ast.InputObject || def.Kind == ast.Enum {
			defs = append(defs, def)
			defMap[def.Name] = def
		}
	}
	for _, ext := range sd.Extensions {
		if existing, ok := defMap[ext.Name]; ok {
			existing.Fields = append(existing.Fields, ext.Fields...)
			existing.Directives = append(existing.Directives, ext.Directives...)
		} else {
			def := &ast.Definition{
				Kind: ext.Kind, Name: ext.Name,
				Fields: ext.Fields, Directives: ext.Directives, Position: ext.Position,
			}
			defs = append(defs, def)
			defMap[def.Name] = def
		}
	}
	return &testSource{defs: defs}
}

type testSource struct {
	defs []*ast.Definition
}

func (s *testSource) ForName(_ context.Context, name string) *ast.Definition {
	for _, d := range s.defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *testSource) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}

func (s *testSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *testSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(_ func(string, *ast.DirectiveDefinition) bool) {}
}

func systemTypesToSkip() []string {
	return []string{
		// Scalar filter types
		"Int_filter", "Int_list_filter", "BigInt_filter", "BigInt_list_filter",
		"Float_filter", "Float_list_filter", "String_filter", "String_list_filter",
		"Boolean_filter", "Date_filter", "Date_list_filter",
		"Timestamp_filter", "Timestamp_list_filter", "Time_filter", "Time_list_filter",
		"JSON_filter", "Geometry_filter", "Geometry_list_filter",
		"Interval_filter", "Interval_list_filter", "H3Cell_filter", "H3Cell_list_filter",
		"Vector_filter",
		// Scalar aggregation types
		"IntAggregation", "BigIntAggregation", "FloatAggregation",
		"StringAggregation", "BooleanAggregation",
		"DateAggregation", "TimestampAggregation", "TimeAggregation",
		"JSONAggregation", "GeometryAggregation",
		"IntMeasurementAggregation", "BigIntMeasurementAggregation",
		"FloatMeasurementAggregation",
		"DateMeasurementAggregation", "TimestampMeasurementAggregation",
		// Sub-aggregation types
		"IntSubAggregation", "BigIntSubAggregation", "FloatSubAggregation",
		"StringSubAggregation", "BooleanSubAggregation",
		"DateSubAggregation", "TimestampSubAggregation", "TimeSubAggregation",
		"JSONSubAggregation", "GeometrySubAggregation",
		// Distribution types
		"_distribution_by", "_distribution_by_aggregation",
		"_distribution_by_bucket", "_distribution_by_bucket_aggregation",
		// System enums and types
		"FilterOperator", "TimeBucket", "TimeExtract",
		"GeometryTransform", "UniqueRuleType",
		"VectorDistanceType", "VectorSearchInput", "SemanticSearchInput",
		"any",
		// Base schema types shared between compilers
		"Query", "Mutation", "OrderByField", "OperationResult",
		"OrderByDirection", "Geometry", "JSON", "BigInt", "Interval",
		"Timestamp", "Date", "Time", "H3Cell", "Vector",
		"GeometryType", "GeometrySpatialQueryType",
		"EmbeddingInput",
	}
}
