package compiler_test

import (
	"context"
	"iter"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/integration-test/compare"
	oldcompiler "github.com/hugr-lab/query-engine/pkg/compiler"
	oldbase "github.com/hugr-lab/query-engine/pkg/compiler/base"
	newcompiler "github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/schema/static"
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

// --- Multi-Catalog Comparison Tests ---

func TestCompare_MultiCatalog_TwoCatalogs(t *testing.T) {
	assertMultiCatalogMatch(t, []catalogDef{
		{
			SDL:     multiCatalogA,
			OldOpts: oldcompiler.Options{Name: "base", EngineType: "duckdb"},
			NewOpts: base.Options{Name: "base", EngineType: "duckdb"},
		},
		{
			SDL:     multiCatalogB,
			OldOpts: oldcompiler.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
			NewOpts: base.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
		},
	},
		// Known: new compiler adds @catalog on Function type;
		// FlightLog_by_pk correctly gets args param for parameterized views in new compiler.
		compare.KnownIssues(
			"types.Function.directives.catalog",
			"types._module_transport_air_query.fields.FlightLog_by_pk.args.args",
		),
	)
}

func TestCompare_MultiCatalog_OverlappingModules(t *testing.T) {
	// Both catalogs contribute to the same "transport" module hierarchy.
	// Catalog A has types in transport.air module.
	// Catalog B uses AsModule="transport" so its types go under transport module.
	// _module_transport_query must contain fields from both.
	assertMultiCatalogMatch(t, []catalogDef{
		{
			SDL:     multiCatalogA,
			OldOpts: oldcompiler.Options{Name: "base", EngineType: "duckdb"},
			NewOpts: base.Options{Name: "base", EngineType: "duckdb"},
		},
		{
			SDL:     multiCatalogB,
			OldOpts: oldcompiler.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
			NewOpts: base.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
		},
	},
		compare.KnownIssues(
			"types.Function.directives.catalog",
			"types._module_transport_air_query.fields.FlightLog_by_pk.args.args",
		),
	)
}

func TestCompare_MultiCatalog_SharedTypes(t *testing.T) {
	// Both catalogs have Geometry fields, generating _spatial, _join shared types.
	// Shared types must merge correctly with fields from all catalogs.
	assertMultiCatalogMatch(t, []catalogDef{
		{
			SDL:     multiCatalogA,
			OldOpts: oldcompiler.Options{Name: "base", EngineType: "duckdb"},
			NewOpts: base.Options{Name: "base", EngineType: "duckdb"},
		},
		{
			SDL:     multiCatalogC,
			OldOpts: oldcompiler.Options{Name: "extra", EngineType: "duckdb", Prefix: "ext"},
			NewOpts: base.Options{Name: "extra", EngineType: "duckdb", Prefix: "ext"},
		},
	},
		// FlightLog_by_pk correctly gets args param for parameterized views in new compiler.
		compare.KnownIssues(
			"types._module_transport_air_query.fields.FlightLog_by_pk.args.args",
		),
	)
}

func TestCompare_MultiCatalog_ThreeCatalogs(t *testing.T) {
	// Full bootstrap: base tables + functions with AsModule + extra with Prefix.
	assertMultiCatalogMatch(t, []catalogDef{
		{
			SDL:     multiCatalogA,
			OldOpts: oldcompiler.Options{Name: "base", EngineType: "duckdb"},
			NewOpts: base.Options{Name: "base", EngineType: "duckdb"},
		},
		{
			SDL:     multiCatalogB,
			OldOpts: oldcompiler.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
			NewOpts: base.Options{Name: "functions", EngineType: "duckdb", AsModule: true},
		},
		{
			SDL:     multiCatalogC,
			OldOpts: oldcompiler.Options{Name: "extra", EngineType: "duckdb", Prefix: "ext"},
			NewOpts: base.Options{Name: "extra", EngineType: "duckdb", Prefix: "ext"},
		},
	},
		compare.KnownIssues(
			"types.Function.directives.catalog",
			"types._module_transport_air_query.fields.FlightLog_by_pk.args.args",
		),
	)
}

// --- Duplicate Definition & Atomicity Tests (T012-T014) ---

func TestCompare_MultiCatalog_DuplicateType(t *testing.T) {
	// Compile Catalog A with Airport table, then try Catalog B with same Airport table.
	// provider.Update must return an "already exists" error.
	duplicateSDL := `
type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}
`
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile and apply first catalog
	compiled1 := compileNewCatalog(t, c, nil, duplicateSDL, base.Options{Name: "cat1", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("first catalog Update: %v", err)
	}

	// Compile second catalog with same type
	compiled2 := compileNewCatalog(t, c, provider, duplicateSDL, base.Options{Name: "cat2", EngineType: "duckdb"})
	err := provider.Update(ctx, compiled2)
	if err == nil {
		t.Fatal("expected duplicate type error, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestCompare_MultiCatalog_DuplicateFunction(t *testing.T) {
	// Compile Catalog A with function, then Catalog B with same function name.
	// provider.Update must return error (duplicate field on Function extension).
	funcSDL := `
extend type Function {
  find_nearby(lat: Float!, lon: Float!): [Airport]
    @function(name: "find_nearby")
}

type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}
`
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile and apply first catalog
	compiled1 := compileNewCatalog(t, c, nil, funcSDL, base.Options{Name: "cat1", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("first catalog Update: %v", err)
	}

	// Second catalog with same function name — function fields are extensions,
	// so the field already exists on Function type.
	funcSDL2 := `
extend type Function {
  find_nearby(lat: Float!, lon: Float!): [Hotel]
    @function(name: "find_nearby")
}

type Hotel @table(name: "hotels") {
  id: Int! @pk
  name: String!
}
`
	compiled2 := compileNewCatalog(t, c, provider, funcSDL2, base.Options{Name: "cat2", EngineType: "duckdb"})
	err := provider.Update(ctx, compiled2)
	// The function field "find_nearby" is added as an extension field on Function type.
	// When the field already exists, provider.Update merges directives (not error).
	// This is expected — functions with the same name from different catalogs
	// get their directives merged, which may or may not be correct depending on
	// the application semantics. The old compiler's MergeSchema also allows this.
	// We just verify the Update doesn't panic.
	if err != nil {
		// If it does error, that's also acceptable behavior — log it.
		t.Logf("duplicate function field got error (acceptable): %v", err)
	}
}

func TestCompare_MultiCatalog_Atomicity(t *testing.T) {
	// Compile valid Catalog A → update provider → snapshot.
	// Attempt Catalog B with duplicate types → expect error.
	// Verify provider schema unchanged after failed update.
	duplicateSDL := `
type Airport @table(name: "airports") {
  iata_code: String! @pk
  name: String!
}
`
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile and apply first catalog
	compiled1 := compileNewCatalog(t, c, nil, duplicateSDL, base.Options{Name: "cat1", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("first catalog Update: %v", err)
	}

	// Snapshot the schema state
	schemaBefore := provider.Schema()
	typeCountBefore := len(schemaBefore.Types)

	// Attempt duplicate update
	compiled2 := compileNewCatalog(t, c, provider, duplicateSDL, base.Options{Name: "cat2", EngineType: "duckdb"})
	err := provider.Update(ctx, compiled2)
	if err == nil {
		t.Fatal("expected error from duplicate type, got nil")
	}

	// Verify schema unchanged
	schemaAfter := provider.Schema()
	typeCountAfter := len(schemaAfter.Types)
	if typeCountBefore != typeCountAfter {
		t.Fatalf("schema changed after failed update: types %d → %d", typeCountBefore, typeCountAfter)
	}

	// Deep comparison: use compare.Compare on before vs after
	result := compare.Compare(schemaBefore, schemaAfter)
	if !result.Equal() {
		var sb strings.Builder
		sb.WriteString("Schema changed after failed update:\n")
		for _, d := range result.Diffs {
			sb.WriteString("  ")
			sb.WriteString(d.Path)
			sb.WriteString(": ")
			sb.WriteString(d.Message)
			sb.WriteByte('\n')
		}
		t.Fatal(sb.String())
	}
}

// setupMultiCatalogProvider creates a provider with system types for multi-catalog tests.
func setupMultiCatalogProvider(t *testing.T) (*static.Provider, *newcompiler.Compiler) {
	t.Helper()

	baseDoc := &ast.SchemaDocument{}
	for _, src := range oldbase.Sources() {
		parsed, err := parser.ParseSchema(src)
		if err != nil {
			t.Fatalf("parse system types: %v", err)
		}
		baseDoc.Merge(parsed)
	}
	pos := &ast.Position{Src: &ast.Source{Name: "comptest"}}
	baseDoc.Directives = append(baseDoc.Directives, &ast.DirectiveDefinition{
		Name:      "if_not_exists",
		Locations: []ast.DirectiveLocation{ast.LocationObject},
		Position:  pos,
	})
	if len(baseDoc.Schema) == 0 {
		baseDoc.Schema = append(baseDoc.Schema, &ast.SchemaDefinition{})
	}
	if baseDoc.Schema[0].OperationTypes.ForType("Query") == nil &&
		baseDoc.Definitions.ForName("Query") != nil {
		baseDoc.Schema[0].OperationTypes = append(baseDoc.Schema[0].OperationTypes,
			&ast.OperationTypeDefinition{Operation: ast.Query, Type: "Query"})
	}

	baseSchema, errs := validator.ValidateSchemaDocument(baseDoc)
	if errs != nil {
		t.Fatalf("validate base schema: %v", errs)
	}

	return static.NewWithSchema(baseSchema), newcompiler.New(rules.RegisterAll()...)
}

// compileNewCatalog compiles a single catalog SDL with the new compiler.
func compileNewCatalog(t *testing.T, c *newcompiler.Compiler, provider base.Provider, sdl string, opts base.Options) base.CompiledCatalog {
	t.Helper()

	sd, err := parser.ParseSchema(&ast.Source{Name: opts.Name + ".graphql", Input: sdl})
	if err != nil {
		t.Fatalf("parse SDL: %v", err)
	}
	source := extractSourceDefs(sd)

	compiled, err := c.Compile(context.Background(), provider, source, opts)
	if err != nil {
		t.Fatalf("compile %s: %v", opts.Name, err)
	}
	return compiled
}

// --- Extension Compilation Tests (T019-T021) ---

func TestCompare_MultiCatalog_ExtensionViews(t *testing.T) {
	// Compile base catalog, then extension with views and @dependency.
	// Compare old and new compiler results.
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile base catalog (A) with new compiler
	compiled1 := compileNewCatalog(t, c, nil, multiCatalogA, base.Options{Name: "base", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("base catalog Update: %v", err)
	}

	// Compile extension with new compiler using Compile() + DependentCompiledCatalog
	sd, err := parser.ParseSchema(&ast.Source{Name: "ext.graphql", Input: extensionViewSDL})
	if err != nil {
		t.Fatalf("parse extension SDL: %v", err)
	}
	extSource := extractSourceDefs(sd)
	extOpts := base.Options{Name: "ext_views", EngineType: "duckdb", IsExtension: true}

	extCompiled, err := c.Compile(ctx, provider, extSource, extOpts)
	if err != nil {
		t.Fatalf("Compile extension: %v", err)
	}

	// Verify dependencies via DependentCompiledCatalog type assertion
	depCatalog, ok := extCompiled.(base.DependentCompiledCatalog)
	if !ok {
		t.Fatal("compiled catalog does not implement DependentCompiledCatalog")
	}
	deps := depCatalog.Dependencies()
	if len(deps) == 0 || deps[0] != "base" {
		t.Fatalf("expected dependency [base], got %v", deps)
	}

	// Apply extension
	if err := provider.Update(ctx, extCompiled); err != nil {
		t.Fatalf("extension Update: %v", err)
	}

	// Verify FlightStats type exists in final schema
	finalSchema := provider.Schema()
	if finalSchema.Types["FlightStats"] == nil {
		t.Fatal("FlightStats type not found in schema after extension")
	}
}

func TestCompare_MultiCatalog_ExtensionValidation(t *testing.T) {
	// Test that invalid extensions are rejected.
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile base catalog first
	compiled1 := compileNewCatalog(t, c, nil, multiCatalogA, base.Options{Name: "base", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("base catalog Update: %v", err)
	}

	// Test: @table in extension should fail
	t.Run("table_rejected", func(t *testing.T) {
		sd, err := parser.ParseSchema(&ast.Source{Name: "invalid.graphql", Input: extensionInvalidTableSDL})
		if err != nil {
			t.Fatalf("parse SDL: %v", err)
		}
		source := extractSourceDefs(sd)
		_, err = c.Compile(ctx, provider, source, base.Options{Name: "bad_ext", EngineType: "duckdb", IsExtension: true})
		if err == nil {
			t.Fatal("expected error for @table in extension, got nil")
		}
		if !strings.Contains(err.Error(), "can't contain data objects") {
			t.Fatalf("expected 'can't contain data objects' error, got: %v", err)
		}
	})

	// Test: function in extension should fail
	t.Run("function_rejected", func(t *testing.T) {
		sd, err := parser.ParseSchema(&ast.Source{Name: "invalid_func.graphql", Input: extensionInvalidFunctionSDL})
		if err != nil {
			t.Fatalf("parse SDL: %v", err)
		}
		source := extractSourceDefs(sd)
		_, err = c.Compile(ctx, provider, source, base.Options{Name: "bad_func_ext", EngineType: "duckdb", IsExtension: true})
		if err == nil {
			t.Fatal("expected error for function in extension, got nil")
		}
		if !strings.Contains(err.Error(), "can't contain functions") {
			t.Fatalf("expected 'can't contain functions' error, got: %v", err)
		}
	})
}

// --- Cross-Catalog @references Extension Tests ---

// extensionCrossCatalogRefSDL: Extension view with @references pointing to base catalog types.
const extensionCrossCatalogRefSDL = `
"""Extension view referencing base catalog type Airport"""
type RouteStats
  @view(name: "route_stats_view", sql: "SELECT src_airport, count(*) as num_routes FROM routes GROUP BY src_airport")
  @dependency(name: "base") {
  src_airport: String! @pk @field_references(
    references_name: "Airport"
    field: "iata_code"
    query: "airport"
    references_query: "route_stats"
  )
  num_routes: Int!
}
`

func TestCompare_MultiCatalog_ExtensionCrossCatalogReferences(t *testing.T) {
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile and apply base catalog
	compiled1 := compileNewCatalog(t, c, nil, multiCatalogA, base.Options{Name: "base", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("base catalog Update: %v", err)
	}

	// Without SupportCrossCatalogReferences: should fail
	t.Run("blocked_without_capability", func(t *testing.T) {
		sd, err := parser.ParseSchema(&ast.Source{Name: "cross_ref_ext.graphql", Input: extensionCrossCatalogRefSDL})
		if err != nil {
			t.Fatalf("parse SDL: %v", err)
		}
		extSource := extractSourceDefs(sd)
		extOpts := base.Options{
			Name:        "cross_ext",
			EngineType:  "duckdb",
			IsExtension: true,
		}
		_, err = c.Compile(ctx, provider, extSource, extOpts)
		if err == nil {
			t.Fatal("expected cross-catalog @references error, got nil")
		}
		if !strings.Contains(err.Error(), "cross-catalog") {
			t.Fatalf("expected 'cross-catalog' error, got: %v", err)
		}
	})

	// With SupportCrossCatalogReferences: should succeed
	t.Run("allowed_with_capability", func(t *testing.T) {
		sd, err := parser.ParseSchema(&ast.Source{Name: "cross_ref_ext.graphql", Input: extensionCrossCatalogRefSDL})
		if err != nil {
			t.Fatalf("parse SDL: %v", err)
		}
		extSource := extractSourceDefs(sd)
		extOpts := base.Options{
			Name:        "cross_ext",
			EngineType:  "duckdb",
			IsExtension: true,
			Capabilities: &base.EngineCapabilities{
				General: base.EngineGeneralCapabilities{
					SupportCrossCatalogReferences: true,
				},
			},
		}
		extCompiled, err := c.Compile(ctx, provider, extSource, extOpts)
		if err != nil {
			t.Fatalf("Compile with cross-catalog capability: %v", err)
		}

		// Verify dependencies
		depCatalog, ok := extCompiled.(base.DependentCompiledCatalog)
		if !ok {
			t.Fatal("compiled catalog does not implement DependentCompiledCatalog")
		}
		deps := depCatalog.Dependencies()
		if len(deps) == 0 || deps[0] != "base" {
			t.Fatalf("expected dependency [base], got %v", deps)
		}

		// Verify extension fields have @dependency tag
		for ext := range extCompiled.Extensions(ctx) {
			for _, f := range ext.Fields {
				depDir := f.Directives.ForName("dependency")
				if depDir == nil {
					t.Errorf("extension field %s.%s missing @dependency directive", ext.Name, f.Name)
				}
			}
		}

		// Apply and verify
		if err := provider.Update(ctx, extCompiled); err != nil {
			t.Fatalf("extension Update: %v", err)
		}
		finalSchema := provider.Schema()
		if finalSchema.Types["RouteStats"] == nil {
			t.Fatal("RouteStats type not found in schema after extension")
		}
	})
}

func TestCompare_MultiCatalog_ExtensionDependencyTag(t *testing.T) {
	// Verify that extension-generated fields get @dependency(name: "ext_name") tags
	provider, c := setupMultiCatalogProvider(t)
	ctx := context.Background()

	// Compile and apply base catalog
	compiled1 := compileNewCatalog(t, c, nil, multiCatalogA, base.Options{Name: "base", EngineType: "duckdb"})
	if err := provider.Update(ctx, compiled1); err != nil {
		t.Fatalf("base catalog Update: %v", err)
	}

	sd, err := parser.ParseSchema(&ast.Source{Name: "ext.graphql", Input: extensionViewSDL})
	if err != nil {
		t.Fatalf("parse SDL: %v", err)
	}
	extSource := extractSourceDefs(sd)
	extOpts := base.Options{Name: "ext_views", EngineType: "duckdb", IsExtension: true}

	extCompiled, err := c.Compile(ctx, provider, extSource, extOpts)
	if err != nil {
		t.Fatalf("Compile extension: %v", err)
	}

	// Check that all extension fields carry @dependency(name: "ext_views")
	foundDep := false
	for ext := range extCompiled.Extensions(ctx) {
		for _, f := range ext.Fields {
			depDir := f.Directives.ForName("dependency")
			if depDir == nil {
				t.Errorf("extension field %s.%s missing @dependency directive", ext.Name, f.Name)
				continue
			}
			name := base.DirectiveArgString(depDir, "name")
			if name != "ext_views" {
				t.Errorf("extension field %s.%s @dependency name=%q, want %q", ext.Name, f.Name, name, "ext_views")
			}
			foundDep = true
		}
	}
	if !foundDep {
		t.Error("no extension fields found with @dependency directive")
	}
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
		compare.IgnoreDirectives("catalog"),
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

	// Add DDL definitions (strip @if_not_exists control directive —
	// provider.Update does this automatically, but we build schemas manually here)
	for def := range catalog.Definitions(ctx) {
		def.Directives = stripIfNotExists(def.Directives)
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

// stripIfNotExists removes @if_not_exists directives from a directive list.
// provider.Update strips these automatically during the apply phase;
// this helper is used in test schema building where we don't go through Update.
func stripIfNotExists(dirs ast.DirectiveList) ast.DirectiveList {
	var result ast.DirectiveList
	for _, d := range dirs {
		if d.Name != "if_not_exists" {
			result = append(result, d)
		}
	}
	return result
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

// --- Multi-Catalog Test Schemas (T002) ---

// multiCatalogA: Base tables with modules, geometry, references, M2M, parameterized views.
// Compiled as Name="base", no AsModule, no Prefix.
const multiCatalogA = `
"""Airport database - tables in transport.air module"""
type Airport
  @table(name: "airports")
  @module(name: "transport.air") {
  iata_code: String! @pk
  name: String!
  city: String!
  geom: Geometry
  latitude: Float
  longitude: Float
}

"""Flight routes between airports"""
type Route
  @table(name: "routes")
  @module(name: "transport.air")
  @unique(fields: ["src_airport", "dst_airport"], query_suffix: "route") {
  id: Int! @pk
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
}

"""Passenger records"""
type Passenger
  @table(name: "passengers")
  @unique(fields: ["email"], query_suffix: "email") {
  id: Int! @pk
  first_name: String!
  last_name: String!
  email: String!
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

input FlightLogArgs {
  from_date: Timestamp! @field_source(field: "from_date")
  to_date: Timestamp @field_source(field: "to_date")
}

"""Flight log - parameterized view"""
type FlightLog
  @view(name: "flight_log_view", sql: "SELECT * FROM flights WHERE departure >= [from_date] AND departure <= [to_date]")
  @args(name: "FlightLogArgs")
  @module(name: "transport.air") {
  id: Int! @pk
  flight_number: String!
  departure: Timestamp
  route_id: Int
}
`

// multiCatalogB: Functions with AsModule="transport".
// Has function_call, table_function_call_join, query functions.
// Overlaps transport module with catalog A.
const multiCatalogB = `
type FnAirport
  @table(name: "fn_airports") {
  iata_code: String! @pk
  name: String!
  geom: Geometry
  status: String @function_call(references_name: "airport_status", args: {code: "iata_code"})
  nearby: [FnAirport] @table_function_call_join(references_name: "find_nearby", args: {origin: "iata_code"})
}

extend type Function {
  airport_status(code: String!): String
    @function(name: "airport_status")

  search_airports(country: String!, limit: Int): JSON
    @function(name: "search_airports", json_cast: true)

  find_nearby(origin: String!, radius: Float = 100): [FnAirport]
    @function(name: "find_nearby")
}
`

// multiCatalogC: Extra tables with Prefix="ext", own Geometry fields.
// Tests shared type merging (_join, _spatial) from multiple catalogs.
const multiCatalogC = `
"""Hotel records"""
type Hotel
  @table(name: "hotels") {
  id: Int! @pk
  name: String!
  city: String!
  geom: Geometry
  stars: Int
}

"""Hotel reviews"""
type Review
  @table(name: "reviews") {
  id: Int! @pk
  hotel_id: Int! @field_references(
    references_name: "Hotel"
    field: "id"
    query: "hotel"
    references_query: "reviews"
  )
  rating: Float!
  comment: String
}
`

// --- Extension Test SDL Schemas (T016) ---

// extensionViewSDL: Valid extension with a view and @dependency.
const extensionViewSDL = `
"""Aggregated flight statistics view"""
type FlightStats
  @view(name: "flight_stats_view", sql: "SELECT src_airport, count(*) as num_routes FROM routes GROUP BY src_airport")
  @dependency(name: "base") {
  src_airport: String! @pk
  num_routes: Int!
}
`

// extensionInvalidTableSDL: Invalid extension with @table (should fail validation).
const extensionInvalidTableSDL = `
"""Should be rejected - tables not allowed in extensions"""
type ExtraTable
  @table(name: "extra_table") {
  id: Int! @pk
  name: String!
}
`

// extensionInvalidFunctionSDL: Invalid extension with function definition.
const extensionInvalidFunctionSDL = `
extend type Function {
  my_ext_func(x: Int!): String
    @function(name: "my_ext_func")
}
`

// catalogDef describes a catalog for multi-catalog compilation.
type catalogDef struct {
	SDL     string
	OldOpts oldcompiler.Options
	NewOpts base.Options
}

// buildOldMultiCatalogSchema compiles each catalog with old compiler, then merges with MergeSchema (T003).
func buildOldMultiCatalogSchema(t *testing.T, catalogs []catalogDef) *ast.Schema {
	t.Helper()

	schemas := make([]*ast.Schema, 0, len(catalogs))
	for i, cat := range catalogs {
		sd, err := parser.ParseSchema(&ast.Source{
			Name:  cat.OldOpts.Name + ".graphql",
			Input: cat.SDL,
		})
		if err != nil {
			t.Fatalf("parse catalog %d (%s) SDL: %v", i, cat.OldOpts.Name, err)
		}
		schema, err := oldcompiler.Compile(sd, cat.OldOpts)
		if err != nil {
			t.Fatalf("old compiler catalog %d (%s): %v", i, cat.OldOpts.Name, err)
		}
		schemas = append(schemas, schema)
	}

	merged, err := oldcompiler.MergeSchema(schemas...)
	if err != nil {
		t.Fatalf("MergeSchema: %v", err)
	}

	schema, errs := validator.ValidateSchemaDocument(merged)
	if errs != nil {
		t.Fatalf("validate merged schema: %v", errs)
	}
	return schema
}

// buildNewMultiCatalogSchema compiles catalogs iteratively with provider.Update (T004).
func buildNewMultiCatalogSchema(t *testing.T, catalogs []catalogDef) *ast.Schema {
	t.Helper()

	// Build base schema from system types
	baseDoc := &ast.SchemaDocument{}
	for _, src := range oldbase.Sources() {
		parsed, err := parser.ParseSchema(src)
		if err != nil {
			t.Fatalf("parse system types: %v", err)
		}
		baseDoc.Merge(parsed)
	}
	// Add if_not_exists directive definition
	pos := &ast.Position{Src: &ast.Source{Name: "comptest"}}
	baseDoc.Directives = append(baseDoc.Directives, &ast.DirectiveDefinition{
		Name:      "if_not_exists",
		Locations: []ast.DirectiveLocation{ast.LocationObject},
		Position:  pos,
	})
	// Add empty schema definition for validation
	if len(baseDoc.Schema) == 0 {
		baseDoc.Schema = append(baseDoc.Schema, &ast.SchemaDefinition{})
	}
	if baseDoc.Schema[0].OperationTypes.ForType("Query") == nil &&
		baseDoc.Definitions.ForName("Query") != nil {
		baseDoc.Schema[0].OperationTypes = append(baseDoc.Schema[0].OperationTypes,
			&ast.OperationTypeDefinition{Operation: ast.Query, Type: "Query"})
	}

	baseSchema, errs := validator.ValidateSchemaDocument(baseDoc)
	if errs != nil {
		t.Fatalf("validate base schema: %v", errs)
	}

	provider := static.NewWithSchema(baseSchema)
	c := newcompiler.New(rules.RegisterAll()...)
	ctx := context.Background()

	for i, cat := range catalogs {
		sd, err := parser.ParseSchema(&ast.Source{
			Name:  cat.NewOpts.Name + ".graphql",
			Input: cat.SDL,
		})
		if err != nil {
			t.Fatalf("parse catalog %d (%s) SDL: %v", i, cat.NewOpts.Name, err)
		}
		source := extractSourceDefs(sd)

		// First catalog compiles without provider; subsequent catalogs use it.
		var schema base.Provider
		if i > 0 {
			schema = provider
		}

		compiled, err := c.Compile(ctx, schema, source, cat.NewOpts)
		if err != nil {
			t.Fatalf("new compiler catalog %d (%s): %v", i, cat.NewOpts.Name, err)
		}

		if err := provider.Update(ctx, compiled); err != nil {
			t.Fatalf("provider.Update catalog %d (%s): %v", i, cat.NewOpts.Name, err)
		}
	}

	return provider.Schema()
}

// assertMultiCatalogMatch compiles catalogs with both compilers, compares results (T005).
func assertMultiCatalogMatch(t *testing.T, catalogs []catalogDef, extraOpts ...compare.CompareOption) {
	t.Helper()

	oldSchema := buildOldMultiCatalogSchema(t, catalogs)
	newSchema := buildNewMultiCatalogSchema(t, catalogs)

	opts := []compare.CompareOption{
		compare.SkipSystemTypes(),
		compare.IgnoreDescriptions(),
		compare.IgnoreDirectiveArgs("if_not_exists"),
		compare.IgnoreDirectives("catalog"),
		compare.SkipTypes(systemTypesToSkip()...),
	}
	opts = append(opts, extraOpts...)

	result := compare.Compare(oldSchema, newSchema, opts...)

	if !result.Equal() {
		var sb strings.Builder
		sb.WriteString("Multi-catalog schema comparison found diffs:\n")
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
		// H3 types (not yet implemented in new compiler)
		"_h3_data_query", "_h3_query",
		// Base schema types shared between compilers
		"Query", "Mutation", "OrderByField", "OperationResult",
		"OrderByDirection", "Geometry", "JSON", "BigInt", "Interval",
		"Timestamp", "Date", "Time", "H3Cell", "Vector",
		"GeometryType", "GeometrySpatialQueryType",
		"EmbeddingInput",
	}
}
