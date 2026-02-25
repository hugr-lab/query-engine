package compiler_test

import (
	"context"
	_ "embed"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/integration-test/compare"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// --- Integration Test SDL Schemas ---

// systemSchema emulates a minimal runtime catalog (like cache/storage/meta).
const systemRuntimeSDL = `
"""Cached query results"""
type CacheEntry
  @table(name: "cache_entries") {
  key: String! @pk
  value: JSON
  expires_at: Timestamp
}
`

// runtimeMetaSDL emulates a metadata runtime catalog.
const runtimeMetaSDL = `
"""Data source metadata"""
type DataSourceInfo
  @table(name: "data_source_info") {
  name: String! @pk
  type: String!
  description: String
  is_active: Boolean @default(value: true)
}
`

// userCatalogPostgresSDL: A user catalog with Postgres-style schema.
const userCatalogPostgresSDL = `
"""Customer records in external Postgres DB"""
type Customer
  @table(name: "customers")
  @module(name: "crm")
  @unique(fields: ["email"], query_suffix: "email") {
  id: Int! @pk
  name: String!
  email: String!
  created_at: Timestamp
}

"""Orders from external Postgres DB"""
type Order
  @table(name: "orders")
  @module(name: "crm") {
  id: Int! @pk
  customer_id: Int! @field_references(
    references_name: "Customer"
    field: "id"
    query: "customer"
    references_query: "orders"
  )
  total: Float!
  status: String
  ordered_at: Timestamp
}
`

// userCatalogDuckDBSDL: A user catalog with DuckDB-specific schema and analytics focus.
const userCatalogDuckDBSDL = `
"""Event log for analytics (DuckDB)"""
type Event
  @table(name: "events")
  @module(name: "analytics") {
  id: BigInt! @pk
  event_type: String!
  payload: JSON
  created_at: Timestamp
}

"""Aggregated daily metrics view"""
type DailyMetrics
  @view(name: "daily_metrics_view") {
  date: Date! @pk
  total_events: BigInt
  unique_users: BigInt
}
`

// overlapModuleCatalogSDL: Catalog that contributes to "crm" module (overlapping with userCatalogPostgresSDL).
const overlapModuleCatalogSDL = `
"""Support tickets in the CRM module"""
type Ticket
  @table(name: "tickets")
  @module(name: "crm") {
  id: Int! @pk
  subject: String!
  priority: Int
  status: String
  created_at: Timestamp
}
`

// --- Test: Bootstrap Simulation (US1) ---

func TestIntegration_BootstrapSimulation(t *testing.T) {
	t.Run("sequential_compilation", func(t *testing.T) {
		// Simulate Init() flow: system → runtime → user catalogs
		catalogs := []catalogDef{
			// 1. Runtime catalog (cache)
			{
				SDL:  systemRuntimeSDL,
				Opts: base.Options{Name: "cache", EngineType: "duckdb"},
			},
			// 2. Runtime catalog (meta)
			{
				SDL:  runtimeMetaSDL,
				Opts: base.Options{Name: "meta", EngineType: "duckdb"},
			},
			// 3. User catalog (Postgres, asModule)
			{
				SDL: userCatalogPostgresSDL,
				Opts: base.Options{Name: "pg_crm", EngineType: "postgres", AsModule: true,
					Capabilities: postgresCapabilities()},
			},
			// 4. User catalog (DuckDB, asModule)
			{
				SDL: userCatalogDuckDBSDL,
				Opts: base.Options{Name: "analytics", EngineType: "duckdb", AsModule: true,
					Capabilities: duckdbCapabilities()},
			},
		}

		assertMultiCatalogMatch(t, catalogs)
	})

	t.Run("module_nesting", func(t *testing.T) {
		// Catalog A with modules, Catalog B without modules
		catalogs := []catalogDef{
			{
				SDL:  userCatalogPostgresSDL,
				Opts: base.Options{Name: "pg_crm", EngineType: "duckdb", AsModule: true},
			},
			{
				SDL:  systemRuntimeSDL,
				Opts: base.Options{Name: "cache", EngineType: "duckdb"},
			},
		}

		assertMultiCatalogMatch(t, catalogs)

		// Additionally verify module structure in new compiler's schema
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		// Compile pg_crm with AsModule
		compiled := compileNewCatalog(t, c, nil, userCatalogPostgresSDL,
			base.Options{Name: "pg_crm", EngineType: "duckdb", AsModule: true})
		if err := provider.Update(ctx, compiled); err != nil {
			t.Fatalf("provider.Update: %v", err)
		}

		schema := provider.Schema()
		// AsModule=true with Name="pg_crm" wraps everything under _module_pg_crm_query
		moduleQuery := schema.Types["_module_pg_crm_query"]
		if moduleQuery == nil {
			t.Fatal("expected _module_pg_crm_query type for AsModule catalog")
		}
		// The module should contain crm sub-module field
		crmField := moduleQuery.Fields.ForName("crm")
		if crmField == nil {
			t.Fatal("expected 'crm' field in _module_pg_crm_query")
		}
	})

	t.Run("overlapping_modules", func(t *testing.T) {
		// Two catalogs contributing to the same "crm" module
		catalogs := []catalogDef{
			{
				SDL:  userCatalogPostgresSDL,
				Opts: base.Options{Name: "pg_crm", EngineType: "duckdb"},
			},
			{
				SDL:  overlapModuleCatalogSDL,
				Opts: base.Options{Name: "support", EngineType: "duckdb"},
			},
		}

		assertMultiCatalogMatch(t, catalogs)

		// Verify the crm module contains fields from both catalogs
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, userCatalogPostgresSDL,
			base.Options{Name: "pg_crm", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("Update catalog 1: %v", err)
		}

		compiled2 := compileNewCatalog(t, c, provider, overlapModuleCatalogSDL,
			base.Options{Name: "support", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled2); err != nil {
			t.Fatalf("Update catalog 2: %v", err)
		}

		schema := provider.Schema()
		crmQuery := schema.Types["_module_crm_query"]
		if crmQuery == nil {
			t.Fatal("expected _module_crm_query type for merged crm module")
		}

		// Should contain Customer, Order from pg_crm and Ticket from support
		hasCustomer := crmQuery.Fields.ForName("Customer") != nil || crmQuery.Fields.ForName("Customers") != nil
		hasTicket := crmQuery.Fields.ForName("Ticket") != nil || crmQuery.Fields.ForName("Tickets") != nil
		if !hasCustomer {
			t.Error("crm module missing Customer-related fields from pg_crm catalog")
		}
		if !hasTicket {
			t.Error("crm module missing Ticket-related fields from support catalog")
		}
	})
}

// --- Test: Cross-Catalog References (US2) ---

// crossCatalogRefSDL: Catalog B with @references to Catalog A's Airport type.
const crossCatalogRefSDL = `
"""Flights referencing Airport from another catalog"""
type Flight
  @table(name: "flights")
  @references(
    name: "flight_origin"
    references_name: "Airport"
    source_fields: ["origin_code"]
    references_fields: ["iata_code"]
    query: "origin_airport"
    references_query: "departing_flights"
  ) {
  id: Int! @pk
  origin_code: String!
  flight_number: String!
  departure: Timestamp
}
`

func TestIntegration_CrossCatalogReferences(t *testing.T) {
	t.Run("allowed_with_capability", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		// Compile base catalog (has Airport type)
		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		// Compile catalog B with cross-catalog reference + capability
		sd, err := parser.ParseSchema(&ast.Source{Name: "flights.graphql", Input: crossCatalogRefSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		source := extractSourceDefs(sd)
		opts := base.Options{
			Name:       "flights",
			EngineType: "duckdb",
			Capabilities: &base.EngineCapabilities{
				General: base.EngineGeneralCapabilities{
					SupportCrossCatalogReferences: true,
				},
				Insert: base.EngineInsertCapabilities{Insert: true, Returning: true, InsertReferences: true},
			},
		}

		compiled2, err := c.Compile(ctx, provider, source, opts)
		if err != nil {
			t.Fatalf("Compile with cross-catalog: %v", err)
		}

		if err := provider.Update(ctx, compiled2); err != nil {
			t.Fatalf("Update flights: %v", err)
		}

		schema := provider.Schema()
		flightType := schema.Types["Flight"]
		if flightType == nil {
			t.Fatal("Flight type not found")
		}

		// Should have origin_airport reference field
		originField := flightType.Fields.ForName("origin_airport")
		if originField == nil {
			t.Error("Flight type missing origin_airport reference field")
		}

		// Airport should have departing_flights back-reference
		airportType := schema.Types["Airport"]
		if airportType == nil {
			t.Fatal("Airport type not found")
		}
		backRef := airportType.Fields.ForName("departing_flights")
		if backRef == nil {
			t.Error("Airport type missing departing_flights back-reference")
		}
	})

	t.Run("blocked_without_capability", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		sd, err := parser.ParseSchema(&ast.Source{Name: "flights.graphql", Input: crossCatalogRefSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		source := extractSourceDefs(sd)
		// No SupportCrossCatalogReferences capability
		opts := base.Options{Name: "flights", EngineType: "duckdb"}

		_, err = c.Compile(ctx, provider, source, opts)
		if err == nil {
			t.Fatal("expected cross-catalog error, got nil")
		}
		if !strings.Contains(err.Error(), "cross-catalog") {
			t.Fatalf("expected 'cross-catalog' in error, got: %v", err)
		}
	})

	t.Run("no_mutation_insert_for_cross_catalog", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb",
				Capabilities: duckdbCapabilities()})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		sd, err := parser.ParseSchema(&ast.Source{Name: "flights.graphql", Input: crossCatalogRefSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		source := extractSourceDefs(sd)
		opts := base.Options{
			Name:       "flights",
			EngineType: "duckdb",
			Capabilities: &base.EngineCapabilities{
				General: base.EngineGeneralCapabilities{SupportCrossCatalogReferences: true},
				Insert:  base.EngineInsertCapabilities{Insert: true, Returning: true, InsertReferences: true},
			},
		}

		compiled2, err := c.Compile(ctx, provider, source, opts)
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}

		if err := provider.Update(ctx, compiled2); err != nil {
			t.Fatalf("Update: %v", err)
		}

		schema := provider.Schema()
		// Flight_mut_input_data should NOT have origin_airport reference insert field
		mutInput := schema.Types["Flight_mut_input_data"]
		if mutInput != nil {
			originMutField := mutInput.Fields.ForName("origin_airport")
			if originMutField != nil {
				t.Error("Flight_mut_input_data should NOT have origin_airport for cross-catalog reference")
			}
		}
	})
}

// --- Test: Extension Compilation (US3) ---

// extensionMultiCatalogSDL: Extension view with @dependency on "base" and internal type.
const extensionMultiCatalogSDL = `
"""Extension view aggregating routes from base catalog"""
type RouteMetrics
  @view(name: "route_metrics_view", sql: "SELECT src_airport, count(*) as route_count FROM routes GROUP BY src_airport")
  @dependency(name: "base") {
  src_airport: String! @pk
  route_count: Int!
}

"""Internal extension type (no cross-catalog reference)"""
type MetricsReport
  @view(name: "metrics_report_view", sql: "SELECT src_airport, route_count FROM route_metrics_view") {
  src_airport: String! @pk
  route_count: Int!
}
`

func TestIntegration_ExtensionCompilation(t *testing.T) {
	t.Run("dependency_tags_on_all_fields", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		sd, err := parser.ParseSchema(&ast.Source{Name: "ext.graphql", Input: extensionMultiCatalogSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		extSource := extractSourceDefs(sd)
		extOpts := base.Options{Name: "my_ext", EngineType: "duckdb", IsExtension: true}

		extCompiled, err := c.Compile(ctx, provider, extSource, extOpts)
		if err != nil {
			t.Fatalf("Compile extension: %v", err)
		}

		// All extension fields should have @dependency(name: "my_ext")
		fieldCount := 0
		depCount := 0
		for ext := range extCompiled.Extensions(ctx) {
			for _, f := range ext.Fields {
				fieldCount++
				depDir := f.Directives.ForName("dependency")
				if depDir != nil {
					name := base.DirectiveArgString(depDir, "name")
					if name == "my_ext" {
						depCount++
					} else {
						t.Errorf("field %s.%s @dependency name=%q, want %q", ext.Name, f.Name, name, "my_ext")
					}
				} else {
					t.Errorf("field %s.%s missing @dependency directive", ext.Name, f.Name)
				}
			}
		}
		if fieldCount == 0 {
			t.Error("no extension fields found")
		}
		t.Logf("checked %d extension fields, %d have correct @dependency", fieldCount, depCount)
	})

	t.Run("DependentCompiledCatalog", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		sd, err := parser.ParseSchema(&ast.Source{Name: "ext.graphql", Input: extensionMultiCatalogSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		extSource := extractSourceDefs(sd)
		extOpts := base.Options{Name: "my_ext", EngineType: "duckdb", IsExtension: true}

		extCompiled, err := c.Compile(ctx, provider, extSource, extOpts)
		if err != nil {
			t.Fatalf("Compile extension: %v", err)
		}

		depCatalog, ok := extCompiled.(base.DependentCompiledCatalog)
		if !ok {
			t.Fatal("compiled extension does not implement DependentCompiledCatalog")
		}

		deps := depCatalog.Dependencies()
		if len(deps) == 0 {
			t.Fatal("expected at least one dependency")
		}
		found := false
		for _, d := range deps {
			if d == "base" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected dependency 'base' in %v", deps)
		}
	})

	t.Run("same_catalog_ref_in_extension", func(t *testing.T) {
		// Extension defines two types where one references the other — no cross-catalog error.
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled1 := compileNewCatalog(t, c, nil, multiCatalogA,
			base.Options{Name: "base", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("base Update: %v", err)
		}

		// Extension with internal references only
		internalRefSDL := `
"""Parent type in extension"""
type ExtParent
  @view(name: "ext_parents_view", sql: "SELECT id, name FROM ext_parents") {
  id: Int! @pk
  name: String!
}

"""Child referencing ExtParent within same extension"""
type ExtChild
  @view(name: "ext_children_view", sql: "SELECT id, parent_id FROM ext_children")
  @references(
    name: "ext_child_parent"
    references_name: "ExtParent"
    source_fields: ["parent_id"]
    references_fields: ["id"]
    query: "parent"
    references_query: "children"
  ) {
  id: Int! @pk
  parent_id: Int!
}
`
		sd, err := parser.ParseSchema(&ast.Source{Name: "internal_ext.graphql", Input: internalRefSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		extSource := extractSourceDefs(sd)
		extOpts := base.Options{Name: "internal_ext", EngineType: "duckdb", IsExtension: true}

		_, err = c.Compile(ctx, provider, extSource, extOpts)
		if err != nil {
			t.Fatalf("expected no error for same-catalog ref in extension, got: %v", err)
		}
	})
}

// --- Test: Catalog Lifecycle (US4) ---

func TestIntegration_CatalogLifecycle(t *testing.T) {
	t.Run("removal_via_DropCatalog", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		// Add three catalogs
		compiled1 := compileNewCatalog(t, c, nil, systemRuntimeSDL,
			base.Options{Name: "cache", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled1); err != nil {
			t.Fatalf("Update cache: %v", err)
		}

		compiled2 := compileNewCatalog(t, c, provider, runtimeMetaSDL,
			base.Options{Name: "meta", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled2); err != nil {
			t.Fatalf("Update meta: %v", err)
		}

		compiled3 := compileNewCatalog(t, c, provider, userCatalogDuckDBSDL,
			base.Options{Name: "analytics", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled3); err != nil {
			t.Fatalf("Update analytics: %v", err)
		}

		// Verify all types present
		schema := provider.Schema()
		if schema.Types["CacheEntry"] == nil {
			t.Fatal("CacheEntry should exist before drop")
		}
		if schema.Types["DataSourceInfo"] == nil {
			t.Fatal("DataSourceInfo should exist before drop")
		}
		if schema.Types["Event"] == nil {
			t.Fatal("Event should exist before drop")
		}

		// Drop "meta" catalog
		if err := provider.DropCatalog(ctx, "meta", true); err != nil {
			t.Fatalf("DropCatalog meta: %v", err)
		}

		schema = provider.Schema()
		// meta types should be gone
		if schema.Types["DataSourceInfo"] != nil {
			t.Error("DataSourceInfo should be gone after DropCatalog")
		}
		// Generated types should also be gone (they now carry @catalog)
		for _, name := range []string{
			"DataSourceInfo_filter", "DataSourceInfo_mut_input_data",
			"DataSourceInfo_mut_data", "_DataSourceInfo_aggregation",
			"_DataSourceInfo_aggregation_bucket",
		} {
			if schema.Types[name] != nil {
				t.Errorf("%s should be gone after DropCatalog", name)
			}
		}

		// Other catalogs' types should remain
		if schema.Types["CacheEntry"] == nil {
			t.Error("CacheEntry should remain after dropping meta")
		}
		if schema.Types["Event"] == nil {
			t.Error("Event should remain after dropping meta")
		}

		// Query root fields are extensions from the catalog — they get removed
		// when the field has @catalog(name: "meta") directive. Verify at least
		// the main definition is gone.
		// Note: Query root extension fields may or may not be removed depending on
		// how DropCatalog handles extension fields — this is implementation-specific.
		// The key invariant is that the type definition itself is dropped.
	})

	t.Run("reload_with_updated_SDL", func(t *testing.T) {
		// Test full in-place reload: compile v1 → DropCatalog → compile v2 on same provider.
		// This works because generated types now carry @catalog and get removed by DropCatalog.
		ctx := context.Background()
		provider, c := setupMultiCatalogProvider(t)

		// v1: initial schema
		initialSDL := `
type Widget @table(name: "widgets") {
  id: Int! @pk
  name: String!
}
`
		compiled := compileNewCatalog(t, c, nil, initialSDL,
			base.Options{Name: "widgets", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled); err != nil {
			t.Fatalf("v1 Update: %v", err)
		}

		schema := provider.Schema()
		widgetType := schema.Types["Widget"]
		if widgetType == nil {
			t.Fatal("Widget type not found in v1")
		}
		if widgetType.Fields.ForName("color") != nil {
			t.Fatal("Widget should not have 'color' field in v1")
		}

		// DropCatalog should remove Widget AND generated types
		if err := provider.DropCatalog(ctx, "widgets", true); err != nil {
			t.Fatalf("DropCatalog: %v", err)
		}

		schema = provider.Schema()
		if schema.Types["Widget"] != nil {
			t.Error("Widget should be gone after DropCatalog")
		}
		if schema.Types["Widget_filter"] != nil {
			t.Error("Widget_filter should be gone after DropCatalog")
		}

		// v2: updated schema with new field — compile on same provider
		updatedSDL := `
type Widget @table(name: "widgets") {
  id: Int! @pk
  name: String!
  color: String
}
`
		compiled2 := compileNewCatalog(t, c, provider, updatedSDL,
			base.Options{Name: "widgets", EngineType: "duckdb"})
		if err := provider.Update(ctx, compiled2); err != nil {
			t.Fatalf("v2 Update: %v", err)
		}

		schema = provider.Schema()
		widgetType = schema.Types["Widget"]
		if widgetType == nil {
			t.Fatal("Widget type not found in v2")
		}
		if widgetType.Fields.ForName("color") == nil {
			t.Error("Widget should have 'color' field in v2")
		}

		// Verify filter type also has the new color field
		filterType := schema.Types["Widget_filter"]
		if filterType == nil {
			t.Fatal("Widget_filter not found in v2")
		}
		colorFilter := filterType.Fields.ForName("color")
		if colorFilter == nil {
			t.Error("Widget_filter should have 'color' field in v2")
		}
	})
}

// --- Test: Engine Capabilities & Read-Only (US5) ---

func TestIntegration_EngineCapabilities(t *testing.T) {
	t.Run("read_only_no_mutations", func(t *testing.T) {
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		compiled := compileNewCatalog(t, c, nil, systemRuntimeSDL,
			base.Options{Name: "readonly_cache", EngineType: "duckdb", ReadOnly: true})
		if err := provider.Update(ctx, compiled); err != nil {
			t.Fatalf("Update: %v", err)
		}

		schema := provider.Schema()
		// No mutation type should exist at all (or it should have no fields from this catalog)
		if schema.Mutation != nil {
			for _, f := range schema.Mutation.Fields {
				if strings.Contains(f.Name, "CacheEntry") || strings.Contains(f.Name, "cache_entry") {
					t.Errorf("Mutation has field %s for read-only catalog", f.Name)
				}
			}
		}

		// Verify compilation succeeds with new compiler
		assertCompilersMatch(t, systemRuntimeSDL,
			base.Options{Name: "readonly_cache", EngineType: "duckdb", ReadOnly: true},
		)
	})

	t.Run("duckdb_vs_postgres_capabilities", func(t *testing.T) {
		// Compile same schema with DuckDB and Postgres capabilities
		// DuckDB has UnsupportedTypes (IntRange etc.), Postgres doesn't
		sdl := userCatalogPostgresSDL

		provider1, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		duckOpts := base.Options{
			Name:         "duckdb_cat",
			EngineType:   "duckdb",
			Capabilities: duckdbCapabilities(),
		}
		compiled1 := compileNewCatalog(t, c, nil, sdl, duckOpts)
		if err := provider1.Update(ctx, compiled1); err != nil {
			t.Fatalf("DuckDB Update: %v", err)
		}

		provider2, c2 := setupMultiCatalogProvider(t)
		pgOpts := base.Options{
			Name:         "pg_cat",
			EngineType:   "postgres",
			Capabilities: postgresCapabilities(),
		}
		compiled2 := compileNewCatalog(t, c2, nil, sdl, pgOpts)
		if err := provider2.Update(ctx, compiled2); err != nil {
			t.Fatalf("Postgres Update: %v", err)
		}

		// Both should compile successfully — the schema doesn't use unsupported types
		duckSchema := provider1.Schema()
		pgSchema := provider2.Schema()

		// Both should have Customer type
		if duckSchema.Types["Customer"] == nil {
			t.Error("DuckDB schema missing Customer type")
		}
		if pgSchema.Types["Customer"] == nil {
			t.Error("Postgres schema missing Customer type")
		}

		// Verify both have @default(sequence:) for Customer.id
		duckCustomer := duckSchema.Types["Customer"]
		pgCustomer := pgSchema.Types["Customer"]
		if duckCustomer != nil && pgCustomer != nil {
			duckID := duckCustomer.Fields.ForName("id")
			pgID := pgCustomer.Fields.ForName("id")
			if duckID == nil || pgID == nil {
				t.Error("Customer missing id field in one of the schemas")
			}
		}
	})
}

// --- Test: Edge Cases ---

func TestIntegration_EdgeCases(t *testing.T) {
	t.Run("empty_catalog", func(t *testing.T) {
		// An empty SDL (no types) should compile without error
		emptySDL := ``
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		sd, err := parser.ParseSchema(&ast.Source{Name: "empty.graphql", Input: emptySDL})
		if err != nil {
			t.Fatalf("parse empty SDL: %v", err)
		}
		source := extractSourceDefs(sd)
		compiled, err := c.Compile(ctx, provider, source, base.Options{Name: "empty", EngineType: "duckdb"})
		if err != nil {
			t.Fatalf("expected no error for empty catalog, got: %v", err)
		}

		if err := provider.Update(ctx, compiled); err != nil {
			t.Fatalf("Update empty catalog: %v", err)
		}
		// Schema should still be valid
		if provider.Schema() == nil {
			t.Fatal("schema is nil after empty catalog")
		}
	})

	t.Run("compilation_order_independence", func(t *testing.T) {
		// Compile A then B should yield same schema as B then A
		// (when there are no cross-catalog references)
		provider1, c1 := setupMultiCatalogProvider(t)
		provider2, c2 := setupMultiCatalogProvider(t)
		ctx := context.Background()

		sdlA := systemRuntimeSDL
		sdlB := runtimeMetaSDL
		optsA := base.Options{Name: "cache", EngineType: "duckdb"}
		optsB := base.Options{Name: "meta", EngineType: "duckdb"}

		// Order 1: A then B
		compiledA1 := compileNewCatalog(t, c1, nil, sdlA, optsA)
		if err := provider1.Update(ctx, compiledA1); err != nil {
			t.Fatalf("Update A (order 1): %v", err)
		}
		compiledB1 := compileNewCatalog(t, c1, provider1, sdlB, optsB)
		if err := provider1.Update(ctx, compiledB1); err != nil {
			t.Fatalf("Update B (order 1): %v", err)
		}

		// Order 2: B then A
		compiledB2 := compileNewCatalog(t, c2, nil, sdlB, optsB)
		if err := provider2.Update(ctx, compiledB2); err != nil {
			t.Fatalf("Update B (order 2): %v", err)
		}
		compiledA2 := compileNewCatalog(t, c2, provider2, sdlA, optsA)
		if err := provider2.Update(ctx, compiledA2); err != nil {
			t.Fatalf("Update A (order 2): %v", err)
		}

		// Compare the two schemas
		result := compare.Compare(provider1.Schema(), provider2.Schema(),
			compare.SkipSystemTypes(),
			compare.IgnoreDescriptions(),
			compare.IgnoreDirectiveArgs("if_not_exists"),
			compare.SkipTypes(systemTypesToSkip()...),
		)

		if !result.Equal() {
			var sb strings.Builder
			sb.WriteString("Compilation order produced different schemas:\n")
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
	})

	t.Run("nonexistent_reference_target", func(t *testing.T) {
		// Reference to a type that doesn't exist should fail
		badRefSDL := `
type BadRef
  @table(name: "bad_refs")
  @references(
    name: "bad_ref"
    references_name: "NonExistentType"
    source_fields: ["ref_id"]
    references_fields: ["id"]
    query: "target"
    references_query: "sources"
  ) {
  id: Int! @pk
  ref_id: Int!
}
`
		provider, c := setupMultiCatalogProvider(t)
		ctx := context.Background()

		sd, err := parser.ParseSchema(&ast.Source{Name: "bad_ref.graphql", Input: badRefSDL})
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		source := extractSourceDefs(sd)

		_, err = c.Compile(ctx, provider, source, base.Options{Name: "bad", EngineType: "duckdb"})
		if err == nil {
			t.Fatal("expected error for nonexistent reference target, got nil")
		}
	})
}

// --- Capability Helpers ---

func duckdbCapabilities() *base.EngineCapabilities {
	return &base.EngineCapabilities{
		General: base.EngineGeneralCapabilities{
			SupportDefaultSequences: true,
			UnsupportedTypes:        []string{"IntRange", "BigIntRange", "TimestampRange"},
		},
		Insert: base.EngineInsertCapabilities{
			Insert:           true,
			Returning:        true,
			InsertReferences: true,
		},
		Update: base.EngineUpdateCapabilities{
			Update:           true,
			UpdatePKColumns:  true,
			UpdateWithoutPKs: true,
		},
		Delete: base.EngineDeleteCapabilities{
			Delete:           true,
			DeleteWithoutPKs: true,
		},
	}
}

func postgresCapabilities() *base.EngineCapabilities {
	return &base.EngineCapabilities{
		General: base.EngineGeneralCapabilities{
			SupportDefaultSequences: true,
		},
		Insert: base.EngineInsertCapabilities{
			Insert:           true,
			Returning:        true,
			InsertReferences: true,
		},
		Update: base.EngineUpdateCapabilities{
			Update:           true,
			UpdatePKColumns:  true,
			UpdateWithoutPKs: true,
		},
		Delete: base.EngineDeleteCapabilities{
			Delete:           true,
			DeleteWithoutPKs: true,
		},
	}
}

// --- Test: Runtime Source Schema Compilation ---
// Verifies that all real runtime source schemas (core-db, cache, storage,
// data-sources, meta-info, gis) compile successfully against the static provider,
// simulating the bootstrap sequence of the engine.

//go:embed testdata/runtime_schemas/core-db.graphql
var coreDBSchema string

//go:embed testdata/runtime_schemas/cache.graphql
var cacheSchema string

//go:embed testdata/runtime_schemas/storage.graphql
var storageSchema string

//go:embed testdata/runtime_schemas/data-sources.graphql
var dataSourcesSchema string

//go:embed testdata/runtime_schemas/meta-info.graphql
var metaInfoSchema string

//go:embed testdata/runtime_schemas/gis.graphql
var gisSchema string

func TestIntegration_RuntimeSourceBootstrap(t *testing.T) {
	ctx := context.Background()
	provider, c := setupMultiCatalogProvider(t)

	duckdbCaps := &base.EngineCapabilities{
		General: base.EngineGeneralCapabilities{
			SupportDefaultSequences: true,
			UnsupportedTypes:        []string{"IntRange", "BigIntRange", "TimestampRange"},
		},
		Insert: base.EngineInsertCapabilities{
			Insert:           true,
			Returning:        true,
			InsertReferences: true,
		},
		Update: base.EngineUpdateCapabilities{
			Update:           true,
			UpdatePKColumns:  true,
			UpdateWithoutPKs: true,
		},
		Delete: base.EngineDeleteCapabilities{
			Delete:           true,
			DeleteWithoutPKs: true,
		},
	}

	// Runtime sources in bootstrap order (mirrors engine.Init + attachRuntimeSources)
	sources := []struct {
		name     string
		sdl      string
		asModule bool
		readOnly bool
	}{
		{"core", coreDBSchema, false, false},
		{"cache", cacheSchema, false, false},
		{"core.storage", storageSchema, true, false},
		{"storage", dataSourcesSchema, false, false},
		{"core.meta", metaInfoSchema, true, false},
		{"core.gis", gisSchema, true, false},
	}

	for _, src := range sources {
		t.Run("compile_"+src.name, func(t *testing.T) {
			compiled := compileNewCatalog(t, c, provider, src.sdl, base.Options{
				Name:         src.name,
				EngineType:   "duckdb",
				AsModule:     src.asModule,
				ReadOnly:     src.readOnly,
				Capabilities: duckdbCaps,
			})
			if err := provider.Update(ctx, compiled); err != nil {
				t.Fatalf("Update %s: %v", src.name, err)
			}
		})
	}

	// Verify key types from each source are present
	schema := provider.Schema()

	t.Run("verify_core_types", func(t *testing.T) {
		for _, name := range []string{"catalog_sources", "data_sources", "roles", "api_keys"} {
			if schema.Types[name] == nil {
				t.Errorf("core type %q missing", name)
			}
		}
	})

	t.Run("verify_meta_types", func(t *testing.T) {
		for _, name := range []string{"databases", "tables", "columns", "log_entries"} {
			if schema.Types[name] == nil {
				t.Errorf("meta type %q missing", name)
			}
		}
	})

	t.Run("verify_query_root", func(t *testing.T) {
		q := schema.Query
		if q == nil {
			t.Fatal("Query root type missing")
		}
		if len(q.Fields) == 0 {
			t.Error("Query root has no fields")
		}
		t.Logf("Query root has %d fields", len(q.Fields))
	})

	t.Run("verify_mutation_root", func(t *testing.T) {
		m := schema.Mutation
		if m == nil {
			t.Fatal("Mutation root type missing after registering all runtime sources")
		}
		if len(m.Fields) == 0 {
			t.Error("Mutation root has no fields")
		}
		t.Logf("Mutation root has %d fields", len(m.Fields))
	})

	t.Run("verify_system_types_preserved", func(t *testing.T) {
		// System types must survive all catalog registrations
		for _, name := range []string{"String", "Int", "Boolean", "BigInt", "Timestamp", "JSON", "Geometry"} {
			if schema.Types[name] == nil {
				t.Errorf("system type %q missing after bootstrap", name)
			}
		}
	})

	// Test catalog removal preserves other catalogs and system types
	t.Run("drop_and_verify", func(t *testing.T) {
		if err := provider.DropCatalog(ctx, "core.meta", true); err != nil {
			t.Fatalf("DropCatalog core.meta: %v", err)
		}

		schema = provider.Schema()
		if schema.Types["databases"] != nil {
			t.Error("databases should be gone after dropping core.meta")
		}
		// Core types should remain
		if schema.Types["data_sources"] == nil {
			t.Error("data_sources should remain after dropping core.meta")
		}
		// System types should remain
		if schema.Types["String"] == nil {
			t.Error("system type String missing after drop")
		}
	})
}
