package catalog_test

import (
	"context"
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	catalogstore "github.com/hugr-lab/query-engine/pkg/catalog/store"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
)

const logicalTestSchema = `
type customers @module(name: "sales") @table(name: "customers") {
  id: Int! @pk
  name: String!
  country: String
}

type orders @module(name: "sales")
  @table(name: "orders")
  @references(
    name: "order_customer"
    references_name: "customers"
    source_fields: ["customer_id"]
    references_fields: ["id"]
    query: "customer"
    description: "Order customer"
    references_query: "orders"
    references_description: "Customer orders"
  ) {
  id: Int! @pk
  customer_id: Int!
  amount: Float
  status: String
  source_info: [tags]
    @join(
      references_name: "tags"
      source_fields: ["status"]
      references_fields: ["name"]
    )
}

type tags @module(name: "sales") @table(name: "tags") {
  name: String! @pk
}

type order_tags @module(name: "sales")
  @table(name: "order_tags", is_m2m: true) {
  order_id: Int! @pk @field_references(references_name: "orders", field: "id", query: "tags", references_query: "orders")
  tag_name: String! @pk @field_references(references_name: "tags", field: "name", query: "orders", references_query: "tags")
}

type sales_by_country @module(name: "sales.reports")
  @view(
    name: "sales_by_country"
    sql: "SELECT c.country AS country, sum(o.amount) AS total FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = [$country] GROUP BY c.country"
  )
  @args(name: "sales_by_country_args") {
  country: String @pk
  total: Float
}

input sales_by_country_args {
  country: String!
}

type root_things @table(name: "root_things") {
  id: Int! @pk
  name: String
}

extend type Function {
  order_status(id: Int!): String @module(name: "sales")
    @function(name: "order_status")
}

extend type MutationFunction {
  reprice_orders(factor: Float!): OperationResult @module(name: "sales")
    @function(name: "reprice_orders")
}

type order_event {
  type: String
  order_id: Int
}

extend type Subscription {
  order_events(status: String): order_event @module(name: "sales")
  tick(interval: String): String @module(name: "events")
}
`

// newStoreProvider spins the entity catalog storage over a fresh in-memory
// CoreDB. It is both the Provider and the CatalogManager, so AddCatalog runs
// the real write path — the storage is what compiles a schema.
func newStoreProvider(t *testing.T) *catalogstore.Store {
	t.Helper()
	ctx := context.Background()
	pool, err := db.NewPool("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	if err := coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store, err := catalogstore.New(ctx, pool, catalogstore.Config{VecSize: 8}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newLogicalTestProvider(t *testing.T) catalog.Provider {
	t.Helper()
	ss := catalog.NewService(newStoreProvider(t))
	e := &engines.DuckDB{}
	cat, err := sources.NewStringSource("test", e, base.Options{
		Name:         "test",
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, logicalTestSchema)
	if err != nil {
		t.Fatal(err)
	}
	if err = ss.AddCatalog(context.Background(), "test", cat); err != nil {
		t.Fatal(err)
	}
	return ss.Provider()
}

func TestLogicalModelFromProvider(t *testing.T) {
	lm, err := catalog.LogicalModelFromProvider(newLogicalTestProvider(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	t.Run("root module", func(t *testing.T) {
		root := lm.Module(ctx, "")
		if root == nil {
			t.Fatal("root module not resolved")
		}
		if root.Name != "" {
			t.Errorf("root module name = %q, want empty", root.Name)
		}
		if qt := root.RootTypes[sdl.ModuleQuery]; qt != "Query" {
			t.Errorf("root query type = %q, want Query", qt)
		}
	})

	t.Run("module existence", func(t *testing.T) {
		if lm.Module(ctx, "sales") == nil {
			t.Error("module sales not resolved")
		}
		if lm.Module(ctx, "sales.reports") == nil {
			t.Error("module sales.reports not resolved")
		}
		if lm.Module(ctx, "nope") != nil {
			t.Error("unknown module resolved, want nil")
		}
	})

	t.Run("subscription-only module", func(t *testing.T) {
		ev := lm.Module(ctx, "events")
		if ev == nil {
			t.Fatal("subscription-only module events not resolved")
		}
		if ev.RootTypes[sdl.ModuleSubscription] == "" {
			t.Error("events module has no subscription root type")
		}
		if len(ev.RootTypes) != 1 {
			t.Errorf("events module has %d root kinds, want 1", len(ev.RootTypes))
		}
		if !slices.Equal(ev.DataSources, []string{"test"}) {
			t.Errorf("events dataSources = %v, want [test]", ev.DataSources)
		}
	})

	t.Run("direct children only", func(t *testing.T) {
		var names []string
		for m := range lm.Modules(ctx, "") {
			names = append(names, m.Name)
		}
		slices.Sort(names)
		for _, want := range []string{"events", "sales"} {
			if !slices.Contains(names, want) {
				t.Errorf("root children %v missing %q", names, want)
			}
		}
		if slices.Contains(names, "sales.reports") {
			t.Errorf("root children %v must not contain nested sales.reports", names)
		}

		names = names[:0]
		for m := range lm.Modules(ctx, "sales") {
			names = append(names, m.Name)
		}
		if !slices.Contains(names, "sales.reports") {
			t.Errorf("sales children %v missing sales.reports", names)
		}
	})

	t.Run("data objects", func(t *testing.T) {
		var names []string
		for o := range lm.DataObjects(ctx, "sales") {
			names = append(names, o.TypeName())
		}
		slices.Sort(names)
		want := []string{"customers", "order_tags", "orders", "tags"}
		if !slices.Equal(names, want) {
			t.Errorf("sales data objects = %v, want %v", names, want)
		}

		names = names[:0]
		for o := range lm.DataObjects(ctx, "") {
			names = append(names, o.TypeName())
		}
		if !slices.Contains(names, "root_things") {
			t.Errorf("root data objects %v missing root_things", names)
		}
	})

	t.Run("data object lookup", func(t *testing.T) {
		if o := lm.DataObject(ctx, "customers"); o == nil || o.Catalog != "test" {
			t.Errorf("customers lookup = %+v, want object with catalog test", o)
		}
		if lm.DataObject(ctx, "Query") != nil {
			t.Error("Query resolved as data object, want nil")
		}
		if lm.DataObject(ctx, "unknown_object") != nil {
			t.Error("unknown object resolved, want nil")
		}
	})

	t.Run("functions", func(t *testing.T) {
		byName := map[string]*catalog.FunctionEntry{}
		for f := range lm.Functions(ctx, "sales") {
			byName[f.Field.Name] = f
		}
		if f := byName["order_status"]; f == nil || f.Kind != sdl.ModuleFunction || f.DataSource != "test" {
			t.Errorf("order_status = %+v, want function kind from test", f)
		}
		if f := byName["reprice_orders"]; f == nil || f.Kind != sdl.ModuleMutationFunction {
			t.Errorf("reprice_orders = %+v, want mutation-function kind", f)
		}
		if f := byName["order_events"]; f == nil || f.Kind != sdl.ModuleSubscription {
			t.Errorf("order_events = %+v, want subscription kind", f)
		}

		if f := lm.Function(ctx, "events", "tick"); f == nil || f.Kind != sdl.ModuleSubscription {
			t.Errorf("events.tick = %+v, want subscription entry", f)
		}
		if lm.Function(ctx, "sales", "missing") != nil {
			t.Error("unknown function resolved, want nil")
		}
	})

	t.Run("module data sources", func(t *testing.T) {
		m := lm.Module(ctx, "sales")
		if m == nil {
			t.Fatal("sales module not resolved")
		}
		if !slices.Equal(m.DataSources, []string{"test"}) {
			t.Errorf("sales dataSources = %v, want [test]", m.DataSources)
		}
	})

	t.Run("types", func(t *testing.T) {
		if d := lm.Type(ctx, "customers"); d == nil || d.Name != "customers" {
			t.Errorf("Type(customers) = %v, want definition", d)
		}
		if d := lm.Type(ctx, "_Module"); d == nil {
			t.Error("Type(_Module) = nil, want system meta-type definition")
		}
		if lm.Type(ctx, "unknown_type") != nil {
			t.Error("Type(unknown) resolved, want nil")
		}

		var source []string
		for _, d := range lm.SourceTypes(ctx) {
			source = append(source, d.Name)
		}
		slices.Sort(source)
		// exactly the residual source-defined base types — what the future
		// entity-storage types table will hold
		want := []string{"order_event", "sales_by_country_args"}
		if !slices.Equal(source, want) {
			t.Errorf("SourceTypes() = %v, want %v", source, want)
		}

		var system []string
		for _, d := range lm.SystemTypes(ctx) {
			system = append(system, d.Name)
		}
		for _, w := range []string{"_Module", "OperationResult", "String", "Query"} {
			if !slices.Contains(system, w) {
				t.Errorf("SystemTypes() missing %q", w)
			}
		}
		for _, absent := range []string{"order_event", "customers", "customers_filter"} {
			if slices.Contains(system, absent) {
				t.Errorf("SystemTypes() must not contain %q", absent)
			}
		}
	})

	t.Run("relations", func(t *testing.T) {
		relsOf := func(object string) map[string]*catalog.RelationInfo {
			t.Helper()
			res := map[string]*catalog.RelationInfo{}
			for r := range lm.Relations(ctx, object) {
				res[string(r.Kind)+"/"+r.Name] = r
			}
			return res
		}

		// orders: FORWARD FK to customers, own M2M leg to tags, JOIN to tags.
		orders := relsOf("orders")
		fk := orders["FK/order_customer"]
		if fk == nil || fk.Direction != catalog.RelationForward || fk.DataObject != "customers" ||
			fk.FieldName != "customer" ||
			!slices.Equal(fk.SourceKeys, []string{"customer_id"}) ||
			!slices.Equal(fk.DestinationKeys, []string{"id"}) {
			t.Errorf("orders FK = %+v, want FORWARD to customers via customer [customer_id]->[id]", fk)
		}
		m2m := orders["M2M/orders_order_id"]
		if m2m == nil || m2m.DataObject != "tags" || m2m.Through != "order_tags" {
			t.Errorf("orders M2M = %+v, want far=tags through=order_tags", m2m)
		}
		join := orders["JOIN/source_info"]
		if join == nil || join.Direction != catalog.RelationForward || join.DataObject != "tags" ||
			!slices.Equal(join.SourceKeys, []string{"status"}) ||
			!slices.Equal(join.DestinationKeys, []string{"name"}) {
			t.Errorf("orders JOIN = %+v, want FORWARD to tags [status]->[name]", join)
		}

		// customers: BACK FK from orders with canonical key orientation.
		customers := relsOf("customers")
		back := customers["FK/order_customer"]
		if back == nil || back.Direction != catalog.RelationBack || back.DataObject != "orders" ||
			back.FieldName != "orders" ||
			!slices.Equal(back.SourceKeys, []string{"customer_id"}) ||
			!slices.Equal(back.DestinationKeys, []string{"id"}) {
			t.Errorf("customers BACK FK = %+v, want BACK from orders via orders field", back)
		}

		// tags: own M2M leg to orders; NO back-JOIN entry (join is one-directional).
		tags := relsOf("tags")
		if m2m := tags["M2M/tags_tag_name"]; m2m == nil || m2m.DataObject != "orders" || m2m.Through != "order_tags" {
			t.Errorf("tags M2M = %+v, want far=orders through=order_tags", m2m)
		}
		for key, r := range tags {
			if r.Kind == catalog.RelationJoin {
				t.Errorf("tags has unexpected JOIN entry %s = %+v (join is one-directional)", key, r)
			}
		}

		// non-data-object → empty
		if len(relsOf("Query")) != 0 {
			t.Error("Query yielded relations, want none")
		}
	})
}
