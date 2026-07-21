package metadata

import (
	"context"
	"slices"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

func newCatalogTestService(t *testing.T) *catalog.Service {
	t.Helper()
	provider, err := static.New()
	if err != nil {
		t.Fatal(err)
	}
	ss := catalog.NewService(provider)
	e := &engines.DuckDB{}
	cat, err := sources.NewStringSource("test", e, compiler.Options{
		Name:         "test",
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}, testSchemaData)
	if err != nil {
		t.Fatal(err)
	}
	if err = ss.AddCatalog(context.Background(), "test", cat); err != nil {
		t.Fatal(err)
	}
	return ss
}

// runMetaQuery parses a query through the full pipeline (walker validation
// included — exercising the synthesized _catalog root field definitions) and
// resolves every classified meta request.
func runMetaQuery(t *testing.T, ss *catalog.Service, query string) map[string]any {
	t.Helper()
	return runMetaQueryDepth(t, ss, query, 20)
}

func runMetaQueryDepth(t *testing.T, ss *catalog.Service, query string, maxDepth int) map[string]any {
	t.Helper()
	op, err := ss.ParseQuery(context.Background(), query, nil, "")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	res := map[string]any{}
	for _, r := range op.Queries {
		data, err := ProcessQuery(t.Context(), ss.Provider(), r, maxDepth, op.Variables)
		if err != nil {
			t.Fatalf("ProcessQuery(%s): %v", r.Name, err)
		}
		res[r.Name] = data
	}
	return res
}

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	if v == nil {
		t.Fatal("expected object, got nil")
	}
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", v)
	}
	return m
}

func namesOf(t *testing.T, v any) []string {
	t.Helper()
	list, ok := v.([]map[string]any)
	if !ok {
		t.Fatalf("expected list of objects, got %T", v)
	}
	var names []string
	for _, item := range list {
		if n, ok := item["name"].(string); ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	return names
}

func TestCatalogQuery_Tree(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		_catalog {
			name
			modules { name functions { name type } }
			dataObjects { name }
			functions { name }
			queryType { name }
			mutationType { name }
			subscriptionType { name }
			functionType { name }
			mutationFunctionType { name }
		}
	}`)
	root := asMap(t, res["_catalog"])

	if root["name"] != "" {
		t.Errorf("root module name = %v, want empty string", root["name"])
	}

	modNames := namesOf(t, root["modules"])
	for _, want := range []string{"core", "events", "sales"} {
		if !slices.Contains(modNames, want) {
			t.Errorf("root modules %v missing %q", modNames, want)
		}
	}
	if slices.Contains(modNames, "sales.reports") {
		t.Errorf("root modules %v must not contain nested sales.reports", modNames)
	}

	// no root-level data objects or functions in the fixture
	if n := namesOf(t, root["dataObjects"]); len(n) != 0 {
		t.Errorf("root dataObjects = %v, want empty", n)
	}
	if n := namesOf(t, root["functions"]); len(n) != 0 {
		t.Errorf("root functions = %v, want empty", n)
	}

	// five root-type back-refs
	for field, want := range map[string]string{
		"queryType":            "Query",
		"mutationType":         "Mutation",
		"subscriptionType":     "Subscription",
		"functionType":         "Function",
		"mutationFunctionType": "MutationFunction",
	} {
		got := asMap(t, root[field])
		if got["name"] != want {
			t.Errorf("%s = %v, want %s", field, got["name"], want)
		}
	}

	// subscription-only module carries its subscription function
	for _, m := range root["modules"].([]map[string]any) {
		if m["name"] != "events" {
			continue
		}
		fns := m["functions"].([]map[string]any)
		if len(fns) != 1 || fns[0]["name"] != "tick" || fns[0]["type"] != "SUBSCRIPTION" {
			t.Errorf("events functions = %v, want [tick SUBSCRIPTION]", fns)
		}
	}
}

func TestCatalogQuery_ModuleLookup(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		_module(name: "sales") {
			name
			dataObjects { name }
			functions { name type }
			modules { name }
			subscriptionType { name }
			dataSources
		}
		root: _module(name: "") { name }
		missing: _module(name: "nope") { name }
	}`)

	sales := asMap(t, res["_module"])
	if sales["name"] != "sales" {
		t.Errorf("module name = %v, want sales", sales["name"])
	}
	objNames := namesOf(t, sales["dataObjects"])
	if !slices.Contains(objNames, "customers") || !slices.Contains(objNames, "orders") {
		t.Errorf("sales dataObjects = %v, want customers+orders", objNames)
	}
	// dedupe: each object exactly once
	for _, n := range []string{"customers", "orders"} {
		count := 0
		for _, o := range objNames {
			if o == n {
				count++
			}
		}
		if count != 1 {
			t.Errorf("object %s appears %d times, want 1", n, count)
		}
	}

	fnByName := map[string]string{}
	for _, f := range sales["functions"].([]map[string]any) {
		fnByName[f["name"].(string)] = f["type"].(string)
	}
	if fnByName["order_events"] != "SUBSCRIPTION" {
		t.Errorf("order_events type = %q, want SUBSCRIPTION", fnByName["order_events"])
	}

	if children := namesOf(t, sales["modules"]); !slices.Equal(children, []string{"sales.reports"}) {
		t.Errorf("sales children = %v, want [sales.reports]", children)
	}
	if st := asMap(t, sales["subscriptionType"]); st["name"] != "_module_sales_subscription" {
		t.Errorf("sales subscriptionType = %v, want _module_sales_subscription", st["name"])
	}
	if ds, ok := sales["dataSources"].([]string); !ok || !slices.Equal(ds, []string{"test"}) {
		t.Errorf("sales dataSources = %v, want [test]", sales["dataSources"])
	}

	if root := asMap(t, res["root"]); root["name"] != "" {
		t.Errorf(`_module(name: "") = %v, want root module`, root["name"])
	}
	if res["missing"] != nil {
		t.Errorf("unknown module = %v, want nil", res["missing"])
	}
}

func TestCatalogQuery_DataObjectLookup(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		_dataObject(name: "customers") {
			name
			type
			moduleName
			primaryKey
			dataSourceName
			dataSources
			properties { isCube isM2M isHypertable softDelete hasVectors }
			module { name }
		}
		view: _dataObject(name: "sales_by_country") {
			name type moduleName
			args { name }
		}
		junction: _dataObject(name: "catalogs") { properties { isM2M } }
		moduleType: _dataObject(name: "_module_core_query") { name }
		missing: _dataObject(name: "no_such_object") { name }
		fields: _dataObject(name: "orders") { fields { name } }
	}`)

	customers := asMap(t, res["_dataObject"])
	if customers["type"] != "TABLE" {
		t.Errorf("customers type = %v, want TABLE", customers["type"])
	}
	if customers["moduleName"] != "sales" {
		t.Errorf("customers moduleName = %v, want sales", customers["moduleName"])
	}
	if pk, ok := customers["primaryKey"].([]string); !ok || !slices.Equal(pk, []string{"id"}) {
		t.Errorf("customers primaryKey = %v, want [id]", customers["primaryKey"])
	}
	if customers["dataSourceName"] != "test" {
		t.Errorf("customers dataSourceName = %v, want test", customers["dataSourceName"])
	}
	if ds, ok := customers["dataSources"].([]string); !ok || !slices.Equal(ds, []string{"test"}) {
		t.Errorf("customers dataSources = %v, want [test]", customers["dataSources"])
	}
	props := asMap(t, customers["properties"])
	for name, want := range map[string]bool{"isCube": false, "isM2M": false, "isHypertable": false, "softDelete": false, "hasVectors": false} {
		if props[name] != want {
			t.Errorf("customers properties.%s = %v, want %v", name, props[name], want)
		}
	}
	if mod := asMap(t, customers["module"]); mod["name"] != "sales" {
		t.Errorf("customers module = %v, want sales", mod["name"])
	}

	view := asMap(t, res["view"])
	if view["type"] != "VIEW" {
		t.Errorf("view type = %v, want VIEW", view["type"])
	}
	if view["moduleName"] != "sales.reports" {
		t.Errorf("view moduleName = %v, want sales.reports", view["moduleName"])
	}
	if args := namesOf(t, view["args"]); !slices.Equal(args, []string{"country"}) {
		t.Errorf("view args = %v, want [country]", args)
	}

	junction := asMap(t, res["junction"])
	if p := asMap(t, junction["properties"]); p["isM2M"] != true {
		t.Errorf("catalogs isM2M = %v, want true", p["isM2M"])
	}

	if res["moduleType"] != nil {
		t.Errorf("module root type resolved as data object: %v, want nil", res["moduleType"])
	}
	if res["missing"] != nil {
		t.Errorf("unknown data object = %v, want nil", res["missing"])
	}

	fieldNames := namesOf(t, asMap(t, res["fields"])["fields"])
	for _, want := range []string{"id", "customer_id", "customer", "source_info"} {
		if !slices.Contains(fieldNames, want) {
			t.Errorf("orders fields %v missing %q", fieldNames, want)
		}
	}
}

func TestCatalogQuery_FunctionLookup(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		fn: _function(module: "core", name: "data_source_status") {
			name type isTable moduleName dataSourceName
			args { name }
			returns { name kind }
		}
		mut: _function(module: "core", name: "load_data_source") { name type }
		sub: _function(module: "sales", name: "order_events") { name type returns { name } }
		missingFn: _function(module: "core", name: "nope") { name }
		rootNav: _function(module: "", name: "core") { name }
	}`)

	fn := asMap(t, res["fn"])
	if fn["type"] != "FUNCTION" {
		t.Errorf("data_source_status type = %v, want FUNCTION", fn["type"])
	}
	if fn["isTable"] != false {
		t.Errorf("data_source_status isTable = %v, want false", fn["isTable"])
	}
	if fn["moduleName"] != "core" {
		t.Errorf("data_source_status moduleName = %v, want core", fn["moduleName"])
	}
	if args := namesOf(t, fn["args"]); !slices.Equal(args, []string{"name"}) {
		t.Errorf("data_source_status args = %v, want [name]", args)
	}
	if ret := asMap(t, fn["returns"]); ret["name"] != "String" {
		t.Errorf("data_source_status returns = %v, want String", ret["name"])
	}

	if mut := asMap(t, res["mut"]); mut["type"] != "MUTATION" {
		t.Errorf("load_data_source type = %v, want MUTATION", mut["type"])
	}
	sub := asMap(t, res["sub"])
	if sub["type"] != "SUBSCRIPTION" {
		t.Errorf("order_events type = %v, want SUBSCRIPTION", sub["type"])
	}
	if ret := asMap(t, sub["returns"]); ret["name"] != "order_event" {
		t.Errorf("order_events returns = %v, want order_event", ret["name"])
	}

	if res["missingFn"] != nil {
		t.Errorf("unknown function = %v, want nil", res["missingFn"])
	}
	// "core" on the root Function type is a submodule navigation field, not a function
	if res["rootNav"] != nil {
		t.Errorf("submodule nav field resolved as function: %v, want nil", res["rootNav"])
	}
}

func TestCatalogQuery_Relations(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		orders: _dataObject(name: "orders") {
			relations {
				name direction kind fieldName description
				dataObject { name }
				through { name }
				sourceKeys destinationKeys
				dataSource
			}
		}
		customers: _dataObject(name: "customers") {
			relations { name direction kind fieldName dataObject { name } }
		}
		junctionSide: _dataObject(name: "catalog_sources") {
			relations { name kind direction dataObject { name } through { name } }
		}
	}`)

	type relKey struct{ kind, name string }
	collect := func(v any) map[relKey]map[string]any {
		t.Helper()
		out := map[relKey]map[string]any{}
		for _, r := range asMap(t, v)["relations"].([]map[string]any) {
			out[relKey{r["kind"].(string), r["name"].(string)}] = r
		}
		return out
	}

	orders := collect(res["orders"])
	fk := orders[relKey{"FK", "order_customer"}]
	if fk == nil {
		t.Fatalf("orders missing FK order_customer: %v", orders)
	}
	if fk["direction"] != "FORWARD" || fk["fieldName"] != "customer" {
		t.Errorf("orders FK = %v, want FORWARD via customer", fk)
	}
	if far := asMap(t, fk["dataObject"]); far["name"] != "customers" {
		t.Errorf("orders FK far = %v, want customers", far["name"])
	}
	if fk["through"] != nil {
		t.Errorf("FK through = %v, want nil", fk["through"])
	}
	if sk, ok := fk["sourceKeys"].([]string); !ok || !slices.Equal(sk, []string{"customer_id"}) {
		t.Errorf("FK sourceKeys = %v, want [customer_id]", fk["sourceKeys"])
	}
	if fk["description"] != "Order customer" {
		t.Errorf("FK description = %v, want Order customer", fk["description"])
	}
	if fk["dataSource"] != "test" {
		t.Errorf("FK dataSource = %v, want test", fk["dataSource"])
	}

	join := orders[relKey{"JOIN", "source_info"}]
	if join == nil {
		t.Fatalf("orders missing JOIN source_info: %v", orders)
	}
	if join["direction"] != "FORWARD" {
		t.Errorf("JOIN direction = %v, want FORWARD", join["direction"])
	}
	if far := asMap(t, join["dataObject"]); far["name"] != "catalog_sources" {
		t.Errorf("JOIN far = %v, want catalog_sources", far["name"])
	}
	// relation SQL must not leak anywhere in the payload
	for _, r := range orders {
		if _, ok := r["sql"]; ok {
			t.Errorf("relation exposes sql: %v", r)
		}
	}

	customers := collect(res["customers"])
	back := customers[relKey{"FK", "order_customer"}]
	if back == nil {
		t.Fatalf("customers missing BACK FK: %v", customers)
	}
	if back["direction"] != "BACK" || back["fieldName"] != "orders" {
		t.Errorf("customers BACK FK = %v, want BACK via orders", back)
	}
	if far := asMap(t, back["dataObject"]); far["name"] != "orders" {
		t.Errorf("customers BACK far = %v, want orders", far["name"])
	}
	// JOIN is one-directional: no BACK JOIN entry on the join target
	for k := range customers {
		if k.kind == "JOIN" {
			t.Errorf("customers has JOIN entry %v (join is one-directional)", k)
		}
	}

	// M2M through the catalogs junction: catalog_sources sees data_sources
	// through catalogs.
	junctionSide := collect(res["junctionSide"])
	var m2m map[string]any
	for k, r := range junctionSide {
		if k.kind == "M2M" {
			m2m = r
		}
	}
	if m2m == nil {
		t.Fatalf("catalog_sources missing M2M relation: %v", junctionSide)
	}
	if th := asMap(t, m2m["through"]); th["name"] != "catalogs" {
		t.Errorf("M2M through = %v, want catalogs", th["name"])
	}
}

// runMetaQueryPerm parses without permissions (meta queries are classified
// ahead of permission validation) and resolves WITH the permission context —
// introspection filtering happens at resolution time, as in production.
func runMetaQueryPerm(t *testing.T, ss *catalog.Service, perms *perm.RolePermissions, query string) map[string]any {
	t.Helper()
	op, err := ss.ParseQuery(context.Background(), query, nil, "")
	if err != nil {
		t.Fatalf("ParseQuery: %v", err)
	}
	ctx := perm.CtxWithPerm(t.Context(), perms)
	res := map[string]any{}
	for _, r := range op.Queries {
		data, err := ProcessQuery(ctx, ss.Provider(), r, 20, op.Variables)
		if err != nil {
			t.Fatalf("ProcessQuery(%s): %v", r.Name, err)
		}
		res[r.Name] = data
	}
	return res
}

func TestCatalogQuery_Permissions(t *testing.T) {
	ss := newCatalogTestService(t)

	t.Run("hidden data object absent everywhere", func(t *testing.T) {
		perms := &perm.RolePermissions{
			Name: "restricted",
			Permissions: []perm.Permission{
				{Object: "data-object:query", Field: "orders", Hidden: true},
			},
		}
		res := runMetaQueryPerm(t, ss, perms, `{
			_module(name: "sales") { dataObjects { name } }
			obj: _dataObject(name: "orders") { name }
			rels: _dataObject(name: "customers") { relations { name kind } }
		}`)

		objs := namesOf(t, asMap(t, res["_module"])["dataObjects"])
		if slices.Contains(objs, "orders") {
			t.Errorf("sales dataObjects %v must not contain hidden orders", objs)
		}
		if !slices.Contains(objs, "customers") {
			t.Errorf("sales dataObjects %v missing customers", objs)
		}
		if res["obj"] != nil {
			t.Errorf("_dataObject on hidden object = %v, want nil", res["obj"])
		}
		// customers' BACK FK to hidden orders must disappear
		rels := asMap(t, res["rels"])["relations"].([]map[string]any)
		for _, r := range rels {
			if r["name"] == "order_customer" {
				t.Errorf("relation to hidden object leaked: %v", r)
			}
		}
	})

	t.Run("hidden junction hides whole M2M relation", func(t *testing.T) {
		perms := &perm.RolePermissions{
			Name: "restricted",
			Permissions: []perm.Permission{
				{Object: "data-object:query", Field: "catalogs", Hidden: true},
			},
		}
		res := runMetaQueryPerm(t, ss, perms, `{
			_dataObject(name: "catalog_sources") { relations { name kind } }
		}`)
		rels := asMap(t, res["_dataObject"])["relations"].([]map[string]any)
		for _, r := range rels {
			if r["kind"] == "M2M" {
				t.Errorf("M2M relation with hidden junction leaked: %v", r)
			}
		}
	})

	t.Run("disabled stays visible", func(t *testing.T) {
		perms := &perm.RolePermissions{
			Name: "restricted",
			Permissions: []perm.Permission{
				{Object: "data-object:query", Field: "customers", Disabled: true},
			},
		}
		res := runMetaQueryPerm(t, ss, perms, `{
			_module(name: "sales") { dataObjects { name } }
			obj: _dataObject(name: "customers") { name }
		}`)
		objs := namesOf(t, asMap(t, res["_module"])["dataObjects"])
		if !slices.Contains(objs, "customers") {
			t.Errorf("disabled customers must stay visible, got %v", objs)
		}
		if res["obj"] == nil {
			t.Error("_dataObject on disabled object = nil, want visible")
		}
	})

	t.Run("empty module omitted from listing but direct lookup resolves", func(t *testing.T) {
		perms := &perm.RolePermissions{
			Name: "restricted",
			Permissions: []perm.Permission{
				{Object: "_module_sales_reports_query", Field: "sales_by_country", Hidden: true},
			},
		}
		res := runMetaQueryPerm(t, ss, perms, `{
			_module(name: "sales") { modules { name } }
			direct: _module(name: "sales.reports") { name }
		}`)
		children := namesOf(t, asMap(t, res["_module"])["modules"])
		if slices.Contains(children, "sales.reports") {
			t.Errorf("empty module sales.reports must be omitted, got %v", children)
		}
		if direct := asMap(t, res["direct"]); direct["name"] != "sales.reports" {
			t.Errorf("direct lookup of empty module = %v, want sales.reports", direct)
		}
	})

	t.Run("fields filter parity with __type", func(t *testing.T) {
		perms := &perm.RolePermissions{
			Name: "restricted",
			Permissions: []perm.Permission{
				{Object: "customers", Field: "country", Hidden: true},
			},
		}
		res := runMetaQueryPerm(t, ss, perms, `{
			_dataObject(name: "customers") { fields { name } }
			__type(name: "customers") { fields { name } }
		}`)
		objFields := namesOf(t, asMap(t, res["_dataObject"])["fields"])
		typeFields := namesOf(t, asMap(t, res["__type"])["fields"])
		if slices.Contains(objFields, "country") {
			t.Errorf("_DataObject.fields %v must not contain hidden country", objFields)
		}
		if !slices.Equal(objFields, typeFields) {
			t.Errorf("fields filter mismatch: _dataObject %v vs __type %v", objFields, typeFields)
		}
	})

	t.Run("full access sees everything", func(t *testing.T) {
		res := runMetaQuery(t, ss, `{
			_module(name: "sales") { dataObjects { name } }
		}`)
		objs := namesOf(t, asMap(t, res["_module"])["dataObjects"])
		for _, want := range []string{"customers", "orders"} {
			if !slices.Contains(objs, want) {
				t.Errorf("full access dataObjects %v missing %q", objs, want)
			}
		}
	})
}

func TestCatalogQuery_SelfIntrospection(t *testing.T) {
	ss := newCatalogTestService(t)

	res := runMetaQuery(t, ss, `{
		mod: __type(name: "_Module") { name kind fields { name } }
		obj: __type(name: "_DataObject") { name kind fields { name } }
		props: __type(name: "_DataObjectProperties") { name kind }
		rel: __type(name: "_Relation") { name kind }
		fn: __type(name: "_Function") { name kind }
		dot: __type(name: "_DataObjectType") { name kind enumValues { name } }
		ft: __type(name: "_FunctionType") { name kind enumValues { name } }
		rd: __type(name: "_RelationDirection") { name kind enumValues { name } }
		rk: __type(name: "_RelationKind") { name kind enumValues { name } }
	}`)

	for alias, want := range map[string]string{
		"mod": "_Module", "obj": "_DataObject", "props": "_DataObjectProperties",
		"rel": "_Relation", "fn": "_Function",
	} {
		got := asMap(t, res[alias])
		if got["name"] != want || got["kind"] != ast.Object {
			t.Errorf("__type(%s) = %v/%v, want %s OBJECT", alias, got["name"], got["kind"], want)
		}
	}

	modFields := namesOf(t, asMap(t, res["mod"])["fields"])
	for _, want := range []string{"name", "dataSources", "modules", "dataObjects", "functions",
		"queryType", "mutationType", "subscriptionType", "functionType", "mutationFunctionType"} {
		if !slices.Contains(modFields, want) {
			t.Errorf("_Module fields %v missing %q", modFields, want)
		}
	}

	for alias, want := range map[string][]string{
		"dot": {"TABLE", "VIEW"},
		"ft":  {"FUNCTION", "MUTATION", "SUBSCRIPTION"},
		"rd":  {"BACK", "FORWARD"},
		"rk":  {"FK", "JOIN", "M2M"},
	} {
		got := asMap(t, res[alias])
		if got["kind"] != ast.Enum {
			t.Errorf("__type(%s) kind = %v, want ENUM", alias, got["kind"])
		}
		vals := namesOf(t, got["enumValues"])
		if !slices.Equal(vals, want) {
			t.Errorf("__type(%s) enumValues = %v, want %v", alias, vals, want)
		}
	}

	// The four root meta queries are ordinary (single-underscore) system
	// fields of Query — they MUST appear in standard introspection so
	// GraphiQL/codegen can autocomplete and validate them (SC-006).
	q := runMetaQuery(t, ss, `{ __type(name: "Query") { fields { name } } }`)
	queryFields := namesOf(t, asMap(t, q["__type"])["fields"])
	for _, want := range []string{"_catalog", "_module", "_dataObject", "_function"} {
		if !slices.Contains(queryFields, want) {
			t.Errorf("Query introspection fields missing %q", want)
		}
	}
}

func TestCatalogQuery_DepthBudget(t *testing.T) {
	ss := newCatalogTestService(t)

	// With a tiny depth budget nested module blocks truncate to empty/null
	// instead of erroring.
	res := runMetaQueryDepth(t, ss, `{
		_catalog { name modules { name modules { name } } }
	}`, 2)
	root := asMap(t, res["_catalog"])
	if root["name"] != "" {
		t.Errorf("root name = %v, want empty", root["name"])
	}
}
