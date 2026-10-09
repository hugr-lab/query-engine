package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	catalogstore "github.com/hugr-lab/query-engine/pkg/catalog/store"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// Both object_id references default to the same name. The reverse sensor
// relation must not resolve to the incident junction's M2M projection.
const referenceCollisionSchema = `
type road_objects @table(name: "road_objects") {
  id: Int! @pk
}
type meteo_sensors @table(name: "sensors") {
  id: Int! @pk
  object_id: Int @field_references(
    references_name: "road_objects", field: "id",
    query: "road_object", references_query: "meteo_sensors")
}
type meteo_sensors_data @table(name: "sensors_data") {
  id: Int! @pk
  sensor_id: Int @field_references(
    references_name: "meteo_sensors", field: "id",
    query: "sensor", references_query: "sensors_data")
  time_stamp: Timestamp
}
type incidents @table(name: "emergency.incidents") {
  id: Int! @pk
}
type incidents_road_objects @table(name: "emergency.incidents_road_objects", is_m2m: true) {
  incident_id: Int! @field_references(
    references_name: "incidents", field: "id",
    query: "incident", references_query: "objects")
  object_id: Int! @field_references(
    references_name: "road_objects", field: "id",
    query: "object", references_query: "incidents")
}
`

func TestReferenceNameCollisionSQL(t *testing.T) {
	before, junction, found := strings.Cut(referenceCollisionSchema, "type incidents_road_objects")
	require.True(t, found)
	for _, order := range []struct{ name, schema string }{
		{"junction_last", referenceCollisionSchema},
		// File sources sort by filename: 10-emergency.graphql precedes
		// 3-digital-twin.graphql, putting the junction before road_objects.
		{"junction_first", "type incidents_road_objects" + junction + before},
	} {
		t.Run(order.name, func(t *testing.T) {
			testReferenceNameCollisionSQL(t, order.schema)
		})
	}
}

func testReferenceNameCollisionSQL(t *testing.T, schema string) {
	ctx := auth.ContextWithFullAccess(context.Background())
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })
	require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
	provider, err := catalogstore.New(ctx, pool, catalogstore.Config{VecSize: 8}, nil)
	require.NoError(t, err)
	ss := catalog.NewService(provider)
	e := &engines.Postgres{}
	src, err := sources.NewStringSource("tf", e, base.Options{
		Name: "tf", Prefix: "tf", EngineType: string(e.Type()), Capabilities: e.Capabilities(),
	}, schema)
	require.NoError(t, err)
	require.NoError(t, ss.AddCatalog(ctx, "tf", src))

	for _, tt := range []struct {
		name, query, want string
		m2m               bool
	}{
		{
			name: "reverse sensors with observations",
			query: `{ tf_road_objects { id meteo_sensors {
              sensors_data_aggregation(filter: {time_stamp: {gte: "2026-09-09T07:06:35Z", lte: "2026-09-10T07:06:35Z"}}) { _rows_count }
            } } }`,
			want: "_objects.id = _meteo_sensors_sub_node.object_id",
		},
		{
			name:  "forward road object",
			query: `{ tf_meteo_sensors { id road_object { id } } }`,
			want:  "_objects.object_id = _road_object_sub_node.id",
		},
		{
			name:  "incident navigation",
			query: `{ tf_road_objects { id incidents { id } } }`,
			want:  "_join_m2m.incident_id = _incidents_sub_node.id",
			m2m:   true,
		},
		{
			name:  "reverse sensors filter",
			query: `{ tf_road_objects(filter: {meteo_sensors: {any_of: {id: {eq: 1}}}}) { id } }`,
			want:  "_objects.id = _where__objects_meteo_sensors.object_id",
		},
		{
			name:  "reverse sensors aggregation",
			query: `{ tf_road_objects { id meteo_sensors_aggregation { _rows_count } } }`,
			want:  "_root_objects.id = _aggregation.object_id",
		},
		{
			name:  "incident filter",
			query: `{ tf_road_objects(filter: {incidents: {any_of: {id: {eq: 1}}}}) { id } }`,
			want:  "_objects.id = _join__objects_incidents.object_id",
			m2m:   true,
		},
		{
			// the branch joins the junction and the root matches its keys
			name:  "incident aggregation",
			query: `{ tf_road_objects { id incidents_aggregation { _rows_count } } }`,
			want:  "_join_m2m.incident_id = _aggregation.id INNER JOIN _objects AS _root_objects ON _root_objects.id = _join_m2m.object_id",
			m2m:   true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			op, err := ss.ParseQuery(ctx, tt.query, nil, "")
			require.NoError(t, err)
			plan, err := New(ss, nil).Plan(ctx, provider, op.Definition.SelectionSet[0].(*ast.Field), op.Variables)
			require.NoError(t, err)
			require.NoError(t, plan.Compile())
			assert.Contains(t, plan.CompiledQuery, tt.want)
			if tt.m2m {
				assert.Contains(t, plan.CompiledQuery, "emergency.incidents_road_objects")
			} else {
				assert.NotContains(t, plan.CompiledQuery, "emergency.incidents_road_objects")
			}
		})
	}
}

// The junction is joined as a raw table: its keys are named by their
// columns, it lives in the source's catalog unless the whole statement runs
// in the source database, and a reference beyond its two legs is not a leg.
const m2mAggregationSchema = `
type products @table(name: "products") {
  id: Int! @pk
}
type tags @table(name: "tags") {
  id: Int! @pk
}
type users @table(name: "users") {
  id: Int! @pk
}
type product_tags @table(name: "product_tags", is_m2m: true) {
  created_by: Int @field_references(references_name: "users", field: "id", query: "author", references_query: "tagged")
  product_id: Int! @pk @field_references(references_name: "products", field: "id", query: "product", references_query: "product_tags")
  tag_id: Int! @pk @field_source(field: "tag_ref") @field_references(references_name: "tags", field: "id", query: "tag", references_query: "tagged_products")
}
`

func TestM2MAggregationJunction(t *testing.T) {
	for _, tt := range []struct {
		name     string
		engine   engines.Engine
		junction string
	}{
		{"postgres", &engines.Postgres{}, "INNER JOIN product_tags AS _join_m2m"},
		{"duckdb", engines.NewDuckDB(), "INNER JOIN tf.product_tags AS _join_m2m"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := auth.ContextWithFullAccess(context.Background())
			pool, err := db.NewPool("")
			require.NoError(t, err)
			t.Cleanup(func() { pool.Close() })
			require.NoError(t, coredb.New(coredb.Config{VectorSize: 8}).Attach(ctx, pool))
			provider, err := catalogstore.New(ctx, pool, catalogstore.Config{VecSize: 8}, nil)
			require.NoError(t, err)
			ss := catalog.NewService(provider)
			src, err := sources.NewStringSource("tf", tt.engine, base.Options{
				Name: "tf", Prefix: "tf", EngineType: string(tt.engine.Type()), Capabilities: tt.engine.Capabilities(),
			}, m2mAggregationSchema)
			require.NoError(t, err)
			require.NoError(t, ss.AddCatalog(ctx, "tf", src))

			for _, q := range []struct{ query, want string }{
				{`{ tf_products { id product_tags_aggregation { _rows_count } } }`,
					"_join_m2m.tag_ref = _aggregation.id INNER JOIN _objects AS _root_objects ON _root_objects.id = _join_m2m.product_id"},
				{`{ tf_tags { id tagged_products_aggregation { _rows_count } } }`,
					"_join_m2m.product_id = _aggregation.id INNER JOIN _objects AS _root_objects ON _root_objects.id = _join_m2m.tag_ref"},
			} {
				op, err := ss.ParseQuery(ctx, q.query, nil, "")
				require.NoError(t, err)
				plan, err := New(ss, nil).Plan(ctx, provider, op.Definition.SelectionSet[0].(*ast.Field), op.Variables)
				require.NoError(t, err)
				require.NoError(t, plan.Compile())
				assert.Contains(t, plan.CompiledQuery, tt.junction+" ON "+q.want)
				assert.NotContains(t, plan.CompiledQuery, "created_by")
			}
		})
	}
}
