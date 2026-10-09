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
