package sdl_test

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

func TestReferencesQueryIdentity(t *testing.T) {
	// Relation names are local to the declaring object. M2M projections can
	// reuse the name of an unrelated incoming FK, or of another junction.
	doc, err := parser.ParseSchema(&ast.Source{Input: `
type road_objects @table(name: "road_objects")
  @references(name: "road_objects_object_id", references_name: "incidents",
    source_fields: ["id"], references_fields: ["object_id"],
    query: "incidents", references_query: "objects",
    is_m2m: true, m2m_name: "incidents_road_objects")
  @references(name: "road_objects_object_id", references_name: "incidents",
    source_fields: ["id"], references_fields: ["old_object_id"],
    query: "old_incidents", references_query: "old_objects",
    is_m2m: true, m2m_name: "old_incidents_road_objects") {
  id: Int!
  meteo_sensors: [sensors] @references_query(name: "road_objects_object_id", references_name: "sensors", is_m2m: false)
  incidents: [incidents] @references_query(name: "road_objects_object_id", references_name: "incidents", is_m2m: true, m2m_name: "incidents_road_objects")
  old_incidents: [incidents] @references_query(name: "road_objects_object_id", references_name: "incidents", is_m2m: true, m2m_name: "old_incidents_road_objects")
  missing: [sensors] @references_query(name: "missing", references_name: "sensors", is_m2m: false)
}
type sensors @table(name: "sensors")
  @references(name: "road_objects_object_id", references_name: "road_objects",
    source_fields: ["object_id"], references_fields: ["id"],
    query: "road_object", references_query: "meteo_sensors", is_m2m: false) {
  id: Int!
  object_id: Int
  road_object: road_objects @references_query(name: "road_objects_object_id", references_name: "road_objects", is_m2m: false)
}
type incidents @table(name: "incidents") { id: Int! }
type tree @table(name: "tree")
  @references(name: "tree_parent_id", references_name: "tree",
    source_fields: ["parent_id"], references_fields: ["id"],
    query: "parent", references_query: "children", is_m2m: false) {
  id: Int!
  parent_id: Int
  parent: tree @references_query(name: "tree_parent_id", references_name: "tree", is_m2m: false)
  children: [tree] @references_query(name: "tree_parent_id", references_name: "tree", is_m2m: false)
}
`})
	require.NoError(t, err)
	defs := static.NewDocumentProvider(doc)
	ctx := context.Background()
	for _, tt := range []struct {
		name, object, field, target, sourceKey, targetKey, junction string
	}{
		{"reverse FK collides with M2M", "road_objects", "meteo_sensors", "sensors", "id", "object_id", ""},
		{"forward FK", "sensors", "road_object", "road_objects", "object_id", "id", ""},
		{"M2M", "road_objects", "incidents", "incidents", "id", "object_id", "incidents_road_objects"},
		{"second M2M with same name and target", "road_objects", "old_incidents", "incidents", "id", "old_object_id", "old_incidents_road_objects"},
		{"self forward", "tree", "parent", "tree", "parent_id", "id", ""},
		{"self reverse", "tree", "children", "tree", "id", "parent_id", ""},
		{"scalar", "road_objects", "id", "", "", "", ""},
		{"missing relation", "road_objects", "missing", "", "", "", ""},
		{"missing field", "road_objects", "unknown", "", "", "", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			def := defs.ForName(ctx, tt.object)
			for _, resolve := range []struct {
				name string
				fn   func() *sdl.References
			}{
				{"object", func() *sdl.References {
					return sdl.DataObjectInfo(def).ReferencesQueryInfo(ctx, defs, tt.field)
				}},
				{"field", func() *sdl.References {
					return sdl.FieldReferencesInfo(ctx, defs, def, def.Fields.ForName(tt.field))
				}},
			} {
				t.Run(resolve.name, func(t *testing.T) {
					ref := resolve.fn()
					if tt.target == "" {
						assert.Nil(t, ref)
						return
					}
					require.NotNil(t, ref)
					assert.Equal(t, tt.junction != "", ref.IsM2M)
					assert.Equal(t, tt.junction, ref.M2MName)
					assert.Equal(t, []string{tt.sourceKey}, ref.SourceFields())
					assert.Equal(t, []string{tt.targetKey}, ref.ReferencesFields())
					require.NotNil(t, ref.ReferencesObjectDef(ctx, defs))
					assert.Equal(t, tt.target, ref.ReferencesObjectDef(ctx, defs).Name)
				})
			}
		})
	}
}
