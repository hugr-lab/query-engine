package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// The queryShapes registry entries (Ш4.4): one per query/mutation kind.
// Today they emit the def-level @query/@mutation markers the planner reads
// off the data-object definition; Ш4.7 extends them with the root-field
// builders that instantiate the same shapes on module roots.

// objectTraits bundles what shape/field rules branch on for ONE object.
type objectTraits struct {
	row    *dataObject
	fields []*field
	src    activeSource
	srcs   map[string]activeSource
}

func (g *genContext) objectTraits(ctx context.Context, row *dataObject) *objectTraits {
	srcs := g.s.activeSources(ctx)
	return &objectTraits{
		row:    row,
		fields: g.s.readFields(ctx, row.Name),
		src:    srcs[row.DataSource],
		srcs:   srcs,
	}
}

// queryName is the marker/root base name: the ORIGINAL (unprefixed) name for
// AsModule sources, the compiled name otherwise.
func (t *objectTraits) queryName() string {
	if t.src.AsModule && t.row.OriginalName != "" {
		return t.row.OriginalName
	}
	return t.row.Name
}

func (t *objectTraits) isM2M() bool {
	return t.row.Properties != nil && t.row.Properties.IsM2M
}

func (t *objectTraits) hasPK() bool {
	for _, f := range t.fields {
		if f.IsPK {
			return true
		}
	}
	return false
}

// writable: mutation shapes exist for tables of non-read-only sources.
func (t *objectTraits) writable() bool {
	return t.row.Kind == "table" && !t.src.ReadOnly
}

func queryMarker(name, queryType string) []*ast.Directive {
	return []*ast.Directive{directive("query",
		strArg(base.ArgName, name),
		enumArg("type", queryType),
	)}
}

func mutationMarker(name, mutationType, dataInput string) []*ast.Directive {
	return []*ast.Directive{directive("mutation",
		strArg("data_input", dataInput),
		strArg(base.ArgName, name),
		enumArg("type", mutationType),
	)}
}

var selectShape = queryShape{
	kind:    "select",
	matches: func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn, "SELECT")
	},
}

var selectOnePKShape = queryShape{
	kind:    "select_one_pk",
	matches: func(t *objectTraits) bool { return t.hasPK() && !t.isM2M() },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_by_pk", "SELECT_ONE")
	},
}

var aggregateShape = queryShape{
	kind:    "aggregate",
	matches: func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_aggregation", "AGGREGATE")
	},
}

var bucketAggShape = queryShape{
	kind:    "bucket_agg",
	matches: func(*objectTraits) bool { return true },
	markers: func(_ *objectTraits, qn string) []*ast.Directive {
		return queryMarker(qn+"_bucket_aggregation", "AGGREGATE_BUCKET")
	},
}

var insertShape = queryShape{
	kind:    "insert",
	matches: (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("insert_"+qn, "INSERT", t.row.Name+mutInputDataSuffix)
	},
}

var updateShape = queryShape{
	kind:    "update",
	matches: (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("update_"+qn, "UPDATE", t.row.Name+mutDataSuffix)
	},
}

var deleteShape = queryShape{
	kind:    "delete",
	matches: (*objectTraits).writable,
	markers: func(t *objectTraits, qn string) []*ast.Directive {
		return mutationMarker("delete_"+qn, "DELETE", "")
	},
}

// shapeMarkers emits every matching shape's def-level markers for the object.
func shapeMarkers(t *objectTraits) ast.DirectiveList {
	qn := t.queryName()
	var out ast.DirectiveList
	for i := range queryShapes {
		if !queryShapes[i].matches(t) {
			continue
		}
		out = append(out, queryShapes[i].markers(t, qn)...)
	}
	return out
}
