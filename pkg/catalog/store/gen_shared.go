package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/vektah/gqlparser/v2/ast"
)

// Shared-type rules (Ш4.6): the per-schema cross-source system types
// JoinSpatialRule / H3Rule assemble — _join, _join_aggregation, _spatial,
// _spatial_aggregation, _h3_data_query. Each rule iterates the ACTIVE
// non-M2M data objects (all sources) and instantiates the corresponding
// member trio; nil when no member qualifies, so the static stub (when any)
// stays authoritative. Argument profiles and the vector-search decorator are
// reused from compiler/rules.

// sharedObjectEntry is the store-side joinObjectEntry.
type sharedObjectEntry struct {
	name          string
	filterName    string
	info          *base.ObjectInfo
	dataSource    string
	hasVector     bool
	hasEmbeddings bool
	spatial       bool
}

// sharedObjects lists every ACTIVE non-M2M data object with the traits the
// shared-type members branch on.
func (g *genContext) sharedObjects(ctx context.Context) []*sharedObjectEntry {
	names := g.s.queryNames(ctx, `SELECT o.name FROM core.catalog.data_objects o`+
		activeMeta("m", "o.data_source")+` ORDER BY o.name`)
	var out []*sharedObjectEntry
	for _, name := range names {
		row, ok := g.s.readDataObject(ctx, name)
		if !ok || (row.Properties != nil && row.Properties.IsM2M) {
			continue
		}
		e := &sharedObjectEntry{
			name:       name,
			filterName: name + filterSuffix,
			info:       &base.ObjectInfo{Name: name, OriginalName: row.OriginalName},
			dataSource: row.DataSource,
		}
		if row.Properties != nil {
			e.info.InputArgsName = row.Properties.ArgsTypeName
			e.info.RequiredArgs = row.Properties.RequiredArgs
			e.hasEmbeddings = row.Properties.Embeddings != nil
		}
		fields := g.s.readFields(ctx, name)
		e.spatial = fieldsHaveGeometry(fields)
		e.hasVector = e.hasEmbeddings
		if !e.hasVector {
			for _, f := range fields {
				if parseFieldType(f.FieldType).Name() == "Vector" {
					e.hasVector = true
					break
				}
			}
		}
		out = append(out, e)
	}
	return out
}

func buildJoinShared(ctx context.Context, g *genContext) *ast.Definition {
	objs := g.sharedObjects(ctx)
	if len(objs) == 0 {
		return nil
	}
	srcs := g.s.activeSources(ctx)
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_join",
		Position:   reconPos,
		Directives: ast.DirectiveList{directive("system")},
	}
	for _, obj := range objs {
		args := func() ast.ArgumentDefinitionList {
			a := rules.JoinObjectQueryArgsWithViewArgs(obj.info, obj.filterName, reconPos)
			return append(a, rules.VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		}
		def.Fields = append(def.Fields, sharedMemberTrio(obj, args, srcs)...)
	}
	return def
}

func buildJoinAggShared(ctx context.Context, g *genContext) *ast.Definition {
	objs := g.sharedObjects(ctx)
	if len(objs) == 0 {
		return nil
	}
	srcs := g.s.activeSources(ctx)
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_join_aggregation",
		Position:   reconPos,
		Directives: ast.DirectiveList{aggregationMarker("_join", false, 1)},
	}
	for _, obj := range objs {
		args := rules.JoinObjectAggArgsWithViewArgs(obj.info, obj.filterName, reconPos)
		args = append(args, rules.VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		def.Fields = append(def.Fields, sharedAggMember(obj, args, srcs))
	}
	return def
}

func buildSpatialShared(ctx context.Context, g *genContext) *ast.Definition {
	objs := spatialOnly(g.sharedObjects(ctx))
	if len(objs) == 0 {
		return nil
	}
	srcs := g.s.activeSources(ctx)
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_spatial",
		Position:   reconPos,
		Directives: ast.DirectiveList{directive("system")},
	}
	for _, obj := range objs {
		def.Fields = append(def.Fields, sharedMemberTrio(obj, spatialMemberArgs(obj), srcs)...)
	}
	return def
}

func buildSpatialAggShared(ctx context.Context, g *genContext) *ast.Definition {
	objs := spatialOnly(g.sharedObjects(ctx))
	if len(objs) == 0 {
		return nil
	}
	srcs := g.s.activeSources(ctx)
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_spatial_aggregation",
		Position:   reconPos,
		Directives: ast.DirectiveList{aggregationMarker("_spatial", false, 1)},
	}
	for _, obj := range objs {
		args := rules.SpatialObjectAggArgs(obj.filterName, reconPos)
		args = append(args, rules.VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		def.Fields = append(def.Fields, sharedAggMember(obj, args, srcs))
	}
	return def
}

// buildH3DataShared mirrors H3Rule: the _spatial member trio with the four
// h3-specific arguments appended.
func buildH3DataShared(ctx context.Context, g *genContext) *ast.Definition {
	objs := spatialOnly(g.sharedObjects(ctx))
	if len(objs) == 0 {
		return nil
	}
	srcs := g.s.activeSources(ctx)
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_h3_data_query",
		Position:   reconPos,
		Directives: ast.DirectiveList{directive("system")},
	}
	for _, obj := range objs {
		args := func() ast.ArgumentDefinitionList {
			return append(spatialMemberArgs(obj)(), h3ExtraArgs()...)
		}
		def.Fields = append(def.Fields, sharedMemberTrio(obj, args, srcs)...)
	}
	return def
}

func spatialOnly(objs []*sharedObjectEntry) []*sharedObjectEntry {
	var out []*sharedObjectEntry
	for _, obj := range objs {
		if obj.spatial {
			out = append(out, obj)
		}
	}
	return out
}

func spatialMemberArgs(obj *sharedObjectEntry) func() ast.ArgumentDefinitionList {
	return func() ast.ArgumentDefinitionList {
		a := rules.SpatialObjectQueryArgs(obj.filterName, reconPos)
		return append(a, rules.VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
	}
}

// sharedMemberTrio is the select / aggregation / bucket-aggregation member
// set every shared query type carries per object. args is a factory — each
// member gets its own argument list instance.
func sharedMemberTrio(obj *sharedObjectEntry, args func() ast.ArgumentDefinitionList, srcs map[string]activeSource) []*ast.FieldDefinition {
	catalog := func() *ast.Directive { return catalogDirective(obj.dataSource, srcs[obj.dataSource].Engine) }
	aggQuery := func(isBucket bool) *ast.Directive {
		return directive(base.FieldAggregationQueryDirectiveName,
			boolArg(base.ArgIsBucket, isBucket),
			strArg(base.ArgName, obj.name),
		)
	}
	return []*ast.FieldDefinition{
		{
			Name:      obj.name,
			Type:      ast.ListType(ast.NamedType(obj.name, reconPos), reconPos),
			Arguments: args(),
			Directives: ast.DirectiveList{
				directive("query", strArg(base.ArgName, obj.name), enumArg("type", "SELECT")),
				catalog(),
			},
			Position: reconPos,
		},
		{
			Name:       obj.name + "_aggregation",
			Type:       ast.NamedType("_"+obj.name+"_aggregation", reconPos),
			Arguments:  args(),
			Directives: ast.DirectiveList{aggQuery(false), catalog()},
			Position:   reconPos,
		},
		{
			Name:       obj.name + "_bucket_aggregation",
			Type:       ast.ListType(ast.NamedType("_"+obj.name+"_aggregation_bucket", reconPos), reconPos),
			Arguments:  args(),
			Directives: ast.DirectiveList{aggQuery(true), catalog()},
			Position:   reconPos,
		},
	}
}

// sharedAggMember is the single aggregation member the *_aggregation shared
// types carry per object.
func sharedAggMember(obj *sharedObjectEntry, args ast.ArgumentDefinitionList, srcs map[string]activeSource) *ast.FieldDefinition {
	return &ast.FieldDefinition{
		Name:      obj.name,
		Type:      ast.NamedType("_"+obj.name+"_aggregation", reconPos),
		Arguments: args,
		Directives: ast.DirectiveList{
			directive(base.FieldAggregationQueryDirectiveName,
				boolArg(base.ArgIsBucket, false),
				strArg(base.ArgName, obj.name),
			),
			catalogDirective(obj.dataSource, srcs[obj.dataSource].Engine),
			fieldAggregationMarker(obj.name),
		},
		Position: reconPos,
	}
}

// h3ExtraArgs are the H3Rule-specific arguments appended to every
// _h3_data_query member.
func h3ExtraArgs() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{
			Name:        "transform_from",
			Description: "Reproject geometry from SRID before aggregation",
			Type:        ast.NamedType("Int", reconPos),
			Position:    reconPos,
		},
		{
			Name:        "buffer",
			Description: "Buffer distance in meters to apply to the geometry before aggregation",
			Type:        ast.NamedType("Float", reconPos),
			Position:    reconPos,
		},
		{
			Name:        "divide_values",
			Description: "Divide the aggregated values by a count of split h3 cells",
			Type:        ast.NamedType("Boolean", reconPos),
			Position:    reconPos,
		},
		{
			Name:         "simplify",
			Description:  "Simplify the geometry before cast to h3 cells",
			Type:         ast.NamedType("Boolean", reconPos),
			DefaultValue: &ast.Value{Kind: ast.BooleanValue, Raw: "true"},
			Position:     reconPos,
		},
	}
}
