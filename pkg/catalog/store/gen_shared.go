package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Shared-type rules (Ш4.6): the per-schema cross-source system types
// JoinSpatialRule / H3Rule assemble — _join, _join_aggregation, _spatial,
// _spatial_aggregation, _h3_data_query. Each rule iterates the ACTIVE
// non-M2M data objects (all sources) and instantiates the corresponding
// member trio; nil when no member qualifies, so the static stub (when any)
// stays authoritative. Argument profiles and the vector-search decorator are
// reused from compiler/ingest.

// sharedObjectEntry is the store-side joinObjectEntry.
type sharedObjectEntry struct {
	name          string
	filterName    string
	info          *base.ObjectInfo
	dataSource    string
	isExtension   bool
	hasVector     bool
	hasEmbeddings bool
	spatial       bool
}

// sharedObjects lists every ACTIVE non-M2M data object with the traits the
// shared-type members branch on.
func (g *genContext) sharedObjects(ctx context.Context) ([]*sharedObjectEntry, error) {
	names, err := g.queryNames(ctx, `SELECT o.name FROM core.catalog.data_objects o`+
		activeMeta("m", "o.data_source")+` ORDER BY o.name`)
	if err != nil {
		return nil, err
	}
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	var out []*sharedObjectEntry
	for _, name := range names {
		row, err := g.readDataObject(ctx, name)
		if err != nil {
			return nil, err
		}
		if row == nil || (row.Properties != nil && row.Properties.IsM2M) {
			continue
		}
		e := &sharedObjectEntry{
			name:        name,
			filterName:  name + filterSuffix,
			info:        &base.ObjectInfo{Name: name, OriginalName: row.OriginalName},
			dataSource:  row.DataSource,
			isExtension: srcs[row.DataSource].IsExtension,
		}
		if row.Properties != nil {
			e.info.InputArgsName = row.Properties.ArgsTypeName
			e.info.RequiredArgs = row.Properties.RequiredArgs
			e.hasEmbeddings = row.Properties.Embeddings != nil
		}
		fields, err := g.readFields(ctx, name)
		if err != nil {
			return nil, err
		}
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
	return out, nil
}

// sharedBuildScope loads the object list and source meta every shared-type
// builder starts from; empty objs = the builder yields nil.
func sharedBuildScope(ctx context.Context, g *genContext) ([]*sharedObjectEntry, map[string]activeSource, error) {
	objs, err := g.sharedObjects(ctx)
	if err != nil {
		return nil, nil, err
	}
	if len(objs) == 0 {
		return nil, nil, nil
	}
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, nil, err
	}
	return objs, srcs, nil
}

func buildJoinShared(ctx context.Context, g *genContext) (*ast.Definition, error) {
	objs, srcs, err := sharedBuildScope(ctx, g)
	if err != nil || len(objs) == 0 {
		return nil, err
	}
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_join",
		Position:   reconPos,
		Directives: ast.DirectiveList{directive("system")},
	}
	for _, obj := range objs {
		args := func() ast.ArgumentDefinitionList {
			a := JoinObjectQueryArgsWithViewArgs(obj.info, obj.filterName, reconPos)
			return append(a, VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		}
		def.Fields = append(def.Fields, sharedMemberTrio(obj, args, srcs)...)
	}
	return def, nil
}

func buildJoinAggShared(ctx context.Context, g *genContext) (*ast.Definition, error) {
	objs, srcs, err := sharedBuildScope(ctx, g)
	if err != nil || len(objs) == 0 {
		return nil, err
	}
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_join_aggregation",
		Position:   reconPos,
		Directives: ast.DirectiveList{aggregationMarker("_join", false, 1)},
	}
	for _, obj := range objs {
		args := JoinObjectAggArgsWithViewArgs(obj.info, obj.filterName, reconPos)
		args = append(args, VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		def.Fields = append(def.Fields, sharedAggMember(obj, args, srcs))
	}
	return def, nil
}

func buildSpatialShared(ctx context.Context, g *genContext) (*ast.Definition, error) {
	objs, srcs, err := sharedBuildScope(ctx, g)
	if err != nil {
		return nil, err
	}
	objs = spatialOnly(objs)
	if len(objs) == 0 {
		return nil, nil
	}
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_spatial",
		Position:   reconPos,
		Directives: ast.DirectiveList{directive("system")},
	}
	for _, obj := range objs {
		def.Fields = append(def.Fields, sharedMemberTrio(obj, spatialMemberArgs(obj), srcs)...)
	}
	return def, nil
}

func buildSpatialAggShared(ctx context.Context, g *genContext) (*ast.Definition, error) {
	objs, srcs, err := sharedBuildScope(ctx, g)
	if err != nil {
		return nil, err
	}
	objs = spatialOnly(objs)
	if len(objs) == 0 {
		return nil, nil
	}
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "_spatial_aggregation",
		Position:   reconPos,
		Directives: ast.DirectiveList{aggregationMarker("_spatial", false, 1)},
	}
	for _, obj := range objs {
		args := SpatialObjectAggArgs(obj.filterName, reconPos)
		args = append(args, VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
		def.Fields = append(def.Fields, sharedAggMember(obj, args, srcs))
	}
	return def, nil
}

// buildH3DataShared mirrors H3Rule: the _spatial member trio with the four
// h3-specific arguments appended.
func buildH3DataShared(ctx context.Context, g *genContext) (*ast.Definition, error) {
	objs, srcs, err := sharedBuildScope(ctx, g)
	if err != nil {
		return nil, err
	}
	objs = spatialOnly(objs)
	if len(objs) == 0 {
		return nil, nil
	}
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
	return def, nil
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
		a := SpatialObjectQueryArgs(obj.filterName, reconPos)
		return append(a, VectorSearchArgs(obj.hasVector, obj.hasEmbeddings, reconPos)...)
	}
}

// sharedMemberTrio is the select / aggregation / bucket-aggregation member
// set every shared query type carries per object. args is a factory — each
// member gets its own argument list instance.
func sharedMemberTrio(obj *sharedObjectEntry, args func() ast.ArgumentDefinitionList, srcs map[string]activeSource) []*ast.FieldDefinition {
	dirs := func(head ...*ast.Directive) ast.DirectiveList {
		out := ast.DirectiveList(head)
		out = append(out, catalogDirective(obj.dataSource, srcs[obj.dataSource].Engine))
		if obj.isExtension {
			out = append(out, dependencyDirective(obj.dataSource))
		}
		return out
	}
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
			Directives: dirs(
				directive("query", strArg(base.ArgName, obj.name), enumArg("type", "SELECT"))),
			Position: reconPos,
		},
		{
			Name:       obj.name + "_aggregation",
			Type:       ast.NamedType("_"+obj.name+"_aggregation", reconPos),
			Arguments:  args(),
			Directives: dirs(aggQuery(false)),
			Position:   reconPos,
		},
		{
			Name:       obj.name + "_bucket_aggregation",
			Type:       ast.ListType(ast.NamedType("_"+obj.name+"_aggregation_bucket", reconPos), reconPos),
			Arguments:  args(),
			Directives: dirs(aggQuery(true)),
			Position:   reconPos,
		},
	}
}

// sharedAggMember is the single aggregation member the *_aggregation shared
// types carry per object.
func sharedAggMember(obj *sharedObjectEntry, args ast.ArgumentDefinitionList, srcs map[string]activeSource) *ast.FieldDefinition {
	dirs := ast.DirectiveList{
		directive(base.FieldAggregationQueryDirectiveName,
			boolArg(base.ArgIsBucket, false),
			strArg(base.ArgName, obj.name),
		),
		catalogDirective(obj.dataSource, srcs[obj.dataSource].Engine),
	}
	if obj.isExtension {
		dirs = append(dirs, dependencyDirective(obj.dataSource))
	}
	dirs = append(dirs, fieldAggregationMarker(obj.name))
	return &ast.FieldDefinition{
		Name:       obj.name,
		Type:       ast.NamedType("_"+obj.name+"_aggregation", reconPos),
		Arguments:  args,
		Directives: dirs,
		Position:   reconPos,
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
