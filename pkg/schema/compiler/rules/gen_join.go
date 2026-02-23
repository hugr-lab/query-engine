package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.BatchRule = (*JoinSpatialRule)(nil)

// JoinSpatialRule creates _join, _join_aggregation, _spatial, _spatial_aggregation
// types and adds _join / _spatial fields to data objects.
type JoinSpatialRule struct{}

func (r *JoinSpatialRule) Name() string     { return "JoinSpatialRule" }
func (r *JoinSpatialRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *JoinSpatialRule) ProcessAll(ctx base.CompilationContext) error {
	pos := compiledPos("join")
	opts := ctx.CompileOptions()
	catalog := optsCatalogDirective(opts)

	// Collect non-M2M data objects for _join type
	var dataObjects []*joinObjectEntry
	var spatialObjects []*joinObjectEntry

	for name, info := range ctx.Objects() {
		if info.IsM2M {
			continue
		}
		def := ctx.LookupType(name)
		if def == nil {
			continue
		}
		if def.Directives.ForName("table") == nil && def.Directives.ForName("view") == nil {
			continue
		}
		filterName := name + "_filter"
		entry := &joinObjectEntry{
			name:       name,
			filterName: filterName,
			info:       info,
		}
		dataObjects = append(dataObjects, entry)

		// Check if this object has Geometry fields for _spatial
		for _, f := range def.Fields {
			if f.Type.Name() == "Geometry" {
				spatialObjects = append(spatialObjects, entry)
				break
			}
		}
	}

	if len(dataObjects) == 0 {
		return nil
	}

	// Create _join type
	joinType := &ast.Definition{
		Kind:     ast.Object,
		Name:     "_join",
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "system", Position: pos},
		},
	}
	for _, obj := range dataObjects {
		// Main query field
		joinType.Fields = append(joinType.Fields, &ast.FieldDefinition{
			Name:      obj.name,
			Type:      ast.ListType(ast.NamedType(obj.name, pos), pos),
			Arguments: joinObjectQueryArgs(obj.filterName, pos),
			Directives: ast.DirectiveList{
				{Name: "query", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
				}, Position: pos},
				catalog,
			},
			Position: pos,
		})

		// Aggregation field
		aggTypeName := "_" + obj.name + "_aggregation"
		if ctx.LookupType(aggTypeName) != nil {
			joinType.Fields = append(joinType.Fields, &ast.FieldDefinition{
				Name:      obj.name + "_aggregation",
				Type:      ast.NamedType(aggTypeName, pos),
				Arguments: joinObjectQueryArgs(obj.filterName, pos),
				Directives: ast.DirectiveList{
					{Name: "aggregation_query", Arguments: ast.ArgumentList{
						{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					catalog,
				},
				Position: pos,
			})

			// Bucket aggregation field
			bucketAggTypeName := "_" + obj.name + "_aggregation_bucket"
			joinType.Fields = append(joinType.Fields, &ast.FieldDefinition{
				Name:      obj.name + "_bucket_aggregation",
				Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
				Arguments: joinObjectQueryArgs(obj.filterName, pos),
				Directives: ast.DirectiveList{
					{Name: "aggregation_query", Arguments: ast.ArgumentList{
						{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					catalog,
				},
				Position: pos,
			})
		}
	}
	ctx.AddDefinition(joinType)

	// Create _join_aggregation type
	joinAggType := &ast.Definition{
		Kind:     ast.Object,
		Name:     "_join_aggregation",
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "aggregation", Arguments: ast.ArgumentList{
				{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				{Name: "level", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos}, Position: pos},
				{Name: "name", Value: &ast.Value{Raw: "_join", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}
	for _, obj := range dataObjects {
		aggTypeName := "_" + obj.name + "_aggregation"
		if ctx.LookupType(aggTypeName) == nil {
			continue
		}
		joinAggType.Fields = append(joinAggType.Fields, &ast.FieldDefinition{
			Name:      obj.name,
			Type:      ast.NamedType(aggTypeName, pos),
			Arguments: joinObjectAggArgs(obj.filterName, pos),
			Directives: ast.DirectiveList{
				{Name: "aggregation_query", Arguments: ast.ArgumentList{
					{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
					{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
				catalog,
				fieldAggregationDirective(obj.name, pos),
			},
			Position: pos,
		})
	}
	ctx.AddDefinition(joinAggType)

	// Add _join field to every non-M2M data object and its aggregation type
	for _, obj := range dataObjects {
		joinFieldExt := &ast.Definition{
			Kind:     ast.Object,
			Name:     obj.name,
			Position: pos,
			Fields: ast.FieldList{
				{
					Name: "_join",
					Type: ast.NamedType("_join", pos),
					Arguments: ast.ArgumentDefinitionList{
						{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
					},
					Position: pos,
				},
			},
		}
		ctx.AddExtension(joinFieldExt)

		// Also add _join to aggregation type with @field_aggregation
		aggName := "_" + obj.name + "_aggregation"
		if ctx.LookupType(aggName) != nil {
			aggJoinExt := &ast.Definition{
				Kind:     ast.Object,
				Name:     aggName,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name: "_join",
						Type: ast.NamedType("_join_aggregation", pos),
						Arguments: ast.ArgumentDefinitionList{
							{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
						},
						Directives: ast.DirectiveList{
							fieldAggregationDirective("_join", pos),
						},
						Position: pos,
					},
				},
			}
			ctx.AddExtension(aggJoinExt)
		}
	}

	// Create _spatial type (only if there are spatial objects)
	if len(spatialObjects) > 0 {
		spatialType := &ast.Definition{
			Kind:     ast.Object,
			Name:     "_spatial",
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "system", Position: pos},
			},
		}
		for _, obj := range spatialObjects {
			// Main query field
			spatialType.Fields = append(spatialType.Fields, &ast.FieldDefinition{
				Name:      obj.name,
				Type:      ast.ListType(ast.NamedType(obj.name, pos), pos),
				Arguments: spatialObjectQueryArgs(obj.filterName, pos),
				Directives: ast.DirectiveList{
					{Name: "query", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						{Name: "type", Value: &ast.Value{Raw: "SELECT", Kind: ast.EnumValue, Position: pos}, Position: pos},
					}, Position: pos},
					catalog,
				},
				Position: pos,
			})

			// Aggregation field
			aggTypeName := "_" + obj.name + "_aggregation"
			if ctx.LookupType(aggTypeName) != nil {
				spatialType.Fields = append(spatialType.Fields, &ast.FieldDefinition{
					Name:      obj.name + "_aggregation",
					Type:      ast.NamedType(aggTypeName, pos),
					Arguments: spatialObjectQueryArgs(obj.filterName, pos),
					Directives: ast.DirectiveList{
						{Name: "aggregation_query", Arguments: ast.ArgumentList{
							{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catalog,
					},
					Position: pos,
				})

				// Bucket aggregation field
				bucketAggTypeName := "_" + obj.name + "_aggregation_bucket"
				spatialType.Fields = append(spatialType.Fields, &ast.FieldDefinition{
					Name:      obj.name + "_bucket_aggregation",
					Type:      ast.ListType(ast.NamedType(bucketAggTypeName, pos), pos),
					Arguments: spatialObjectQueryArgs(obj.filterName, pos),
					Directives: ast.DirectiveList{
						{Name: "aggregation_query", Arguments: ast.ArgumentList{
							{Name: "is_bucket", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
							{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
						}, Position: pos},
						catalog,
					},
					Position: pos,
				})
			}
		}
		ctx.AddDefinition(spatialType)

		// Create _spatial_aggregation type
		spatialAggType := &ast.Definition{
			Kind:     ast.Object,
			Name:     "_spatial_aggregation",
			Position: pos,
			Directives: ast.DirectiveList{
				{Name: "aggregation", Arguments: ast.ArgumentList{
					{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
					{Name: "level", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos}, Position: pos},
					{Name: "name", Value: &ast.Value{Raw: "_spatial", Kind: ast.StringValue, Position: pos}, Position: pos},
				}, Position: pos},
			},
		}
		for _, obj := range spatialObjects {
			aggTypeName := "_" + obj.name + "_aggregation"
			if ctx.LookupType(aggTypeName) == nil {
				continue
			}
			spatialAggType.Fields = append(spatialAggType.Fields, &ast.FieldDefinition{
				Name:      obj.name,
				Type:      ast.NamedType(aggTypeName, pos),
				Arguments: spatialObjectAggArgs(obj.filterName, pos),
				Directives: ast.DirectiveList{
					{Name: "aggregation_query", Arguments: ast.ArgumentList{
						{Name: "is_bucket", Value: &ast.Value{Raw: "false", Kind: ast.BooleanValue, Position: pos}, Position: pos},
						{Name: "name", Value: &ast.Value{Raw: obj.info.Name, Kind: ast.StringValue, Position: pos}, Position: pos},
					}, Position: pos},
					catalog,
					fieldAggregationDirective(obj.name, pos),
				},
				Position: pos,
			})
		}
		ctx.AddDefinition(spatialAggType)

		// Add _spatial field to spatial objects and their aggregation types
		for _, obj := range spatialObjects {
			spatialFieldExt := &ast.Definition{
				Kind:     ast.Object,
				Name:     obj.name,
				Position: pos,
				Fields: ast.FieldList{
					{
						Name: "_spatial",
						Type: ast.NamedType("_spatial", pos),
						Arguments: ast.ArgumentDefinitionList{
							{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
							{Name: "type", Type: ast.NonNullNamedType("GeometrySpatialQueryType", pos), Position: pos},
							{Name: "buffer", Type: ast.NamedType("Int", pos), Position: pos},
						},
						Position: pos,
					},
				},
			}
			ctx.AddExtension(spatialFieldExt)

			// Also add to aggregation type with @field_aggregation
			aggName := "_" + obj.name + "_aggregation"
			if ctx.LookupType(aggName) != nil {
				aggSpatialExt := &ast.Definition{
					Kind:     ast.Object,
					Name:     aggName,
					Position: pos,
					Fields: ast.FieldList{
						{
							Name: "_spatial",
							Type: ast.NamedType("_spatial_aggregation", pos),
							Arguments: ast.ArgumentDefinitionList{
								{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
								{Name: "type", Type: ast.NonNullNamedType("GeometrySpatialQueryType", pos), Position: pos},
								{Name: "buffer", Type: ast.NamedType("Int", pos), Position: pos},
							},
							Directives: ast.DirectiveList{
								fieldAggregationDirective("_spatial", pos),
							},
							Position: pos,
						},
					},
				}
				ctx.AddExtension(aggSpatialExt)
			}
		}
	}

	return nil
}

type joinObjectEntry struct {
	name       string
	filterName string
	info       *base.ObjectInfo
}

// joinObjectQueryArgs creates args for _join type fields (includes limit/offset).
func joinObjectQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// joinObjectAggArgs creates args for _join_aggregation type fields (no limit/offset).
func joinObjectAggArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", pos), pos), Position: pos},
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// spatialObjectQueryArgs creates args for _spatial type fields (includes limit/offset).
func spatialObjectQueryArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "limit", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "2000", Kind: ast.IntValue}},
		{Name: "offset", Type: ast.NamedType("Int", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "0", Kind: ast.IntValue}},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// spatialObjectAggArgs creates args for _spatial_aggregation type fields (no limit/offset).
func spatialObjectAggArgs(filterName string, pos *ast.Position) ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "field", Type: ast.NonNullNamedType("String", pos), Position: pos},
		{Name: "filter", Type: ast.NamedType(filterName, pos), Position: pos},
		{Name: "order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "distinct_on", Type: ast.ListType(ast.NamedType("String", pos), pos), Position: pos},
		{Name: "inner", Type: ast.NamedType("Boolean", pos), Position: pos,
			DefaultValue: &ast.Value{Raw: "false", Kind: ast.BooleanValue}},
		{Name: "nested_order_by", Type: ast.ListType(ast.NamedType("OrderByField", pos), pos), Position: pos},
		{Name: "nested_limit", Type: ast.NamedType("Int", pos), Position: pos},
		{Name: "nested_offset", Type: ast.NamedType("Int", pos), Position: pos},
	}
}

// fieldAggregationDirective creates a @field_aggregation(name=X) directive.
func fieldAggregationDirective(name string, pos *ast.Position) *ast.Directive {
	return &ast.Directive{
		Name: "field_aggregation",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}
