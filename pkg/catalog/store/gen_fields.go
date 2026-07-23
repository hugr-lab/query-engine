package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// The fieldRules (Ш4.5): ordered contributions to the reconstructed data
// object, mirroring the compiler's field-level GENERATE work: scalar
// argument sets, @join subqueries + aggregation twins, nav fields from
// relations (+ their aggregation twins), ExtraFieldProvider fields and the
// shared _join/_spatial members.

// scalarArgsFieldRule applies setScalarFieldArguments on read: non-list
// fields of FieldArgumentsProvider scalars carry the scalar's arguments
// (bucket for Timestamp, transforms for Geometry, …).
var scalarArgsFieldRule = fieldRule{
	name: "scalar_args",
	apply: func(_ context.Context, _ *genContext, _ *objectTraits, def *ast.Definition) {
		for _, fd := range def.Fields {
			if fd.Type.NamedType == "" {
				continue
			}
			s := types.Lookup(fd.Type.Name())
			if s == nil {
				continue
			}
			if args := scalarFieldArguments(s); args != nil {
				fd.Arguments = args
			}
		}
	},
}

// joinFieldsRule finishes declared @join list fields whose target is a data
// object: the field gets the subquery argument profile, the object gets the
// {name}_aggregation / {name}_bucket_aggregation twins.
var joinFieldsRule = fieldRule{
	name: "join_fields",
	apply: func(ctx context.Context, g *genContext, t *objectTraits, def *ast.Definition) {
		for _, f := range t.fields {
			if f.Properties == nil || f.Properties.Join == nil {
				continue
			}
			typ := parseFieldType(f.FieldType)
			target := typ.Name()
			if typ.NamedType != "" || !g.s.dataObjectExists(ctx, target) {
				continue
			}
			if fd := def.Fields.ForName(f.Name); fd != nil && len(fd.Arguments) == 0 {
				fd.Arguments = rules.SubQueryArgs(target+filterSuffix, reconPos)
			}
			def.Fields = append(def.Fields,
				relationAggTwinFields(f.Name, target, t.srcs, f.DataSource)...)
		}
	},
}

// navFieldsRule adds the navigation fields the relation projections define:
// forward FK single (inner arg), back FK and M2M lists (subquery profile),
// plus the aggregation twins for the list ones. An is_m2m junction gets no
// forward navigation of its own.
var navFieldsRule = fieldRule{
	name: "nav_fields",
	apply: func(ctx context.Context, g *genContext, t *objectTraits, def *ast.Definition) {
		if !t.isM2M() {
			for _, leg := range g.s.relationsBySource(ctx, t.row.Name) {
				def.Fields = append(def.Fields, &ast.FieldDefinition{
					Name: leg.SourceField,
					Type: &ast.Type{NamedType: leg.Destination},
					Arguments: ast.ArgumentDefinitionList{
						{Name: "inner", Description: base.DescInnerJoinRef, Type: ast.NamedType("Boolean", reconPos), Position: reconPos},
					},
					Directives: ast.DirectiveList{
						navQueryDirective(leg.Destination, leg.Name, false, ""),
						catalogDirective(leg.DataSource, t.srcs[leg.DataSource].Engine),
					},
					Position: reconPos,
				})
			}
		}
		for _, leg := range g.s.relationsByDestination(ctx, t.row.Name) {
			if leg.m2mJunction {
				for _, co := range g.s.relationsBySource(ctx, leg.Source) {
					if co.Name == leg.Name {
						continue
					}
					def.Fields = append(def.Fields, navListField(
						leg.DestinationField, co.Destination,
						navQueryDirective(co.Destination, leg.Name, true, leg.Source),
						t.srcs, leg.DataSource))
					def.Fields = append(def.Fields,
						relationAggTwinFields(leg.DestinationField, co.Destination, t.srcs, leg.DataSource)...)
				}
				continue
			}
			def.Fields = append(def.Fields, navListField(
				leg.DestinationField, leg.Source,
				navQueryDirective(leg.Source, leg.Name, false, ""),
				t.srcs, leg.DataSource))
			def.Fields = append(def.Fields,
				relationAggTwinFields(leg.DestinationField, leg.Source, t.srcs, leg.DataSource)...)
		}
	},
}

func navListField(name, target string, refDir *ast.Directive, srcs map[string]activeSource, dataSource string) *ast.FieldDefinition {
	return &ast.FieldDefinition{
		Name:      name,
		Type:      ast.ListType(ast.NamedType(target, reconPos), reconPos),
		Arguments: rules.SubQueryArgs(target+filterSuffix, reconPos),
		Directives: ast.DirectiveList{
			refDir,
			catalogDirective(dataSource, srcs[dataSource].Engine),
		},
		Position: reconPos,
	}
}

// navQueryDirective is the @references_query marker on generated nav fields.
func navQueryDirective(referencesName, relName string, isM2M bool, m2mName string) *ast.Directive {
	return directive(base.FieldReferencesQueryDirectiveName,
		strArg(base.ArgReferencesName, referencesName),
		strArg(base.ArgName, relName),
		boolArg(base.ArgIsM2M, isM2M),
		strArg(base.ArgM2MName, m2mName),
	)
}

// relationAggTwinFields is addReferenceAggregationFields: the
// {name}_aggregation / {name}_bucket_aggregation members every list relation
// (@join / back FK / M2M) puts on the base object.
func relationAggTwinFields(fieldName, target string, srcs map[string]activeSource, dataSource string) []*ast.FieldDefinition {
	targetFilter := target + filterSuffix
	catalog := func() *ast.Directive { return catalogDirective(dataSource, srcs[dataSource].Engine) }
	aggQuery := func(isBucket bool) *ast.Directive {
		return directive(base.FieldAggregationQueryDirectiveName,
			boolArg(base.ArgIsBucket, isBucket),
			strArg(base.ArgName, fieldName),
		)
	}
	return []*ast.FieldDefinition{
		{
			Name:        fieldName + "_aggregation",
			Description: "The aggregation for " + fieldName,
			Type:        ast.NamedType("_"+target+"_aggregation", reconPos),
			Arguments:   rules.SubQueryArgs(targetFilter, reconPos),
			Directives:  ast.DirectiveList{aggQuery(false), catalog()},
			Position:    reconPos,
		},
		{
			Name:        fieldName + "_bucket_aggregation",
			Description: "The bucket aggregation for " + fieldName,
			Type:        ast.ListType(ast.NamedType("_"+target+"_aggregation_bucket", reconPos), reconPos),
			Arguments:   rules.SubQueryArgs(targetFilter, reconPos),
			Directives:  ast.DirectiveList{aggQuery(true), catalog()},
			Position:    reconPos,
		},
	}
}

// extraFieldsRule appends the ExtraFieldProvider fields (e.g. _<f>_part for
// Timestamp, _<f>_measurement for Geometry) — the provider returns the
// complete field definition.
var extraFieldsRule = fieldRule{
	name: "extra_fields",
	apply: func(_ context.Context, _ *genContext, t *objectTraits, def *ast.Definition) {
		for _, f := range t.fields {
			if f.Name == "_stub" {
				continue
			}
			s := types.Lookup(parseFieldType(f.FieldType).Name())
			if s == nil {
				continue
			}
			efp, ok := s.(types.ExtraFieldProvider)
			if !ok {
				continue
			}
			if extraField := efp.GenerateExtraField(f.Name); extraField != nil {
				def.Fields = append(def.Fields, extraField)
			}
		}
	},
}

// embeddingsFieldRule adds `_distance_to_query` on @embeddings objects
// (EmbeddingsRule): a computed Float over the vector field with the
// QueryEmbeddingDistance extra-field binding.
var embeddingsFieldRule = fieldRule{
	name: "embeddings_fields",
	apply: func(_ context.Context, _ *genContext, t *objectTraits, def *ast.Definition) {
		_, emb := t.vectorTraits()
		if emb == nil {
			return
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:        "_distance_to_query",
			Description: "Calculate vector distance to the specified vector for field " + emb.Vector,
			Type:        ast.NamedType("Float", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{
					Name:        "query",
					Description: "Query to calculate distance to",
					Type:        ast.NonNullNamedType("String", reconPos),
					Position:    reconPos,
				},
			},
			Directives: ast.DirectiveList{
				directive("sql", strArg("exp", "["+emb.Vector+"]")),
				directive("extra_field",
					strArg(base.ArgName, "QueryEmbeddingDistance"),
					strArg("base_field", emb.Vector),
					enumArg("base_type", "Vector"),
				),
			},
			Position: reconPos,
		})
	},
}

// sharedFieldsRule adds the shared cross-source members: `_join` on every
// non-M2M data object, `_spatial` when the object has a Geometry field
// (JoinSpatialRule).
var sharedFieldsRule = fieldRule{
	name: "shared_fields",
	apply: func(_ context.Context, _ *genContext, t *objectTraits, def *ast.Definition) {
		if t.isM2M() {
			return
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name: "_join",
			Type: ast.NamedType("_join", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", reconPos), reconPos), Position: reconPos},
			},
			Position: reconPos,
		})
		if fieldsHaveGeometry(t.fields) {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:      "_spatial",
				Type:      ast.NamedType("_spatial", reconPos),
				Arguments: spatialFieldArgs(),
				Position:  reconPos,
			})
		}
	},
}
