package store

import (
	"context"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Derived rules for the aggregation family (Ш4.3): _X_aggregation,
// _X_aggregation_bucket and _X_aggregation(_sub_aggregation){1,2}, mirroring
// generateAggregationType / generateBucketAggregationType /
// buildSubAggregationType / generateStructSubAggType plus the relation and
// virtual-field aggregations gen_references.go / gen_table.go add through
// extensions. Argument profiles are reused from compiler/rules (AggRefArgs /
// AggSubRefArgs) — one source of truth for the arg shapes.
//
// The compiler creates sub-aggregation types lazily in declaration order; the
// store serves the deterministic full shape (every existing base gets its
// depth-1 and depth-2 types). For schemas where a lazy type was never forced
// the store serves a superset — the extra names are unreferenced.

var aggregationRule = derivedRule{
	name:  "aggregation",
	match: matchAggregationName,
	build: buildAggregation,
}

// matchAggregationName parses `_<base>_aggregation[_bucket|(_sub_aggregation){1,2}]`.
func matchAggregationName(name string) (string, bool) {
	rest, ok := strings.CutPrefix(name, "_")
	if !ok {
		return "", false
	}
	rest, _ = strings.CutSuffix(rest, "_bucket")
	for strings.HasSuffix(rest, "_sub_aggregation") {
		rest = strings.TrimSuffix(rest, "_sub_aggregation")
	}
	baseName, ok := strings.CutSuffix(rest, "_aggregation")
	return baseName, ok && baseName != ""
}

func buildAggregation(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error) {
	// Re-parse the variant off the full name.
	aggName := "_" + baseName + "_aggregation"
	isBucket := name == aggName+"_bucket"
	depth := 0
	if !isBucket {
		for n := aggName; n != name; n += "_sub_aggregation" {
			if !strings.HasPrefix(name, n) {
				return nil, nil
			}
			depth++
			if depth > maxSubAggDepth {
				return nil, nil
			}
		}
	}

	row, err := g.readDataObject(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if row != nil {
		switch {
		case isBucket:
			return buildObjectAggBucket(ctx, g, row, name)
		case depth == 0:
			return buildObjectAggregation(ctx, g, row, name)
		default:
			return buildObjectSubAggregation(ctx, g, row, name, depth)
		}
	}
	td, ds, err := g.structType(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if td != nil {
		switch {
		case isBucket:
			return nil, nil // structural types take no bucket aggregation
		case depth == 0:
			return buildStructAggregation(ctx, g, td, ds, name)
		case depth == 1:
			return buildStructSubAggregation(ctx, g, td, ds, name)
		default:
			return nil, nil // struct sub-aggs stop at depth 1
		}
	}
	return nil, nil
}

const maxSubAggDepth = 2

func aggregationMarker(baseName string, isBucket bool, level int) *ast.Directive {
	return directive(base.ObjectAggregationDirectiveName,
		strArg(base.ArgName, baseName),
		boolArg(base.ArgIsBucket, isBucket),
		&ast.Argument{Name: "level", Value: &ast.Value{Raw: strconv.Itoa(level), Kind: ast.IntValue}, Position: reconPos},
	)
}

// scalarFieldArguments is setScalarFieldArguments applied on read: the
// compiled base field of a FieldArgumentsProvider scalar carries the scalar's
// argument list (bucket for Timestamp, transforms for Geometry, …).
func scalarFieldArguments(s types.ScalarType) ast.ArgumentDefinitionList {
	if fap, ok := s.(types.FieldArgumentsProvider); ok {
		return fap.FieldArguments()
	}
	return nil
}

// buildObjectAggregation assembles the FINAL `_X_aggregation` for a data
// object: _rows_count + scalar twins + structural members + virtual @join
// list twins + relation members + ExtraFieldProvider twins.
func buildObjectAggregation(ctx context.Context, g *genContext, row *dataObject, name string) (*ast.Definition, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			aggregationMarker(row.Name, false, 1),
			catalogDirective(row.DataSource, srcs[row.DataSource].Engine),
		},
		Fields: ast.FieldList{
			{Name: "_rows_count", Type: ast.NamedType("BigInt", reconPos), Position: reconPos},
		},
	}

	fields, err := g.readFields(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		typ := parseFieldType(f.FieldType)
		typeName := typ.Name()
		isList := typ.NamedType == ""
		if f.Name == "_stub" {
			continue
		}
		if f.DependencyDataSource != "" && !isVirtualStoreField(f) {
			continue // plain extension fields join no derived types
		}
		if s := types.Lookup(typeName); s != nil {
			// Scalar twin — the compiler takes every non-list Aggregatable
			// scalar here, virtual (@function_call) fields included;
			// extension-contributed ones keep their @dependency marker.
			if isList {
				continue
			}
			a, ok := s.(types.Aggregatable)
			if !ok {
				continue
			}
			args := scalarFieldArguments(s)
			if extra := cubeHypertableArgs(row, f); extra != nil {
				args = append(args, extra)
			}
			dirs := ast.DirectiveList{}
			if f.DependencyDataSource != "" {
				dirs = append(dirs, dependencyDirective(f.DependencyDataSource))
			}
			dirs = append(dirs, fieldAggregationMarker(f.Name))
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType(a.AggregationTypeName(), reconPos),
				Arguments:  args,
				Directives: dirs,
				Position:   reconPos,
			})
			continue
		}
		if isList || isVirtualStoreField(f) {
			continue
		}
		if exists, err := g.dataObjectExists(ctx, typeName); err != nil {
			return nil, err
		} else if exists {
			continue
		}
		if td, _, err := g.structType(ctx, typeName); err != nil {
			return nil, err
		} else if td != nil {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType("_"+typeName+"_aggregation", reconPos),
				Directives: ast.DirectiveList{fieldAggregationMarker(f.Name)},
				Position:   reconPos,
			})
		}
	}

	// Virtual object-list twins (addVirtualFieldAggregations): @join pairs
	// use the agg-ref profiles, TFCJ pairs reuse the DECLARED call arguments.
	// A cross-source @join target generates nothing (bare declared field).
	for _, f := range fields {
		j := f.Properties
		if j == nil || (j.Join == nil && j.TableFunctionCallJoin == nil) {
			continue
		}
		typ := parseFieldType(f.FieldType)
		target := typ.Name()
		if typ.NamedType != "" {
			continue
		}
		if j.Join != nil {
			if _, ok, err := g.joinMachineryTarget(ctx, f); err != nil {
				return nil, err
			} else if !ok {
				continue
			}
		} else if exists, err := g.dataObjectExists(ctx, target); err != nil {
			return nil, err
		} else if !exists {
			continue
		}
		targetFilter := target + filterSuffix
		// A twin over a parameterized view runs that view, so it takes the
		// view's args exactly as the field it mirrors does.
		targetView, err := g.viewArgsOf(ctx, target)
		if err != nil {
			return nil, err
		}
		directArgs := rules.PrependViewArgs(targetView, rules.AggRefArgs(targetFilter, reconPos), reconPos)
		subArgs := rules.PrependViewArgs(targetView, rules.AggSubRefArgs(targetFilter, reconPos), reconPos)
		if j.TableFunctionCallJoin != nil {
			directArgs = emitArgumentDefs(f.Args)
			subArgs = emitArgumentDefs(f.Args)
		}
		memberDirs := func() ast.DirectiveList {
			var out ast.DirectiveList
			if f.DependencyDataSource != "" {
				out = append(out, dependencyDirective(f.DependencyDataSource))
			}
			return append(out, fieldAggregationMarker(f.Name))
		}
		def.Fields = append(def.Fields,
			&ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType("_"+target+"_aggregation", reconPos),
				Arguments:  directArgs,
				Directives: memberDirs(),
				Position:   reconPos,
			},
			&ast.FieldDefinition{
				Name:       f.Name + "_aggregation",
				Type:       ast.NamedType(rules.AggTypeNameAtDepth(target, 1), reconPos),
				Arguments:  subArgs,
				Directives: memberDirs(),
				Position:   reconPos,
			},
		)
	}

	if err := appendRelationAggFields(ctx, g, row, def, 0); err != nil {
		return nil, err
	}
	appendExtraAggFields(fields, def)

	// @embeddings objects aggregate the query distance too (EmbeddingsRule).
	if row.Properties != nil && row.Properties.Embeddings != nil {
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name: "_distance_to_query",
			Type: ast.NamedType("FloatAggregation", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "query", Type: ast.NonNullNamedType("String", reconPos), Position: reconPos},
			},
			Directives: ast.DirectiveList{fieldAggregationMarker("_distance_to_query")},
			Position:   reconPos,
		})
	}

	// Every non-M2M data object's aggregation carries the shared cross-source
	// `_join` member — and `_spatial` when the object has a Geometry field
	// (JoinSpatialRule); their args are skipped by the sub-agg mapping, so
	// they stay base-level-only members. On an extension-owned object they
	// carry @dependency like every generated member.
	if row.Properties == nil || !row.Properties.IsM2M {
		sharedDirs := func(name string) ast.DirectiveList {
			var out ast.DirectiveList
			if srcs[row.DataSource].IsExtension {
				out = append(out, dependencyDirective(row.DataSource))
			}
			return append(out, fieldAggregationMarker(name))
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name: "_join",
			Type: ast.NamedType("_join_aggregation", reconPos),
			Arguments: ast.ArgumentDefinitionList{
				{Name: "fields", Type: ast.NonNullListType(ast.NonNullNamedType("String", reconPos), reconPos), Position: reconPos},
			},
			Directives: sharedDirs("_join"),
			Position:   reconPos,
		})
		if fieldsHaveGeometry(fields) {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       "_spatial",
				Type:       ast.NamedType("_spatial_aggregation", reconPos),
				Arguments:  spatialFieldArgs(),
				Directives: sharedDirs("_spatial"),
				Position:   reconPos,
			})
		}
	}
	return def, nil
}

func fieldsHaveGeometry(fields []*field) bool {
	for _, f := range fields {
		if parseFieldType(f.FieldType).Name() == "Geometry" {
			return true
		}
	}
	return false
}

// spatialFieldArgs is the argument set of the `_spatial` members on data
// objects and their aggregations (JoinSpatialRule).
func spatialFieldArgs() ast.ArgumentDefinitionList {
	return ast.ArgumentDefinitionList{
		{Name: "field", Type: ast.NonNullNamedType("String", reconPos), Position: reconPos},
		{Name: "type", Type: ast.NonNullNamedType("GeometrySpatialQueryType", reconPos), Position: reconPos},
		{Name: "buffer", Type: ast.NamedType("Int", reconPos), Position: reconPos},
	}
}

// appendRelationAggFields mirrors addRefToAggAtDepth for one depth: forward
// FK single members (depth 0 only), list members (BACK/M2M) with their
// `_aggregation` sub-member one level deeper.
func appendRelationAggFields(ctx context.Context, g *genContext, row *dataObject, def *ast.Definition, depth int) error {
	rels, err := g.relations(ctx, row.Name)
	if err != nil {
		return err
	}
	for _, r := range rels {
		if r.Kind == catalog.RelationJoin {
			continue // @join members come from the field bags
		}
		if row.Properties != nil && row.Properties.IsM2M &&
			r.Kind == catalog.RelationFK && r.Direction == catalog.RelationForward {
			continue
		}
		isList := r.Kind == catalog.RelationM2M || r.Direction == catalog.RelationBack
		if !isList {
			if depth > 0 {
				continue // single refs live on the base aggregation only
			}
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name: r.FieldName,
				Type: ast.NamedType("_"+r.DataObject+"_aggregation", reconPos),
				Arguments: ast.ArgumentDefinitionList{
					{Name: "inner", Description: base.DescInnerJoinRef, Type: ast.NamedType("Boolean", reconPos), Position: reconPos},
				},
				Directives: ast.DirectiveList{fieldAggregationMarker(r.FieldName)},
				Position:   reconPos,
			})
			continue
		}
		targetFilter := r.DataObject + filterSuffix
		targetView, err := g.viewArgsOf(ctx, r.DataObject)
		if err != nil {
			return err
		}
		subMarker := r.FieldName
		if depth > 0 {
			subMarker = r.FieldName + "_aggregation"
		}
		def.Fields = append(def.Fields,
			&ast.FieldDefinition{
				Name:       r.FieldName,
				Type:       ast.NamedType(rules.AggTypeNameAtDepth(r.DataObject, depth), reconPos),
				Arguments:  rules.PrependViewArgs(targetView, rules.AggRefArgs(targetFilter, reconPos), reconPos),
				Directives: ast.DirectiveList{fieldAggregationMarker(r.FieldName)},
				Position:   reconPos,
			},
			&ast.FieldDefinition{
				Name:       r.FieldName + "_aggregation",
				Type:       ast.NamedType(rules.AggTypeNameAtDepth(r.DataObject, depth+1), reconPos),
				Arguments:  rules.PrependViewArgs(targetView, rules.AggSubRefArgs(targetFilter, reconPos), reconPos),
				Directives: ast.DirectiveList{fieldAggregationMarker(subMarker)},
				Position:   reconPos,
			},
		)
	}
	return nil
}

// appendExtraAggFields adds ExtraFieldProvider twins with the extra field's
// Aggregatable type. Base aggregation only — sub-aggregations map extras via
// the scalar field mapping (buildObjectSubAggregation).
func appendExtraAggFields(fields []*field, def *ast.Definition) {
	for _, f := range fields {
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
		extraField := efp.GenerateExtraField(f.Name)
		if extraField == nil {
			continue
		}
		retScalar := types.Lookup(extraField.Type.Name())
		if retScalar == nil {
			continue
		}
		a, ok := retScalar.(types.Aggregatable)
		if !ok {
			continue
		}
		typeName := a.AggregationTypeName()
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:       extraField.Name,
			Type:       ast.NamedType(typeName, reconPos),
			Arguments:  extraField.Arguments,
			Directives: ast.DirectiveList{fieldAggregationMarker(extraField.Name)},
			Position:   reconPos,
		})
	}
}

// buildObjectAggBucket is the literal bucket shape: key + aggregations.
func buildObjectAggBucket(ctx context.Context, g *genContext, row *dataObject, name string) (*ast.Definition, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	return &ast.Definition{
		Kind:        ast.Object,
		Name:        name,
		Description: "Bucket aggregation for " + row.Name,
		Position:    reconPos,
		Directives: ast.DirectiveList{
			aggregationMarker(row.Name, true, 1),
			catalogDirective(row.DataSource, srcs[row.DataSource].Engine),
		},
		Fields: ast.FieldList{
			{
				Name:        "key",
				Description: base.DescBucketKey,
				Type:        ast.NamedType(row.Name, reconPos),
				Position:    reconPos,
			},
			{
				Name:        "aggregations",
				Description: "The aggregations of the bucket",
				Type:        ast.NamedType("_"+row.Name+"_aggregation", reconPos),
				Arguments: ast.ArgumentDefinitionList{
					{Name: "filter", Description: base.DescFilter, Type: ast.NamedType(row.Name+filterSuffix, reconPos), Position: reconPos},
					{Name: "order_by", Description: base.DescOrderBy, Type: ast.ListType(ast.NamedType("OrderByField", reconPos), reconPos), Position: reconPos},
				},
				Position: reconPos,
			},
		},
	}, nil
}

// buildObjectSubAggregation maps the base aggregation into its depth-1 or
// depth-2 variant (buildSubAggregationType): scalar members flip to
// SubAggregation types (directives + args copied), structural members to
// their _sub_aggregation, relation list members re-instantiate one level
// deeper; depth 2 keeps only _rows_count.
func buildObjectSubAggregation(ctx context.Context, g *genContext, row *dataObject, name string, depth int) (*ast.Definition, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	level := depth + 1
	def := &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			aggregationMarker(rules.AggTypeNameAtDepth(row.Name, depth-1), false, level),
			catalogDirective(row.DataSource, srcs[row.DataSource].Engine),
		},
	}
	rowsCountType := "BigIntAggregation"
	if depth >= maxSubAggDepth {
		rowsCountType = "BigIntSubAggregation"
	}
	def.Fields = append(def.Fields, &ast.FieldDefinition{
		Name:       "_rows_count",
		Type:       ast.NamedType(rowsCountType, reconPos),
		Directives: ast.DirectiveList{fieldAggregationMarker("aggregation_field")},
		Position:   reconPos,
	})
	if depth >= maxSubAggDepth {
		return def, nil
	}

	baseAgg, err := buildObjectAggregation(ctx, g, row, "_"+row.Name+"_aggregation")
	if err != nil {
		return nil, err
	}
	// Virtual @join/TFCJ twin members never reach the sub-aggregations — the
	// compiler drops BOTH the direct member and the `_aggregation` twin at
	// sub level (args-ful ones fall out of the generic mapping anyway; the
	// set closes the argless extension-contributed case).
	fields, err := g.readFields(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	virtualTwins := map[string]bool{}
	for _, f := range fields {
		if f.Properties == nil || (f.Properties.Join == nil && f.Properties.TableFunctionCallJoin == nil) {
			continue
		}
		if parseFieldType(f.FieldType).NamedType != "" {
			continue
		}
		virtualTwins[f.Name] = true
		virtualTwins[f.Name+"_aggregation"] = true
	}
	for _, f := range baseAgg.Fields {
		if f.Name == "_rows_count" {
			continue
		}
		if f.Name == "_distance_to_query" {
			// EmbeddingsRule extends the base aggregation only — the member
			// never reaches the sub-aggregations.
			continue
		}
		if f.Directives.ForName(base.DependencyDirectiveName) != nil {
			// Extension-contributed members stay base-level-only.
			continue
		}
		if virtualTwins[f.Name] {
			continue
		}
		if subType := types.SubAggregationTypeName(f.Type.Name()); subType != "" {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType(subType, reconPos),
				Directives: f.Directives,
				Arguments:  f.Arguments,
				Position:   reconPos,
			})
		} else if len(f.Arguments) == 0 {
			// Structural member by convention → its sub-aggregation type.
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType(f.Type.Name()+"_sub_aggregation", reconPos),
				Directives: f.Directives,
				Position:   reconPos,
			})
		}
		// Members with args (relation twins) re-instantiate below.
	}
	// Extra members flip through the scalar mapping above (their base twins
	// are Aggregatable-typed), so no separate pass is needed here.
	if err := appendRelationAggFields(ctx, g, row, def, depth); err != nil {
		return nil, err
	}
	return def, nil
}

// buildStructAggregation mirrors generateAggregationType on a structural
// type: _rows_count + scalar twins (with scalar args) + nested struct members.
func buildStructAggregation(ctx context.Context, g *genContext, td *ast.Definition, dataSource, name string) (*ast.Definition, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			aggregationMarker(td.Name, false, 1),
			catalogDirective(dataSource, srcs[dataSource].Engine),
		},
		Fields: ast.FieldList{
			{Name: "_rows_count", Type: ast.NamedType("BigInt", reconPos), Position: reconPos},
		},
	}
	for _, f := range td.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		isList := f.Type.NamedType == ""
		if s := types.Lookup(typeName); s != nil {
			if isList {
				continue
			}
			a, ok := s.(types.Aggregatable)
			if !ok {
				continue
			}
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType(a.AggregationTypeName(), reconPos),
				Arguments:  scalarFieldArguments(s),
				Directives: ast.DirectiveList{fieldAggregationMarker(f.Name)},
				Position:   reconPos,
			})
			continue
		}
		if isList {
			continue
		}
		if exists, err := g.dataObjectExists(ctx, typeName); err != nil {
			return nil, err
		} else if exists {
			continue
		}
		if nested, _, err := g.structType(ctx, typeName); err != nil {
			return nil, err
		} else if nested != nil {
			def.Fields = append(def.Fields, &ast.FieldDefinition{
				Name:       f.Name,
				Type:       ast.NamedType("_"+typeName+"_aggregation", reconPos),
				Directives: ast.DirectiveList{fieldAggregationMarker(f.Name)},
				Position:   reconPos,
			})
		}
	}
	return def, nil
}

// buildStructSubAggregation mirrors generateStructSubAggType: scalar members
// flip to SubAggregation variants (directives copied, args dropped), nested
// struct members are omitted.
func buildStructSubAggregation(ctx context.Context, g *genContext, td *ast.Definition, dataSource, name string) (*ast.Definition, error) {
	aggName := "_" + td.Name + "_aggregation"
	baseAgg, err := buildStructAggregation(ctx, g, td, dataSource, aggName)
	if err != nil {
		return nil, err
	}
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			aggregationMarker(aggName, false, 2),
			catalogDirective(dataSource, srcs[dataSource].Engine),
		},
		Fields: ast.FieldList{
			{
				Name:       "_rows_count",
				Type:       ast.NamedType("BigIntAggregation", reconPos),
				Directives: ast.DirectiveList{fieldAggregationMarker("aggregation_field")},
				Position:   reconPos,
			},
		},
	}
	for _, f := range baseAgg.Fields {
		if f.Name == "_rows_count" {
			continue
		}
		subType := types.SubAggregationTypeName(f.Type.Name())
		if subType == "" {
			continue
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:       f.Name,
			Type:       ast.NamedType(subType, reconPos),
			Directives: f.Directives,
			Position:   reconPos,
		})
	}
	return def, nil
}

func fieldAggregationMarker(name string) *ast.Directive {
	return directive(base.ObjectFieldAggregationDirectiveName, strArg(base.ArgName, name))
}
