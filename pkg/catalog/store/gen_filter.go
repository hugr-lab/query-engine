package store

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Derived rules for the filter family (Ш4.1): X_filter / X_list_filter,
// mirroring the compiler's generateFilterInput / generateNestedFilterInput /
// generateListFilterInput (gen_table.go) + the nav fields gen_references.go
// adds through extensions.

const (
	filterSuffix     = "_filter"
	listFilterSuffix = "_list_filter"
)

func matchSuffix(suffix string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		baseName, ok := strings.CutSuffix(name, suffix)
		return baseName, ok && baseName != ""
	}
}

var filterRule = derivedRule{
	name:  "filter",
	match: matchSuffix(filterSuffix),
	build: buildFilter,
}

var listFilterRule = derivedRule{
	name:  "list_filter",
	match: matchSuffix(listFilterSuffix),
	build: buildListFilter,
}

// buildFilter serves X_filter for a data object (full shape: scalars with
// directive copies, structural nesting, nav fields from relations, logical
// ops) or for a structural type (scalar members + logical ops only).
func buildFilter(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error) {
	row, err := g.readDataObject(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if row != nil {
		return buildObjectFilter(ctx, g, row, name)
	}
	td, ds, err := g.structType(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if td != nil {
		return buildStructFilter(ctx, g, td, ds, name)
	}
	return nil, nil
}

func buildObjectFilter(ctx context.Context, g *genContext, row *dataObject, name string) (*ast.Definition, error) {
	engines, err := g.activeEngines(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:        ast.InputObject,
		Name:        name,
		Description: "Filter for " + row.Name + " objects",
		Position:    reconPos,
		Directives: ast.DirectiveList{
			directive(base.FilterInputDirectiveName, strArg(base.ArgName, row.Name)),
			catalogDirective(row.DataSource, engines[row.DataSource]),
		},
	}

	fields, err := g.readFields(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
		fd, err := filterFieldFor(ctx, g, f)
		if err != nil {
			return nil, err
		}
		if fd != nil {
			def.Fields = append(def.Fields, fd)
		}
	}

	// Nav fields from the relation projections. The compiler adds them via
	// extensions in gen_references.go: forward FK → single filter, back FK and
	// M2M → list filter; parameterized-view targets are excluded; an is_m2m
	// junction contributes no forward nav (its @references processing stops at
	// the endpoint synthesis).
	rels, err := g.relations(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	for _, r := range rels {
		if r.Kind == catalog.RelationJoin {
			continue
		}
		if row.Properties != nil && row.Properties.IsM2M &&
			r.Kind == catalog.RelationFK && r.Direction == catalog.RelationForward {
			continue
		}
		target, err := g.readDataObject(ctx, r.DataObject)
		if err != nil {
			return nil, err
		}
		if target == nil {
			continue
		}
		if target.Properties != nil && target.Properties.ArgsTypeName != "" {
			continue // parameterized views are excluded from filter nesting
		}
		filterType := r.DataObject + filterSuffix
		if r.Kind == catalog.RelationM2M || r.Direction == catalog.RelationBack {
			filterType = r.DataObject + listFilterSuffix
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:     r.FieldName,
			Type:     ast.NamedType(filterType, reconPos),
			Position: reconPos,
			Directives: ast.DirectiveList{
				relationQueryDirective(r),
				catalogDirective(r.DataSource, engines[r.DataSource]),
			},
		})
	}

	// Field-declared legs are mirrored onto the matching scalar filter field
	// as @field_references (gen_references.go: addFieldReferencesToFilter).
	legs, err := g.relationsBySource(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	for _, rel := range legs {
		if !rel.FieldDeclared || len(rel.SourceKeys) != 1 {
			continue
		}
		if fd := def.Fields.ForName(rel.SourceKeys[0]); fd != nil {
			fd.Directives = append(fd.Directives, fieldReferencesDirective(rel))
		}
	}

	appendFilterLogicalOps(def, name)
	return def, nil
}

// filterFieldFor maps one stored field into its filter member (nil = not
// filterable): scalars via the Filterable registry, structural members via
// the nested filter names; _stub and virtual fields are skipped.
func filterFieldFor(ctx context.Context, g *genContext, f *field) (*ast.FieldDefinition, error) {
	if f.Name == "_stub" || isVirtualStoreField(f) {
		return nil, nil
	}
	if f.DependencyDataSource != "" {
		// The compiler's extension path extends ONLY the object definition —
		// extension fields join none of the derived types; mirrored here.
		return nil, nil
	}
	typ := parseFieldType(f.FieldType)
	typeName := typ.Name()
	isList := typ.NamedType == ""

	if s := types.Lookup(typeName); s != nil {
		fi, ok := s.(types.Filterable)
		if !ok {
			return nil, nil
		}
		filterType := fi.FilterTypeName()
		if isList {
			if lfi, ok := s.(types.ListFilterable); ok {
				filterType = lfi.ListFilterTypeName()
			}
		}
		fd := &ast.FieldDefinition{
			Name:        f.Name,
			Description: f.Description,
			Type:        ast.NamedType(filterType, reconPos),
			Position:    reconPos,
		}
		for _, d := range reconstructField(f).Directives {
			switch d.Name {
			case base.FieldPrimaryKeyDirectiveName, base.FieldDefaultDirectiveName,
				base.FieldMeasurementDirectiveName, base.FieldTimescaleKeyDirectiveName:
				fd.Directives = append(fd.Directives, d)
			}
		}
		if f.Properties != nil && f.Properties.FilterRequired {
			fd.Type.NonNull = true
		}
		return fd, nil
	}

	// Structural member: nested filter by name (the struct's own filter is a
	// derived name too). Data objects and non-Object residual types (enums,
	// inputs) contribute no filter member.
	if exists, err := g.dataObjectExists(ctx, typeName); err != nil || exists {
		return nil, err
	}
	if td, _, err := g.structType(ctx, typeName); err != nil || td == nil {
		return nil, err
	}
	filterType := typeName + filterSuffix
	if isList {
		filterType = typeName + listFilterSuffix
	}
	return &ast.FieldDefinition{
		Name:     f.Name,
		Type:     ast.NamedType(filterType, reconPos),
		Position: reconPos,
	}, nil
}

// buildStructFilter mirrors generateNestedFilterInput: scalar members only,
// no directive copies, logical ops.
func buildStructFilter(ctx context.Context, g *genContext, td *ast.Definition, dataSource, name string) (*ast.Definition, error) {
	engines, err := g.activeEngines(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			directive(base.FilterInputDirectiveName, strArg(base.ArgName, td.Name)),
			catalogDirective(dataSource, engines[dataSource]),
		},
	}
	for _, f := range td.Fields {
		if f.Name == "_stub" {
			continue
		}
		s := types.Lookup(f.Type.Name())
		if s == nil {
			continue
		}
		fi, ok := s.(types.Filterable)
		if !ok {
			continue
		}
		filterType := fi.FilterTypeName()
		if f.Type.NamedType == "" {
			if lfi, ok := s.(types.ListFilterable); ok {
				filterType = lfi.ListFilterTypeName()
			}
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     ast.NamedType(filterType, reconPos),
			Position: reconPos,
		})
	}
	appendFilterLogicalOps(def, name)
	return def, nil
}

// buildListFilter serves X_list_filter (any_of / all_of / none_of over
// X_filter) for any base that has a filter: a data object or a structural
// type. The compiler creates these lazily on first use; serving them for
// every filterable base is a superset — nothing references the extra names.
func buildListFilter(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error) {
	var dataSource string
	row, err := g.readDataObject(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if row != nil {
		dataSource = row.DataSource
	} else if td, ds, err := g.structType(ctx, baseName); err != nil {
		return nil, err
	} else if td != nil {
		dataSource = ds
	} else {
		return nil, nil
	}
	engines, err := g.activeEngines(ctx)
	if err != nil {
		return nil, err
	}
	filterName := baseName + filterSuffix
	return &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			directive(base.FilterListInputDirectiveName, strArg(base.ArgName, baseName)),
			catalogDirective(dataSource, engines[dataSource]),
		},
		Fields: ast.FieldList{
			{Name: "any_of", Type: ast.NamedType(filterName, reconPos), Position: reconPos},
			{Name: "all_of", Type: ast.NamedType(filterName, reconPos), Position: reconPos},
			{Name: "none_of", Type: ast.NamedType(filterName, reconPos), Position: reconPos},
		},
	}, nil
}

func appendFilterLogicalOps(def *ast.Definition, name string) {
	def.Fields = append(def.Fields,
		&ast.FieldDefinition{Name: "_and", Type: ast.ListType(ast.NamedType(name, reconPos), reconPos), Position: reconPos},
		&ast.FieldDefinition{Name: "_or", Type: ast.ListType(ast.NamedType(name, reconPos), reconPos), Position: reconPos},
		&ast.FieldDefinition{Name: "_not", Type: ast.NamedType(name, reconPos), Position: reconPos},
	)
}

// structType resolves a STRUCTURAL residual type: a stored Object definition
// that is not a table/view (those are data objects). Returns the definition
// (without @catalog) and its owning data source.
func (g *genContext) structType(ctx context.Context, name string) (*ast.Definition, string, error) {
	td, ds, err := g.readType(ctx, name)
	if err != nil {
		return nil, "", err
	}
	if td == nil || td.Kind != ast.Object {
		return nil, "", nil
	}
	return td, ds, nil
}

// isVirtualStoreField reports a declared @join / @function_call /
// @table_function_call_join field (the compiler's isVirtualField).
func isVirtualStoreField(f *field) bool {
	return f.Properties != nil &&
		(f.Properties.Join != nil || f.Properties.FunctionCall != nil || f.Properties.TableFunctionCallJoin != nil)
}

// relationQueryDirective is the @references_query marker the compiler puts on
// nav fields (referencesQueryDirective in gen_references.go): the reference
// target, the relation name, and the m2m projection (m2m_name = junction).
func relationQueryDirective(r *catalog.RelationInfo) *ast.Directive {
	return directive(base.FieldReferencesQueryDirectiveName,
		strArg(base.ArgReferencesName, r.DataObject),
		strArg(base.ArgName, r.Name),
		boolArg(base.ArgIsM2M, r.Kind == catalog.RelationM2M),
		strArg(base.ArgM2MName, r.Through),
	)
}

// fieldReferencesDirective re-emits a field-declared leg as @field_references
// (the compiler copies the source directive onto the filter field verbatim;
// the store emits the declared argument set — the relation name only when the
// user overrode the default `<references_name>_<field>` pattern).
func fieldReferencesDirective(rel *relation) *ast.Directive {
	d := directive(base.FieldReferencesDirectiveName,
		strArg(base.ArgReferencesName, rel.Destination))
	// Arguments the user left to their DEFAULTS are omitted — the compiler
	// copies the directive as written (name → `<references_name>_<field>`,
	// query → references_name, references_query → the declaring object).
	if len(rel.SourceKeys) == 1 && rel.Name != rel.Destination+"_"+rel.SourceKeys[0] {
		d.Arguments = append(d.Arguments, strArg(base.ArgName, rel.Name))
	}
	if len(rel.DestinationKeys) == 1 {
		d.Arguments = append(d.Arguments, strArg(base.ArgField, rel.DestinationKeys[0]))
	}
	if rel.SourceField != "" && rel.SourceField != rel.Destination {
		d.Arguments = append(d.Arguments, strArg(base.ArgQuery, rel.SourceField))
	}
	// An explicit references_query: "" is part of the declared set and
	// re-emits verbatim (it does NOT suppress the back navigation —
	// generation treats "" as the default).
	if rel.DestinationField != rel.Source {
		d.Arguments = append(d.Arguments, strArg(base.ArgReferencesQuery, rel.DestinationField))
	}
	if rel.SourceFieldDescription != "" {
		d.Arguments = append(d.Arguments, strArg(base.ArgDescription, rel.SourceFieldDescription))
	}
	if rel.DestinationFieldDescription != "" {
		d.Arguments = append(d.Arguments, strArg(base.ArgReferencesDescription, rel.DestinationFieldDescription))
	}
	return d
}
