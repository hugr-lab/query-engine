package store

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// Derived rules for the mutation-input family (Ш4.2): X_mut_input_data
// (insert) / X_mut_data (update), mirroring generateMutInputData /
// generateMutData / generateNestedMut* (gen_table.go) + the insert relation
// subqueries gen_references.go adds through extensions. Only TABLES of
// non-read-only sources get mutation inputs; views never do.

const (
	mutInputDataSuffix = "_mut_input_data"
	mutDataSuffix      = "_mut_data"
)

var mutInputDataRule = derivedRule{
	name:  "mut_input_data",
	match: matchSuffix(mutInputDataSuffix),
	build: buildMutInputData,
}

var mutDataRule = derivedRule{
	name:  "mut_data",
	match: matchSuffix(mutDataSuffix),
	build: buildMutData,
}

func buildMutInputData(ctx context.Context, g *genContext, baseName, name string) *ast.Definition {
	if row, ok := g.s.readDataObject(ctx, baseName); ok {
		def := buildObjectMutInput(ctx, g, row, name, mutInputDataSuffix)
		if def == nil {
			return nil
		}
		appendInsertRelationFields(ctx, g, row, def)
		return def
	}
	if td, ds := g.structType(ctx, baseName); td != nil {
		return buildStructMutInput(ctx, g, td, ds, name, mutInputDataSuffix)
	}
	return nil
}

func buildMutData(ctx context.Context, g *genContext, baseName, name string) *ast.Definition {
	if row, ok := g.s.readDataObject(ctx, baseName); ok {
		return buildObjectMutInput(ctx, g, row, name, mutDataSuffix)
	}
	if td, ds := g.structType(ctx, baseName); td != nil {
		return buildStructMutInput(ctx, g, td, ds, name, mutDataSuffix)
	}
	return nil
}

// buildObjectMutInput builds the column part of a table's mutation input:
// stored fields minus _stub / @sql computed / virtual; scalar members drop
// their null constraints, structural members nest by suffix, data-object
// typed members are skipped (relations own them).
func buildObjectMutInput(ctx context.Context, g *genContext, row *dataObject, name, suffix string) *ast.Definition {
	if row.Kind != "table" {
		return nil // views can't be mutated
	}
	srcs := g.s.activeSources(ctx)
	if srcs[row.DataSource].ReadOnly {
		return nil
	}
	def := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			directive(base.DataInputDirectiveName, strArg(base.ArgName, row.Name)),
			catalogDirective(row.DataSource, srcs[row.DataSource].Engine),
		},
	}
	for _, f := range g.s.readFields(ctx, row.Name) {
		if f.Name == "_stub" || isVirtualStoreField(f) || f.DependencyDataSource != "" {
			continue // extension fields join no derived types
		}
		if f.Properties != nil && (f.Properties.Computed || f.Properties.SQL != "") {
			continue // computed @sql fields are not writable
		}
		typ := parseFieldType(f.FieldType)
		typeName := typ.Name()
		fieldType := ast.NamedType(typeName, reconPos)
		if types.Lookup(typeName) == nil {
			if g.s.dataObjectExists(ctx, typeName) {
				continue // table/view references are handled as relations
			}
			if td, _ := g.structType(ctx, typeName); td != nil {
				fieldType = ast.NamedType(typeName+suffix, reconPos)
			}
		}
		if typ.NamedType == "" {
			fieldType = ast.ListType(fieldType, reconPos)
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: reconPos,
		})
	}
	return def
}

// appendInsertRelationFields adds the relation subqueries the compiler puts
// on insert inputs (addReferenceToMutInput): forward FK single, back FK and
// M2M list — same catalog only, both endpoints writable tables. An is_m2m
// junction contributes no forward fields of its own.
func appendInsertRelationFields(ctx context.Context, g *genContext, row *dataObject, def *ast.Definition) {
	srcs := g.s.activeSources(ctx)
	for r := range g.s.Relations(ctx, row.Name) {
		if r.Kind == catalog.RelationJoin {
			continue
		}
		if row.Properties != nil && row.Properties.IsM2M &&
			r.Kind == catalog.RelationFK && r.Direction == catalog.RelationForward {
			continue
		}
		target, ok := g.s.readDataObject(ctx, r.DataObject)
		if !ok || target.Kind != "table" || srcs[target.DataSource].ReadOnly {
			continue // the other endpoint has no mutation input
		}
		if target.DataSource != row.DataSource {
			continue // inserts only work within a single catalog
		}
		fieldType := ast.NamedType(r.DataObject+mutInputDataSuffix, reconPos)
		if r.Kind == catalog.RelationM2M || r.Direction == catalog.RelationBack {
			fieldType = ast.ListType(fieldType, reconPos)
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:     r.FieldName,
			Type:     fieldType,
			Position: reconPos,
		})
	}
}

// buildStructMutInput mirrors generateNestedMutInputData / generateNestedMutData:
// every member except _stub, Object-typed members nest by suffix.
func buildStructMutInput(ctx context.Context, g *genContext, td *ast.Definition, dataSource, name, suffix string) *ast.Definition {
	def := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			directive(base.DataInputDirectiveName, strArg(base.ArgName, td.Name)),
			catalogDirective(dataSource, g.s.activeSources(ctx)[dataSource].Engine),
		},
	}
	for _, f := range td.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, reconPos)
		if types.Lookup(typeName) == nil {
			if g.s.dataObjectExists(ctx, typeName) {
				fieldType = ast.NamedType(typeName+suffix, reconPos)
			} else if td2, _ := g.structType(ctx, typeName); td2 != nil {
				fieldType = ast.NamedType(typeName+suffix, reconPos)
			}
		}
		if f.Type.NamedType == "" {
			fieldType = ast.ListType(fieldType, reconPos)
		}
		def.Fields = append(def.Fields, &ast.FieldDefinition{
			Name:     f.Name,
			Type:     fieldType,
			Position: reconPos,
		})
	}
	return def
}
