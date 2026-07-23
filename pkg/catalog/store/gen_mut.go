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

func buildMutInputData(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error) {
	row, err := g.readDataObject(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if row != nil {
		def, err := buildObjectMutInput(ctx, g, row, name, mutInputDataSuffix)
		if err != nil || def == nil {
			return nil, err
		}
		if err := appendInsertRelationFields(ctx, g, row, def); err != nil {
			return nil, err
		}
		return def, nil
	}
	td, ds, err := g.structType(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if td != nil {
		return buildStructMutInput(ctx, g, td, ds, name, mutInputDataSuffix)
	}
	return nil, nil
}

func buildMutData(ctx context.Context, g *genContext, baseName, name string) (*ast.Definition, error) {
	row, err := g.readDataObject(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if row != nil {
		return buildObjectMutInput(ctx, g, row, name, mutDataSuffix)
	}
	td, ds, err := g.structType(ctx, baseName)
	if err != nil {
		return nil, err
	}
	if td != nil {
		return buildStructMutInput(ctx, g, td, ds, name, mutDataSuffix)
	}
	return nil, nil
}

// buildObjectMutInput builds the column part of a table's mutation input:
// stored fields minus _stub / @sql computed / virtual; scalar members drop
// their null constraints, structural members nest by suffix, data-object
// typed members are skipped (relations own them).
func buildObjectMutInput(ctx context.Context, g *genContext, row *dataObject, name, suffix string) (*ast.Definition, error) {
	if row.Kind != "table" {
		return nil, nil // views can't be mutated
	}
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	if srcs[row.DataSource].ReadOnly {
		return nil, nil
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
	fields, err := g.readFields(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	for _, f := range fields {
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
			if exists, err := g.dataObjectExists(ctx, typeName); err != nil {
				return nil, err
			} else if exists {
				continue // table/view references are handled as relations
			}
			if td, _, err := g.structType(ctx, typeName); err != nil {
				return nil, err
			} else if td != nil {
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
	return def, nil
}

// appendInsertRelationFields adds the relation subqueries the compiler puts
// on insert inputs (addReferenceToMutInput): forward FK single, back FK and
// M2M list — same catalog only, both endpoints writable tables. An is_m2m
// junction contributes no forward fields of its own.
func appendInsertRelationFields(ctx context.Context, g *genContext, row *dataObject, def *ast.Definition) error {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return err
	}
	rels, err := g.relations(ctx, row.Name)
	if err != nil {
		return err
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
			return err
		}
		if target == nil || target.Kind != "table" || srcs[target.DataSource].ReadOnly {
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
	return nil
}

// buildStructMutInput mirrors generateNestedMutInputData / generateNestedMutData:
// every member except _stub, Object-typed members nest by suffix.
func buildStructMutInput(ctx context.Context, g *genContext, td *ast.Definition, dataSource, name, suffix string) (*ast.Definition, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:     ast.InputObject,
		Name:     name,
		Position: reconPos,
		Directives: ast.DirectiveList{
			directive(base.DataInputDirectiveName, strArg(base.ArgName, td.Name)),
			catalogDirective(dataSource, srcs[dataSource].Engine),
		},
	}
	for _, f := range td.Fields {
		if f.Name == "_stub" {
			continue
		}
		typeName := f.Type.Name()
		fieldType := ast.NamedType(typeName, reconPos)
		if types.Lookup(typeName) == nil {
			exists, err := g.dataObjectExists(ctx, typeName)
			if err != nil {
				return nil, err
			}
			if exists {
				fieldType = ast.NamedType(typeName+suffix, reconPos)
			} else if td2, _, err := g.structType(ctx, typeName); err != nil {
				return nil, err
			} else if td2 != nil {
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
	return def, nil
}
