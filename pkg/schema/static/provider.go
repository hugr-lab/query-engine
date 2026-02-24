package static

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Compile-time check that Provider implements schema.Provider.
var _ base.Provider = (*Provider)(nil)

// Provider implements schema.Provider backed by *ast.Schema.
// All lookups are O(1) via maps.
type Provider struct {
	schema *ast.Schema
}

// New creates a Provider wrapping the given compiled schema.
func New(s *ast.Schema) *Provider {
	return &Provider{schema: s}
}

func (p *Provider) ForName(_ context.Context, name string) *ast.Definition {
	return p.schema.Types[name]
}

func (p *Provider) DirectiveForName(_ context.Context, name string) *ast.DirectiveDefinition {
	return p.schema.Directives[name]
}

func (p *Provider) QueryType(_ context.Context) *ast.Definition {
	return p.schema.Query
}

func (p *Provider) MutationType(_ context.Context) *ast.Definition {
	return p.schema.Mutation
}

func (p *Provider) SubscriptionType(_ context.Context) *ast.Definition {
	return p.schema.Subscription
}

func (p *Provider) PossibleTypes(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, t := range p.schema.GetPossibleTypes(def) {
			if !yield(t) {
				return
			}
		}
	}
}

func (p *Provider) Implements(_ context.Context, name string) iter.Seq[*ast.Definition] {
	def := p.schema.Types[name]
	if def == nil {
		return nil
	}
	return func(yield func(*ast.Definition) bool) {
		for _, iface := range p.schema.GetImplements(def) {
			if !yield(iface) {
				return
			}
		}
	}
}

func (p *Provider) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, def := range p.schema.Types {
			if !yield(def) {
				return
			}
		}
	}
}

func (p *Provider) Types(_ context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for name, def := range p.schema.Types {
			if !yield(name, def) {
				return
			}
		}
	}
}

func (p *Provider) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for name, dir := range p.schema.Directives {
			if !yield(name, dir) {
				return
			}
		}
	}
}

func (p *Provider) Description(_ context.Context) string {
	return p.schema.Description
}

// Schema returns the underlying ast.Schema for direct access (e.g., comparison tests).
func (p *Provider) Schema() *ast.Schema {
	return p.schema
}

// MutableProvider implementation
var _ base.MutableProvider = (*Provider)(nil)

func (p *Provider) SetDefinitionDescription(ctx context.Context, name, desc string) error {
	def := p.schema.Types[name]
	if def == nil {
		return base.ErrDefinitionNotFound
	}
	def.Description = desc
	return nil
}

var ErrCascadeDependency = errors.New("cannot drop catalog with dependent definitions without cascade")

// Drops catalog schema objects from the provider. For static provider, this means dropping all definitions with the given catalog name.
func (p *Provider) DropCatalog(ctx context.Context, name string, cascade bool) error {
	// 1. collect definitions to drop
	toDrop := []string{}
	patriallyDropped := map[string]struct {
		fields        []string
		enumValues    []string
		ifaces        []string
		possibleTypes []string
	}{}

	for defName, def := range p.schema.Types {
		// definitions that belong to the catalog should be dropped
		if base.DefinitionCatalog(def) == name {
			toDrop = append(toDrop, defName)
			continue
		}
		// definitions that depend on the dropped catalog should be dropped as well
		if slices.Contains(base.DefinitionDependencies(def), name) {
			if !cascade {
				return fmt.Errorf("%w: catalog %s, dependent: %s", ErrCascadeDependency, name, defName)
			}
			toDrop = append(toDrop, defName)
			continue
		}
		patrial := struct {
			fields        []string
			enumValues    []string
			ifaces        []string
			possibleTypes []string
		}{}
		// Drop fields that belong to the catalog
		for _, field := range def.Fields {
			if base.FieldDefCatalog(field) == name {
				patrial.fields = append(patrial.fields, field.Name)
			}
		}

		// enum values that belong to the catalog
		for _, enumValue := range def.EnumValues {
			if base.EnumValueCatalog(enumValue) == name {
				if err := p.DropEnumValue(ctx, defName, enumValue.Name); err != nil {
					return err
				}
			}
		}

		// interface implementations that belong to the catalog
		for _, name := range def.Interfaces {
			iface := p.schema.Types[name]
			if iface == nil {
				continue
			}
			if base.DefinitionCatalog(iface) == name {
				def.Interfaces = slices.DeleteFunc(def.Interfaces, func(i string) bool {
					return i == name
				})
			}
		}

		// possible types that belong to the catalog
		for _, name := range def.Types {
			possible := p.schema.Types[name]
			if possible == nil {
				continue
			}
			if base.DefinitionCatalog(possible) == name {
				def.Types = slices.DeleteFunc(def.Types, func(i string) bool {
					return i == name
				})
			}
		}
	}
	// 2. drop definitions
	for _, defName := range toDrop {
		delete(p.schema.Types, defName)
		// drop possible types of the dropped definition
		for name, defs := range p.schema.PossibleTypes {
			p.schema.PossibleTypes[name] = slices.DeleteFunc(defs, func(d *ast.Definition) bool {
				return d.Name == defName
			})
		}
		// drop implementations of the dropped definition
		for name := range p.schema.Implements {
			p.schema.Implements[name] = slices.DeleteFunc(p.schema.Implements[name], func(d *ast.Definition) bool {
				return d.Name == defName
			})
		}
		delete(p.schema.Implements, defName)
		delete(p.schema.PossibleTypes, defName)
	}
	// patrially dropped definitions should be dropped if cascade is true
	for defName, partial := range patriallyDropped {
		def := p.schema.Types[defName]
		if def == nil {
			continue
		}
		for _, fieldName := range partial.fields {
			if err := p.DropField(ctx, defName, fieldName); err != nil {
				return err
			}
		}
		for _, enumValue := range partial.enumValues {
			if err := p.DropEnumValue(ctx, defName, enumValue); err != nil {
				return err
			}
		}
		for _, iface := range partial.ifaces {
			def.Interfaces = slices.DeleteFunc(def.Interfaces, func(i string) bool {
				return i == iface
			})
		}
		for _, possible := range partial.possibleTypes {
			def.Types = slices.DeleteFunc(def.Types, func(i string) bool {
				return i == possible
			})
		}
	}

	return nil
}


func (p *Provider) DropDefinition(ctx context.Context, name string) error {
	if _, exists := p.schema.Types[name]; exists {
		delete(p.schema.Types, name)
		return nil
	}
	return base.ErrDefinitionNotFound
}

func (p *Provider) DropField(ctx context.Context, typeName, fieldName string) error {
	def := p.schema.Types[typeName]
	if def == nil {
		return base.ErrDefinitionNotFound
	}
	if def.Fields.ForName(fieldName) == nil {
		return base.ErrDefinitionNotFound
	}
	def.Fields = slices.DeleteFunc(def.Fields, func(field *ast.FieldDefinition) bool {
		return field.Name == fieldName
	})
	return nil
}

func (p *Provider) DropEnumValue(ctx context.Context, typeName, valueName string) error {
	def := p.schema.Types[typeName]
	if def == nil {
		return base.ErrDefinitionNotFound
	}
	if def.EnumValues.ForName(valueName) == nil {
		return base.ErrDefinitionNotFound
	}
	def.EnumValues = slices.DeleteFunc(def.EnumValues, func(ev *ast.EnumValueDefinition) bool {
		return ev.Name == valueName
	})
	return nil
}
