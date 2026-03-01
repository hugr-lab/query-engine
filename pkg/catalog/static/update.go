package static

import (
	"context"
	"fmt"
	"slices"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// updateChangeset holds all validated mutations collected during the validation phase.
// Once validation passes, the changeset is applied atomically.
type updateChangeset struct {
	// definitions
	toDrop []string          // names to delete from schema.Types
	toAdd  []*ast.Definition // cloned definitions to insert

	// extensions
	extensions []extensionChangeset
}

type extensionChangeset struct {
	targetName string

	// description
	newDescription string // empty = no change

	// directives on the type itself
	directivesToDrop []base.DropDirectiveSpec
	directivesToAdd  ast.DirectiveList

	// fields
	fieldsToDrop  []string
	fieldsToAdd   []*ast.FieldDefinition // new or replaced
	fieldsToMerge []fieldMerge           // existing fields: directive/description updates

	// enum values
	enumValuesToDrop []string
	enumValuesToAdd  []*ast.EnumValueDefinition

	// interfaces / union types
	interfacesToAdd []string
	unionTypesToAdd []string
}

type fieldMerge struct {
	fieldName        string
	directivesToAdd  ast.DirectiveList
	directivesToDrop []base.DropDirectiveSpec
	newDescription   string
}

// Update applies DDL changes to the provider's schema.
// The changes source provides definitions (add/drop/replace) and optionally
// extensions (field/directive/description modifications on existing types).
func (p *Provider) Update(ctx context.Context, changes base.DefinitionsSource) error {
	ext, hasExtensions := changes.(base.ExtensionsSource)

	// Phase 1: validate — collect changeset, check all preconditions
	cs, err := p.validateChanges(ctx, changes, ext, hasExtensions)
	if err != nil {
		return err
	}

	// Phase 2: apply — mutate the schema atomically
	p.applyChangeset(cs)

	// Phase 3: update relationships — incremental, only for changed defs
	p.updateRelationships(cs)

	// Phase 4: clean up dangling references left by dropped types
	if len(cs.toDrop) > 0 {
		p.cleanupDanglingReferences()
	}

	return nil
}

// --- Phase 1: Validation ---

func (p *Provider) validateChanges(
	ctx context.Context,
	changes base.DefinitionsSource,
	ext base.ExtensionsSource,
	hasExtensions bool,
) (*updateChangeset, error) {
	cs := &updateChangeset{}

	for def := range changes.Definitions(ctx) {
		if err := p.validateDefinition(def, cs); err != nil {
			return nil, err
		}
	}

	if hasExtensions {
		for extDef := range ext.Extensions(ctx) {
			if err := p.validateExtension(extDef, cs); err != nil {
				return nil, err
			}
		}
	}

	return cs, nil
}

func (p *Provider) validateDefinition(def *ast.Definition, cs *updateChangeset) error {
	switch {
	case base.IsDropDefinition(def):
		return p.validateDropDefinition(def, cs)
	case base.IsReplaceDefinition(def):
		return p.validateReplaceDefinition(def, cs)
	case base.IsIfNotExistsDefinition(def):
		return p.validateIfNotExistsDefinition(def, cs)
	default:
		return p.validateAddDefinition(def, cs)
	}
}

func (p *Provider) validateDropDefinition(def *ast.Definition, cs *updateChangeset) error {
	if p.schema.Types[def.Name] == nil {
		if base.DropDirectiveIfExists(def) {
			return nil // silently skip
		}
		return fmt.Errorf("drop type %s: %w", def.Name, base.ErrDefinitionNotFound)
	}
	cs.toDrop = append(cs.toDrop, def.Name)
	return nil
}

func (p *Provider) validateReplaceDefinition(def *ast.Definition, cs *updateChangeset) error {
	if p.schema.Types[def.Name] == nil {
		return fmt.Errorf("replace type %s: %w", def.Name, base.ErrDefinitionNotFound)
	}
	cs.toDrop = append(cs.toDrop, def.Name)
	cloned := base.CloneDefinition(def, nil)
	cloned.Directives = base.StripControlDirectives(cloned.Directives)
	cs.toAdd = append(cs.toAdd, cloned)
	return nil
}

func (p *Provider) validateIfNotExistsDefinition(def *ast.Definition, cs *updateChangeset) error {
	if p.schema.Types[def.Name] != nil {
		return nil // already exists, skip
	}
	cloned := base.CloneDefinition(def, nil)
	cloned.Directives = base.StripControlDirectives(cloned.Directives)
	cs.toAdd = append(cs.toAdd, cloned)
	return nil
}

func (p *Provider) validateAddDefinition(def *ast.Definition, cs *updateChangeset) error {
	// Check against current schema, but also account for pending drops in this DDL
	exists := p.schema.Types[def.Name] != nil
	if exists && !slices.Contains(cs.toDrop, def.Name) {
		return fmt.Errorf("add type %s: already exists", def.Name)
	}
	cloned := base.CloneDefinition(def, nil)
	cloned.Directives = base.StripControlDirectives(cloned.Directives)
	cs.toAdd = append(cs.toAdd, cloned)
	return nil
}

func (p *Provider) validateExtension(ext *ast.Definition, cs *updateChangeset) error {
	target := p.schema.Types[ext.Name]
	if target == nil && !isInToAdd(cs, ext.Name) {
		return fmt.Errorf("extend type %s: %w", ext.Name, base.ErrDefinitionNotFound)
	}

	ec := extensionChangeset{targetName: ext.Name}

	// @drop_directive specs
	ec.directivesToDrop = base.DropDirectiveSpecs(ext)

	// non-control directives to add (with validation)
	for _, dir := range ext.Directives {
		if !base.IsControlDirective(dir.Name) {
			if err := p.validateDirectiveUsage(dir); err != nil {
				return fmt.Errorf("extend type %s: %w", ext.Name, err)
			}
			ec.directivesToAdd = append(ec.directivesToAdd, base.CloneDirective(dir))
		}
	}

	// description
	if ext.Description != "" {
		ec.newDescription = ext.Description
	}

	// fields
	for _, field := range ext.Fields {
		if err := p.validateExtensionField(target, field, &ec); err != nil {
			return fmt.Errorf("extend type %s, field %s: %w", ext.Name, field.Name, err)
		}
	}

	// enum values
	for _, ev := range ext.EnumValues {
		if err := p.validateExtensionEnumValue(target, ev, &ec); err != nil {
			return fmt.Errorf("extend type %s, enum value %s: %w", ext.Name, ev.Name, err)
		}
	}

	// interface/union additions
	ec.interfacesToAdd = ext.Interfaces
	ec.unionTypesToAdd = ext.Types

	cs.extensions = append(cs.extensions, ec)
	return nil
}

func (p *Provider) validateExtensionField(target *ast.Definition, field *ast.FieldDefinition, ec *extensionChangeset) error {
	var existing *ast.FieldDefinition
	if target != nil {
		existing = target.Fields.ForName(field.Name)
	}

	// If this field was already scheduled for drop in this extension,
	// treat subsequent operations as if the field doesn't exist (drop+add pattern).
	pendingDrop := slices.Contains(ec.fieldsToDrop, field.Name)
	if pendingDrop {
		existing = nil
	}

	switch {
	case base.IsDropField(field):
		if existing == nil {
			if base.DropFieldIfExists(field) {
				return nil // silently skip
			}
			return base.ErrDefinitionNotFound
		}
		ec.fieldsToDrop = append(ec.fieldsToDrop, field.Name)

	case base.IsReplaceField(field):
		if existing == nil {
			return base.ErrDefinitionNotFound
		}
		ec.fieldsToDrop = append(ec.fieldsToDrop, field.Name)
		cloned := base.CloneFieldDefinition(field)
		cloned.Directives = base.StripControlDirectives(cloned.Directives)
		if err := p.validateDirectives(cloned.Directives); err != nil {
			return err
		}
		ec.fieldsToAdd = append(ec.fieldsToAdd, cloned)

	case existing != nil:
		// field exists, no @drop/@replace → merge directives + update description
		merge := fieldMerge{fieldName: field.Name}
		for _, dir := range field.Directives {
			if !base.IsControlDirective(dir.Name) {
				if err := p.validateDirectiveUsage(dir); err != nil {
					return err
				}
				merge.directivesToAdd = append(merge.directivesToAdd, base.CloneDirective(dir))
			}
		}
		merge.directivesToDrop = base.FieldDropDirectiveSpecs(field)
		if field.Description != "" {
			merge.newDescription = field.Description
		}
		ec.fieldsToMerge = append(ec.fieldsToMerge, merge)

	default:
		// new field — validate non-control directives
		if err := p.validateDirectives(field.Directives); err != nil {
			return err
		}
		ec.fieldsToAdd = append(ec.fieldsToAdd, base.CloneFieldDefinition(field))
	}
	return nil
}

func (p *Provider) validateExtensionEnumValue(target *ast.Definition, ev *ast.EnumValueDefinition, ec *extensionChangeset) error {
	var existing *ast.EnumValueDefinition
	if target != nil {
		existing = target.EnumValues.ForName(ev.Name)
	}

	if base.IsDropEnumValue(ev) {
		if existing == nil {
			return base.ErrDefinitionNotFound
		}
		ec.enumValuesToDrop = append(ec.enumValuesToDrop, ev.Name)
	} else {
		if existing != nil {
			return fmt.Errorf("already exists")
		}
		ec.enumValuesToAdd = append(ec.enumValuesToAdd, &ast.EnumValueDefinition{
			Name:        ev.Name,
			Description: ev.Description,
			Directives:  base.CloneDirectiveList(ev.Directives),
		})
	}
	return nil
}

// --- Phase 2: Apply ---

func (p *Provider) applyChangeset(cs *updateChangeset) {
	// Drop definitions
	for _, name := range cs.toDrop {
		delete(p.schema.Types, name)
	}

	// Add definitions
	for _, def := range cs.toAdd {
		p.schema.Types[def.Name] = def
		// Update root type pointers when adding Query/Mutation/Subscription
		switch def.Name {
		case "Mutation":
			p.schema.Mutation = def
		case "Subscription":
			p.schema.Subscription = def
		}
	}

	// Apply extensions
	for _, ec := range cs.extensions {
		target := p.schema.Types[ec.targetName]
		if target == nil {
			continue
		}

		// Drop directives from target
		for _, spec := range ec.directivesToDrop {
			target.Directives = dropMatchingDirectives(target.Directives, spec)
		}

		// Add directives to target
		target.Directives = append(target.Directives, ec.directivesToAdd...)

		// Update description
		if ec.newDescription != "" {
			target.Description = ec.newDescription
		}

		// Drop fields
		for _, fieldName := range ec.fieldsToDrop {
			target.Fields = slices.DeleteFunc(target.Fields, func(f *ast.FieldDefinition) bool {
				return f.Name == fieldName
			})
		}

		// Add fields (new or replaced)
		target.Fields = append(target.Fields, ec.fieldsToAdd...)

		// Merge existing fields
		for _, merge := range ec.fieldsToMerge {
			field := target.Fields.ForName(merge.fieldName)
			if field == nil {
				continue
			}
			for _, spec := range merge.directivesToDrop {
				field.Directives = dropMatchingDirectives(field.Directives, spec)
			}
			field.Directives = append(field.Directives, merge.directivesToAdd...)
			if merge.newDescription != "" {
				field.Description = merge.newDescription
			}
		}

		// Drop enum values
		for _, evName := range ec.enumValuesToDrop {
			target.EnumValues = slices.DeleteFunc(target.EnumValues, func(ev *ast.EnumValueDefinition) bool {
				return ev.Name == evName
			})
		}

		// Add enum values
		target.EnumValues = append(target.EnumValues, ec.enumValuesToAdd...)

		// Add interfaces
		for _, iface := range ec.interfacesToAdd {
			if !slices.Contains(target.Interfaces, iface) {
				target.Interfaces = append(target.Interfaces, iface)
			}
		}

		// Add union types
		for _, typeName := range ec.unionTypesToAdd {
			if !slices.Contains(target.Types, typeName) {
				target.Types = append(target.Types, typeName)
			}
		}
	}
}

func dropMatchingDirectives(dirs ast.DirectiveList, spec base.DropDirectiveSpec) ast.DirectiveList {
	return slices.DeleteFunc(dirs, func(d *ast.Directive) bool {
		if d.Name != spec.Name {
			return false
		}
		if spec.Match == nil {
			return true // drop ALL instances
		}
		for key, val := range spec.Match {
			arg := d.Arguments.ForName(key)
			if arg == nil || arg.Value == nil || arg.Value.Raw != val {
				return false
			}
		}
		return true
	})
}

// --- Phase 3: Incremental relationship updates ---

func (p *Provider) updateRelationships(cs *updateChangeset) {
	// Remove relationships for dropped definitions
	for _, name := range cs.toDrop {
		delete(p.schema.PossibleTypes, name)
		for key, defs := range p.schema.PossibleTypes {
			p.schema.PossibleTypes[key] = slices.DeleteFunc(defs, func(d *ast.Definition) bool {
				return d.Name == name
			})
		}
		delete(p.schema.Implements, name)
		for key, defs := range p.schema.Implements {
			p.schema.Implements[key] = slices.DeleteFunc(defs, func(d *ast.Definition) bool {
				return d.Name == name
			})
		}
	}

	// Add relationships for new definitions
	for _, def := range cs.toAdd {
		switch def.Kind {
		case ast.Object:
			p.schema.PossibleTypes[def.Name] = append(p.schema.PossibleTypes[def.Name], def)
			for _, iface := range def.Interfaces {
				p.schema.PossibleTypes[iface] = append(p.schema.PossibleTypes[iface], def)
				if ifaceDef := p.schema.Types[iface]; ifaceDef != nil {
					p.schema.Implements[def.Name] = append(p.schema.Implements[def.Name], ifaceDef)
				}
			}
		case ast.Union:
			for _, typeName := range def.Types {
				if memberDef := p.schema.Types[typeName]; memberDef != nil {
					p.schema.PossibleTypes[def.Name] = append(p.schema.PossibleTypes[def.Name], memberDef)
				}
			}
		}
	}

	// Update relationships for extensions that add interfaces/union types
	for _, ec := range cs.extensions {
		target := p.schema.Types[ec.targetName]
		if target == nil {
			continue
		}
		for _, iface := range ec.interfacesToAdd {
			p.schema.PossibleTypes[iface] = append(p.schema.PossibleTypes[iface], target)
			if ifaceDef := p.schema.Types[iface]; ifaceDef != nil {
				p.schema.Implements[target.Name] = append(p.schema.Implements[target.Name], ifaceDef)
			}
		}
		for _, typeName := range ec.unionTypesToAdd {
			if memberDef := p.schema.Types[typeName]; memberDef != nil {
				p.schema.PossibleTypes[target.Name] = append(p.schema.PossibleTypes[target.Name], memberDef)
			}
		}
	}
}

// --- directive validation ---

func (p *Provider) validateDirectiveUsage(dir *ast.Directive) error {
	dirDef := p.schema.Directives[dir.Name]
	if dirDef == nil {
		return fmt.Errorf("directive @%s: not defined in schema", dir.Name)
	}
	// Check required arguments (non-null type without default value)
	for _, argDef := range dirDef.Arguments {
		if argDef.Type.NonNull && argDef.DefaultValue == nil {
			if dir.Arguments.ForName(argDef.Name) == nil {
				return fmt.Errorf("directive @%s: missing required argument %q", dir.Name, argDef.Name)
			}
		}
	}
	return nil
}

func (p *Provider) validateDirectives(dirs ast.DirectiveList) error {
	for _, dir := range dirs {
		if base.IsControlDirective(dir.Name) {
			continue
		}
		if err := p.validateDirectiveUsage(dir); err != nil {
			return err
		}
	}
	return nil
}

// --- Phase 4: Dangling reference cleanup ---

// cleanupDanglingReferences removes fields from surviving types whose return type
// or argument types reference types that no longer exist in the schema.
// This handles back-reference fields (e.g. @field_references with references_query)
// that point to dropped types and their derived types.
func (p *Provider) cleanupDanglingReferences() {
	for _, def := range p.schema.Types {
		if def.Kind != ast.Object && def.Kind != ast.InputObject {
			continue
		}
		def.Fields = slices.DeleteFunc(def.Fields, func(f *ast.FieldDefinition) bool {
			if !isKnownType(p.schema, f.Type.Name()) {
				return true
			}
			for _, arg := range f.Arguments {
				if !isKnownType(p.schema, arg.Type.Name()) {
					return true
				}
			}
			return false
		})
	}
}

// --- helpers ---

func isInToAdd(cs *updateChangeset, name string) bool {
	for _, def := range cs.toAdd {
		if def.Name == name {
			return true
		}
	}
	return false
}
