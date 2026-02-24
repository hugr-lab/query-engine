package compiler

import (
	"iter"

	"github.com/vektah/gqlparser/v2/ast"
)

// indexedOutput accumulates compilation output definitions and extensions
// with O(1) lookup by name.
type indexedOutput struct {
	defs      []*ast.Definition
	defIndex  map[string]int // name → index in defs
	exts      []*ast.Definition
	extIndex  map[string]int // target type name → index in exts (for merging)
	dirDefs   []*ast.DirectiveDefinition
	dirIndex  map[string]int
}

func newIndexedOutput(cap int) *indexedOutput {
	return &indexedOutput{
		defs:     make([]*ast.Definition, 0, cap),
		defIndex: make(map[string]int, cap),
		exts:     make([]*ast.Definition, 0, cap),
		extIndex: make(map[string]int, cap),
		dirDefs:  make([]*ast.DirectiveDefinition, 0),
		dirIndex: make(map[string]int),
	}
}

// AddDefinition adds a new definition to the output.
func (o *indexedOutput) AddDefinition(def *ast.Definition) {
	if _, exists := o.defIndex[def.Name]; exists {
		return // skip duplicates
	}
	o.defIndex[def.Name] = len(o.defs)
	o.defs = append(o.defs, def)
}

// AddDefinitionReplaceOrCreate adds a definition, replacing any existing one with the same name.
func (o *indexedOutput) AddDefinitionReplaceOrCreate(def *ast.Definition) {
	if idx, exists := o.defIndex[def.Name]; exists {
		o.defs[idx] = def
		return
	}
	o.defIndex[def.Name] = len(o.defs)
	o.defs = append(o.defs, def)
}

// AddExtension adds an extension, merging fields into an existing extension
// for the same target type (FR-013).
func (o *indexedOutput) AddExtension(ext *ast.Definition) {
	if idx, exists := o.extIndex[ext.Name]; exists {
		existing := o.exts[idx]
		existing.Fields = append(existing.Fields, ext.Fields...)
		existing.EnumValues = append(existing.EnumValues, ext.EnumValues...)
		existing.Directives = append(existing.Directives, ext.Directives...)
		existing.Interfaces = append(existing.Interfaces, ext.Interfaces...)
		return
	}
	o.extIndex[ext.Name] = len(o.exts)
	o.exts = append(o.exts, ext)
}

// LookupDefinition returns a definition by name, or nil.
func (o *indexedOutput) LookupDefinition(name string) *ast.Definition {
	if idx, ok := o.defIndex[name]; ok {
		return o.defs[idx]
	}
	return nil
}

// LookupExtension returns the accumulated extension for a given type name, or nil.
func (o *indexedOutput) LookupExtension(name string) *ast.Definition {
	if idx, ok := o.extIndex[name]; ok {
		return o.exts[idx]
	}
	return nil
}

// LookupDirective returns a directive definition by name, or nil.
func (o *indexedOutput) LookupDirective(name string) *ast.DirectiveDefinition {
	if idx, ok := o.dirIndex[name]; ok {
		return o.dirDefs[idx]
	}
	return nil
}

// Definitions returns an iterator over all output definitions.
func (o *indexedOutput) Definitions() iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range o.defs {
			if !yield(d) {
				return
			}
		}
	}
}

// Extensions returns an iterator over all output extensions.
func (o *indexedOutput) Extensions() iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, e := range o.exts {
			if !yield(e) {
				return
			}
		}
	}
}
