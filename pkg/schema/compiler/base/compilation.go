package base

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// CompilationContext provides rules access to source, target schema lookups,
// output writing, scalar registry, and shared state.
type CompilationContext interface {
	Context() context.Context
	Source() DefinitionsSource
	CompileOptions() Options
	ApplyPrefix(name string) string

	// Scalar registry access
	ScalarLookup(name string) types.ScalarType
	IsScalar(name string) bool

	// Output writing
	AddDefinition(def *ast.Definition)
	AddExtension(ext *ast.Definition)
	AddDefinitionReplaceOrCreate(def *ast.Definition)

	// Type lookup: checks compilation output first, then target schema
	LookupType(name string) *ast.Definition
	LookupDirective(name string) *ast.DirectiveDefinition

	// Shared state management
	RegisterObject(name string, info *ObjectInfo)
	GetObject(name string) *ObjectInfo

	// Field collectors for ASSEMBLE phase
	RegisterQueryFields(objectName string, fields []*ast.FieldDefinition)
	RegisterMutationFields(objectName string, fields []*ast.FieldDefinition)
	RegisterFunctionFields(fields []*ast.FieldDefinition)
	RegisterFunctionMutationFields(fields []*ast.FieldDefinition)

	// Field collector getters for ASSEMBLE phase
	QueryFields() map[string][]*ast.FieldDefinition
	MutationFields() map[string][]*ast.FieldDefinition
	FunctionFields() []*ast.FieldDefinition
	FunctionMutationFields() []*ast.FieldDefinition

	// Object iteration
	Objects() iter.Seq2[string, *ObjectInfo]
}

// CompiledCatalog is the DDL feed output of compilation,
// consumable by static.Provider.Update().
type CompiledCatalog interface {
	DefinitionsSource
	ExtensionsSource
}
