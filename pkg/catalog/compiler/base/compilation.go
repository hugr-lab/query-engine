package base

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/types"
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

	// PromoteToSource adds a definition to the source-level pool.
	// Definitions added here are dispatched to DefinitionRules in subsequent phases,
	// alongside source definitions. Used by PREPARE rules (e.g. InternalExtensionMerger)
	// when an extension targets a provider type not present in the source.
	PromoteToSource(def *ast.Definition)

	// PromotedDefinitions returns definitions promoted via PromoteToSource.
	// Used by PREPARE batch rules (e.g. PrefixPreparer) to also process promoted defs.
	PromotedDefinitions() []*ast.Definition

	// Type lookup: checks compilation output first, then target schema
	LookupType(name string) *ast.Definition
	LookupDirective(name string) *ast.DirectiveDefinition

	// LookupExtension returns the accumulated extension for a given type name,
	// or nil if no extensions have been added for that type.
	LookupExtension(name string) *ast.Definition

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

	// Output iteration (for FINALIZE rules)
	OutputDefinitions() iter.Seq[*ast.Definition]
	OutputExtensions() iter.Seq[*ast.Definition]

	// Dependency registration (extension compilation)
	RegisterDependency(name string)
}

// CompiledCatalog is the DDL feed output of compilation,
// consumable by static.Provider.Update().
type CompiledCatalog interface {
	DefinitionsSource
	ExtensionsSource
}

// DependentCompiledCatalog extends CompiledCatalog with dependency information
// collected from @dependency directives during extension compilation.
type DependentCompiledCatalog interface {
	Dependencies() []string
}
