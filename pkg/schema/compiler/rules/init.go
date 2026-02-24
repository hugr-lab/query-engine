package rules

import "github.com/hugr-lab/query-engine/pkg/schema/compiler/base"

// RegisterAll returns all built-in rules in correct phase order.
// Call compiler.RegisterRules(rules.RegisterAll()...) to wire them up.
func RegisterAll() []base.Rule {
	return []base.Rule{
		// VALIDATE phase
		&ExtensionValidator{},
		&DependencyCollector{},
		&SourceValidator{},
		&DefinitionValidator{},

		// PREPARE phase
		&CatalogTagger{},
		&PrefixPreparer{},

		// GENERATE phase
		&TableRule{},
		&ViewRule{},
		&CubeHypertableRule{},
		&UniqueRule{},
		&ReferencesRule{},
		&JoinSpatialRule{},
		&H3Rule{},
		&AggregationRule{},
		&FunctionRule{},
		&ExtraFieldRule{},
		&VectorSearchRule{},
		&EmbeddingsRule{},

		// ASSEMBLE phase
		&ModuleAssembler{},
		&RootTypeAssembler{},

		// FINALIZE phase
		&ReadOnlyFinalizer{},
		&JoinValidator{},
		&FunctionCallValidator{},
		&PostValidator{},
	}
}
