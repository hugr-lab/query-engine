package rules

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"

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
		&InternalExtensionMerger{}, // must run before prefix — merges extend type into definitions
		&CatalogTagger{},
		&PrefixPreparer{},

		// GENERATE phase
		&PassthroughRule{}, // must be first — adds structural types not handled by other rules
		&TableRule{},
		&ViewRule{},
		&CubeHypertableRule{},
		&UniqueRule{},
		&AggregationRule{},               // generates _X_aggregation/_X_aggregation_bucket types + @query directives
		&ExtensionFieldAggregationRule{}, // generates agg/filter for extension fields — must run after AggregationRule
		&ReferencesRule{},
		&JoinSpatialRule{},
		&H3Rule{},
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
		&ArgumentTypeValidator{},
		&PostValidator{},
	}
}
