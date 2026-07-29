package ingest

import "github.com/hugr-lab/query-engine/pkg/catalog/base"

// Default returns the built-in rules in phase order — the whole pipeline.
//
// What is left after design-036 is the WRITE-side pipeline: validate a
// source's SDL, merge its own extensions, tag the catalog, apply the prefix,
// then check the two source-facing declarations that reach beyond a single
// field — @join and @function_call. The output is the PHYSICAL model the
// catalog storage persists; the served GraphQL surface is generated on read
// from `catalog.*`, which is why the GENERATE / ASSEMBLE phases have no rules
// at all and the pipeline skips them.
func Default() []base.Rule {
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

		// FINALIZE phase — the source-facing validators. They run last so the
		// definitions they walk are already prefixed and extension-merged.
		&JoinValidator{},
		&FunctionCallValidator{},
	}
}
