package rules

import (
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/pkg/catalog/validator"
)

// DefaultRules returns the full set of GraphQL spec validation rules.
// Mirrors gqlparser's default rules minus NoUnusedVariablesRule.
func DefaultRules() []validator.Option {
	return []validator.Option{
		// Inline rules (called during AST walk)
		validator.WithInlineRule(&FieldsOnCorrectType{}),
		validator.WithInlineRule(&ScalarLeafs{}),
		validator.WithInlineRule(&FragmentsOnCompositeTypes{}),
		validator.WithInlineRule(&KnownDirectives{}),
		validator.WithInlineRule(&KnownArgumentNames{}),
		validator.WithInlineRule(&KnownTypeNames{}),
		// permission checker over types/fields
		validator.WithInlineRule(&perm.PermissionFieldRule{}),

		// Post-walk rules (called after AST walk)
		validator.WithPostRule(&LoneAnonymousOperation{}),
		validator.WithPostRule(&UniqueOperationNames{}),
		validator.WithPostRule(&UniqueFragmentNames{}),
		validator.WithPostRule(&UniqueArgumentNames{}),
		validator.WithPostRule(&UniqueInputFieldNames{}),
		validator.WithPostRule(&UniqueVariableNames{}),
		validator.WithPostRule(&UniqueDirectivesPerLocation{}),
		validator.WithPostRule(&NoFragmentCycles{}),
		validator.WithPostRule(&NoUndefinedVariables{}),
		validator.WithPostRule(&NoUnusedFragments{}),
		validator.WithPostRule(&PossibleFragmentSpreads{}),
		validator.WithPostRule(&ProvidedRequiredArguments{}),
		validator.WithPostRule(&ValuesOfCorrectType{}),
		validator.WithPostRule(&VariablesAreInputTypes{}),
		validator.WithPostRule(&VariablesInAllowedPosition{}),
		validator.WithPostRule(&OverlappingFieldsCanBeMerged{}),
		validator.WithPostRule(&SingleFieldSubscriptions{}),
		validator.WithPostRule(&MaxIntrospectionDepth{}),
	}
}
