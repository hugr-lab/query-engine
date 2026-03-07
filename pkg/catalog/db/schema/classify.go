package schema

import (
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
)

// ClassifyType determines the HugrType classification for a type definition
// based on its directives and name. Returns empty string for unclassified types.
func ClassifyType(def *ast.Definition) base.HugrType {
	if def == nil {
		return ""
	}

	// @module_root (or well-known root type names)
	if base.ModuleRootInfo(def) != nil {
		return base.HugrTypeModule
	}

	// @table
	if def.Directives.ForName(base.ObjectTableDirectiveName) != nil {
		return base.HugrTypeTable
	}

	// @view
	if def.Directives.ForName(base.ObjectViewDirectiveName) != nil {
		return base.HugrTypeView
	}

	// InputObject with filter/data directives
	if def.Kind == ast.InputObject {
		if def.Directives.ForName(base.FilterInputDirectiveName) != nil {
			return base.HugrTypeFilter
		}
		if def.Directives.ForName(base.FilterListInputDirectiveName) != nil {
			return base.HugrTypeFilterList
		}
		if def.Directives.ForName(base.DataInputDirectiveName) != nil {
			return base.HugrTypeDataInput
		}
	}

	// Well-known query-time type names
	if def.Kind == ast.Object {
		switch def.Name {
		case base.QueryTimeJoinsTypeName:
			return base.HugrTypeJoin
		case base.QueryTimeSpatialTypeName:
			return base.HugrTypeSpatial
		case base.H3QueryTypeName:
			return base.HugrTypeH3Agg
		case base.H3DataQueryTypeName:
			return base.HugrTypeH3Data
		}
	}

	return ""
}

// ClassifyField determines the HugrTypeField classification for a field
// definition based on its directives, naming patterns, and parent type.
//
// The parentDef parameter provides context about where the field is defined.
// typeLookup resolves type names to definitions for field-type checks
// (e.g., checking if the field's type has @module_root). Pass nil to skip
// type-dependent classification.
func ClassifyField(field *ast.FieldDefinition, parentDef *ast.Definition, typeLookup func(string) *ast.Definition) base.HugrTypeField {
	if field == nil {
		return ""
	}

	// Check if field's type is a module root (submodule detection).
	if typeLookup != nil && field.Type != nil && field.Type.NamedType != "" {
		td := typeLookup(field.Type.Name())
		if td != nil && base.ModuleRootInfo(td) != nil {
			return base.HugrTypeFieldSubmodule
		}
	}

	// @aggregation_query (non-bucket) → aggregate
	if d := field.Directives.ForName(base.FieldAggregationQueryDirectiveName); d != nil {
		if base.DirectiveArgString(d, base.ArgIsBucket) == "true" {
			return base.HugrTypeFieldBucketAgg
		}
		return base.HugrTypeFieldAgg
	}

	// @query with type argument
	if d := field.Directives.ForName(base.QueryDirectiveName); d != nil {
		switch base.DirectiveArgString(d, base.ArgType) {
		case base.QueryTypeTextSelectOne:
			return base.HugrTypeFieldSelectOne
		case base.QueryTypeTextSelect:
			return base.HugrTypeFieldSelect
		}
	}

	// @extra_field → extra_field
	if field.Directives.ForName(base.FieldExtraFieldDirectiveName) != nil {
		return base.HugrTypeFieldExtraField
	}

	// @function_call or @table_function_call_join → function
	if field.Directives.ForName(base.FunctionCallDirectiveName) != nil ||
		field.Directives.ForName(base.FunctionCallTableJoinDirectiveName) != nil {
		return base.HugrTypeFieldFunction
	}

	// @function → function
	if field.Directives.ForName(base.FunctionDirectiveName) != nil {
		return base.HugrTypeFieldFunction
	}

	// @join → select (join subquery)
	if field.Directives.ForName(base.JoinDirectiveName) != nil {
		return base.HugrTypeFieldSelect
	}

	// @references_query → select (references subquery)
	if field.Directives.ForName(base.FieldReferencesQueryDirectiveName) != nil {
		return base.HugrTypeFieldSelect
	}

	// @mutation with type argument
	if d := field.Directives.ForName(base.MutationDirectiveName); d != nil {
		switch base.DirectiveArgString(d, base.ArgType) {
		case base.MutationTypeTextInsert:
			return base.HugrTypeFieldMutationInsert
		case base.MutationTypeTextUpdate:
			return base.HugrTypeFieldMutationUpdate
		case base.MutationTypeTextDelete:
			return base.HugrTypeFieldMutationDelete
		}
	}

	// Well-known field names on specific parent types.
	// These fields are classified by their name + the parent type they belong to,
	// not by their return type.
	if parentDef != nil {
		switch field.Name {
		case base.QueryTimeJoinsFieldName:
			if parentDef.Directives.ForName(base.ObjectTableDirectiveName) != nil ||
				parentDef.Directives.ForName(base.ObjectViewDirectiveName) != nil {
				return base.HugrTypeFieldJoin
			}
		case base.QueryTimeSpatialFieldName:
			if parentDef.Directives.ForName(base.ObjectTableDirectiveName) != nil ||
				parentDef.Directives.ForName(base.ObjectViewDirectiveName) != nil {
				return base.HugrTypeFieldSpatial
			}
		case base.JQTransformQueryName:
			if parentDef.Directives.ForName(base.ObjectTableDirectiveName) != nil ||
				parentDef.Directives.ForName(base.ObjectViewDirectiveName) != nil {
				return base.HugrTypeFieldJQ
			}
		case base.H3QueryFieldName:
			if parentDef.Directives.ForName(base.ObjectTableDirectiveName) != nil ||
				parentDef.Directives.ForName(base.ObjectViewDirectiveName) != nil {
				return base.HugrTypeFieldH3Agg
			}
		}
	}

	return ""
}
