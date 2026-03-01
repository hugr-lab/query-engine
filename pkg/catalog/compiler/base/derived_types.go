package base

// DerivedTypeNames returns the deterministic list of all type names
// generated from a source type during compilation. Used by incremental
// compilation to identify all types that must be dropped when a source
// type is dropped or replaced.
func DerivedTypeNames(name string) []string {
	return []string{
		name + "_filter",
		name + "_list_filter",
		name + "_mut_input_data",
		name + "_mut_data",
		"_" + name + "_aggregation",
		"_" + name + "_aggregation_bucket",
		"_" + name + "_aggregation_sub_aggregation",
		"_" + name + "_aggregation_sub_aggregation_sub_aggregation",
		name + "_unique_filter",
	}
}

// DerivedExtensionFields returns field names that compilation adds to shared
// types (Query, Mutation, _join, _join_aggregation) for a given data object.
// Used by incremental compilation to emit @drop fields when a type is dropped
// or replaced. The fieldName parameter is the query/mutation field name
// (original name when AsModule, compiled name otherwise).
func DerivedExtensionFields(name, fieldName string) map[string][]string {
	return map[string][]string{
		"Query": {
			fieldName,
			fieldName + "_by_pk",
			fieldName + "_aggregation",
			fieldName + "_bucket_aggregation",
		},
		"Mutation": {
			"insert_" + fieldName,
			"update_" + fieldName,
			"delete_" + fieldName,
		},
		"_join": {
			name,
			name + "_aggregation",
			name + "_bucket_aggregation",
		},
		"_join_aggregation": {
			name,
		},
	}
}
