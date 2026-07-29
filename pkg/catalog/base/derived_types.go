package base

// DerivedTypeNames returns the deterministic list of all type names
// generated from a source type during compilation. It is the forward mapping
// pkg/mcp inverts to tell a generated type from a source-declared one.
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
