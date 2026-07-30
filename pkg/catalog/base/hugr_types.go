package base

// HugrType identifies the kind of a compiled schema definition for introspection.
type HugrType string

const (
	HugrTypeModule     HugrType = "module"
	HugrTypeTable      HugrType = "table"
	HugrTypeView       HugrType = "view"
	HugrTypeJoin       HugrType = "join_queries"
	HugrTypeSpatial    HugrType = "spatial_queries"
	HugrTypeH3Data     HugrType = "h3_data"
	HugrTypeH3Agg      HugrType = "h3_aggregate"
	HugrTypeFilter     HugrType = "filter"
	HugrTypeFilterList HugrType = "filter_list"
	HugrTypeDataInput  HugrType = "data_input"
)

// HugrTypeField identifies the kind of a field within a compiled definition.
type HugrTypeField string

const (
	// Members of a DATA OBJECT. These three were the blank spots: hugr_type
	// answered "" for every stored column, every @sql expression and every
	// @extra_field companion, which is most of what a data object is made of.
	//
	// "calculated" is deliberately not called "extra": in hugr an EXTRA field
	// is the compiler-generated companion of a base field (_<f>_part for a
	// Timestamp, _<f>_measurement for a Geometry), and overloading the word
	// for "computed by SQL" would collide with the term the docs and the
	// skills already use.
	HugrTypeFieldColumn     HugrTypeField = "column"
	HugrTypeFieldCalculated HugrTypeField = "calculated"

	HugrTypeFieldSubmodule      HugrTypeField = "submodule"
	HugrTypeFieldSelectOne      HugrTypeField = "select_one"
	HugrTypeFieldSelect         HugrTypeField = "select"
	HugrTypeFieldAgg            HugrTypeField = "aggregate"
	HugrTypeFieldBucketAgg      HugrTypeField = "bucket_agg"
	HugrTypeFieldFunction       HugrTypeField = "function"
	HugrTypeFieldJoin           HugrTypeField = "join"
	HugrTypeFieldSpatial        HugrTypeField = "spatial"
	HugrTypeFieldH3Agg          HugrTypeField = "h3_aggregate"
	HugrTypeFieldJQ             HugrTypeField = "jq"
	HugrTypeFieldMutationInsert HugrTypeField = "mutation_insert"
	HugrTypeFieldMutationUpdate HugrTypeField = "mutation_update"
	HugrTypeFieldMutationDelete HugrTypeField = "mutation_delete"
	HugrTypeFieldExtraField     HugrTypeField = "extra_field"
	HugrTypeFieldSubscription   HugrTypeField = "subscription"
)
