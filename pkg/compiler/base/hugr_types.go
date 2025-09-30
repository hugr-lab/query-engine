package base

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

type HugrTypeField string

const (
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
)
