package base

// Object-level directive names.
const (
	ObjectTableDirectiveName = "table"
	ObjectViewDirectiveName  = "view"
)

// Field-level directive names.
const (
	FieldPrimaryKeyDirectiveName           = "pk"
	FieldGeometryInfoDirectiveName         = "geometry_info"
	FieldSqlDirectiveName                  = "sql"
	FieldExtraFieldDirectiveName           = "extra_field"
	FieldDefaultDirectiveName              = "default"
	FieldDefaultDirectiveInsertExprArgName = "insert_exp"
	FieldDefaultDirectiveUpdateExprArgName = "update_exp"
	FieldMeasurementDirectiveName          = "measurement"
	FieldMeasurementFuncArgName            = "measurement_func"
	FieldDimDirectiveName                  = "dim"
	FieldExcludeMCPDirectiveName           = "exclude_mcp"
)

// Query-side directive names.
const (
	WithDeletedDirectiveName = "with_deleted"
	StatsDirectiveName       = "stats"
	RawResultsDirectiveName  = "raw"
	UnnestDirectiveName      = "unnest"
	NoPushdownDirectiveName  = "no_pushdown"
	AtDirectiveName          = "at"
)

// Query/mutation directive names.
const (
	QueryDirectiveName                     = "query"
	MutationDirectiveName                  = "mutation"
	FunctionDirectiveName                  = "function"
	FunctionCallDirectiveName              = "function_call"
	FunctionCallTableJoinDirectiveName     = "table_function_call_join"
	JoinDirectiveName                      = "join"
	ReferencesDirectiveName                = "references"
	FieldReferencesDirectiveName           = "field_references"
	ReferencesQueryDirectiveName           = "references_query"
	FieldAggregationQueryDirectiveName     = "aggregation_query"
	ModuleDirectiveName                    = "module"
	ModuleRootDirectiveName                = "module_root"
	ViewArgsDirectiveName                  = "args"
	OriginalNameDirectiveName              = "original_name"
	DeprecatedDirectiveName                = "deprecated"
	FilterInputDirectiveName               = "filter_input"
	FilterListInputDirectiveName           = "filter_list_input"
	DataInputDirectiveName                 = "data_input"
)

// Cache directive names.
const (
	CacheDirectiveName           = "cache"
	NoCacheDirectiveName         = "no_cache"
	InvalidateCacheDirectiveName = "invalidate_cache"
)

// H3 directive and type names.
const (
	AddH3DirectiveName          = "add_h3"
	H3QueryFieldName            = "h3"
	H3QueryTypeName             = "_h3_query"
	H3DataQueryTypeName         = "_h3_data_query"
	H3DataFieldName             = "data"
	DistributionFieldName       = "distribution_by"
	DistributionTypeName        = "_distribution_by"
	BucketDistributionFieldName = "distribution_by_bucket"
	BucketDistributionTypeName  = "_distribution_by_bucket"
)

// GIS/WFS directive and type names.
const (
	GisFeatureDirectiveName    = "feature"
	GisWFSDirectiveName        = "wfs"
	GisWFSFieldDirectiveName   = "wfs_field"
	GisWFSExcludeDirectiveName = "wfs_exclude"
	GisWFSTypeName             = "_wfs_features"
)

// Query-time join type/field names.
const (
	QueryTimeJoinsFieldName   = "_join"
	QueryTimeJoinsTypeName    = "_join"
	QueryTimeSpatialFieldName = "_spatial"
	QueryTimeSpatialTypeName  = "_spatial"
)

// Vector search directive and type names.
const (
	VectorTypeName                       = "Vector"
	VectorSearchInputName                = "VectorSearchInput"
	VectorDistanceTypeEnumName           = "VectorDistanceType"
	VectorSearchDistanceL2               = "L2"
	VectorSearchDistanceIP               = "Inner"
	VectorSearchDistanceCosine           = "Cosine"
	SimilaritySearchArgumentName         = "similarity"
	EmbeddingsDirectiveName              = "embeddings"
	SemanticSearchArgumentName           = "semantic"
	SemanticSearchInputName              = "SemanticSearchInput"
	SummaryForEmbeddedArgumentName       = "summary"
	DistanceFieldNameSuffix              = "distance"
	VectorDistanceExtraFieldName         = "VectorDistance"
	QueryEmbeddingDistanceExtraFieldName = "QueryEmbeddingDistance"
	QueryEmbeddingsDistanceFieldName     = "_distance_to_query"
)

// Type name constants.
const (
	FunctionTypeName         = "Function"
	FunctionMutationTypeName = "MutationFunction"
	QueryBaseName            = "Query"
	MutationBaseName         = "Mutation"
	OperationResultTypeName  = "OperationResult"
)

// Scalar type name constants.
const (
	JSONTypeName                          = "JSON"
	TimestampTypeName                     = "Timestamp"
	H3CellTypeName                        = "H3Cell"
	GeometryTypeName                      = "Geometry"
	GeometryAggregationTypeName           = "GeometryAggregation"
	GeometryMeasurementExtraFieldName     = "Measurement"
	TimestampExtractExtraFieldName        = "Extract"
)

// Aggregation constants.
const (
	AggregateKeyFieldName   = "key"
	AggregateFieldName      = "aggregations"
	AggRowsCountFieldName   = "_rows_count"
	AggregationSuffix       = "_aggregation"
	BucketAggregationSuffix = "_bucket_aggregation"
)

// Input/filter constants.
const (
	ListFilterInputSuffix = "_list_filter"
	FilterInputSuffix     = "_filter"
)

// Mutation type text constants.
const (
	MutationTypeTextInsert = "INSERT"
	MutationTypeTextUpdate = "UPDATE"
	MutationTypeTextDelete = "DELETE"
)

// Join field name prefixes.
const (
	JoinSourceFieldPrefix = "source"
	JoinRefFieldPrefix    = "dest"
)

// Object query constants.
const (
	ObjectQueryByPKSuffix = "_by_pk"
	StubFieldName         = "_stub"
)

// Query type text constants (for @query directive "type" argument).
const (
	QueryTypeTextSelect          = "SELECT"
	QueryTypeTextSelectOne       = "SELECT_ONE"
	QueryTypeTextAggregate       = "AGGREGATE"
	QueryTypeTextAggregateBucket = "AGGREGATE_BUCKET"
)

// Object-level secondary directive names.
const (
	ObjectHyperTableDirectiveName = "hypertable"
	ObjectCubeDirectiveName       = "cube"
	ObjectUniqueDirectiveName     = "unique"

	FieldReferencesQueryDirectiveName = "references_query"

	FieldTimescaleKeyDirectiveName = "timescale_key"

	InputFieldNamedArgDirectiveName = "named_arg"

	ObjectAggregationDirectiveName      = "aggregation"
	ObjectFieldAggregationDirectiveName = "field_aggregation"
)

// Catalog system variable.
const CatalogSystemVariableName = "$catalog"

// Query argument descriptions.
const (
	DescFilter          = "Filter"
	DescOrderBy         = "Sort options for the result set"
	DescLimit           = "Limit the number of returned objects"
	DescOffset          = "Skip the first n objects"
	DescDistinctOn      = "Distinct on the given fields"
	DescInnerJoin       = "Apply inner join to the result set"
	DescInnerJoinRef    = "Apply inner join to reference record"
	DescArgs            = "Arguments for the view"
	DescBucketKey       = "The key of the bucket"
	DescNestedOrderBy   = "Sort options for the nested result set"
	DescNestedLimit     = "Limit the number of returned nested objects"
	DescNestedOffset    = "Skip the first n nested objects"
)

// Directive argument name constants (shared between sdl and rules).
const (
	ArgName                  = "name"
	ArgType                  = "type"
	ArgSQL                   = "sql"
	ArgExp                   = "exp"
	ArgField                 = "field"
	ArgRequired              = "required"
	ArgDescription           = "description"
	ArgReferencesName        = "references_name"
	ArgSourceFields          = "source_fields"
	ArgReferencesFields      = "references_fields"
	ArgReferencesQuery       = "references_query"
	ArgReferencesDescription = "references_description"
	ArgIsM2M                 = "is_m2m"
	ArgM2MName               = "m2m_name"
	ArgIsTable               = "is_table"
	ArgIsTableFuncJoin       = "is_table_func_join"
	ArgIsBucket              = "is_bucket"
	ArgBaseField             = "base_field"
	ArgBaseType              = "base_type"
	ArgQuery                 = "query"
	ArgSoftDelete            = "soft_delete"
	ArgSoftDeleteCond        = "soft_delete_cond"
	ArgSoftDeleteSet         = "soft_delete_set"
	ArgSkipNullArg           = "skip_null_arg"
	ArgJsonCast              = "json_cast"
	ArgSequence              = "sequence"
	ArgQuerySuffix           = "query_suffix"
	ArgSkipQuery             = "skip_query"
	ArgModule                = "module"
	ArgModel                 = "model"
	ArgVector                = "vector"
	ArgDistance               = "distance"
	ArgLen                   = "len"
	ArgSRID                  = "srid"
	ArgArgs                  = "args"
)
