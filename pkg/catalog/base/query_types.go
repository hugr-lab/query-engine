package base

// ObjectQueryType represents the kind of query generated for a data object.
type ObjectQueryType int

const (
	QueryTypeSelect ObjectQueryType = iota
	QueryTypeSelectOne
	QueryTypeAggregate
	QueryTypeAggregateBucket

	SubQueryTypeReferencesData
	SubQueryTypeJoinData
	SubQueryTypeFunctionCallData
	SubQueryTypeFunctionCallTableJoinData
)

// ObjectMutationType represents the kind of mutation generated for a data object.
type ObjectMutationType int

const (
	MutationTypeInsert ObjectMutationType = iota
	MutationTypeUpdate
	MutationTypeDelete
)

func (m ObjectMutationType) String() string {
	switch m {
	case MutationTypeInsert:
		return "insert"
	case MutationTypeUpdate:
		return "update"
	case MutationTypeDelete:
		return "delete"
	}
	return ""
}

const (
	TableDataObject = "table"
	ViewDataObject  = "view"
)
