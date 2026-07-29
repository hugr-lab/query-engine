package engines

import "github.com/hugr-lab/query-engine/pkg/catalog/base"

// HTTP Engine is a query engine for HTTP data sources.

type MySqlEngine struct {
	*DuckDB
}

func NewMySql() *MySqlEngine {
	return &MySqlEngine{
		DuckDB: &DuckDB{},
	}
}

func (e *MySqlEngine) Type() Type {
	return TypeMySql
}

func (e *MySqlEngine) Capabilities() *base.EngineCapabilities {
	return &base.EngineCapabilities{
		General: base.EngineGeneralCapabilities{
			SupportDefaultSequences: false,
			UnsupportedTypes: []string{
				"IntRange", "BigIntRange", "TimestampRange",
				"JSON", "H3Cell", "Vector", "Geometry",
			},
			UnsupportStructuredTypes: true,
			UnsupportArrays:          true,
		},
		Insert: base.EngineInsertCapabilities{
			Insert:           true,
			Returning:        true,
			InsertReferences: true,
		},
		Update: base.EngineUpdateCapabilities{
			Update: true,
		},
		Delete: base.EngineDeleteCapabilities{
			Delete: true,
		},
	}
}
