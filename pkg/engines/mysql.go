package engines

import "github.com/hugr-lab/query-engine/pkg/compiler"

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

func (e *MySqlEngine) Capabilities() *compiler.EngineCapabilities {
	return &compiler.EngineCapabilities{
		General: compiler.EngineGeneralCapabilities{
			SupportDefaultSequences: false,
			UnsupportedTypes: []string{
				"IntRange", "BigIntRange", "TimestampRange",
				"JSON", "H3Cell", "Vector", "Geometry",
			},
			UnsupportStructuredTypes: true,
			UnsupportArrays:          true,
		},
		Insert: compiler.EngineInsertCapabilities{
			Insert:           true,
			Returning:        true,
			InsertReferences: true,
		},
		Update: compiler.EngineUpdateCapabilities{
			Update: true,
		},
		Delete: compiler.EngineDeleteCapabilities{
			Delete: true,
		},
	}
}
