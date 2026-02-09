package engines

import "github.com/hugr-lab/query-engine/pkg/compiler"

// HTTP Engine is a query engine for HTTP data sources.

type MssqlEngine struct {
	*DuckDB
}

func NewMssql() *MssqlEngine {
	return &MssqlEngine{
		DuckDB: &DuckDB{},
	}
}

func (e *MssqlEngine) Type() Type {
	return TypeMssql
}

func (e *MssqlEngine) Capabilities() *compiler.EngineCapabilities {
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
