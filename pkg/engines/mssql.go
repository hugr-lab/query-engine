package engines

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"

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

func (e *MssqlEngine) Capabilities() *base.EngineCapabilities {
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
