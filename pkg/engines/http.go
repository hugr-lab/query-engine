package engines

import "github.com/hugr-lab/query-engine/pkg/schema/compiler"

// HTTP Engine is a query engine for HTTP data sources.

type HttpEngine struct {
	*DuckDB
}

func NewHttp() *HttpEngine {
	return &HttpEngine{
		DuckDB: &DuckDB{},
	}
}

func (e *HttpEngine) Type() Type {
	return TypeHttp
}

func (e *HttpEngine) Capabilities() *compiler.EngineCapabilities {
	return &compiler.EngineCapabilities{
		General: compiler.EngineGeneralCapabilities{
			SupportDefaultSequences: true,
			UnsupportedTypes: []string{
				"IntRange", "BigIntRange", "TimestampRange",
			},
			UnsupportTables: true,
		},
		Insert: compiler.EngineInsertCapabilities{
			Insert:           false,
			Returning:        true,
			InsertReferences: true,
		},
	}
}
