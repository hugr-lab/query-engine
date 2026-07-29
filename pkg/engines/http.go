package engines

import "github.com/hugr-lab/query-engine/pkg/catalog/base"

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

func (e *HttpEngine) Capabilities() *base.EngineCapabilities {
	return &base.EngineCapabilities{
		General: base.EngineGeneralCapabilities{
			SupportDefaultSequences: true,
			UnsupportedTypes: []string{
				"IntRange", "BigIntRange", "TimestampRange",
			},
			UnsupportTables: true,
		},
		Insert: base.EngineInsertCapabilities{
			Insert:           false,
			Returning:        true,
			InsertReferences: true,
		},
	}
}
