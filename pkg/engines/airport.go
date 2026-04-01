package engines

import "github.com/hugr-lab/query-engine/pkg/catalog/compiler"

type AirportEngine struct {
	*DuckDB
}

func NewAirport() *AirportEngine {
	return &AirportEngine{
		DuckDB: &DuckDB{},
	}
}

func (e *AirportEngine) Type() Type {
	return TypeAirport
}

func (e *AirportEngine) Capabilities() *compiler.EngineCapabilities {
	cap := e.DuckDB.Capabilities()
	cap.General.SupportDefaultSequences = false
	return cap
}

// IsFunctionCallWithCatalog implements [EngineFunctionCallWithCatalog].
// Airport functions must be qualified with catalog name: catalog."schema"."FUNC"()
func (e *AirportEngine) IsFunctionCallWithCatalog() bool {
	return true
}

var _ EngineFunctionCallWithCatalog = (*AirportEngine)(nil)
