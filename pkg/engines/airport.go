package engines

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
)

type AirportEngine struct {
	*DuckDB
	dbName string
}

func NewAirport(dbName string) *AirportEngine {
	return &AirportEngine{
		DuckDB: &DuckDB{},
		dbName: dbName,
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

// FunctionCall implements [pkg/catalog/sql/sqlBuilder] to propagate catalog bame for function calls in airport engine.
func (e *AirportEngine) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	var args []string
	for _, v := range positional {
		s, err := e.SQLValue(v)
		if err != nil {
			return "", err
		}
		args = append(args, s)
	}
	for k, v := range named {
		s, err := e.SQLValue(v)
		if err != nil {
			return "", err
		}
		args = append(args, fmt.Sprintf("%s:=%s", k, s))
	}
	return Ident(e.dbName) + "." + name + "(" + strings.Join(args, ",") + ")", nil
}
