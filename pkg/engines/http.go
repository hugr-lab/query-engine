package engines

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
