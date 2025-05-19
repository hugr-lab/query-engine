package extension

import (
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/duckdb"
	"github.com/hugr-lab/query-engine/pkg/types"
)

// Provide data source type - extension

type Source struct {
	*duckdb.Source
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	base, err := duckdb.New(types.DataSource{
		Name:        ds.Name,
		Description: ds.Description,
		Prefix:      ds.Prefix,
		Path:        "",
		Type:        ds.Type,
		AsModule:    ds.AsModule,
		ReadOnly:    ds.ReadOnly,
		Disabled:    ds.Disabled,
		SelfDefined: ds.SelfDefined,
		Sources:     ds.Sources,
	}, attached)
	if err != nil {
		return nil, err
	}
	return &Source{
		Source: base,
	}, nil
}

func (s *Source) IsExtension() bool {
	return true
}
