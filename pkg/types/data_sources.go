package types

type DataSourceType string

type CatalogSourceType string

type DataSource struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Type        DataSourceType  `json:"type"`
	Prefix      string          `json:"prefix"`
	Path        string          `json:"path"`
	SelfDefined bool            `json:"self_defined"`
	ReadOnly    bool            `json:"read_only"`
	Sources     []CatalogSource `json:"catalogs"`
}

type CatalogSource struct {
	Name string            `json:"name"`
	Type CatalogSourceType `json:"type"`
	Path string            `json:"path"`
}
