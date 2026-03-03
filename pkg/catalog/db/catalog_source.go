package db

import (
	"bytes"
	"context"
	"text/template"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sources"
	dbpool "github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"

	_ "embed"
)

//go:embed schema_catalog.graphql.tmpl
var catalogSchemaTmpl string

// CatalogSource returns a RuntimeSource for the core.catalog module.
// The source exposes _schema_* tables as read-only GraphQL views.
func (p *Provider) CatalogSource() *CatalogRuntimeSource {
	return &CatalogRuntimeSource{provider: p}
}

// CatalogRuntimeSource is the core.catalog runtime source backed by a Provider.
type CatalogRuntimeSource struct {
	provider *Provider
}

func (s *CatalogRuntimeSource) Name() string          { return "core.catalog" }
func (s *CatalogRuntimeSource) Engine() engines.Engine { return engines.NewDuckDB() }
func (s *CatalogRuntimeSource) IsReadonly() bool       { return true }
func (s *CatalogRuntimeSource) AsModule() bool         { return true }

func (s *CatalogRuntimeSource) Attach(_ context.Context, _ *dbpool.Pool) error {
	// UDFs are registered by Provider.RegisterUDFs, not here.
	return nil
}

func (s *CatalogRuntimeSource) Catalog(_ context.Context) (sources.Catalog, error) {
	params := struct {
		EmbeddingsEnabled bool
		VectorSize        int
		IsPostgres        bool
	}{
		EmbeddingsEnabled: s.provider.HasEmbeddings(),
		VectorSize:        s.provider.VecSize(),
		IsPostgres:        s.provider.isPostgres,
	}

	tmpl, err := template.New("schema").Parse(catalogSchemaTmpl)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, params); err != nil {
		return nil, err
	}

	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		Prefix:       "_schema",
		ReadOnly:     s.IsReadonly(),
		AsModule:     s.AsModule(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return sources.NewStringSource(s.Name(), e, opts, buf.String())
}
