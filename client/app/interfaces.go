package app

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/hugr-lab/airport-go/catalog"
)

// DefaultSchema is the fixed schema name treated as root level (no @module directive).
// All other schema names become nested modules.
const DefaultSchema = "default"

// ReservedSchemas that cannot be used by app developers.
var ReservedSchemas = map[string]bool{
	"_mount": true,
	"_funcs": true,
}

type AppInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Version     string `json:"version"`
	URI         string `json:"uri"`
}

type Application interface {
	Listner() (net.Listener, error)
	Info() AppInfo
	Catalog(ctx context.Context) (catalog.Catalog, error)

	// Init is called during server startup to perform any necessary initialization.
	// This performs after registering the application data sources on the hugr side.
	// This will be called by the Hugr server
	Init(ctx context.Context) error

	// Shutdown is called during graceful shutdown to clean up resources.
	// Called before unregistering from hugr.
	Shutdown(ctx context.Context) error
}

type TLSConfigProvider interface {
	TLSConfig(ctx context.Context) (*tls.Config, error)
}

type DataSourceInfo struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	ReadOnly    bool   `json:"read_only"`
	Path        string `json:"path"`
	Version     string `json:"version"`
	HugrSchema  string `json:"hugr_schema"`
}

type DataSourceUser interface {
	Application
	DataSources(ctx context.Context) ([]DataSourceInfo, error)
}

type ApplicationDBInitializer interface {
	InitDBSchemaTemplate(ctx context.Context, name string) (string, error)
}

type ApplicationDBMigrator interface {
	ApplicationDBInitializer
	MigrateDBSchemaTemplate(ctx context.Context, name, version string) (string, error)
}

type MultiCatalogProvider interface {
	SetMultiCatalogMux(mux MultiCatalogMux)
	InitMultiCatalog(ctx context.Context) error
}

type MultiCatalogMux interface {
	AddCatalog(cat catalog.Catalog) error
	RemoveCatalog(name string) error
	IsExists(name string) bool
}
