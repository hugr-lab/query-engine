package gis

import (
	"net/http"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// GIS service provides endpoints for:
// - WFS1.2 (includes WFS-T) features and collections
// - WFS3 (OGC API Features) features and collections, based on saved queries
type schemaRetriever interface {
	Schema() *ast.Schema
}

type Service struct {
	qe     types.Querier
	schema schemaRetriever
	router *http.ServeMux
	lru    *expirable.LRU[string, *Collection]
}

type Config struct {
	Querier types.Querier
	Schema  schemaRetriever

	// Other dependencies can be added here
	CacheTTL  time.Duration // Cache TTL for collections
	CacheSize int           // Size of the cache for collections
}

func New(cfg Config) *Service {
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 5 * time.Minute // Default TTL for collections
	}
	return &Service{
		qe:     cfg.Querier,
		schema: cfg.Schema,
		router: http.NewServeMux(),
		lru:    expirable.NewLRU[string, *Collection](cfg.CacheSize, nil, cfg.CacheTTL),
	}
}

func (s *Service) Init() error {
	s.router.HandleFunc("/query", s.queryHandler)
	// wfs 3 (OGC API Features) endpoints
	//s.router.HandleFunc("/api/api", s.apiHandler)
	s.router.HandleFunc("/wfs/conformance", s.wfs3ConformanceHandler)
	/*s.router.HandleFunc("/api/collections", s.collectionsHandler)
	s.router.HandleFunc("/api/collections/{name}", s.collectionMetadataHandler)
	s.router.HandleFunc("/api/collections/{name}/items", s.dataHandler)
	s.router.HandleFunc("/api/collections/{name}/items/{id}", s.dataHandler)
	*/
	s.router.HandleFunc("/wfs/", s.wfs3RootHandler)
	// wfs 1.2 (OGC WFS) endpoints
	//s.router.HandleFunc("/wfs", s.wfs1Handler)
	return nil
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}
