package datasources

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/catalogs"
	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/duckdb"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/extension"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/http"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/mysql"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/postgres"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

type Service struct {
	mu          sync.RWMutex
	dataSources map[string]Source

	db       *db.Pool
	qe       types.Querier
	catalogs *catalogs.Service
}

func New(qe types.Querier, db *db.Pool, cs *catalogs.Service) *Service {
	return &Service{
		dataSources: make(map[string]Source),
		catalogs:    cs,
		db:          db,
		qe:          qe,
	}
}

func (s *Service) AttachRuntimeSource(ctx context.Context, source RuntimeSource) error {
	err := source.Attach(ctx, s.db)
	if err != nil {
		return err
	}

	c, err := catalogs.NewCatalog(ctx, source.Name(), "", source.Engine(), source.Catalog(ctx), source.AsModule(), source.IsReadonly())
	if err != nil {
		return err
	}

	err = s.catalogs.AddCatalog(ctx, source.Name(), c)
	if err != nil {
		return err
	}

	return s.catalogs.RebuildSchema(ctx)
}

func (s *Service) Engine(name string) (engines.Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	return ds.Engine(), nil
}

func (s *Service) Schema() *ast.Schema {
	return s.catalogs.Schema()
}

func (s *Service) Register(ctx context.Context, name string, ds Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.dataSources[name]
	if ok {
		return ErrDataSourceExists
	}
	s.dataSources[name] = ds
	return nil
}

func (s *Service) Unregister(ctx context.Context, name string) error {
	isAttached := s.IsAttached(name)
	if isAttached {
		return ErrDataSourceAttached
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	delete(s.dataSources, name)

	return nil
}

func (s *Service) Attach(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	if ds.IsAttached() {
		return ErrDataSourceAttached
	}

	err := ds.Attach(ctx, s.db)
	if err != nil {
		return err
	}
	if s.catalogs.ExistsCatalog(name) {
		return ErrDataSourceExists
	}

	if e, ok := ds.(ExtensionSource); ok && e.IsExtension() {
		// add extension
		source, err := s.extensionCatalog(ctx, name)
		if err != nil {
			return err
		}
		return s.catalogs.AddExtension(ctx, source)
	}

	// create data source catalog
	c, err := s.dataSourceCatalog(ctx, name)
	if err != nil {
		return err
	}

	// add catalog
	return s.catalogs.AddCatalog(ctx, name, c)
}

func (s *Service) Detach(ctx context.Context, name string, db *db.Pool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return ErrDataSourceNotFound
	}

	if !ds.IsAttached() {
		return ErrDataSourceAttached
	}

	// remove catalog
	if e, ok := ds.(ExtensionSource); ok && e.IsExtension() {
		err := s.catalogs.RemoveExtension(ctx, name)
		if err != nil {
			return err
		}
		return ds.Detach(ctx, db)
	}
	err := s.catalogs.RemoveCatalog(ctx, name)
	if !errors.Is(err, catalogs.ErrCatalogNotFound) && err != nil {
		return err
	}

	return ds.Detach(ctx, db)
}

func (s *Service) IsAttached(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return false
	}

	return ds.IsAttached()
}

func (s *Service) DataSource(name string) (Source, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ds, ok := s.dataSources[name]
	if !ok {
		return nil, ErrDataSourceNotFound
	}

	return ds, nil
}

func NewDataSource(ctx context.Context, ds types.DataSource, attached bool) (Source, error) {
	switch ds.Type {
	case Postgres:
		return postgres.New(ds, attached)
	case DuckDB:
		return duckdb.New(ds, attached)
	case MySQL:
		return mysql.New(ds, attached)
	case Http:
		return http.New(ds, attached)
	case Extension:
		return extension.New(ds, attached)
	default:
		return nil, ErrUnknownDataSourceType
	}
}

func (s *Service) dataSourceCatalog(ctx context.Context, name string) (*catalogs.Catalog, error) {
	ds := s.dataSources[name]
	source, err := s.catalogSource(ctx, ds)
	if err != nil {
		return nil, err
	}
	def := ds.Definition()
	return catalogs.NewCatalog(ctx, def.Name, def.Prefix, ds.Engine(), source, def.AsModule, ds.ReadOnly())
}

func (s *Service) extensionCatalog(ctx context.Context, name string) (*catalogs.Extension, error) {
	ds := s.dataSources[name]
	source, err := s.catalogSource(ctx, ds)
	if err != nil {
		return nil, err
	}
	def := ds.Definition()
	return s.catalogs.NewExtension(ctx, def, source)
}

func (s *Service) catalogSource(ctx context.Context, ds Source) (sources.Source, error) {
	var ss []sources.Source
	def := ds.Definition()
	for _, cs := range def.Sources {
		s, err := s.loadCatalogSource(ctx, cs.Type, cs.Path)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	if ds, ok := ds.(SelfDescriber); def.SelfDefined && ok {
		s, err := ds.CatalogSource(ctx, s.db)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	if len(ss) == 0 {
		return nil, errors.New("data source does not have catalog source")
	}
	var source sources.Source
	if len(ss) == 1 {
		source = ss[0]
	}
	if len(ss) > 1 {
		source = sources.MergeSource(ss...)
	}
	return source, nil
}

func (s *Service) HttpRequest(ctx context.Context, source, path, method, headers, params, body, jqq string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ds, ok := s.dataSources[source]
	if !ok {
		return nil, ErrDataSourceNotFound
	}
	if !ds.IsAttached() {
		return nil, ErrDataSourceNotAttached
	}
	httpDs, ok := ds.(*http.Source)
	if !ok {
		return nil, errors.New("data source is not http source")
	}
	res, err := httpDs.Request(ctx, path, method, headers, params, body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("request failed with status code %d:%s", res.StatusCode, res.Status)
	}
	if res.Body == nil {
		return nil, errors.New("response body is nil")
	}
	var data any
	err = json.NewDecoder(res.Body).Decode(&data)
	if err != nil {
		return nil, err
	}
	if jqq != "" {
		transform, err := jq.NewTransformer(jqq, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create jq transformer: %v", err)
		}
		data, err = transform.Transform(ctx, data, nil)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}
