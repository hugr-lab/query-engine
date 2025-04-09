package catalogs

import (
	"context"
	"errors"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/catalogs/sources"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

var ErrCatalogNotFound = errors.New("catalog not found")

type Service struct {
	mu         sync.RWMutex
	schema     *ast.Schema
	catalogs   map[string]*Catalog
	extensions map[string]sources.Source
}

func New() *Service {
	return &Service{
		catalogs: make(map[string]*Catalog),
	}
}

func (s *Service) ExistsCatalog(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.catalogs[name]
	return ok
}

func (s *Service) AddCatalog(ctx context.Context, name string, c *Catalog) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.catalogs[name] = c
	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		delete(s.catalogs, name)
		return err
	}

	s.schema = newSchema
	return nil
}

func (s *Service) RemoveCatalog(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, ok := s.catalogs[name]
	if !ok {
		return ErrCatalogNotFound
	}

	delete(s.catalogs, name)

	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		s.catalogs[name] = c
		return err
	}

	s.schema = newSchema
	return nil
}

func (s *Service) AddExtension(ctx context.Context, name string, source sources.Source) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extensions[name] = source

	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		return err
	}
	s.schema = newSchema
	return nil
}

func (s *Service) ExistsExtension(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.extensions[name]
	return ok
}

func (s *Service) RemoveExtension(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ext, ok := s.extensions[name]
	if !ok {
		return ErrCatalogNotFound
	}

	delete(s.extensions, name)
	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		s.extensions[name] = ext
		return err
	}
	s.schema = newSchema
	return nil
}

func (s *Service) CatalogSchema(name string) (*ast.Schema, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if name == "" {
		return s.schema, nil
	}

	c, ok := s.catalogs[name]
	if !ok {
		return nil, ErrCatalogNotFound
	}
	return c.schema, nil
}

func (s *Service) Engine(name string) (engines.Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.catalogs[name]
	if !ok {
		return nil, ErrCatalogNotFound
	}
	return c.engine, nil
}

func (s *Service) Schema() *ast.Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.schema
}

func (s *Service) Reload(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for cn, c := range s.catalogs {
		if name != "" && cn != name {
			continue
		}
		err := c.Reload(ctx)
		if err != nil {
			return err
		}
	}

	for _, ext := range s.extensions {
		if l, ok := ext.(sources.CatalogSourceLoader); ok {
			err := l.Reload(ctx)
			if err != nil {
				return err
			}
		}
	}

	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		return err
	}

	s.schema = newSchema
	return nil
}

func (s *Service) RebuildSchema(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	newSchema, err := s.rebuildSchema(ctx)
	if err != nil {
		return err
	}

	s.schema = newSchema
	return nil
}

func (s *Service) mergeSchema() (*ast.Schema, error) {
	var cs []*ast.Schema
	for _, c := range s.catalogs {
		cs = append(cs, c.Schema())
	}

	return compiler.MergeSchema(cs...)
}

func (s *Service) rebuildSchema(ctx context.Context) (*ast.Schema, error) {
	schema, err := s.mergeSchema()
	if err != nil {
		return nil, err
	}

	err = s.loadExtensions(ctx, schema)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (s *Service) loadExtensions(ctx context.Context, schema *ast.Schema) error {
	doc := &ast.SchemaDocument{}
	for _, ext := range s.extensions {
		sd, err := ext.SchemaDocument(ctx)
		if err != nil {
			return err
		}
		doc.Merge(sd)
	}

	return compiler.AddExtensions(schema, doc)
}
