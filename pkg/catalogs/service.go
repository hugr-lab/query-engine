package catalogs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

var ErrCatalogNotFound = errors.New("catalog not found")
var generalEngine = engines.NewDuckDB()

type Service struct {
	mu         sync.RWMutex
	schema     *ast.Schema
	catalogs   map[string]*Catalog
	extensions map[string]*Extension
}

func New() *Service {
	return &Service{
		catalogs:   make(map[string]*Catalog),
		extensions: make(map[string]*Extension),
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

	// check dependencies
	for _, ext := range s.extensions {
		if slices.Contains(ext.Depends(), name) {
			return fmt.Errorf("cannot remove catalog, it is used by extension %s", ext.Name())
		}
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

func (s *Service) AddExtension(ctx context.Context, ext *Extension) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extensions[ext.Name()] = ext // check if dependency is loaded

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

	// check dependencies
	for _, ext := range s.extensions {
		if slices.Contains(ext.Depends(), name) {
			return fmt.Errorf("cannot remove catalog, it is used by extension %s", ext.Name())
		}
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

	if c, ok := s.catalogs[name]; ok {
		return c.engine, nil
	}
	if ext, ok := s.extensions[name]; ok {
		return ext.engine, nil
	}

	return nil, ErrCatalogNotFound
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
		if name != "" && ext.Name() != name {
			continue
		}
		err := ext.Reload(ctx, s.schema)
		if err != nil {
			return err
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

func (s *Service) mergeSchema() (*ast.SchemaDocument, error) {
	var cs []*ast.Schema
	for _, c := range s.catalogs {
		cs = append(cs, c.Schema())
	}
	for _, ext := range s.extensions {
		if es := ext.Schema(); es != nil {
			cs = append(cs, es)
		}
	}

	return compiler.MergeSchema(cs...)
}

func (s *Service) rebuildSchema(ctx context.Context) (*ast.Schema, error) {
	schemaDoc, err := s.mergeSchema()
	if err != nil {
		return nil, err
	}

	schema, err := s.loadExtensions(ctx, schemaDoc)
	if err != nil {
		return nil, err
	}

	return schema, nil
}

func (s *Service) loadExtensions(ctx context.Context, schemaDoc *ast.SchemaDocument) (*ast.Schema, error) {
	doc := &ast.SchemaDocument{}
	for _, ext := range s.extensions {
		if sd := ext.Source(); sd != nil {
			doc.Merge(sd)
		}
	}

	return compiler.AddExtensions(schemaDoc, doc)
}
