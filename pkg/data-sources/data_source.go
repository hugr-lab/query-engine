package datasources

import (
	"context"
	"errors"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"

	//lint:ignore ST1001 "github.com/hugr-lab/query-engine/pkg/data-sources/sources" is a valid package name
	. "github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

func (s *Service) dataSource(ctx context.Context, name string) (types.DataSource, error) {
	res, err := s.qe.Query(ctx, `query($name: String!){
		core {
			data_sources_by_pk(name: $name){
				name
				type
				prefix
				path
				as_module
				self_defined
				read_only
				disabled
				catalogs{
					name
					type
					path
				}
			}
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return types.DataSource{}, err
	}
	defer res.Close()
	var ds types.DataSource
	err = res.ScanData("core.data_sources_by_pk", &ds)
	return ds, err
}

func (s *Service) LoadDataSource(ctx context.Context, name string) error {
	// read from db and if not found only source reload
	item, err := s.dataSource(ctx, name)
	if errors.Is(err, types.ErrNoData) {
		return ErrDataSourceNotFound
	}
	if err != nil {
		return err
	}

	_, err = s.DataSource(name)
	if err == nil {
		// Reloading a source that is still attached. A catalog storage that
		// REPLACES a catalog on add is detached softly: the catalog is
		// suspended, its stored schema stays, and the re-add's version gate
		// below decides whether anything is rewritten — the catalog is
		// re-created from the current config and re-introspected where the
		// source is self-defined, so a DDL change lands in the version and
		// forces the rewrite, while an unchanged schema costs one flag flip
		// instead of a full recompile. A storage that applies an incremental
		// diff cannot do that: what the source stopped declaring would survive,
		// so it is dropped first (catalog.ReplacingCatalogManager).
		hard := true
		if r, ok := s.catalogs.(catalog.ReplacingCatalogManager); ok {
			hard = !r.ReplacesCatalogOnAdd()
		}
		err = s.UnloadDataSource(ctx, name, hard)
		if err != nil && !errors.Is(err, errAlreadyUnloaded) {
			return err
		}
	}

	ds, err := NewDataSource(ctx, item, false)
	if err != nil {
		return err
	}

	if err := s.Register(ctx, item.Name, ds); err != nil {
		return err
	}

	// Attach handles catalog ops based on skipCatalogOps flag:
	// - management/standalone: full compile (AddCatalog)
	// - readonly/worker: attach + RegisterEngine only
	return s.Attach(ctx, item.Name)
}

var errAlreadyUnloaded = errors.New("data source already unloaded")

func (s *Service) UnloadDataSource(ctx context.Context, name string, hard bool) error {
	if !s.IsAttached(name) {
		_ = s.Unregister(ctx, name)
		return errAlreadyUnloaded
	}
	if err := s.Detach(ctx, name, s.db, hard); err != nil {
		return err
	}
	return s.Unregister(ctx, name)
}

func (s *Service) DescribeDataSource(ctx context.Context, name string, self bool) (string, error) {
	ds, err := s.DataSource(name)
	if err != nil {
		return "", err
	}

	source, err := s.catalogSource(ctx, ds, self)
	if err != nil {
		return "", err
	}
	if source == nil {
		return "", nil
	}

	// Reconstruct a SchemaDocument from the catalog's definitions and extensions.
	sd := &ast.SchemaDocument{}
	for def := range source.Definitions(ctx) {
		sd.Definitions = append(sd.Definitions, def)
	}
	if es, ok := source.(base.ExtensionsSource); ok {
		for ext := range es.Extensions(ctx) {
			sd.Extensions = append(sd.Extensions, ext)
		}
	}
	var sb strings.Builder
	formatter.NewFormatter(&sb).FormatSchemaDocument(sd)
	return sb.String(), nil
}
