package sources

import (
	"context"
	"crypto/sha256"
	"fmt"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	FileSourceType    types.CatalogSourceType = "localFS"
	URISourceType     types.CatalogSourceType = "uri"
	URIFileSourceType types.CatalogSourceType = "uriFile"
	TextSourceType    types.CatalogSourceType = "text"
)

var _ Catalog = (*MergedCatalog)(nil)

// MergedCatalog combines multiple Catalogs into a single Catalog.
// Definitions are merged from all sub-catalogs.
type MergedCatalog struct {
	catalogs []Catalog
	opts     compiler.Options
	desc     string
	engine   engines.Engine
}

func NewMergedCatalog(engine engines.Engine, opts compiler.Options, desc string, catalogs ...Catalog) *MergedCatalog {
	return &MergedCatalog{catalogs: catalogs, opts: opts, desc: desc, engine: engine}
}

func (mc *MergedCatalog) ForName(ctx context.Context, name string) *ast.Definition {
	for _, c := range mc.catalogs {
		if def := c.ForName(ctx, name); def != nil {
			return def
		}
	}
	return nil
}

func (mc *MergedCatalog) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	for _, c := range mc.catalogs {
		if def := c.DirectiveForName(ctx, name); def != nil {
			return def
		}
	}
	return nil
}

func (mc *MergedCatalog) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, c := range mc.catalogs {
			for def := range c.Definitions(ctx) {
				if !yield(def) {
					return
				}
			}
		}
	}
}

func (mc *MergedCatalog) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {
		for _, c := range mc.catalogs {
			for name, def := range c.DirectiveDefinitions(ctx) {
				if !yield(name, def) {
					return
				}
			}
		}
	}
}

func (mc *MergedCatalog) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, c := range mc.catalogs {
			if es, ok := c.(base.ExtensionsSource); ok {
				for def := range es.Extensions(ctx) {
					if !yield(def) {
						return
					}
				}
			}
		}
	}
}

func (mc *MergedCatalog) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, c := range mc.catalogs {
			if es, ok := c.(base.ExtensionsSource); ok {
				for def := range es.DefinitionExtensions(ctx, name) {
					if !yield(def) {
						return
					}
				}
			}
		}
	}
}

func (mc *MergedCatalog) Reload(ctx context.Context) error {
	for _, c := range mc.catalogs {
		if rc, ok := c.(ReloadableCatalog); ok {
			if err := rc.Reload(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (mc *MergedCatalog) Name() string                      { return mc.opts.Name }
func (mc *MergedCatalog) Description() string               { return mc.desc }
func (mc *MergedCatalog) CompileOptions() compiler.Options   { return mc.opts }
func (mc *MergedCatalog) Engine() engines.Engine             { return mc.engine }

func (mc *MergedCatalog) Version(ctx context.Context) (string, error) {
	h := sha256.New()
	for _, c := range mc.catalogs {
		v, err := c.Version(ctx)
		if err != nil {
			return "", err
		}
		h.Write([]byte(v))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
