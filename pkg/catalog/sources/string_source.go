package sources

import (
	"context"
	"crypto/sha256"
	"fmt"
	"iter"
	"strconv"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

var _ Catalog = (*StringSource)(nil)

type StringSource struct {
	name     string
	desc     string
	rawSrcs  []string
	opts     compiler.Options
	provider *static.DocProvider
	engine   engines.Engine
	version  string
}

func NewStringSource(name string, engine engines.Engine, opts compiler.Options, sources ...string) (*StringSource, error) {
	s := &StringSource{
		name:    name,
		opts:    opts,
		rawSrcs: sources,
		engine:  engine,
	}
	if len(sources) == 0 {
		s.rawSrcs = []string{name}
	}
	if err := s.init(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StringSource) init() error {
	var astSrcs []*ast.Source
	for i, src := range s.rawSrcs {
		name := s.name
		if len(s.rawSrcs) == 1 && name == "" {
			name = "string"
		}
		if len(s.rawSrcs) != 1 && name == "" {
			name = strconv.Itoa(i) + "-string"
		}
		astSrcs = append(astSrcs, &ast.Source{Name: name, Input: src})
	}
	doc, err := parser.ParseSchemas(astSrcs...)
	if err != nil {
		return err
	}
	s.provider = static.NewDocumentProvider(doc)

	h := sha256.New()
	for _, src := range s.rawSrcs {
		h.Write([]byte(src))
	}
	s.version = fmt.Sprintf("%x", h.Sum(nil))
	return nil
}

func (s *StringSource) ForName(ctx context.Context, name string) *ast.Definition {
	return s.provider.ForName(ctx, name)
}

func (s *StringSource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.provider.DirectiveForName(ctx, name)
}

func (s *StringSource) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	return s.provider.Definitions(ctx)
}

func (s *StringSource) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.provider.DirectiveDefinitions(ctx)
}

func (s *StringSource) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	return s.provider.Extensions(ctx)
}

func (s *StringSource) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	return s.provider.DefinitionExtensions(ctx, name)
}

func (s *StringSource) Name() string                      { return s.opts.Name }
func (s *StringSource) Description() string               { return s.desc }
func (s *StringSource) CompileOptions() compiler.Options   { return s.opts }
func (s *StringSource) Engine() engines.Engine             { return s.engine }

func (s *StringSource) Version(_ context.Context) (string, error) {
	return s.version, nil
}
