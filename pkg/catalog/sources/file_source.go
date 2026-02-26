package sources

import (
	"context"
	"crypto/sha256"
	"fmt"
	"iter"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

var _ Catalog = (*FileSource)(nil)

type FileSource struct {
	dirPath  string
	opts     compiler.Options
	desc     string
	provider *static.DocProvider
	engine   engines.Engine
	version  string
}

func NewFileSource(dirPath string, engine engines.Engine, opts compiler.Options) *FileSource {
	return &FileSource{dirPath: dirPath, opts: opts, engine: engine}
}

func (s *FileSource) Reload(_ context.Context) error {
	ff, err := os.ReadDir(s.dirPath)
	if err != nil {
		return err
	}

	var names []string
	contents := make(map[string][]byte)
	for _, f := range ff {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".graphql") {
			continue
		}
		data, err := os.ReadFile(path.Join(s.dirPath, f.Name()))
		if err != nil {
			return err
		}
		names = append(names, f.Name())
		contents[f.Name()] = data
	}

	sort.Strings(names)

	var astSrcs []*ast.Source
	h := sha256.New()
	for _, name := range names {
		data := contents[name]
		astSrcs = append(astSrcs, &ast.Source{Name: name, Input: string(data)})
		h.Write(data)
	}

	doc, err := parser.ParseSchemas(astSrcs...)
	if err != nil {
		return err
	}
	s.provider = static.NewDocumentProvider(doc)
	s.version = fmt.Sprintf("%x", h.Sum(nil))
	return nil
}

func (s *FileSource) ForName(ctx context.Context, name string) *ast.Definition {
	if s.provider == nil {
		return nil
	}
	return s.provider.ForName(ctx, name)
}

func (s *FileSource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	if s.provider == nil {
		return nil
	}
	return s.provider.DirectiveForName(ctx, name)
}

func (s *FileSource) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.Definitions(ctx)
}

func (s *FileSource) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	if s.provider == nil {
		return func(yield func(string, *ast.DirectiveDefinition) bool) {}
	}
	return s.provider.DirectiveDefinitions(ctx)
}

func (s *FileSource) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.Extensions(ctx)
}

func (s *FileSource) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.DefinitionExtensions(ctx, name)
}

func (s *FileSource) Name() string                      { return s.opts.Name }
func (s *FileSource) Description() string               { return s.desc }
func (s *FileSource) CompileOptions() compiler.Options   { return s.opts }
func (s *FileSource) Engine() engines.Engine             { return s.engine }

func (s *FileSource) Version(_ context.Context) (string, error) {
	return s.version, nil
}
