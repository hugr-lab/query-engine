package sources

import (
	"context"
	"errors"
	"iter"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

var ErrNoSourcesByURI = errors.New("no sources found by URI")

var _ Catalog = (*URISource)(nil)

type URISource struct {
	pool     *db.Pool
	path     string
	isFile   bool
	opts     compiler.Options
	desc     string
	provider *static.DocProvider
	engine   engines.Engine
	version  string
}

func NewURISource(pool *db.Pool, path string, isFile bool, engine engines.Engine, opts compiler.Options) *URISource {
	return &URISource{pool: pool, path: path, isFile: isFile, opts: opts, engine: engine}
}

func (s *URISource) Reload(ctx context.Context) error {
	c, err := s.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	path := s.path
	if !s.isFile && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	if !s.isFile {
		path += "*.graphql"
	}
	rows, err := c.Query(ctx, "SELECT parse_filename(filename, false, 'system') AS name, content FROM read_text($1) ORDER BY filename ", path)
	if err != nil {
		return err
	}
	defer rows.Close()

	var astSrcs []*ast.Source
	for rows.Next() {
		var name, content string
		err = rows.Scan(&name, &content)
		if err != nil {
			return err
		}
		astSrcs = append(astSrcs, &ast.Source{Name: name, Input: content})
	}
	if len(astSrcs) == 0 {
		return ErrNoSourcesByURI
	}

	doc, err := parser.ParseSchemas(astSrcs...)
	if err != nil {
		return err
	}
	s.provider = static.NewDocumentProvider(doc)
	s.version = time.Now().Format(time.RFC3339Nano)
	return nil
}

func (s *URISource) ForName(ctx context.Context, name string) *ast.Definition {
	if s.provider == nil {
		return nil
	}
	return s.provider.ForName(ctx, name)
}

func (s *URISource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	if s.provider == nil {
		return nil
	}
	return s.provider.DirectiveForName(ctx, name)
}

func (s *URISource) Definitions(ctx context.Context) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.Definitions(ctx)
}

func (s *URISource) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	if s.provider == nil {
		return func(yield func(string, *ast.DirectiveDefinition) bool) {}
	}
	return s.provider.DirectiveDefinitions(ctx)
}

func (s *URISource) Extensions(ctx context.Context) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.Extensions(ctx)
}

func (s *URISource) DefinitionExtensions(ctx context.Context, name string) iter.Seq[*ast.Definition] {
	if s.provider == nil {
		return func(yield func(*ast.Definition) bool) {}
	}
	return s.provider.DefinitionExtensions(ctx, name)
}

func (s *URISource) Name() string                      { return s.opts.Name }
func (s *URISource) Description() string               { return s.desc }
func (s *URISource) CompileOptions() compiler.Options   { return s.opts }
func (s *URISource) Engine() engines.Engine             { return s.engine }

func (s *URISource) Version(_ context.Context) (string, error) {
	return s.version, nil
}
