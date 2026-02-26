package sources

import (
	"context"
	"os"
	"path"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

type FileSource struct {
	path     string
	document *ast.SchemaDocument
}

func NewFileSource(path string) *FileSource {
	return &FileSource{path: path}
}

func (s *FileSource) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	if s.document == nil {
		err := s.Reload(ctx)
		if err != nil {
			return nil, err
		}
	}
	return s.document, nil
}

func (s *FileSource) Reload(_ context.Context) error {
	ff, err := os.ReadDir(s.path)
	if err != nil {
		return err
	}

	var sources []*ast.Source
	for _, f := range ff {
		if f.IsDir() {
			continue
		}
		if !strings.HasSuffix(f.Name(), ".graphql") {
			continue
		}

		data, err := os.ReadFile(path.Join(s.path, f.Name()))
		if err != nil {
			return err
		}

		sources = append(sources, &ast.Source{Name: f.Name(), Input: string(data)})
	}

	s.document, err = parser.ParseSchemas(sources...)
	return err
}
