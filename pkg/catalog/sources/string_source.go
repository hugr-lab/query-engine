package sources

import (
	"context"
	"strconv"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

type StringSource struct {
	name    string
	sources []string
}

func NewStringSource(name string, sources ...string) StringSource {
	if len(sources) == 0 {
		return StringSource{sources: []string{name}}
	}

	return StringSource{
		name:    name,
		sources: sources,
	}
}

func (s StringSource) SchemaDocument(_ context.Context) (*ast.SchemaDocument, error) {
	var sources []*ast.Source
	for i, source := range s.sources {
		name := s.name
		if len(s.sources) == 1 && name == "" {
			name = "string"
		}
		if len(s.sources) != 1 && name == "" {
			name = strconv.Itoa(i) + "-string"
		}
		sources = append(sources, &ast.Source{Name: name, Input: source})
	}

	return parser.ParseSchemas(sources...)
}
