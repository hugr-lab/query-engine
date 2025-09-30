package sources

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	FileSourceType    types.CatalogSourceType = "localFS"
	URISourceType     types.CatalogSourceType = "uri"
	URIFileSourceType types.CatalogSourceType = "uriFile"
	TextSourceType    types.CatalogSourceType = "text"
)

type Source interface {
	SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error)
}

type CatalogSourceLoader interface {
	Reload(ctx context.Context) error
}

type MergedSource struct {
	sources []Source
}

func MergeSource(sources ...Source) *MergedSource {
	return &MergedSource{sources: sources}
}

func (s *MergedSource) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	doc := &ast.SchemaDocument{}
	for _, source := range s.sources {
		sd, err := source.SchemaDocument(ctx)
		if err != nil {
			return nil, err
		}
		doc.Merge(sd)
	}
	return doc, nil
}

func (s *MergedSource) Reload(ctx context.Context) error {
	for _, source := range s.sources {
		l, ok := source.(CatalogSourceLoader)
		if ok {
			err := l.Reload(ctx)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
