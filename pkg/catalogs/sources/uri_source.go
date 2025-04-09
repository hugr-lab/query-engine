package sources

import (
	"context"
	"errors"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/vektah/gqlparser/v2/ast"
)

var ErrNoSourcesByURI = errors.New("no sources found by URI")

type URISource struct {
	db       *db.Pool
	path     string
	isFile   bool
	document *ast.SchemaDocument
}

func NewURISource(db *db.Pool, path string, isFile bool) *URISource {
	return &URISource{db: db, path: path, isFile: isFile}
}

func (s URISource) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	if s.document == nil {
		err := s.Reload(ctx)
		if err != nil {
			return nil, err
		}
	}
	return s.document, nil
}

func (s *URISource) Reload(ctx context.Context) error {
	c, err := s.db.Conn(ctx)
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
	var sources []Source
	for rows.Next() {
		var name, content string
		err = rows.Scan(&name, &content)
		if err != nil {
			return err
		}
		sources = append(sources, NewStringSource(name, content))
	}
	if len(sources) == 0 {
		return ErrNoSourcesByURI
	}
	s.document, err = MergeSource(sources...).SchemaDocument(ctx)
	if err != nil {
		return err
	}

	return nil
}
