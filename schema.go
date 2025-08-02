package hugr

import (
	"net/http"

	"github.com/vektah/gqlparser/v2/formatter"
)

func (s *Service) schemaHandler(w http.ResponseWriter, r *http.Request) {
	schema := s.catalog.Schema()
	w.Header().Set("Content-Type", "application/text")

	formatter.NewFormatter(w, formatter.WithComments(), formatter.WithBuiltin()).FormatSchema(schema)
}
