package hugr

// schemaHandler is temporarily disabled while migrating from *ast.Schema
// to schema.Provider. The gqlparser formatter requires *ast.Schema, so this
// endpoint will be re-enabled once an alternative formatting approach is
// available.
//
// import (
// 	"net/http"
//
// 	"github.com/vektah/gqlparser/v2/formatter"
// )
//
// func (s *Service) schemaHandler(w http.ResponseWriter, r *http.Request) {
// 	schema := s.catalog.Schema()
// 	w.Header().Set("Content-Type", "application/text")
//
// 	formatter.NewFormatter(w, formatter.WithComments(), formatter.WithBuiltin()).FormatSchema(schema)
// }
