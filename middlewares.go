package hugr

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/hugr-lab/query-engine/pkg/auth"
)

func (s *Service) middlewares() func(next http.Handler) http.Handler {
	var mm []func(next http.Handler) http.Handler
	// auth
	if s.config.Auth == nil {
		s.config.Auth = &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{
					Allowed: true,
					Role:    "admin",
				}),
			},
		}
	}
	authMiddleware := auth.AuthMiddleware(*s.config.Auth)
	mm = append(mm, authMiddleware)

	// compress
	mm = append(mm, compressMW)

	return buildMW(mm...)
}

func buildMW(middlewares ...func(next http.Handler) http.Handler) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

func compressMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add your compression logic here
		accept := r.Header.Get("Accept-Encoding")
		switch {
		case strings.Contains(accept, "br"):
			w.Header().Set("Content-Encoding", "br")
			w.Header().Add("Vary", "Accept-Encoding")

			brWriter := brotli.NewWriterLevel(w, brotli.BestSpeed)
			defer brWriter.Close()

			cw := &compressResponseWriter{
				ResponseWriter: w,
				compressWriter: brWriter,
			}
			next.ServeHTTP(cw, r)
		case strings.Contains(accept, "gzip"):
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Add("Vary", "Accept-Encoding")

			gz, _ := gzip.NewWriterLevel(w, gzip.BestSpeed)
			defer gz.Close()

			cw := &compressResponseWriter{
				ResponseWriter: w,
				compressWriter: gz,
			}
			next.ServeHTTP(cw, r)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

type compressResponseWriter struct {
	http.ResponseWriter
	compressWriter io.Writer
	wroteHeader    bool
}

func (c *compressResponseWriter) WriteHeader(statusCode int) {
	if c.wroteHeader {
		return
	}
	c.wroteHeader = true

	c.Header().Del("Content-Length")
	c.ResponseWriter.WriteHeader(statusCode)
}

func (c *compressResponseWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	return c.compressWriter.Write(b)
}

func (c *compressResponseWriter) Flush() {
	if _, ok := c.ResponseWriter.(http.Flusher); ok {
		c.ResponseWriter.(http.Flusher).Flush()
	}
}
