package hugr

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/hugr-lab/query-engine/types"
)

func (s *Service) middlewares() func(next http.Handler) http.Handler {
	var mm []func(next http.Handler) http.Handler
	var pp []auth.AuthProvider
	// auth
	if s.config.Auth.DBApiKeysEnabled {
		pp = append(pp,
			auth.NewDBApiKey(s, "managed-api-keys", "x-hugr-api-key"),
		)
	}
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
	// Add cluster auth provider if cluster mode is enabled with a secret.
	if s.config.Cluster.Enabled && s.config.Cluster.Secret != "" {
		pp = append([]auth.AuthProvider{
			auth.NewApiKey("cluster-internal", auth.ApiKeyConfig{
				Key:         s.config.Cluster.Secret,
				Header:      "x-hugr-secret",
				DefaultRole: "admin",
			}),
		}, pp...)
	}
	pp = append(pp, s.config.Auth.Providers...)
	s.config.Auth.Providers = pp
	authMiddleware := auth.AuthMiddleware(*s.config.Auth)
	mm = append(mm, authMiddleware)
	// check endpoint access permissions
	if s.perm != nil {
		mm = append(mm, s.checkEndpointPermissionsMW)
	}
	// timezone
	mm = append(mm, timezoneMW)
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

func (s *Service) checkEndpointPermissionsMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := s.perm.ContextWithPermissions(r.Context())
		if errors.Is(err, auth.ErrForbidden) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(types.ErrResponse(err))
			return
		}
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(types.ErrResponse(err))
			return
		}
		perm := perm.PermissionsFromCtx(ctx)
		if perm == nil {
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		pp := strings.Split(r.URL.Path, "/")
		start := 0
		if strings.HasPrefix(r.URL.Path, "/") {
			start = 1
		}
		for i := start; i < len(pp); i++ {
			path := strings.Join(pp[:i], "/")
			if start == 0 {
				path = "/" + path
			}
			if _, ok := perm.Enabled("_endpoints", path); !ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				json.NewEncoder(w).Encode(types.ErrResponse(auth.ErrForbidden))
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func timezoneMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tz := r.Header.Get("X-Hugr-Timezone")
		if tz == "" {
			tz = r.Header.Get("Time-Zone")
		}
		if tz != "" {
			// Validate IANA timezone name to prevent injection.
			// If validation fails (e.g., missing tzdata), fall back to no timezone
			// rather than blocking the request.
			if _, err := time.LoadLocation(tz); err == nil {
				ctx := db.ContextWithTimezone(r.Context(), tz)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func compressMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip compression for WebSocket upgrades — the connection will be
		// hijacked and deferred Close() on gzip/brotli writers would write
		// to the hijacked connection causing "response.Write on hijacked connection".
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			next.ServeHTTP(w, r)
			return
		}
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

// Hijack implements http.Hijacker to allow WebSocket upgrades through compression middleware.
func (c *compressResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := c.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
}
