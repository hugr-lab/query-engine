package gis

import (
	"net/http"
	"strings"
)

func baseURL(r *http.Request) string {
	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	if xfproto := r.Header.Get("X-Forwarded-Proto"); xfproto != "" {
		proto = xfproto
	}

	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}

	prefix := r.Header.Get("X-Forwarded-Prefix") // for example: /hugr or /api/wfs
	if prefix == "" {
		// fallback: base path by URL (trim all after last /)
		prefix = basePath(r.URL.Path)
	}

	return strings.TrimSuffix(proto+"://"+host+prefix, "/")
}

// basePath returns the base path, for example: /hugr/api → /hugr
func basePath(path string) string {
	if path == "" || path == "/" {
		return ""
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) <= 1 {
		return "/" + parts[0]
	}
	return "/" + parts[0]
}
