package gis

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
)

func Test_baseURL(t *testing.T) {
	tests := []struct {
		name       string
		tls        bool
		headers    map[string]string
		host       string
		urlPath    string
		wantPrefix string
		wantProto  string
		wantHost   string
		want       string
	}{
		{
			name:    "no headers, http, root path",
			tls:     false,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "/",
			want:    "http://example.com",
		},
		{
			name:    "with TLS, no headers, root path",
			tls:     true,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "/",
			want:    "https://example.com",
		},
		{
			name: "X-Forwarded-Proto and Host, with prefix",
			tls:  false,
			headers: map[string]string{
				"X-Forwarded-Proto":  "https",
				"X-Forwarded-Host":   "proxy.com",
				"X-Forwarded-Prefix": "/api/v1",
			},
			host:    "example.com",
			urlPath: "/api/v1/folder/resource",
			want:    "https://proxy.com/api/v1",
		},
		{
			name: "X-Forwarded-Proto and Host, no prefix",
			tls:  false,
			headers: map[string]string{
				"X-Forwarded-Proto": "https",
				"X-Forwarded-Host":  "proxy.com",
			},
			host:    "example.com",
			urlPath: "/hugr/api",
			want:    "https://proxy.com/hugr",
		},
		{
			name: "X-Forwarded-Host only, no prefix",
			tls:  false,
			headers: map[string]string{
				"X-Forwarded-Host": "proxy.com",
			},
			host:    "example.com",
			urlPath: "/hugr/api",
			want:    "http://proxy.com/hugr",
		},
		{
			name:    "no forwarded headers, subpath",
			tls:     false,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "/hugr/api",
			want:    "http://example.com/hugr",
		},
		{
			name: "prefix header with trailing slash",
			tls:  false,
			headers: map[string]string{
				"X-Forwarded-Prefix": "/api/",
			},
			host:    "example.com",
			urlPath: "/api/resource",
			want:    "http://example.com/api",
		},
		{
			name:    "empty path",
			tls:     false,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "",
			want:    "http://example.com",
		},
		{
			name:    "single segment path",
			tls:     false,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "/foo",
			want:    "http://example.com/foo",
		},
		{
			name:    "multi-segment path",
			tls:     false,
			headers: map[string]string{},
			host:    "example.com",
			urlPath: "/foo/bar/baz",
			want:    "http://example.com/foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{
				Header: make(http.Header),
				Host:   tt.host,
				URL:    &url.URL{Path: tt.urlPath},
			}
			if tt.tls {
				req.TLS = &tls.ConnectionState{}
			}
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			got := baseURL(req)
			if got != tt.want {
				t.Errorf("baseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
