package auth

import (
	_ "embed"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	yaml "gopkg.in/yaml.v2"
)

// A validly-signed but expired token must surface as ErrTokenExpired (clear 401),
// not a generic parse error or a fallthrough to the next provider.
func TestJwtProvider_ExpiredToken(t *testing.T) {
	p, err := NewJwt(&JwtConfig{Issuer: "rsa", PublicKey: rsaPubKey})
	if err != nil {
		t.Fatalf("NewJwt: %v", err)
	}
	tok, err := GenerateToken(rsaKey, jwt.MapClaims{"sub": "u", "x-hugr-role": "admin", "exp": 1})
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	if _, err := p.Authenticate(req); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("err = %v, want ErrTokenExpired", err)
	}
}

var (
	//go:embed internal/fixture/rsa
	rsaKey []byte
	//go:embed internal/fixture/rsa.pub
	rsaPubKey string
	//go:embed internal/fixture/ed25519
	ed25519Key []byte
	//go:embed internal/fixture/ed25519.pub
	ed25519PubKey string
	//go:embed internal/fixture/ecdsa
	ecdsaKey []byte
	//go:embed internal/fixture/ecdsa.pub
	ecdsaPubKey string
)

// Regression: a PEM public key must parse from a plain YAML block scalar.
// PublicKey used to be []byte, which YAML cannot decode from a multi-line PEM
// string (it would need a !!binary/base64 value), so JWT provider configs
// failed to load. It is now a string. hugr loads this config via yaml, and a
// string field parses identically across yaml.v2/v3 and JSON.
func TestJwtConfig_YAML_PublicKeyBlockScalar(t *testing.T) {
	var b strings.Builder
	b.WriteString("issuer: https://issuer.example\npublic-key: |\n")
	for line := range strings.SplitSeq(strings.TrimRight(rsaPubKey, "\n"), "\n") {
		b.WriteString("  ")
		b.WriteString(line)
		b.WriteByte('\n')
	}

	var cfg JwtConfig
	if err := yaml.Unmarshal([]byte(b.String()), &cfg); err != nil {
		t.Fatalf("unmarshal JwtConfig from YAML: %v", err)
	}
	if cfg.Issuer != "https://issuer.example" {
		t.Fatalf("issuer = %q, want https://issuer.example", cfg.Issuer)
	}
	if strings.TrimSpace(cfg.PublicKey) != strings.TrimSpace(rsaPubKey) {
		t.Fatalf("public key not parsed from YAML block scalar")
	}
	if _, err := NewJwt(&cfg); err != nil {
		t.Fatalf("NewJwt with YAML-loaded public key: %v", err)
	}
}

// Cluster deployments may deliver the public key base64-encoded (config
// serialized between nodes, a Kubernetes secret, an env var) rather than as a
// raw PEM string. parsePublicKey must accept both.
func TestJwtConfig_PublicKey_Base64Delivered(t *testing.T) {
	cases := map[string]*base64.Encoding{
		"std": base64.StdEncoding,
		"raw": base64.RawStdEncoding,
	}
	for name, enc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := JwtConfig{Issuer: "https://issuer.example", PublicKey: enc.EncodeToString([]byte(rsaPubKey))}
			if _, err := NewJwt(&cfg); err != nil {
				t.Fatalf("NewJwt with %s-base64 public key: %v", name, err)
			}
		})
	}
	// Raw PEM must still work.
	if _, err := NewJwt(&JwtConfig{Issuer: "https://issuer.example", PublicKey: rsaPubKey}); err != nil {
		t.Fatalf("NewJwt with PEM public key: %v", err)
	}
}

// The JWT provider must carry the token's scalar claims through on AuthInfo so
// they are exposed as [$auth.<claim>] placeholders; nested/array claims are
// dropped.
func TestJwtProvider_CustomClaims(t *testing.T) {
	p, err := NewJwt(&JwtConfig{Issuer: "rsa", PublicKey: rsaPubKey})
	if err != nil {
		t.Fatalf("NewJwt: %v", err)
	}
	tok, err := GenerateToken(rsaKey, jwt.MapClaims{
		"sub":           "u",
		"x-hugr-role":   "admin",
		"tenant_id":     "acme",
		"department_id": 7,
		"nested":        map[string]any{"x": 1}, // must be dropped
		"groups":        []any{"a", "b"},        // must be dropped
		"exp":           time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	ai, err := p.Authenticate(req)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if ai.Claims["tenant_id"] != "acme" {
		t.Errorf("tenant_id claim = %v, want acme", ai.Claims["tenant_id"])
	}
	if _, ok := ai.Claims["nested"]; ok {
		t.Error("nested object claim must be dropped")
	}
	if _, ok := ai.Claims["groups"]; ok {
		t.Error("array claim must be dropped")
	}
}

func TestJwtProvider_Authenticate(t *testing.T) {
	tests := []struct {
		name       string
		config     *JwtConfig
		privateKey []byte
		claims     jwt.MapClaims
		headers    map[string]string
		cookie     *http.Cookie
		wantRole   string
		wantErr    bool
	}{
		{
			name: "RSA token",
			config: &JwtConfig{
				Issuer:    "test-issuer",
				PublicKey: rsaPubKey,
				Claims:    UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
			},
			privateKey: rsaKey,
			claims: jwt.MapClaims{
				"sub":  "user1",
				"name": "User One",
				"role": "admin",
				"exp":  time.Now().Add(time.Hour).Unix(),
			},
			wantRole: "admin",
		},
		{
			name: "ECDSA token",
			config: &JwtConfig{
				Issuer:    "test-issuer",
				PublicKey: ecdsaPubKey,
				Claims:    UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
			},
			privateKey: ecdsaKey,
			claims: jwt.MapClaims{
				"sub":  "user2",
				"name": "User Two",
				"role": "user",
				"exp":  time.Now().Add(time.Hour).Unix(),
			},
			wantRole: "user",
		},
		{
			name: "Ed25519 token",
			config: &JwtConfig{
				Issuer:    "test-issuer",
				PublicKey: ed25519PubKey,
				Claims:    UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
			},
			privateKey: ed25519Key,
			claims: jwt.MapClaims{
				"sub":  "user3",
				"name": "User Three",
				"role": "guest",
				"exp":  time.Now().Add(time.Hour).Unix(),
			},
			wantRole: "guest",
		},
		{
			name: "Token in cookie",
			config: &JwtConfig{
				Issuer:     "test-issuer",
				PublicKey:  rsaPubKey,
				CookieName: "auth_token",
				Claims:     UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
			},
			privateKey: rsaKey,
			claims: jwt.MapClaims{
				"sub":  "user4",
				"name": "User Four",
				"role": "member",
				"exp":  time.Now().Add(time.Hour).Unix(),
			},
			cookie:   &http.Cookie{Name: "auth_token", Value: ""},
			wantRole: "member",
		},
		{
			name: "Role from scopes",
			config: &JwtConfig{
				Issuer:          "test-issuer",
				PublicKey:       rsaPubKey,
				ScopeRolePrefix: "role:",
				Claims:          UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
			},
			privateKey: rsaKey,
			claims: jwt.MapClaims{
				"sub":    "user5",
				"name":   "User Five",
				"scopes": []any{"role:admin", "role:user"},
				"exp":    time.Now().Add(time.Hour).Unix(),
			},
			wantRole: "admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := GenerateToken(tt.privateKey, tt.claims)
			if err != nil {
				t.Fatalf("failed to generate token: %v", err)
			}

			if tt.cookie != nil {
				tt.cookie.Value = token
			}

			req := httptest.NewRequest("GET", "/", nil)
			if tt.cookie != nil {
				req.AddCookie(tt.cookie)
			} else {
				req.Header.Set("Authorization", "Bearer "+token)
			}

			provider, err := NewJwt(tt.config)
			if err != nil {
				t.Fatalf("failed to create JwtProvider: %v", err)
			}

			authInfo, err := provider.Authenticate(req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Authenticate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && authInfo.Role != tt.wantRole {
				t.Errorf("Authenticate() role = %v, want %v", authInfo.Role, tt.wantRole)
			}
		})
	}
}
