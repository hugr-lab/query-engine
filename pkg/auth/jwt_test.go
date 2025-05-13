package auth

import (
	_ "embed"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	//go:embed internal/fixture/rsa
	rsaKey []byte
	//go:embed internal/fixture/rsa.pub
	rsaPubKey []byte
	//go:embed internal/fixture/ed25519
	ed25519Key []byte
	//go:embed internal/fixture/ed25519.pub
	ed25519PubKey []byte
	//go:embed internal/fixture/ecdsa
	ecdsaKey []byte
	//go:embed internal/fixture/ecdsa.pub
	ecdsaPubKey []byte
)

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
