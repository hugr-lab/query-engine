package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

// TestAuthMiddleware_MultiProviderFallthrough covers the multi-provider chain:
// a JWT provider whose key does not match a presented token must not fail the
// whole request — the middleware falls through to the next provider. A token
// that no provider can verify is rejected (not downgraded to anonymous), and the
// anonymous fallback is always evaluated last regardless of config order.
func TestAuthMiddleware_MultiProviderFallthrough(t *testing.T) {
	rsaProvider, err := NewJwt(&JwtConfig{Issuer: "rsa", PublicKey: rsaPubKey})
	if err != nil {
		t.Fatalf("NewJwt rsa: %v", err)
	}
	ecdsaProvider, err := NewJwt(&JwtConfig{Issuer: "ecdsa", PublicKey: ecdsaPubKey})
	if err != nil {
		t.Fatalf("NewJwt ecdsa: %v", err)
	}
	anon := NewAnonymous(AnonymousConfig{Allowed: true, Role: "guest"})

	ecdsaToken, err := GenerateToken(ecdsaKey, jwt.MapClaims{"sub": "u1", "x-hugr-role": "admin"})
	if err != nil {
		t.Fatalf("gen ecdsa token: %v", err)
	}
	rsaToken, err := GenerateToken(rsaKey, jwt.MapClaims{"sub": "u2", "x-hugr-role": "admin"})
	if err != nil {
		t.Fatalf("gen rsa token: %v", err)
	}

	newServer := func(providers ...AuthProvider) http.Handler {
		mw := AuthMiddleware(Config{Providers: providers})
		return mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ai := AuthInfoFromContext(r.Context())
			if ai == nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("X-Auth-Type", ai.AuthType)
			w.Header().Set("X-Role", ai.Role)
			w.WriteHeader(http.StatusOK)
		}))
	}
	do := func(h http.Handler, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	t.Run("falls through to the provider whose key matches", func(t *testing.T) {
		rec := do(newServer(rsaProvider, ecdsaProvider), ecdsaToken)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (RSA provider must fall through to ECDSA)", rec.Code)
		}
		if got := rec.Header().Get("X-Auth-Type"); got != "jwt" {
			t.Fatalf("auth type = %q, want jwt", got)
		}
		if got := rec.Header().Get("X-Role"); got != "admin" {
			t.Fatalf("role = %q, want admin", got)
		}
	})

	t.Run("present-but-invalid token is rejected, not downgraded to anonymous", func(t *testing.T) {
		rec := do(newServer(rsaProvider, anon), ecdsaToken)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401 (invalid token must not become anonymous)", rec.Code)
		}
	})

	t.Run("no token falls back to anonymous", func(t *testing.T) {
		rec := do(newServer(rsaProvider, anon), "")
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (no token -> anonymous)", rec.Code)
		}
		if got := rec.Header().Get("X-Auth-Type"); got != "anonymous" {
			t.Fatalf("auth type = %q, want anonymous", got)
		}
	})

	t.Run("anonymous is evaluated last regardless of config order", func(t *testing.T) {
		// anonymous is FIRST in config, but a valid RSA token must still win.
		rec := do(newServer(anon, rsaProvider), rsaToken)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("X-Auth-Type"); got != "jwt" {
			t.Fatalf("auth type = %q, want jwt (anonymous must not win over a valid token)", got)
		}
	})
}
