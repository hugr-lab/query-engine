package auth

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestBuildImpersonatedAuthInfo(t *testing.T) {
	original := &AuthInfo{
		Role: "admin", UserId: "api", UserName: "api",
		AuthType: "apiKey", AuthProvider: "x-hugr-secret",
	}
	result := BuildImpersonatedAuthInfo(original, "user-123", "John", "viewer")

	if result.Role != "viewer" {
		t.Errorf("Role = %q, want %q", result.Role, "viewer")
	}
	if result.UserId != "user-123" {
		t.Errorf("UserId = %q, want %q", result.UserId, "user-123")
	}
	if result.UserName != "John" {
		t.Errorf("UserName = %q, want %q", result.UserName, "John")
	}
	if result.AuthType != "impersonation" {
		t.Errorf("AuthType = %q, want %q", result.AuthType, "impersonation")
	}
	if result.AuthProvider != "x-hugr-secret" {
		t.Errorf("AuthProvider = %q, want %q", result.AuthProvider, "x-hugr-secret")
	}
	if result.ImpersonatedBy != original {
		t.Error("ImpersonatedBy should point to original AuthInfo")
	}
}

func TestIsImpersonated(t *testing.T) {
	tests := []struct {
		name    string
		auth    *AuthInfo
		wantImp bool
	}{
		{
			name: "impersonated",
			auth: &AuthInfo{
				Role: "viewer", AuthType: "impersonation",
				ImpersonatedBy: &AuthInfo{Role: "admin"},
			},
			wantImp: true,
		},
		{
			name: "not impersonated",
			auth: &AuthInfo{
				Role: "admin", AuthType: "apiKey",
			},
			wantImp: false,
		},
		{
			name:    "nil auth",
			wantImp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.auth != nil {
				ctx = ContextWithAuthInfo(ctx, tt.auth)
			}
			if got := IsImpersonated(ctx); got != tt.wantImp {
				t.Errorf("IsImpersonated = %v, want %v", got, tt.wantImp)
			}
		})
	}
}

func TestImpersonatedByFromContext(t *testing.T) {
	original := &AuthInfo{Role: "admin", UserId: "api", AuthType: "apiKey"}
	impersonated := &AuthInfo{
		Role: "viewer", UserId: "user-123", AuthType: "impersonation",
		ImpersonatedBy: original,
	}
	ctx := ContextWithAuthInfo(context.Background(), impersonated)

	impBy := ImpersonatedByFromContext(ctx)
	if impBy == nil {
		t.Fatal("expected ImpersonatedBy, got nil")
	}
	if impBy.Role != "admin" {
		t.Errorf("ImpersonatedBy.Role = %q, want %q", impBy.Role, "admin")
	}

	// No impersonation
	ctx2 := ContextWithAuthInfo(context.Background(), &AuthInfo{Role: "admin"})
	if ImpersonatedByFromContext(ctx2) != nil {
		t.Error("expected nil ImpersonatedBy for non-impersonated auth")
	}

	// Nil context
	if ImpersonatedByFromContext(context.Background()) != nil {
		t.Error("expected nil ImpersonatedBy for empty context")
	}
}

func TestApplyImpersonationHeaders(t *testing.T) {
	tests := []struct {
		name         string
		authInfo     *AuthInfo
		impRole      string
		impUserId    string
		impUserName  string
		wantImpBy    bool
		wantRole     string
		wantAuthType string
	}{
		{
			name: "with impersonation headers — applied",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			impRole:      "viewer",
			impUserId:    "user-123",
			impUserName:  "John",
			wantImpBy:    true,
			wantRole:     "viewer",
			wantAuthType: "impersonation",
		},
		{
			name: "no impersonation headers — unchanged",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			wantImpBy:    false,
			wantRole:     "admin",
			wantAuthType: "apiKey",
		},
		{
			name: "nested impersonation — ignored",
			authInfo: &AuthInfo{
				Role: "viewer", UserId: "user-123", UserName: "John",
				AuthType: "impersonation", AuthProvider: "x-hugr-secret",
				ImpersonatedBy: &AuthInfo{
					Role: "admin", UserId: "api", AuthType: "apiKey",
				},
			},
			impRole:      "another",
			impUserId:    "user-456",
			wantImpBy:    true,     // already impersonated
			wantRole:     "viewer", // unchanged
			wantAuthType: "impersonation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if tt.impRole != "" {
				req.Header.Set("x-hugr-impersonated-role", tt.impRole)
				req.Header.Set("x-hugr-impersonated-user-id", tt.impUserId)
				req.Header.Set("x-hugr-impersonated-user-name", tt.impUserName)
			}

			result := applyImpersonationHeaders(req, tt.authInfo)

			if result.Role != tt.wantRole {
				t.Errorf("Role = %q, want %q", result.Role, tt.wantRole)
			}
			if result.AuthType != tt.wantAuthType {
				t.Errorf("AuthType = %q, want %q", result.AuthType, tt.wantAuthType)
			}
			hasImpBy := result.ImpersonatedBy != nil
			if hasImpBy != tt.wantImpBy {
				t.Errorf("ImpersonatedBy present = %v, want %v", hasImpBy, tt.wantImpBy)
			}
		})
	}
}

func TestJwtIgnoresOverrideHeaders(t *testing.T) {
	provider, err := NewJwt(&JwtConfig{
		Issuer:    "test-issuer",
		PublicKey: rsaPubKey,
		Claims:    UserAuthInfoConfig{Role: "role", UserId: "sub", UserName: "name"},
	})
	if err != nil {
		t.Fatalf("failed to create JwtProvider: %v", err)
	}
	token, err := GenerateToken(rsaKey, jwt.MapClaims{
		"sub":  "jwt-user",
		"name": "JWT User",
		"role": "viewer",
		"exp":  float64(9999999999),
	})
	if err != nil {
		t.Fatalf("failed to generate token: %v", err)
	}
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	// Set override headers — JWT provider must ignore them
	req.Header.Set("x-hugr-role", "admin")
	req.Header.Set("x-hugr-user-id", "attacker")
	req.Header.Set("x-hugr-user-name", "Attacker")

	authInfo, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authInfo.Role != "viewer" {
		t.Errorf("role = %q, want %q — override headers should be ignored by JWT provider", authInfo.Role, "viewer")
	}
	if authInfo.UserId != "jwt-user" {
		t.Errorf("userId = %q, want %q — override headers should be ignored", authInfo.UserId, "jwt-user")
	}
}

func TestAnonymousIgnoresOverrideHeaders(t *testing.T) {
	provider := NewAnonymous(AnonymousConfig{
		Allowed: true,
		Role:    "public",
	})
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("x-hugr-role", "admin")
	req.Header.Set("x-hugr-user-id", "attacker")

	authInfo, err := provider.Authenticate(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if authInfo.Role != "public" {
		t.Errorf("role = %q, want %q — override headers should be ignored by anonymous provider", authInfo.Role, "public")
	}
	if authInfo.UserId != "anonymous" {
		t.Errorf("userId = %q, want %q", authInfo.UserId, "anonymous")
	}
}
