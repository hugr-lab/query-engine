package auth

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/hugr-lab/query-engine/types"
)

func TestApplyImpersonationCtx(t *testing.T) {
	tests := []struct {
		name        string
		authInfo    *AuthInfo
		asUser      *types.UserIdentity
		wantErr     bool
		wantRole    string
		wantImpBy   bool
		wantAuthTyp string
	}{
		{
			name: "admin with AsUser — impersonation applied",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			asUser:      &types.UserIdentity{UserId: "user-123", UserName: "John", Role: "viewer"},
			wantErr:     false,
			wantRole:    "viewer",
			wantImpBy:   true,
			wantAuthTyp: "impersonation",
		},
		{
			name: "no AsUser — no-op",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			asUser:      nil,
			wantErr:     false,
			wantRole:    "admin",
			wantImpBy:   false,
			wantAuthTyp: "apiKey",
		},
		{
			name: "OIDC user with AsUser — rejected",
			authInfo: &AuthInfo{
				Role: "user", UserId: "oidc-user", UserName: "OIDCUser",
				AuthType: "oidc", AuthProvider: "oidc",
			},
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "admin"},
			wantErr: true,
		},
		{
			name: "JWT user with AsUser — rejected",
			authInfo: &AuthInfo{
				Role: "user", UserId: "jwt-user", UserName: "JWTUser",
				AuthType: "jwt", AuthProvider: "https://issuer.example.com",
			},
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "admin"},
			wantErr: true,
		},
		{
			name: "anonymous with AsUser — rejected",
			authInfo: &AuthInfo{
				Role: "anonymous", UserId: "anonymous", UserName: "anonymous",
				AuthType: "anonymous", AuthProvider: "anonymous",
			},
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "admin"},
			wantErr: true,
		},
		{
			name: "db-api-key with AsUser — rejected",
			authInfo: &AuthInfo{
				Role: "user", UserId: "key-user", UserName: "KeyUser",
				AuthType: "db-api-key", AuthProvider: "db-api-key",
			},
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "admin"},
			wantErr: true,
		},
		{
			name: "cluster-internal with AsUser — rejected",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "cluster-internal",
			},
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "viewer"},
			wantErr: true,
		},
		{
			name:    "nil auth with AsUser — rejected",
			asUser:  &types.UserIdentity{UserId: "victim", UserName: "Victim", Role: "admin"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.authInfo != nil {
				ctx = ContextWithAuthInfo(ctx, tt.authInfo)
			}
			if tt.asUser != nil {
				ctx = types.AsUser(ctx, tt.asUser.UserId, tt.asUser.UserName, tt.asUser.Role)
			}

			got, err := ApplyImpersonationCtx(ctx)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ai := AuthInfoFromContext(got)
			if ai == nil {
				t.Fatal("expected AuthInfo in context")
			}
			if ai.Role != tt.wantRole {
				t.Errorf("role = %q, want %q", ai.Role, tt.wantRole)
			}
			if ai.AuthType != tt.wantAuthTyp {
				t.Errorf("authType = %q, want %q", ai.AuthType, tt.wantAuthTyp)
			}
			if tt.wantImpBy != IsImpersonated(got) {
				t.Errorf("IsImpersonated = %v, want %v", IsImpersonated(got), tt.wantImpBy)
			}
			if tt.wantImpBy {
				impBy := ImpersonatedByFromContext(got)
				if impBy == nil {
					t.Fatal("expected ImpersonatedBy in context")
				}
				if impBy.AuthProvider != "x-hugr-secret" {
					t.Errorf("impersonatedBy.AuthProvider = %q, want %q", impBy.AuthProvider, "x-hugr-secret")
				}
			}
		})
	}
}

func TestApplyImpersonationFromMessage(t *testing.T) {
	tests := []struct {
		name     string
		authInfo *AuthInfo
		userId   string
		wantErr  bool
	}{
		{
			name: "admin — allowed",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			userId: "user-123",
		},
		{
			name:   "empty userId — no-op",
			userId: "",
		},
		{
			name: "OIDC — rejected",
			authInfo: &AuthInfo{
				Role: "user", UserId: "oidc-user", AuthType: "oidc", AuthProvider: "oidc",
			},
			userId:  "victim",
			wantErr: true,
		},
		{
			name: "cluster — rejected",
			authInfo: &AuthInfo{
				Role: "admin", UserId: "api", AuthType: "apiKey", AuthProvider: "cluster-internal",
			},
			userId:  "victim",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.authInfo != nil {
				ctx = ContextWithAuthInfo(ctx, tt.authInfo)
			}

			got, err := ApplyImpersonationFromMessage(ctx, tt.userId, "Name", "viewer")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.userId != "" {
				ai := AuthInfoFromContext(got)
				if ai.UserId != tt.userId {
					t.Errorf("userId = %q, want %q", ai.UserId, tt.userId)
				}
				if ai.AuthType != "impersonation" {
					t.Errorf("authType = %q, want %q", ai.AuthType, "impersonation")
				}
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

func TestDetectImpersonation(t *testing.T) {
	tests := []struct {
		name    string
		auth    *AuthInfo
		wantImp bool
	}{
		{
			name: "secret key with overridden role",
			auth: &AuthInfo{
				Role: "viewer", UserId: "user-123", UserName: "John",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			wantImp: true,
		},
		{
			name: "secret key with defaults — no impersonation",
			auth: &AuthInfo{
				Role: "admin", UserId: "api", UserName: "api",
				AuthType: "apiKey", AuthProvider: "x-hugr-secret",
			},
			wantImp: false,
		},
		{
			name: "OIDC — no impersonation",
			auth: &AuthInfo{
				Role: "user", UserId: "oidc-user",
				AuthType: "oidc", AuthProvider: "oidc",
			},
			wantImp: false,
		},
		{
			name:    "nil auth — no impersonation",
			wantImp: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.auth != nil {
				ctx = ContextWithAuthInfo(ctx, tt.auth)
			}
			got := DetectImpersonation(ctx)
			if IsImpersonated(got) != tt.wantImp {
				t.Errorf("IsImpersonated = %v, want %v", IsImpersonated(got), tt.wantImp)
			}
		})
	}
}
