package auth

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type Config struct {
	Providers []AuthProvider

	RedirectLoginPaths []string
	LoginUrl           string
	RedirectUrl        string
	DBApiKeysEnabled   bool
}

type ProviderInfo struct {
	Name string `json:"name" yaml:"name"`
	Type string `json:"type" yaml:"type"`
}

func (c *Config) Info() []ProviderInfo {
	providers := make([]ProviderInfo, len(c.Providers))
	for i, p := range c.Providers {
		providers[i] = ProviderInfo{
			Name: p.Name(),
			Type: p.Type(),
		}
	}
	return providers
}

type UserAuthInfoConfig struct {
	Role     string `json:"role" yaml:"role"`
	UserId   string `json:"user_id" yaml:"user-id"`
	UserName string `json:"user_name" yaml:"user-name"`
}

type AuthProvider interface {
	Authenticate(r *http.Request) (*AuthInfo, error)
	Name() string
	Type() string
}

var ErrSkipAuth = errors.New("skip authentication")
var ErrForbidden = errors.New("forbidden")
var ErrTokenExpired = errors.New("token expired")
var ErrNeedAuth = errors.New("authentication required")

// ErrInvalidKeyType is returned by a provider when a token is present but is not
// verifiable with that provider's key (wrong signing algorithm/key) — i.e. it was
// most likely issued for a different provider. The middleware treats it as a
// signal to try the next provider. Unlike ErrSkipAuth (no token at all), it also
// records that a token was rejected, so a failed authentication attempt is not
// silently downgraded to anonymous access.
var ErrInvalidKeyType = errors.New("invalid key type")

// applyImpersonationHeaders checks for x-hugr-impersonated-* headers on the request.
// If present, wraps the original AuthInfo as ImpersonatedBy and sets the target identity.
func applyImpersonationHeaders(r *http.Request, original *AuthInfo) *AuthInfo {
	targetRole := r.Header.Get("x-hugr-impersonated-role")
	if targetRole == "" {
		return original
	}
	// Don't allow nested impersonation
	if original.ImpersonatedBy != nil {
		return original
	}
	targetUserId := r.Header.Get("x-hugr-impersonated-user-id")
	targetUserName := r.Header.Get("x-hugr-impersonated-user-name")
	return BuildImpersonatedAuthInfo(original, targetUserId, targetUserName, targetRole)
}

// Provide middleware for authentication
// Checks if api key allowed or token is valid
// Get user and role from headers or token
// if request is anonymous, check if it allowed and add role
func AuthMiddleware(c Config) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			var authInfo *AuthInfo
			var anonProvider AuthProvider // anonymous fallback is always evaluated last
			tokenRejected := false        // a token was presented but no provider could verify it
			for _, p := range c.Providers {
				// Defer the anonymous provider to the end so real providers
				// always get a chance first, regardless of config order.
				if _, ok := p.(*AnonymousProvider); ok {
					if anonProvider == nil {
						anonProvider = p // keep the first anonymous provider (config order)
					}
					continue
				}
				authInfo, err = p.Authenticate(r)
				if errors.Is(err, ErrSkipAuth) {
					// Skip authentication for this provider
					continue
				}
				if errors.Is(err, ErrInvalidKeyType) {
					// Token present but not signed for this provider — try the
					// next one, but remember it so a rejected token is never
					// downgraded to anonymous access.
					tokenRejected = true
					continue
				}
				if errors.Is(err, ErrTokenExpired) {
					http.Error(w, err.Error(), http.StatusUnauthorized)
					return
				}
				if errors.Is(err, ErrNeedAuth) {
					tokenRejected = true
					break
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusUnauthorized)
					return
				}
				if authInfo != nil {
					break
				}
			}
			// Anonymous fallback — evaluated last, and only when no token was
			// rejected (a present-but-invalid token must not become anonymous).
			if authInfo == nil && !tokenRejected && anonProvider != nil {
				authInfo, err = anonProvider.Authenticate(r)
			}
			if err == nil && authInfo != nil {
				// Check for explicit impersonation headers
				authInfo = applyImpersonationHeaders(r, authInfo)
				r = r.WithContext(ContextWithAuthInfo(r.Context(), authInfo))
				next.ServeHTTP(w, r)
				return
			}
			for _, path := range c.RedirectLoginPaths {
				if strings.HasSuffix(r.URL.Path, path) {
					loginUrl := c.LoginUrl
					if c.RedirectUrl != "" {
						redirectUrl, err := url.JoinPath(c.RedirectUrl, r.URL.String())
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}
						redirectUrl = url.QueryEscape(redirectUrl)
						loginUrl = fmt.Sprintf("%s?redirect_uri=%s", c.LoginUrl, redirectUrl)
					}
					http.Redirect(w, r, loginUrl, http.StatusFound)
					return
				}
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		})
	}
}
