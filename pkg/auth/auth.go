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

var ErrForbidden = errors.New("forbidden")
var ErrNeedAuth = errors.New("authentication required")

// Provide middleware for authentication
// Checks if api key allowed or token is valid
// Get user and role from headers or token
// if request is anonymous, check if it allowed and add role
func AuthMiddleware(c Config) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			var authInfo *AuthInfo
			for _, p := range c.Providers {
				authInfo, err = p.Authenticate(r)
				if errors.Is(err, ErrNeedAuth) {
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
			if err == nil && authInfo != nil {
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
