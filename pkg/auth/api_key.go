package auth

import "net/http"

type ApiKeyConfig struct {
	Key         string `json:"key" yaml:"key"`
	Header      string `json:"header" yaml:"header"`
	DefaultRole string `json:"default_role" yaml:"default-role"`

	Headers UserAuthInfoConfig `json:"headers" yaml:"headers"`
}

type ApiKeyProvider struct {
	name string
	c    ApiKeyConfig
}

func NewApiKey(name string, config ApiKeyConfig) *ApiKeyProvider {
	if config.Header == "" {
		config.Header = "x-hugr-api-key"
	}
	if config.DefaultRole == "" {
		config.DefaultRole = "admin"
	}
	if config.Headers.Role == "" {
		config.Headers.Role = "x-hugr-role"
	}
	if config.Headers.UserId == "" {
		config.Headers.UserId = "x-hugr-user-id"
	}
	if config.Headers.UserName == "" {
		config.Headers.UserName = "x-hugr-user-name"
	}
	return &ApiKeyProvider{
		c:    config,
		name: name,
	}
}

func (p *ApiKeyProvider) Name() string {
	return p.name
}

func (p *ApiKeyProvider) Type() string {
	return "apiKey"
}

func (p *ApiKeyProvider) Authenticate(r *http.Request) (*AuthInfo, error) {
	key := r.Header.Get(p.c.Header)
	if key == "" {
		return nil, ErrSkipAuth
	}
	if key != p.c.Key {
		return nil, ErrForbidden
	}

	role := r.Header.Get(p.c.Headers.Role)
	if role == "" {
		role = p.c.DefaultRole
	}

	userId := r.Header.Get(p.c.Headers.UserId)
	if userId == "" || p.c.Headers.UserId == "" {
		userId = "api"
	}

	userName := r.Header.Get(p.c.Headers.UserName)
	if userName == "" || p.c.Headers.UserName == "" {
		userName = "api"
	}

	return &AuthInfo{
		Role:         role,
		UserId:       userId,
		UserName:     userName,
		AuthType:     "apiKey",
		AuthProvider: p.name,
	}, nil
}
