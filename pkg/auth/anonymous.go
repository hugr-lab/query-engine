package auth

import "net/http"

type AnonymousConfig struct {
	Allowed bool   `json:"allowed" yaml:"allowed"`
	Role    string `json:"role" yaml:"role"`
}

type AnonymousProvider struct {
	Config AnonymousConfig
}

func NewAnonymous(config AnonymousConfig) *AnonymousProvider {
	return &AnonymousProvider{
		Config: config,
	}
}

func (p *AnonymousProvider) Authenticate(r *http.Request) (*AuthInfo, error) {
	if !p.Config.Allowed {
		return nil, nil
	}
	return &AuthInfo{
		Role:         p.Config.Role,
		UserId:       "anonymous",
		UserName:     "anonymous",
		AuthType:     "anonymous",
		AuthProvider: "anonymous",
	}, nil
}
