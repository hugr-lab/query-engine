package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"gopkg.in/yaml.v2"
)

type AuthConfig struct {
	AllowedAnonymous bool
	AnonymousRole    string

	// API Key with default admin role should be provided in the header x-hugr-secret-key
	SecretKey string

	ConfigFile string
}

func (c *AuthConfig) Configure() (*auth.Config, error) {
	config := &auth.Config{}
	if c.ConfigFile != "" {
		pc, err := loadAuthProviderConfigFile(c.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load auth config file: %w", err)
		}
		for name, apiKeyConfig := range pc.APIKeys {
			config.Providers = append(config.Providers,
				auth.NewApiKey(name, apiKeyConfig),
			)
		}
		for _, jwtConfig := range pc.JWT {
			jwtProvider, err := auth.NewJwt(&jwtConfig)
			if err != nil {
				return nil, fmt.Errorf("failed to create jwt provider: %w", err)
			}
			config.Providers = append(config.Providers, jwtProvider)
		}
		if pc.Anonymous.Allowed {
			c.AllowedAnonymous = true
			c.AnonymousRole = pc.Anonymous.Role
		}
		if pc.SecretKey != "" {
			c.SecretKey = pc.SecretKey
		}
	}

	if c.SecretKey != "" {
		config.Providers = append(config.Providers,
			auth.NewApiKey("x-hugr-secret", auth.ApiKeyConfig{
				Key:         c.SecretKey,
				Header:      "x-hugr-secret-key",
				DefaultRole: "admin",
				Headers: auth.UserAuthInfoConfig{
					Role:     "x-hugr-role",
					UserId:   "x-hugr-user-id",
					UserName: "x-hugr-user-name",
				},
			}),
		)
	}

	if c.AllowedAnonymous {
		config.Providers = append(config.Providers,
			auth.NewAnonymous(auth.AnonymousConfig{
				Allowed: true,
				Role:    c.AnonymousRole,
			}),
		)
	}

	if len(config.Providers) == 0 {
		return nil, nil
	}
	return config, nil
}

type providersConfig struct {
	Anonymous auth.AnonymousConfig         `json:"anonymous" yaml:"anonymous"`
	APIKeys   map[string]auth.ApiKeyConfig `json:"api_keys" yaml:"api-keys"`
	JWT       map[string]auth.JwtConfig    `json:"jwt" yaml:"jwt"`

	RedirectLoginPaths []string `json:"redirect_login_paths" yaml:"redirect-login-paths"`
	LoginUrl           string   `json:"login_url" yaml:"login-url"`
	RedirectUrl        string   `json:"redirect_url" yaml:"redirect-url"`
	SecretKey          string   `json:"secret_key" yaml:"secret-key"`
}

func loadAuthProviderConfigFile(configFile string) (c *providersConfig, err error) {
	b, err := os.ReadFile(configFile)
	if err != nil {
		return nil, err
	}
	switch {
	case strings.HasSuffix(configFile, ".json"):
		err = json.Unmarshal(b, c)
	case strings.HasSuffix(configFile, ".yaml"):
		err = yaml.Unmarshal(b, c)
	default:
		return nil, fmt.Errorf("unsupported config file format: %s", configFile)
	}
	return c, err
}

func printAuthSummary(c *auth.Config) {
	log.Printf("Auth: Number of providers: %d", len(c.Providers))
	for i, p := range c.Providers {
		switch v := p.(type) {
		case *auth.ApiKeyProvider:
			if v.Name() == "x-hugr-secret" {
				log.Printf("Provider %d: Type: Secret", i)
				continue
			}
			log.Printf("Auth: Provider %d: Type: APIKey, Name: %s", i, v.Name())
		case *auth.JwtProvider:
			log.Printf("Auth: Provider %d: Type: JWT, Issuer: %s", i, v.Issuer)
		case *auth.AnonymousProvider:
			log.Printf("Auth: Provider %d: Type: Anonymous, Allowed: %t, Role: %s", i, v.Config.Allowed, v.Config.Role)
		default:
			log.Printf("Auth: Provider %d: Type: %T", i, v)
		}
	}
	if c.LoginUrl != "" {
		log.Printf("Auth: LoginUrl: %+v", c.LoginUrl)
	}
	if c.RedirectUrl != "" {
		log.Printf("Auth: RedirectUrl: %+v", c.RedirectUrl)
	}
	if len(c.RedirectLoginPaths) != 0 {
		log.Printf("Auth: RedirectLoginPaths: %+v", c.RedirectLoginPaths)
	}
}
