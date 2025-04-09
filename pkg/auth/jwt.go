package auth

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/golang-jwt/jwt/v5/request"
	"golang.org/x/crypto/ssh"
)

type JwtConfig struct {
	Issuer    string `json:"issuer" yaml:"issuer"`
	PublicKey []byte `json:"public_key" yaml:"public-key"`

	CookieName string `json:"cookie_name" yaml:"cookie-name"`

	ScopeRolePrefix string `json:"scope_role_prefix" yaml:"scope-role-prefix"`
	// RoleHeader is the header to check for role if not in claims than check that scope contains prefix+role (if the many roles a)
	RoleHeader string             `json:"role_header" yaml:"role-header"`
	Claims     UserAuthInfoConfig `json:"claims" yaml:"claims"`
}

type JwtProvider struct {
	Issuer string

	key       any
	extractor request.Extractor

	c *JwtConfig
}

func NewJwt(config *JwtConfig) (*JwtProvider, error) {
	if config.Claims.Role == "" {
		config.Claims.Role = "x-hugr-role"
	}
	if config.Claims.UserId == "" {
		config.Claims.UserId = "sub"
	}
	if config.Claims.UserName == "" {
		config.Claims.UserName = "name"
	}
	if config.ScopeRolePrefix == "" {
		config.ScopeRolePrefix = "hugr:"
	}

	p := &JwtProvider{
		c:      config,
		Issuer: config.Issuer,
	}

	pubKey, err := parsePublicKey(config.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}
	p.key = pubKey

	p.extractor = request.OAuth2Extractor
	if config.CookieName != "" {
		p.extractor = request.MultiExtractor{
			request.OAuth2Extractor,
			cookieExtractor(config.CookieName),
		}
	}

	return p, nil
}

func (p *JwtProvider) Authenticate(r *http.Request) (*AuthInfo, error) {
	var claims jwt.MapClaims
	t, err := request.ParseFromRequest(r, p.extractor, func(token *jwt.Token) (interface{}, error) {
		return p.key, nil
	}, request.WithClaims(&claims))
	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}
	if !t.Valid {
		return nil, ErrNeedAuth
	}

	role, _ := claims[p.c.Claims.Role].(string)
	userId, _ := claims[p.c.Claims.UserId].(string)
	userName, _ := claims[p.c.Claims.UserName].(string)

	// check scopes if role is empty
	if role == "" {
		role = r.Header.Get(p.c.RoleHeader)
		scopes, ok := claims["scopes"].([]any)
		if ok {
			for _, scope := range scopes {
				if s, ok := scope.(string); ok {
					if strings.HasPrefix(s, p.c.ScopeRolePrefix) {
						if role == "" || strings.HasSuffix(s, role) {
							role = strings.TrimPrefix(s, p.c.ScopeRolePrefix)
							break
						}
					}
				}
			}
		}
	}

	return &AuthInfo{
		Role:         role,
		UserId:       userId,
		UserName:     userName,
		AuthType:     "jwt",
		AuthProvider: p.c.Issuer,
	}, nil
}

type cookieExtractor string

func (c cookieExtractor) ExtractToken(r *http.Request) (string, error) {
	cookie, err := r.Cookie(string(c))
	if err != nil {
		return "", err
	}
	return cookie.Value, nil
}

func parsePublicKey(key []byte) (interface{}, error) {
	pubKey, _, _, _, err := ssh.ParseAuthorizedKey(key)
	if err == nil {
		parsedKey, ok := pubKey.(ssh.CryptoPublicKey)
		if !ok {
			return nil, fmt.Errorf("unsupported key type")
		}
		key, err = x509.MarshalPKIXPublicKey(parsedKey.CryptoPublicKey())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal key to PKIX: %w", err)
		}
		key = pem.EncodeToMemory(&pem.Block{
			Type:  "PUBLIC KEY",
			Bytes: key,
		})
	}
	block, _ := pem.Decode(key)
	if block == nil {
		return nil, fmt.Errorf("failed to parse PEM block")
	}

	if pubKey, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		return pubKey, nil
	}
	return x509.ParsePKCS1PublicKey(block.Bytes)
}
