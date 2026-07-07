package auth

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/hugr-lab/query-engine/types"
)

type DBApiKey struct {
	qe         types.Querier
	name       string
	headerName string
}

func NewDBApiKey(qe types.Querier, name, headerName string) *DBApiKey {
	if headerName == "" {
		headerName = "x-hugr-api-key"
	}
	return &DBApiKey{
		qe:         qe,
		name:       name,
		headerName: headerName,
	}
}

func (p *DBApiKey) Name() string {
	return "db-api-key"
}

func (p *DBApiKey) Type() string {
	return "db-api-key"
}

func (p *DBApiKey) Authenticate(r *http.Request) (*AuthInfo, error) {
	key := r.Header.Get(p.headerName)
	if key == "" {
		return nil, ErrSkipAuth
	}

	res, err := p.qe.Query(ContextWithFullAccess(r.Context()), `
		query ($key: String!, $cacheKey: String) {
			core {
				api_keys_by_key (
					key: $key
				) @cache(key: $cacheKey, tags: ["$AuthAPIKeys"]) {
				 	default_role
					headers
					claims
					disabled
					is_temporal
					expires_at
				}
			}
		}
	`,
		map[string]any{"key": key, "cacheKey": "AUTH:API_KEY:" + key},
	)
	if errors.Is(err, types.ErrNoData) {
		return nil, ErrForbidden
	}
	if err != nil {
		log.Println("querying api key:", err)
		return nil, ErrForbidden
	}
	defer res.Close()
	if res.Err() != nil {
		log.Println("querying api key:", res.Err())
		return nil, ErrForbidden
	}

	var keyInfo struct {
		DefaultRole string             `json:"default_role"`
		Disabled    bool               `json:"disabled"`
		IsTemporal  bool               `json:"is_temporal"`
		ExpiresAt   string             `json:"expires_at"`
		Headers     UserAuthInfoConfig `json:"headers"`
		// Claims is the api_keys.claims JSON column. Its "role"/"user_id"/
		// "user_name" keys set a static identity (as before); any other scalar
		// keys are exposed as [$auth.<claim>] placeholders, so a managed API
		// key can scope row-level security the same way a JWT claim does.
		Claims map[string]any `json:"claims"`
	}

	err = res.ScanData("core.api_keys_by_key", &keyInfo)
	if err != nil {
		log.Println("scanning api key:", err)
		return nil, ErrForbidden
	}
	if keyInfo.Disabled {
		return nil, ErrForbidden
	}
	if keyInfo.IsTemporal {
		if keyInfo.ExpiresAt == "" {
			return nil, ErrForbidden
		}
		expiredAt, err := time.Parse("2006-01-02 15:04:05.999999", keyInfo.ExpiresAt)
		if err != nil {
			log.Println("parsing api key expiration:", err)
			return nil, ErrForbidden
		}
		if expiredAt.IsZero() || expiredAt.Before(time.Now()) {
			return nil, ErrForbidden
		}
	}
	// identity carried directly on the key's claims (role/user_id/user_name)
	userInfo := UserAuthInfoConfig{
		Role:     claimString(keyInfo.Claims, "role"),
		UserId:   claimString(keyInfo.Claims, "user_id"),
		UserName: claimString(keyInfo.Claims, "user_name"),
	}

	role := keyInfo.DefaultRole
	if keyInfo.Headers.Role != "" {
		role = r.Header.Get(keyInfo.Headers.Role)
	}
	if userInfo.Role != "" {
		role = userInfo.Role
	}
	if keyInfo.Headers.UserId != "" {
		keyInfo.Headers.UserId = "x-hugr-user-id"
	}
	if keyInfo.Headers.UserName != "" {
		keyInfo.Headers.UserName = "x-hugr-user-name"
	}
	userId := r.Header.Get(keyInfo.Headers.UserId)
	if userInfo.UserId != "" {
		userId = userInfo.UserId
	}
	if userId == "" {
		userId = "anonymous"
	}

	userName := r.Header.Get(keyInfo.Headers.UserName)
	if userInfo.UserName != "" {
		userName = userInfo.UserName
	}
	if userName == "" {
		userName = "anonymous"
	}

	return &AuthInfo{
		UserId:       userId,
		UserName:     userName,
		Role:         role,
		AuthType:     "db-api-key",
		AuthProvider: p.name,
		Claims:       ScalarClaims(keyInfo.Claims),
	}, nil
}

// claimString returns the string value of a claim, or "" if absent/non-string.
func claimString(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}
