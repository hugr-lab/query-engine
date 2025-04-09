package auth

import (
	"net/http"
	"reflect"
	"testing"
)

func TestApiKeyProvider_Authenticate(t *testing.T) {
	tests := []struct {
		name     string
		config   ApiKeyConfig
		headers  map[string]string
		wantAuth *AuthInfo
		wantErr  error
	}{
		{
			name: "valid api key with default headers",
			config: ApiKeyConfig{
				Key:         "valid-api-key",
				DefaultRole: "user",
			},
			headers: map[string]string{
				"x-hugr-api-key":   "valid-api-key",
				"x-hugr-role":      "admin",
				"x-hugr-user-id":   "123",
				"x-hugr-user-name": "testuser",
			},
			wantAuth: &AuthInfo{
				Role:         "admin",
				UserId:       "123",
				UserName:     "testuser",
				AuthType:     "apiKey",
				AuthProvider: "testProvider",
			},
			wantErr: nil,
		},
		{
			name: "valid api key with all headers",
			config: ApiKeyConfig{
				Key:         "valid-api-key",
				Header:      "X-API-KEY",
				DefaultRole: "user",
				Headers: UserAuthInfoConfig{
					Role:     "X-Role",
					UserId:   "X-User-Id",
					UserName: "X-User-Name",
				},
			},
			headers: map[string]string{
				"X-API-KEY":   "valid-api-key",
				"X-Role":      "admin",
				"X-User-Id":   "123",
				"X-User-Name": "testuser",
			},
			wantAuth: &AuthInfo{
				Role:         "admin",
				UserId:       "123",
				UserName:     "testuser",
				AuthType:     "apiKey",
				AuthProvider: "testProvider",
			},
			wantErr: nil,
		},
		{
			name: "invalid api key",
			config: ApiKeyConfig{
				Key:         "valid-api-key",
				Header:      "X-API-KEY",
				DefaultRole: "user",
				Headers: UserAuthInfoConfig{
					Role:     "X-Role",
					UserId:   "X-User-Id",
					UserName: "X-User-Name",
				},
			},
			headers: map[string]string{
				"X-API-KEY": "invalid-api-key",
			},
			wantAuth: nil,
			wantErr:  ErrForbidden,
		},
		{
			name: "missing api key header",
			config: ApiKeyConfig{
				Key:         "valid-api-key",
				Header:      "X-API-KEY",
				DefaultRole: "user",
				Headers: UserAuthInfoConfig{
					Role:     "X-Role",
					UserId:   "X-User-Id",
					UserName: "X-User-Name",
				},
			},
			headers:  map[string]string{},
			wantAuth: nil,
			wantErr:  nil,
		},
		{
			name: "valid api key with missing optional headers",
			config: ApiKeyConfig{
				Key:         "valid-api-key",
				Header:      "X-API-KEY",
				DefaultRole: "user",
				Headers: UserAuthInfoConfig{
					Role:     "X-Role",
					UserId:   "X-User-Id",
					UserName: "X-User-Name",
				},
			},
			headers: map[string]string{
				"X-API-KEY": "valid-api-key",
			},
			wantAuth: &AuthInfo{
				Role:         "user",
				UserId:       "api",
				UserName:     "api",
				AuthType:     "apiKey",
				AuthProvider: "testProvider",
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := NewApiKey("testProvider", tt.config)
			req, _ := http.NewRequest("GET", "/", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			gotAuth, gotErr := provider.Authenticate(req)
			if gotErr != tt.wantErr {
				t.Errorf("Authenticate() error = %v, wantErr %v", gotErr, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotAuth, tt.wantAuth) {
				t.Errorf("Authenticate() gotAuth = %v, want %v", gotAuth, tt.wantAuth)
			}
		})
	}
}
