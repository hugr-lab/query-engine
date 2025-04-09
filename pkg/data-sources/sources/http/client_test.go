package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"

	_ "embed"
)

func Test_customTokenRequest(t *testing.T) {
	// generate tokens for testing

	testServer := httptest.NewServer(&testServer{})
	defer testServer.Close()

	tests := []struct {
		name      string
		tokenUrl  string
		data      any
		param     *tokenRequestTransform
		sp        httpSecurityParams
		wantToken *oauth2.Token
		wantErr   bool
	}{
		{
			name:     "valid post token response",
			tokenUrl: "/custom_login",
			sp:       httpSecurityParams{Username: "user", Password: "pass"},
			data:     httpSecurityParams{Username: "user", Password: "pass"},
			param: &tokenRequestTransform{
				Method:   http.MethodPost,
				Headers:  map[string]string{"Content-Type": "application/x-www-form-urlencoded"},
				Body:     `{login: $username, password: $password}`,
				Response: `{access_token: .accessToken, refresh_token: .refreshToken, token_type: "bearer", "expires_in": 3600}`,
			},
			wantToken: &oauth2.Token{
				AccessToken:  testToken,
				RefreshToken: testRefreshToken,
				TokenType:    "bearer",
				Expiry:       time.Now().Add(3600 * time.Second),
			},
			wantErr: false,
		},
		{
			name:     "valid get token response",
			tokenUrl: "/custom_login_get",
			sp:       httpSecurityParams{Username: "user", Password: "pass"},
			param: &tokenRequestTransform{
				Method:   http.MethodGet,
				Params:   map[string]string{"login": "$username", "password": "$password"},
				Response: `{access_token: .accessToken, refresh_token: .refreshToken, token_type: "bearer", "expires_in": 3600}`,
			},
			wantToken: &oauth2.Token{
				AccessToken:  testToken,
				RefreshToken: testRefreshToken,
				TokenType:    "bearer",
				Expiry:       time.Now().Add(3600 * time.Second),
			},
			wantErr: false,
		},
		{
			name:     "valid refresh response",
			tokenUrl: "/custom_refresh",
			data:     map[string]string{"refresh_token": testRefreshToken},
			param: &tokenRequestTransform{
				Method:   http.MethodPost,
				Body:     `{refreshToken: .refresh_token}`,
				Response: `{access_token: .accessToken, refresh_token: .refreshToken, token_type: "bearer", "expires_in": 3600}`,
			},
			wantToken: &oauth2.Token{
				AccessToken:  testToken,
				RefreshToken: testRefreshToken,
				TokenType:    "bearer",
				Expiry:       time.Now().Add(3600 * time.Second),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.tokenUrl = testServer.URL + tt.tokenUrl
			if tt.param.Body != "" || tt.param.Response != "" {
				err := tt.param.parse(tt.sp)
				if err != nil {
					t.Fatalf("parse error: %v", err)
				}
			}

			got, err := customTokenRequest(context.Background(), tt.tokenUrl, tt.data, tt.param)
			if tt.wantErr != (err != nil) {
				t.Errorf("customTokenRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			if got.AccessToken != tt.wantToken.AccessToken ||
				got.RefreshToken != tt.wantToken.RefreshToken ||
				got.TokenType != tt.wantToken.TokenType ||
				!got.Expiry.After(tt.wantToken.Expiry) {
				t.Errorf("customTokenRequest() = %v, want %v", got, tt.wantToken)
			}

		})
	}
}
