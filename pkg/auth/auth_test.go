package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockAuthProvider struct {
	authInfo *AuthInfo
	err      error
}

func (m *mockAuthProvider) Name() string {
	return "mock"
}

func (m *mockAuthProvider) Type() string {
	return "mock"
}

func (m *mockAuthProvider) Authenticate(r *http.Request) (*AuthInfo, error) {
	return m.authInfo, m.err
}

func TestAuthMiddleware(t *testing.T) {
	tests := []struct {
		name               string
		config             Config
		request            *http.Request
		expectedStatusCode int
	}{
		{
			name: "Authorized request",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: &AuthInfo{},
						err:      nil,
					},
				},
			},
			request:            httptest.NewRequest("GET", "/", nil),
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "Unauthorized request",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: nil,
						err:      ErrNeedAuth,
					},
				},
			},
			request:            httptest.NewRequest("GET", "/", nil),
			expectedStatusCode: http.StatusUnauthorized,
		},
		{
			name: "Redirect to login",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: nil,
						err:      ErrNeedAuth,
					},
				},
				RedirectLoginPaths: []string{"/login"},
				LoginUrl:           "http://example.com/login",
			},
			request:            httptest.NewRequest("GET", "/login", nil),
			expectedStatusCode: http.StatusFound,
		},
		{
			name: "Authentication by second provider",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: nil,
						err:      nil,
					},
					&mockAuthProvider{
						authInfo: &AuthInfo{},
						err:      nil,
					},
				},
			},
			request:            httptest.NewRequest("GET", "/", nil),
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "Authentication by third provider",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: nil,
						err:      nil,
					},
					&mockAuthProvider{
						authInfo: nil,
						err:      nil,
					},
					&mockAuthProvider{
						authInfo: &AuthInfo{},
						err:      nil,
					},
				},
			},
			request:            httptest.NewRequest("GET", "/", nil),
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "All providers fail",
			config: Config{
				Providers: []AuthProvider{
					&mockAuthProvider{
						authInfo: nil,
						err:      ErrNeedAuth,
					},
					&mockAuthProvider{
						authInfo: nil,
						err:      ErrNeedAuth,
					},
					&mockAuthProvider{
						authInfo: nil,
						err:      ErrNeedAuth,
					},
				},
			},
			request:            httptest.NewRequest("GET", "/", nil),
			expectedStatusCode: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			handler := AuthMiddleware(tt.config)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			handler.ServeHTTP(rr, tt.request)

			if status := rr.Code; status != tt.expectedStatusCode {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tt.expectedStatusCode)
			}
		})
	}
}
