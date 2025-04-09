package http

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
)

func generateTestToken(username string, expires time.Time) string {
	// Создаём новый токен
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":  strconv.Itoa(int(time.Since(expires).Seconds())),
		"name": username,
		"iat":  time.Now().Unix(),
		"exp":  expires,
	})

	secretKey := []byte("super-secret-key")
	tokenString, _ := token.SignedString(secretKey)

	return tokenString
}

var testToken = generateTestToken("user", time.Now().Add(600*time.Second))
var testRefreshToken = generateTestToken("refresh_token", time.Now().Add(3600*time.Second))

type testServer struct {
	authParam httpSecurityParams
}

func (s *testServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/token":
		if r.Method != http.MethodPost ||
			r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.ParseForm()
		grantType := r.Form.Get("grant_type")
		clientID := r.Form.Get("client_id")
		clientSecret := r.Form.Get("client_secret")
		username := r.Form.Get("username")
		password := r.Form.Get("password")
		switch grantType {
		case "password":
			// Проверяем логин и пароль
			if username != "user" || password != "pass" {
				http.Error(w, "invalid_credentials", http.StatusUnauthorized)
				return
			}
		case "client_credentials":
			if clientID != "client" || clientSecret != "client-secret" {
				http.Error(w, "invalid_client", http.StatusUnauthorized)
				return
			}
		default:
			http.Error(w, "unsupported_grant_type", http.StatusBadRequest)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token": "%s", "token_type": "bearer", "expires_in": 3600}`, testToken)
	case "/token_with_refresh":
		if r.Method != http.MethodPost ||
			r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		r.ParseForm()
		grantType := r.Form.Get("grant_type")
		clientID := r.Form.Get("client_id")
		clientSecret := r.Form.Get("client_secret")
		username := r.Form.Get("username")
		password := r.Form.Get("password")
		refreshToken := r.Form.Get("refresh_token")
		switch grantType {
		case "password":
			// Проверяем логин и пароль
			if username != "user" || password != "pass" {
				http.Error(w, "invalid_credentials", http.StatusUnauthorized)
				return
			}
		case "client_credentials":
			if clientID != "client" || clientSecret != "client-secret" {
				http.Error(w, "invalid_client", http.StatusUnauthorized)
				return
			}
		case "refresh_token":
			if refreshToken != "" && refreshToken != testRefreshToken {
				http.Error(w, "bad refresh", http.StatusUnauthorized)
				return
			}
		default:
			http.Error(w, "unsupported_grant_type", http.StatusBadRequest)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token": "%s", "refresh_token": "%s:", "token_type": "bearer", "expires_in": 3600}`, testToken, testRefreshToken)
	case "/custom_login":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var data struct {
			Login string `json:"login"`
			Pass  string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if data.Login != "user" || data.Pass != "pass" {
			http.Error(w, "invalid_credentials", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"accessToken": "%s", "refreshToken": "%s"}`, testToken, testRefreshToken)
	case "/custom_login_get":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var data struct {
			Login string `json:"login"`
			Pass  string `json:"password"`
		}
		data.Login = r.URL.Query().Get("login")
		data.Pass = r.URL.Query().Get("password")
		if data.Login != "user" || data.Pass != "pass" {
			http.Error(w, "invalid_credentials", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"accessToken": "%s", "refreshToken": "%s"}`, testToken, testRefreshToken)
	case "/custom_refresh":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var data struct {
			RefreshToken string `json:"refreshToken"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if data.RefreshToken != testRefreshToken {
			http.Error(w, "bad refresh", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"accessToken": "%s", "refreshToken": "%s"}`, testToken, testRefreshToken)
	case "/get_data_object":
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id": 1, "name": "test"}`)
	case "/get_data_array":
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]`)
	case "/post_data":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var data struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	case "/put_data":
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var data struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "ok", "id": %v}`, data.ID)
	case "/patch_data":
		if r.Method != http.MethodPatch {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var data struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "ok", "id": %v}`, data.ID)
	case "/delete_data":
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		p := r.URL.Query().Get("id")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status": "ok", "id": %s}`, p)
	case "/delete_data_path/1":
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !s.authRequest(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status": "ok", "id": 1}`)
	case "/openapi":
		w.Header().Set("Content-Type", "application/yaml")
		fmt.Fprint(w, testServerSpec)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (s *testServer) authRequest(r *http.Request) bool {
	//check is request authenticated
	switch s.authParam.Type {
	case "":
		return true
	case "http":
		if s.authParam.Scheme != "basic" {
			return false
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != s.authParam.Username || password != s.authParam.Password {
			return false
		}
		return true
	case "apiKey":
		if s.authParam.In == "query" && r.URL.Query().Get(s.authParam.Name) == s.authParam.ApiKey {
			return true
		}
		if s.authParam.In == "header" && r.Header.Get(s.authParam.Name) == s.authParam.ApiKey {
			return true
		}
		return false
	case "oauth2":
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		return token == testToken
	default:
		return false
	}
}

//go:embed open-api-test.yaml
var testServerSpec string

func TestHttpSource_Request(t *testing.T) {
	testServer := &testServer{}
	server := httptest.NewServer(testServer)
	defer server.Close()
	testDB, err := db.NewPool("")
	if err != nil {
		t.Fatal(err)
	}
	defer testDB.Close()
	tests := []struct {
		name       string
		ds         types.DataSource
		pathParams map[string]any
		method     string
		query      string
		params     any
		headers    any
		body       any
		wantData   string
		wantErr    bool
	}{
		{
			name: "get data object without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodGet,
			query:    "/get_data_object",
			params:   map[string]any{"id": 1},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
		{
			name: "get data array without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodGet,
			query:    "/get_data_array",
			params:   map[string]any{"id": 1},
			wantData: `[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]`,
			wantErr:  false,
		},
		{
			name: "post data without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodPost,
			query:    "/post_data",
			body:     map[string]any{"id": 1, "name": "test"},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
		{
			name: "put data without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodPut,
			query:    "/put_data",
			body:     map[string]any{"id": 1, "name": "test"},
			wantData: `{"status": "ok", "id": 1}`,
			wantErr:  false,
		},
		{
			name: "patth data without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodPatch,
			query:    "/patch_data",
			body:     map[string]any{"id": 1, "name": "test"},
			wantData: `{"status": "ok", "id": 1}`,
			wantErr:  false,
		},
		{
			name: "delete data without auth and scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			method:   http.MethodDelete,
			query:    "/delete_data",
			params:   map[string]any{"id": 1},
			wantData: `{"status": "ok", "id": 1}`,
			wantErr:  false,
		},
		{
			name: "delete data by id",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					SchemaName: "oauth2",
					Username:   "user",
					Password:   "pass",
				},
				specUrlParamKey: "/openapi",
			},
			method:   http.MethodDelete,
			query:    "/delete_data_path/1",
			wantData: `{"status": "ok", "id": 1}`,
			wantErr:  false,
		},
		{
			name: "get data object with api key in query auth and without scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					Type:   "apiKey",
					In:     "query",
					Name:   "api_key",
					ApiKey: "test",
				},
			},
			method:   http.MethodGet,
			query:    "/get_data_object",
			params:   map[string]any{"id": 1},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
		{
			name: "delete data object with api key in query auth and without scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					Type:   "apiKey",
					In:     "query",
					Name:   "api_key",
					ApiKey: "test",
				},
			},
			method:   http.MethodDelete,
			query:    "/delete_data",
			params:   map[string]any{"id": 1},
			wantData: `{"status": "ok", "id": 1}`,
			wantErr:  false,
		},
		{
			name: "get data object with api key in header auth and without scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					Type:   "apiKey",
					In:     "header",
					Name:   "api_key",
					ApiKey: "test",
				},
			},
			method:   http.MethodGet,
			query:    "/get_data_object",
			params:   map[string]any{"id": 1},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
		{
			name: "get data object with oauth2 auth and with scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					SchemaName: "oauth2",
					Username:   "user",
					Password:   "pass",
				},
				specUrlParamKey: "/openapi",
			},
			method:   http.MethodGet,
			query:    "/get_data_object",
			params:   map[string]any{"id": 1},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
		{
			name: "get data object with custom oauth2 auth and with scheme",
			ds: types.DataSource{
				Name:   "test",
				Type:   sources.Http,
				Prefix: "http",
				Path:   server.URL,
			},
			pathParams: map[string]any{
				securityParamsKey: httpSecurityParams{
					SchemaName: "oauth2_custom",
					Username:   "user",
					Password:   "pass",
				},
				specUrlParamKey: "/openapi",
			},
			method:   http.MethodGet,
			query:    "/get_data_object",
			params:   map[string]any{"id": 1},
			wantData: `{"id": 1, "name": "test"}`,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.pathParams) != 0 {
				params := url.Values{}
				for k, v := range tt.pathParams {
					if s, ok := v.(string); ok {
						params.Add(k, s)
						continue
					}
					b, err := json.Marshal(v)
					if err != nil {
						t.Fatal(err)
					}
					params.Add(k, string(b))
				}
				tt.ds.Path += "?" + params.Encode()
			}
			s, err := New(tt.ds, false)
			if err != nil {
				t.Fatal(err)
			}
			sp, err := sourceParamsFromPath(s.ds.Path)
			if err != nil {
				t.Fatal(err)
			}
			s.params = sp
			if sp.hasSpec {
				err := s.loadSpecs(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				doc, err := s.SchemaDocument(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				_ = doc
			}
			testServer.authParam = s.params.securityParams

			err = s.Attach(context.Background(), testDB)
			if err != nil {
				t.Fatal(err)
			}
			headers, err := json.Marshal(tt.headers)
			if err != nil {
				t.Fatal(err)
			}
			params, err := json.Marshal(tt.params)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(tt.body)
			if err != nil {
				t.Fatal(err)
			}
			res, err := s.Request(context.Background(), tt.query, tt.method, string(headers), string(params), string(body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("HttpSource.Request() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if res.StatusCode != http.StatusOK {
				t.Fatalf("HttpSource.Request() status = %v, want %v", res.StatusCode, http.StatusOK)
			}
			var data interface{}
			if res.Body != nil {
				err = json.NewDecoder(res.Body).Decode(&data)
				if err != nil {
					t.Fatal(err)
				}
			}
			var want any
			err = json.Unmarshal([]byte(tt.wantData), &want)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(data, want) {
				t.Errorf("HttpSource.Request() = %v, want %v", data, want)
			}
		})
	}
}
