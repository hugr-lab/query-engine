package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/jq"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

const (
	securityTypeApiKey = "apiKey"
	securityTypeOAuth2 = "oauth2"
	securityTypeHttp   = "http"

	securityInQuery  = "query"
	securityInHeader = "header"

	securityBasicScheme = "basic"
)

type ErrUnauthorizedTokenRequest string

func (e *ErrUnauthorizedTokenRequest) Error() string {
	return string(*e)
}

type httpSourceParams struct {
	hasSpec        bool
	isFile         bool
	specPath       string
	serverURL      string
	Timeout        int
	securityParams httpSecurityParams
}

const (
	securityParamsKey = "x-hugr-security"
	serverParamKey    = "x-hugr-server"
	specPathParamKey  = "x-hugr-spec-path"
	specUrlParamKey   = "x-hugr-spec-url"
)

func sourceParamsFromPath(path string) (httpSourceParams, error) {
	pp := strings.SplitN(path, "?", 2)
	var sp httpSourceParams
	if len(pp) > 1 {
		params, err := url.ParseQuery(pp[1])
		if err != nil {
			return httpSourceParams{}, err
		}
		sp.serverURL = strings.TrimSuffix(strings.TrimPrefix(params.Get(serverParamKey), "\""), "\"")
		if params.Has(securityParamsKey) {
			p := params.Get(securityParamsKey)
			p = strings.TrimPrefix(p, "\"")
			p = strings.TrimSuffix(p, "\"")
			err = json.Unmarshal([]byte(p), &sp.securityParams)
			if err != nil {
				return httpSourceParams{}, err
			}
		}
		specPath := strings.TrimSuffix(strings.TrimPrefix(params.Get(specPathParamKey), "\""), "\"")
		specUrl := strings.TrimSuffix(strings.TrimPrefix(params.Get(specUrlParamKey), "\""), "\"")
		switch {
		case specPath != "":
			sp.specPath = specPath
			sp.hasSpec = true
			sp.isFile = true
		case specUrl != "":
			sp.specPath = specUrl
			sp.hasSpec = true
		}
	}
	if len(strings.SplitN(pp[0], "://", 2)) == 1 || strings.HasPrefix(pp[0], "file://") {
		sp.specPath = pp[0]
		sp.isFile = true
		sp.hasSpec = true
		return sp, nil
	}
	url, err := url.ParseRequestURI(pp[0])
	if err != nil {
		return httpSourceParams{}, err
	}
	qp := url.Query()
	qp.Del(securityParamsKey)
	qp.Del(serverParamKey)
	qp.Del(specPathParamKey)
	qp.Del(specUrlParamKey)
	url.RawQuery = qp.Encode()
	if sp.serverURL == "" {
		sp.serverURL = url.String()
	}
	if sp.hasSpec && !sp.isFile && strings.HasPrefix(sp.specPath, "/") {
		u, err := url.Parse(sp.serverURL)
		if err != nil {
			return httpSourceParams{}, err
		}
		if u.Path != "" {
			u.JoinPath(sp.specPath)
		}
		if u.Path == "" {
			u.Path = sp.specPath
		}
		sp.specPath = u.String()
	}
	return sp, nil
}

type httpSecurityParams struct {
	SchemaName   string               `json:"schema_name"`
	Timeout      time.Duration        `json:"timeout"`
	Type         string               `json:"type"`
	Scheme       string               `json:"scheme"`
	Name         string               `json:"name"`
	In           string               `json:"in"`
	FlowName     string               `json:"flow_name"`
	ApiKey       string               `json:"api_key"`
	Username     string               `json:"username"`
	Password     string               `json:"password"`
	ClientID     string               `json:"client_id"`
	ClientSecret string               `json:"client_secret"`
	Flows        *openapi3.OAuthFlows `json:"flows"`
}

func (p *httpSecurityParams) validate() error {
	switch p.Type {
	case "":
		return nil
	case "apiKey":
		if p.Name == "" || p.In == "" || p.ApiKey == "" {
			return fmt.Errorf("security schema %s is apiKey but missing required fields", p.SchemaName)
		}
	case "oauth2":
		if p.Flows == nil {
			return fmt.Errorf("security schema %s is oauth2 but no flows found in the openAPI specs", p.SchemaName)
		}
		if p.FlowName == "" && p.Flows.Password != nil {
			p.FlowName = "password"
		}
		if p.FlowName == "" && p.Flows.ClientCredentials != nil {
			p.FlowName = "client_credentials"
		}
		if p.FlowName == "" && p.Flows.AuthorizationCode != nil {
			p.FlowName = "authorization_code"
		}
		if p.Flows == nil {
			return fmt.Errorf("security schema %s is oauth2 but no flows found in the openAPI specs", p.SchemaName)
		}
		switch p.FlowName {
		case "password":
			if p.Username == "" || p.Password == "" {
				return fmt.Errorf("security schema %s is oauth2 password but missing required fields", p.SchemaName)
			}
			if p.Flows.Password == nil {
				return fmt.Errorf("security schema %s is oauth2 password but no password flow found in the openAPI specs", p.SchemaName)
			}
			if p.Flows.Password.TokenURL == "" {
				return fmt.Errorf("security schema %s is oauth2 password but no token URL found in the openAPI specs", p.SchemaName)
			}
			if p.Flows.Password.Scopes == nil {
				return fmt.Errorf("security schema %s is oauth2 password but no scopes found in the openAPI specs", p.SchemaName)
			}
		case "client_credentials":
			if p.ClientID == "" || p.ClientSecret == "" {
				return fmt.Errorf("security schema %s is oauth2 client_credentials but missing required fields", p.SchemaName)
			}
			if p.Flows.ClientCredentials == nil {
				return fmt.Errorf("security schema %s is oauth2 client_credentials but no client_credentials flow found in the openAPI specs", p.SchemaName)
			}
			if p.Flows.ClientCredentials.TokenURL == "" {
				return fmt.Errorf("security schema %s is oauth2 client_credentials but no token URL found in the openAPI specs", p.SchemaName)
			}
			if p.Flows.ClientCredentials.Scopes == nil {
				return fmt.Errorf("security schema %s is oauth2 client_credentials but no scopes found in the openAPI specs", p.SchemaName)
			}
		default:
			return fmt.Errorf("security schema %s is oauth2 but unsupported flow %s", p.SchemaName, p.FlowName)
		}
	case "http":
		if p.Scheme != "basic" {
			return fmt.Errorf("security schema %s is supported only basic", p.SchemaName)
		}
		if p.Username == "" || p.Password == "" {
			return fmt.Errorf("security schema %s is http but missing required fields - username and password", p.SchemaName)
		}
	default:
		return fmt.Errorf("security schema %s has unsupported type %s", p.SchemaName, p.Type)
	}
	return nil
}

func newHttpClient(params httpSourceParams, base http.RoundTripper) (*http.Client, error) {
	switch params.securityParams.Type {
	case "":
		return http.DefaultClient, nil
	case securityTypeApiKey:
		return &http.Client{
			Transport: &httpApiKeyTransport{
				base: base,
				name: params.securityParams.Name,
				key:  params.securityParams.ApiKey,
				in:   params.securityParams.In,
			},
		}, nil
	case securityTypeOAuth2:
		ts, err := newTokenSource(context.Background(), params.serverURL, params.securityParams)
		if err != nil {
			return nil, err
		}
		return &http.Client{
			Transport: &httpOauth2Transport{
				base:        base,
				tokenSource: ts,
			},
		}, nil
	case securityTypeHttp:
		if params.securityParams.Scheme != securityBasicScheme {
			return nil, fmt.Errorf("unsupported security scheme %s", params.securityParams.Scheme)
		}
		return &http.Client{
			Transport: &httpBasicTransport{
				base:     base,
				username: params.securityParams.Username,
				password: params.securityParams.Password,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported security type %s", params.securityParams.Type)
	}
}

type httpApiKeyTransport struct {
	base http.RoundTripper
	name string
	key  string
	in   string
}

func (t *httpApiKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch t.in {
	case securityInQuery:
		q := req.URL.Query()
		q.Add(t.name, t.key)
		req.URL.RawQuery = q.Encode()
	case securityInHeader:
		req.Header.Set(t.name, t.key)
	default:
		return nil, fmt.Errorf("unsupported apiKey in %s", t.in)
	}
	if t.base == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.base.RoundTrip(req)
}

type httpBasicTransport struct {
	base     http.RoundTripper
	username string
	password string
}

func (t *httpBasicTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(t.username, t.password)
	if t.base == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.base.RoundTrip(req)
}

type httpOauth2Transport struct {
	base        http.RoundTripper
	tokenSource oauth2.TokenSource
}

func (t *httpOauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token.AccessToken)
	if t.base == nil {
		return http.DefaultTransport.RoundTrip(req)
	}

	return t.base.RoundTrip(req)
}

func newTokenSource(ctx context.Context, serverUrl string, params httpSecurityParams) (ts oauth2.TokenSource, err error) {
	switch params.Type {
	case securityTypeOAuth2:
		if params.Flows == nil {
			return nil, fmt.Errorf("no flows found in the openAPI specs")
		}
		switch params.FlowName {
		case "password":
			if params.Flows.Password == nil {
				return nil, fmt.Errorf("no password flow found in the openAPI specs")
			}
			if !hasCustomAuthEndpoint(params.Flows.Password) {
				ts, err = newOAuth2PasswordTokenSource(ctx, serverUrl, params)
			} else {
				ts, err = newCustomOauth2TokenSource(serverUrl, params)
			}
			if err != nil {
				return nil, err
			}
			return &cachedTokenSource{base: ts}, nil
		case "client_credentials":
			if params.Flows.ClientCredentials == nil {
				return nil, fmt.Errorf("no password flow found in the openAPI specs")
			}
			if !hasCustomAuthEndpoint(params.Flows.Password) {
				ts, err = newOAuth2ClientCredentialsTokenSource(ctx, serverUrl, params)
			} else {
				ts, err = newCustomOauth2TokenSource(serverUrl, params)
			}
			if err != nil {
				return nil, err
			}
			return &cachedTokenSource{base: ts}, nil
		default:
			return nil, fmt.Errorf("unsupported oauth2 flow %s", params.FlowName)
		}
	default:
		return nil, fmt.Errorf("unsupported security type %s", params.Type)
	}
}

const (
	oauth2TokenUrlCustomParam   = "x-hugr-token-transform"
	oauth2RefreshUrlCustomParam = "x-hugr-refresh-transform"
)

func hasRefreshUrl(flow *openapi3.OAuthFlow) bool {
	if flow == nil || flow.Extensions == nil {
		return false
	}
	return flow.RefreshURL != flow.TokenURL
}

func hasCustomAuthEndpoint(flow *openapi3.OAuthFlow) bool {
	if flow == nil || flow.Extensions == nil {
		return false
	}
	if _, ok := flow.Extensions[oauth2TokenUrlCustomParam]; ok {
		return true
	}
	if _, ok := flow.Extensions[oauth2RefreshUrlCustomParam]; ok {
		return true
	}
	return false
}

type cachedTokenSource struct {
	base oauth2.TokenSource

	mu    sync.Mutex
	cache *oauth2.Token
}

func (c *cachedTokenSource) Token() (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache.Valid() {
		return c.cache, nil
	}

	token, err := c.base.Token()
	if err != nil {
		return nil, err
	}

	c.cache = token
	return token, nil
}

func newOAuth2PasswordTokenSource(ctx context.Context, serverUrl string, params httpSecurityParams) (oauth2.TokenSource, error) {
	tp := params.Flows.Password.TokenURL
	if strings.HasPrefix(tp, "/") {
		u, err := url.Parse(serverUrl)
		if err != nil {
			return nil, err
		}
		if u.Path != "" {
			u.JoinPath(tp)
		}
		if u.Path == "" {
			u.Path = tp
		}
		tp = u.String()
	}
	c := &oauth2.Config{
		ClientID:     params.ClientID,
		ClientSecret: params.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: tp,
		},
	}
	token, err := c.PasswordCredentialsToken(ctx, params.Username, params.Password)
	if err != nil {
		return nil, err
	}
	return c.TokenSource(ctx, token), nil
}

func newOAuth2ClientCredentialsTokenSource(ctx context.Context, serverUrl string, params httpSecurityParams) (oauth2.TokenSource, error) {
	var scopes []string
	for k := range params.Flows.ClientCredentials.Scopes {
		scopes = append(scopes, k)
	}
	tp := params.Flows.ClientCredentials.TokenURL
	if strings.HasPrefix(tp, "/") {
		u, err := url.Parse(serverUrl)
		if err != nil {
			return nil, err
		}
		if u.Path != "" {
			u.JoinPath(tp)
		}
		if u.Path == "" {
			u.Path = tp
		}
		tp = u.String()
	}
	c := &clientcredentials.Config{
		ClientID:     params.ClientID,
		ClientSecret: params.ClientSecret,
		TokenURL:     tp,
		Scopes:       scopes,
	}
	return c.TokenSource(ctx), nil
}

type customOauth2TokenSource struct {
	flowName       string
	params         httpSecurityParams
	serverUrl      string
	tokenUrl       string
	tokenRequest   *tokenRequestTransform
	refreshUrl     string
	refreshRequest *tokenRequestTransform

	mu    sync.Mutex
	token *oauth2.Token
}

func newCustomOauth2TokenSource(serverUrl string, params httpSecurityParams) (*customOauth2TokenSource, error) {
	var flow *openapi3.OAuthFlow
	switch params.FlowName {
	case "password":
		flow = params.Flows.Password
	case "client_credentials":
		flow = params.Flows.ClientCredentials
	default:
		return nil, fmt.Errorf("unsupported oauth2 flow %s", params.FlowName)
	}
	if flow == nil {
		return nil, fmt.Errorf("no flow found in the openAPI specs")
	}
	if params.Timeout == 0 {
		params.Timeout = 1 * time.Second
	}
	tokenUrl := flow.TokenURL
	if strings.HasPrefix(tokenUrl, "/") {
		u, err := url.Parse(serverUrl)
		if err != nil {
			return nil, err
		}
		if u.Path != "" {
			u.JoinPath(tokenUrl)
		}
		if u.Path == "" {
			u.Path = tokenUrl
		}
		tokenUrl = u.String()
	}
	refreshUrl := flow.RefreshURL
	if strings.HasPrefix(refreshUrl, "/") {
		u, err := url.Parse(serverUrl)
		if err != nil {
			return nil, err
		}
		if u.Path != "" {
			u.JoinPath(refreshUrl)
		}
		if u.Path == "" {
			u.Path = refreshUrl
		}
		refreshUrl = u.String()
	}

	tokenRequest, err := parseTokenRequestTransform(params, flow.Extensions[oauth2TokenUrlCustomParam])
	if err != nil {
		return nil, err
	}
	refreshRequest, err := parseTokenRequestTransform(params, flow.Extensions[oauth2RefreshUrlCustomParam])
	if err != nil {
		return nil, err
	}
	return &customOauth2TokenSource{
		flowName:       params.FlowName,
		params:         params,
		serverUrl:      serverUrl,
		tokenUrl:       tokenUrl,
		tokenRequest:   tokenRequest,
		refreshUrl:     refreshUrl,
		refreshRequest: refreshRequest,
	}, nil
}

func (s *customOauth2TokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token == nil {
		err := s.requestToken()
		if err != nil {
			return nil, err
		}
		return s.token, nil
	}

	if s.token.Valid() {
		return s.token, nil
	}

	if s.needRefresh() {
		err := s.refreshToken()
		var e *ErrUnauthorizedTokenRequest
		if errors.As(err, &e) { // second try to request token
			err = s.requestToken()
		}
		if err != nil {
			return nil, err
		}
		return s.token, nil
	}

	err := s.requestToken()
	if err != nil {
		return nil, err
	}

	return s.token, nil

}

func (s *customOauth2TokenSource) needRefresh() bool {
	return s.token.RefreshToken != ""
}

func (s *customOauth2TokenSource) requestToken() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.params.Timeout)*time.Second)
	defer cancel()
	token, err := customTokenRequest(ctx, s.tokenUrl, s.params, s.tokenRequest)
	if err != nil {
		return err
	}
	s.token = token
	return err
}

func (s *customOauth2TokenSource) refreshToken() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.params.Timeout)*time.Second)
	defer cancel()
	token, err := customTokenRequest(ctx, s.refreshUrl, s.token, s.refreshRequest)
	if err != nil {
		return err
	}
	s.token = token
	return err
}

func customTokenRequest(ctx context.Context, tokenUrl string, data any, param *tokenRequestTransform) (*oauth2.Token, error) {
	if param == nil {
		return nil, fmt.Errorf("no token request transform found")
	}
	pp := url.Values{}
	for k, v := range param.Params {
		if t, ok := data.(*oauth2.Token); ok {
			switch v {
			case "$access_token":
				v = t.AccessToken
			case "$refresh_token":
				v = t.RefreshToken
			}
		}
		pp.Add(k, v)
	}
	u, err := url.Parse(tokenUrl)
	if err != nil {
		return nil, err
	}
	u.RawQuery = pp.Encode()
	var body io.Reader
	if param.Method == http.MethodPost {
		if param.body != nil {
			val, err := param.body.Transform(ctx, data, param.vars)
			if err != nil {
				return nil, fmt.Errorf("failed to transform token request: %w", err)
			}
			if v, ok := val.([]any); ok && len(v) > 0 {
				val = v[0]
			}
			b, err := json.Marshal(val)
			if err != nil {
				return nil, err
			}
			body = bytes.NewReader(b)
		}
	}
	if param.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, param.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, param.Method, u.String(), body)
	if err != nil {
		return nil, err
	}

	for k, v := range param.Headers {
		if t, ok := data.(*oauth2.Token); ok {
			switch v {
			case "$access_token":
				v = t.AccessToken
			case "$refresh_token":
				v = t.RefreshToken
			}
		}
		req.Header.Set(k, v)
	}
	if len(param.Headers) == 0 || param.Headers["Content-Type"] == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusUnauthorized {
		e := ErrUnauthorizedTokenRequest(res.Status)
		return nil, &e
	}
	if res.StatusCode != http.StatusOK {
		msg, err := io.ReadAll(res.Body)
		if err == nil {
			return nil, fmt.Errorf("unexpected status code %d: %s", res.StatusCode, msg)
		}
		return nil, fmt.Errorf("unexpected status code %d", res.StatusCode)
	}

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if param.response != nil {
		var resData any
		err = json.Unmarshal(resBody, &resData)
		if err != nil {
			return nil, err
		}
		val, err := param.response.Transform(ctx, resData, param.vars)
		if err != nil {
			return nil, fmt.Errorf("failed to transform token response: %w", err)
		}
		if v, ok := val.([]any); ok && len(v) > 0 {
			val = v[0]
		}
		resBody, err = json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response body: %w", err)
		}
	}

	var token tokenJSON
	err = json.Unmarshal(resBody, &token)
	if err != nil {
		return nil, err
	}

	if token.ErrorCode != "" {
		return nil, fmt.Errorf("error %s: %s", token.ErrorCode, token.ErrorDescription)
	}

	var expiresIn time.Time
	if token.ExpiresIn != nil {
		switch v := token.ExpiresIn.(type) {
		case string:
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			expiresIn = time.Now().Add(d)
		case float64:
			expiresIn = time.Now().Add(time.Duration(v) * time.Second)
		}
	}

	return &oauth2.Token{
		AccessToken:  token.AccessToken,
		TokenType:    token.TokenType,
		RefreshToken: token.RefreshToken,
		Expiry:       expiresIn,
	}, nil
}

type tokenRequestTransform struct {
	Method   string            `json:"method"`
	Timeout  time.Duration     `json:"timeout"`
	Params   map[string]string `json:"params"`
	Headers  map[string]string `json:"headers"`
	Body     string            `json:"request_body"`
	Response string            `json:"response_body"`

	vars     map[string]any  `json:"-"`
	body     *jq.Transformer `json:"-"`
	response *jq.Transformer `json:"-"`
}

func (t *tokenRequestTransform) parse(param httpSecurityParams) error {
	t.vars = map[string]any{
		"$type":          param.Type,
		"$schema_name":   param.SchemaName,
		"$flow":          param.FlowName,
		"$scheme":        param.Scheme,
		"$api_key_name":  param.Name,
		"$api_key_in":    param.In,
		"$api_key":       param.ApiKey,
		"$username":      param.Username,
		"$password":      param.Password,
		"$client_id":     param.ClientID,
		"$client_secret": param.ClientSecret,
	}
	for k, v := range t.Params {
		if strings.HasPrefix(v, "$") {
			if newVal, ok := t.vars[v]; ok {
				v = fmt.Sprint(newVal)
			}
		}
		if strings.HasPrefix(v, "~$") {
			v = strings.TrimPrefix(v, "~")
		}
		t.Params[k] = v
	}
	for k, v := range t.Headers {
		if strings.HasPrefix(v, "$") {
			if newVal, ok := t.vars[strings.TrimPrefix(v, "$")]; ok {
				v = fmt.Sprint(newVal)
			}
		}
		if strings.HasPrefix(v, "~$") {
			v = strings.TrimPrefix(v, "~")
		}
		t.Headers[k] = v
	}
	var err error
	if t.Body != "" {
		t.body, err = jq.NewTransformer(context.Background(), t.Body, jq.WithVariables(t.vars))
		if err != nil {
			return fmt.Errorf("failed to parse request body jq transform: %w", err)
		}
	}
	if t.Response != "" {
		t.response, err = jq.NewTransformer(context.Background(), t.Response, jq.WithVariables(t.vars))
		if err != nil {
			return fmt.Errorf("failed to parse response body jq transform: %w", err)
		}
	}
	if t.Method == "" {
		t.Method = http.MethodPost
	}
	return nil
}

func parseTokenRequestTransform(param httpSecurityParams, data any) (*tokenRequestTransform, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var tr tokenRequestTransform
	err = json.Unmarshal(b, &tr)
	if err != nil {
		return nil, err
	}

	if tr.Timeout != 0 {
		tr.Timeout = time.Millisecond * tr.Timeout
	}

	err = tr.parse(param)
	if err != nil {
		return nil, err
	}
	return &tr, nil
}

// tokenJSON is the struct representing the HTTP response from OAuth2
// providers returning a token or error in JSON form.
// https://datatracker.ietf.org/doc/html/rfc6749#section-5.1
type tokenJSON struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    any    `json:"expires_in"` // at least PayPal returns string, while most return number
	// error fields
	// https://datatracker.ietf.org/doc/html/rfc6749#section-5.2
	ErrorCode        string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorURI         string `json:"error_uri"`
}
