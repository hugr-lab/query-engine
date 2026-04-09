// Package client provides an IPC client for the Hugr query engine.
// It communicates via the Hugr IPC multipart/mixed protocol.
package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"encoding/json"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/query-engine/types"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

var _ types.Querier = (*Client)(nil)

type Option func(*ClientConfig)

func WithApiKey(apiKey string) Option {
	return func(c *ClientConfig) {
		c.Transport = &apiKeyTransport{
			apiKey:    apiKey,
			transport: c.Transport,
		}
	}
}

// WithSecretKeyAuth sets the x-hugr-secret-key header for admin authentication.
// This enables impersonation via types.AsUser context.
func WithSecretKeyAuth(key string) Option {
	return func(c *ClientConfig) {
		c.Transport = &apiKeyTransport{
			apiKey:       key,
			apiKeyHeader: "x-hugr-secret-key",
			transport:    c.Transport,
		}
	}
}

func WithApiKeyCustomHeader(apiKey, header string) Option {
	return func(c *ClientConfig) {
		c.Transport = &apiKeyTransport{
			apiKey:       apiKey,
			apiKeyHeader: header,
			transport:    c.Transport,
		}
	}
}

func WithUserRole(role string) Option {
	return func(c *ClientConfig) {
		c.Transport = &withUserRoleTransport{
			userRole:  role,
			transport: c.Transport,
		}
	}
}

func WithUserRoleCustomHeader(role, header string) Option {
	return func(c *ClientConfig) {
		c.Transport = &withUserRoleTransport{
			userRole:       role,
			userRoleHeader: header,
			transport:      c.Transport,
		}
	}
}

func WithUserInfo(id, name string) Option {
	return func(c *ClientConfig) {
		c.Transport = &withUserInfoTransport{
			id:        id,
			name:      name,
			transport: c.Transport,
		}
	}
}

func WithUserInfoCustomHeader(id, name, idHeader, nameHeader string) Option {
	return func(c *ClientConfig) {
		c.Transport = &withUserInfoTransport{
			id:         id,
			name:       name,
			idHeader:   idHeader,
			nameHeader: nameHeader,
			transport:  c.Transport,
		}
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *ClientConfig) {
		c.Timeout = timeout
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(c *ClientConfig) {
		c.Transport = transport
	}
}

func WithToken(token string) Option {
	return func(c *ClientConfig) {
		c.Transport = &tokenTransport{
			token:     token,
			transport: c.Transport,
		}
	}
}

func WithHttpUrl(httpUrl string) Option {
	return func(c *ClientConfig) {
		c.HttpUrl = httpUrl
	}
}

func WithJQQueryUrl(jq string) Option {
	return func(c *ClientConfig) {
		c.JQQueryUrl = jq
	}
}

func WithTimezone(timezone string) Option {
	return func(c *ClientConfig) {
		c.Transport = &timezoneTransport{
			timezone:  timezone,
			transport: c.Transport,
		}
	}
}

func WithoutTimezone() Option {
	return func(c *ClientConfig) {
		c.Transport = &noTimezoneTransport{
			transport: c.Transport,
		}
	}
}

func WithArrowStructFlatten() Option {
	return func(c *ClientConfig) {
		c.ArrowStructFlatten = true
	}
}

type ClientConfig struct {
	Timeout            time.Duration
	HttpUrl            string
	JQQueryUrl         string
	Transport          http.RoundTripper
	ArrowStructFlatten bool
	SubPool            SubscriptionPoolConfig
}

type Client struct {
	url    string
	c      *http.Client
	config ClientConfig

	subPoolMu sync.Mutex
	subPool   *subscriptionPool
}

func NewClient(url string, opts ...Option) *Client {
	var config ClientConfig
	for _, opt := range opts {
		opt(&config)
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.Transport == nil {
		config.Transport = http.DefaultTransport
	}
	// Auto-detect timezone if not explicitly set
	if !hasTimezoneTransport(config.Transport) {
		localTZ := time.Now().Location().String()
		if localTZ != "" && localTZ != "Local" {
			config.Transport = &timezoneTransport{
				timezone:  localTZ,
				transport: config.Transport,
			}
		}
	}
	if config.HttpUrl == "" {
		config.HttpUrl = strings.TrimSuffix(url, "/ipc") + "/query"
	}
	if config.JQQueryUrl == "" {
		config.JQQueryUrl = strings.TrimSuffix(url, "/ipc") + "/jq-query"
	}
	return &Client{
		c: &http.Client{
			Timeout:   config.Timeout,
			Transport: config.Transport,
		},
		config: config,
		url:    url,
	}
}

func (c *Client) Ping(ctx context.Context) (string, error) {
	res, err := c.Query(ctx, `{
		function { core { info { version } } }
	}`, nil)
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Err() != nil {
		return "", res.Err()
	}
	var nv struct {
		Version string `json:"version"`
	}
	err = res.ScanData("function.core.info", &nv)
	return nv.Version, err
}

// VerifyAdmin queries the server to verify the client has admin privileges.
// Call after NewClient to ensure impersonation via AsUser will work.
// VerifyAdmin queries the server to verify the client has admin privileges.
// Call after NewClient to ensure impersonation via AsUser will work.
func (c *Client) VerifyAdmin(ctx context.Context) error {
	resp, err := c.Query(ctx, `{ function { core { auth { me { role auth_type } } } } }`, nil)
	if err != nil {
		return fmt.Errorf("admin verification failed: %w", err)
	}
	defer resp.Close()
	if resp.Err() != nil {
		return fmt.Errorf("admin verification failed: %w", resp.Err())
	}
	var me struct {
		Role     string `json:"role"`
		AuthType string `json:"auth_type"`
	}
	if err := resp.ScanData("function.core.auth.me", &me); err != nil {
		return fmt.Errorf("admin verification: %w", err)
	}
	if me.Role != "admin" {
		return fmt.Errorf("client is not admin (role: %s), impersonation not available", me.Role)
	}
	return nil
}

func (c *Client) RegisterDataSource(ctx context.Context, ds types.DataSource) error {
	res, err := c.Query(ctx, `mutation($data: core_data_sources_mut_input_data!){
		core{
			insert_data_sources(data:$data){
				name
			}
		}
	}`, map[string]any{
		"data": ds,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Err() != nil {
		return res.Err()
	}
	return nil
}

func (c *Client) LoadDataSource(ctx context.Context, name string) error {
	res, err := c.Query(ctx, `mutation($name: String!){
		function {
			core{
				load_data_source(name:$name){
					success
					message
				}
			}
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Err() != nil {
		return res.Err()
	}
	var or types.OperationResult
	err = res.ScanData("function.core.load_data_source", &or)
	if err != nil {
		return err
	}
	if !or.Succeed {
		return errors.New(or.Msg)
	}
	return nil
}

func (c *Client) UnloadDataSource(ctx context.Context, name string, opts ...types.UnloadOpt) error {
	var cfg types.UnloadOpts
	for _, opt := range opts {
		opt(&cfg)
	}

	res, err := c.Query(ctx, `mutation($name: String!, $hard: Boolean=false){
		function {
			core{
				unload_data_source(name:$name, hard:$hard){
					success
					message
				}
			}
		}
	}`, map[string]any{
		"name": name,
		"hard": cfg.Hard,
	})
	if err != nil {
		return err
	}
	defer res.Close()
	if res.Err() != nil {
		return res.Err()
	}
	var or types.OperationResult
	err = res.ScanData("function.core.unload_data_source", &or)
	if err != nil {
		return err
	}
	if !or.Succeed {
		return errors.New(or.Msg)
	}
	return nil
}

func (c *Client) DataSourceStatus(ctx context.Context, name string) (string, error) {
	res, err := c.Query(ctx, `query($name: String!){
		function {
			core{
				data_source_status(name: $name)
			}
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Err() != nil {
		return "", res.Err()
	}
	var status string
	err = res.ScanData("function.core.data_source_status", &status)

	return status, err
}

// DescribeDataSource returns the description of the data source.
func (c *Client) DescribeDataSource(ctx context.Context, name string, self bool) (string, error) {
	res, err := c.Query(ctx, `query($name: String!, $self: Boolean=false){
		function {
			core{
				describe_data_source_schema(name: $name, self: $self)
			}
		}
	}`, map[string]any{
		"name": name,
		"self": self,
	})
	if err != nil {
		return "", err
	}
	defer res.Close()
	if res.Err() != nil {
		return "", res.Err()
	}
	var desc string
	err = res.ScanData("function.core.describe_data_source_schema", &desc)
	if err != nil {
		return "", err
	}
	if desc == "" {
		return "", errors.New("data source not found")
	}
	return desc, nil
}

func (c *Client) QueryJSON(ctx context.Context, req types.JQRequest) (*types.JsonValue, error) {
	var buf bytes.Buffer
	url := c.config.HttpUrl
	if req.JQ != "" {
		url = c.config.JQQueryUrl
		err := json.NewEncoder(&buf).Encode(req)
		if err != nil {
			return nil, err
		}
	}
	if req.JQ == "" {
		err := json.NewEncoder(&buf).Encode(req.Query)
		if err != nil {
			return nil, err
		}
	}
	reqHttp, err := http.NewRequestWithContext(ctx, "POST", url, &buf)
	if err != nil {
		return nil, err
	}
	reqHttp.Header.Set("Content-Type", "application/json")
	setAsUserHeaders(ctx, reqHttp)

	resp, err := c.c.Do(reqHttp)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, errors.New(string(b))
	}
	var out types.JsonValue
	out = types.JsonValue(b)
	return &out, nil
}

func (c *Client) ValidateQueryJSON(ctx context.Context, req types.JQRequest) error {
	req.Query.ValidateOnly = true
	res, err := c.QueryJSON(ctx, req)
	if err != nil {
		return err
	}
	if res == nil {
		return errors.New("no response")
	}
	var data types.Response
	err = json.Unmarshal([]byte(*res), &data)
	if err != nil {
		return err
	}
	if data.Err() != nil {
		return data.Err()
	}

	return nil
}

func (c *Client) Subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	return c.subscribe(ctx, query, vars)
}

func (c *Client) Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(map[string]any{
		"query":         query,
		"variables":     vars,
		"validate_only": types.IsValidateOnlyContext(ctx),
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.url, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	setAsUserHeaders(ctx, req)

	resp, err := c.c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusInternalServerError {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.New("unrecognized error")
		}
		return nil, errors.New(string(b))
	}

	return c.parseMultipartResponse(resp)
}

func (c *Client) ValidateQuery(ctx context.Context, query string, vars map[string]any) error {
	_, err := c.Query(types.ContextWithValidateOnly(ctx), query, vars)
	return err
}

func (c *Client) parseMultipartResponse(resp *http.Response) (*types.Response, error) {
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("unexpected status code: " + resp.Status)
	}
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, fmt.Errorf("invalid content type: %w", err)
	}

	if !strings.HasPrefix(mediaType, "multipart/") {
		return nil, fmt.Errorf("expected multipart/*, got %s", mediaType)
	}

	mr := multipart.NewReader(resp.Body, params["boundary"])

	r := &types.Response{}
	pool := memory.NewGoAllocator()

	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading part: %w", err)
		}

		path := p.Header.Get("X-Hugr-Path")
		cp := p.Header.Get("Content-Type")
		format := p.Header.Get("X-Hugr-Format")
		part := p.Header.Get("X-Hugr-Part-Type")
		switch {
		case part == "errors":
			var errs gqlerror.List
			err = json.NewDecoder(p).Decode(&errs)
			if err != nil {
				return nil, fmt.Errorf("decoding errors: %w", err)
			}
			r.Errors = append(r.Errors, errs...)
			return r, nil
		case part == "extensions":
			var ext map[string]any
			if err := json.NewDecoder(p).Decode(&ext); err != nil {
				return nil, fmt.Errorf("decoding extensions: %w", err)
			}
			if r.Extensions == nil {
				r.Extensions = make(map[string]any)
			}
			for k, v := range ext {
				r.Extensions[k] = v
			}
		case strings.HasPrefix(cp, "application/json") && format == "object":
			b, err := io.ReadAll(p)
			if err != nil {
				return nil, fmt.Errorf("decoding json value: %w", err)
			}
			if strings.HasPrefix(string(b), "null") {
				err = addResponseData(r, part, path, nil)
				if err != nil {
					return nil, fmt.Errorf("adding null value: %w", err)
				}
				continue
			}
			val := types.JsonValue(b)
			err = addResponseData(r, part, path, &val)
			if err != nil {
				return nil, fmt.Errorf("adding json value: %w", err)
			}
		case strings.HasPrefix(cp, "application/vnd.apache.arrow.stream") && format == "table":
			t := types.NewArrowTable()
			t.SetInfo(p.Header.Get("X-Hugr-Table-Info"))
			if p.Header.Get("X-Hugr-Empty") != "true" {
				reader, err := ipc.NewReader(p, ipc.WithAllocator(pool))
				if err != nil {
					return nil, fmt.Errorf("creating arrow reader: %w", err)
				}
				flatten := c.config.ArrowStructFlatten && types.NeedsFlatten(reader.Schema())
				for reader.Next() {
					rec := reader.RecordBatch()
					if rec == nil {
						continue
					}
					if flatten {
						flat := types.FlattenRecord(rec, pool)
						t.Append(flat)
						flat.Release()
					} else {
						t.Append(rec)
					}
				}
			}
			err = addResponseData(r, part, path, t)
			if err != nil {
				return nil, fmt.Errorf("adding table value: %w", err)
			}
		}
	}

	return r, nil
}

func addResponseData(r *types.Response, part, path string, data any) error {
	pp := strings.SplitN(path, ".", 2)
	switch {
	case len(pp) == 1 && part == path:
		return errors.New("path is empty")
	case len(pp) == 1:
		path = pp[0]
	case len(pp) == 2 && part == pp[0]:
		path = pp[1]
	default:
		return fmt.Errorf("path %s is not valid", path)
	}
	if part == "data" {
		if r.Data == nil {
			r.Data = make(map[string]any)
		}
		return addMapData(r.Data, path, data)
	}
	if len(pp) == 1 && part == "extensions" {
		if r.Extensions == nil {
			r.Extensions = make(map[string]any)
		}
		return addMapData(r.Extensions, path, data)
	}
	return fmt.Errorf("path %s is not valid", path)
}

func addMapData(m map[string]any, path string, data any) error {
	if path == "" {
		return errors.New("path is empty")
	}
	if m == nil {
		m = make(map[string]any)
	}
	pp := strings.SplitN(path, ".", 2)
	if len(pp) == 1 {
		m[path] = data
		return nil
	}
	if _, ok := m[pp[0]]; !ok {
		m[pp[0]] = make(map[string]any)
	}
	mm, ok := m[pp[0]].(map[string]any)
	if !ok {
		return fmt.Errorf("path %s already is used", pp[0])
	}
	return addMapData(mm, pp[1], data)
}

// --- Transport implementations ---

type apiKeyTransport struct {
	apiKey       string
	apiKeyHeader string
	transport    http.RoundTripper
}

func (t *apiKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	header := t.apiKeyHeader
	if header == "" {
		header = "x-hugr-api-key"
	}
	req.Header.Set(header, t.apiKey)
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

type withUserRoleTransport struct {
	userRole       string
	userRoleHeader string
	transport      http.RoundTripper
}

func (t *withUserRoleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	header := t.userRoleHeader
	if header == "" {
		header = "x-hugr-role"
	}
	req.Header.Set(header, t.userRole)
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

type withUserInfoTransport struct {
	id         string
	idHeader   string
	name       string
	nameHeader string
	transport  http.RoundTripper
}

func (t *withUserInfoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	header := t.idHeader
	if header == "" {
		header = "x-hugr-user-id"
	}
	req.Header.Set(header, t.id)
	header = t.nameHeader
	if header == "" {
		header = "x-hugr-name"
	}
	req.Header.Set(header, t.name)
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

type tokenTransport struct {
	token     string
	transport http.RoundTripper
}

func (t *tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	header := "Authorization"
	req.Header.Set(header, "Bearer "+t.token)
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

type timezoneTransport struct {
	timezone  string
	transport http.RoundTripper
}

func (t *timezoneTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Hugr-Timezone", t.timezone)
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

// noTimezoneTransport is a marker to prevent auto-detection of timezone.
type noTimezoneTransport struct {
	transport http.RoundTripper
}

func (t *noTimezoneTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.transport == nil {
		return http.DefaultTransport.RoundTrip(req)
	}
	return t.transport.RoundTrip(req)
}

// hasTimezoneTransport walks the transport chain and returns true if a
// timezoneTransport or noTimezoneTransport is already present.
func hasTimezoneTransport(rt http.RoundTripper) bool {
	for rt != nil {
		switch t := rt.(type) {
		case *timezoneTransport:
			return true
		case *noTimezoneTransport:
			return true
		case *apiKeyTransport:
			rt = t.transport
		case *withUserRoleTransport:
			rt = t.transport
		case *withUserInfoTransport:
			rt = t.transport
		case *tokenTransport:
			rt = t.transport
		default:
			return false
		}
	}
	return false
}
