package hugr

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

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// The Hugr IPC client package provides a client for the Hugr IPC protocol (https://github.com/hugr-labs/query-engine/hugr-ipc.md).
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

type ClientConfig struct {
	Timeout   time.Duration
	Transport http.RoundTripper
}

type Client struct {
	url    string
	c      *http.Client
	config ClientConfig
}

func NewClient(url string, opts ...Option) *Client {
	var config ClientConfig
	for _, opt := range opts {
		opt(&config)
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	if config.Transport == nil {
		config.Transport = http.DefaultTransport
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
	if res.Err() != nil {
		return "", res.Err()
	}

	var nv string
	err = res.ScanData("function.core.info.version", &nv)
	return nv, err
}

func (c *Client) RegisterDataSource(ctx context.Context, ds types.DataSource) error {
	res, err := c.Query(ctx, `mutation($data: data_sources_mut_input_data!){
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
	if len(res.Errors) > 0 {
		return res.Errors
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
	var or types.OperationResult
	err = res.ScanData("data.function.core.load_data_source", &or)
	if err != nil {
		return err
	}
	if !or.Succeed {
		return errors.New(or.Msg)
	}
	return nil
}

func (c *Client) UnloadDataSource(ctx context.Context, name string) error {
	res, err := c.Query(ctx, `mutation($name: String!){
		function {
			core{
				unload_data_source(name:$name){
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
	var or types.OperationResult
	err = res.ScanData("data.function.core.unload_data_source", &or)
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
				data_source_status(name:$name)
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return "", err
	}
	var status string
	err = res.ScanData("data.function.core.data_source_status", &status)

	return status, err
}

func (c *Client) Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(map[string]any{
		"query": query,
		"vars":  vars,
	})
	if err != nil {
		return nil, err
	}
	resp, err := c.c.Post(c.url, "application/json", &buf)
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
		part := p.Header.Get("X-Hugr-Type")
		switch {
		case part == "errors":
			var errs gqlerror.List
			err = json.NewDecoder(p).Decode(&errs)
			if err != nil {
				return nil, fmt.Errorf("decoding errors: %w", err)
			}
			r.Errors = append(r.Errors, errs...)
			return r, nil
		case strings.HasPrefix(cp, "application/json") && format == "object":
			b, err := io.ReadAll(p)
			if err != nil {
				return nil, fmt.Errorf("decoding json value: %w", err)
			}
			val := db.JsonValue(b)
			err = addResponseData(r, part, path, &val)
			if err != nil {
				return nil, fmt.Errorf("adding json value: %w", err)
			}
		case strings.HasPrefix(cp, "application/vnd.apache.arrow.stream") && format == "table":
			t := db.NewArrowTable(true)
			t.SetInfo(p.Header.Get("X-Hugr-Table-Info"))
			reader, err := ipc.NewReader(p, ipc.WithAllocator(pool))
			if err != nil {
				return nil, fmt.Errorf("creating arrow reader: %w", err)
			}
			for reader.Next() {
				rec := reader.Record()
				if rec == nil {
					continue
				}
				t.Append(rec)
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

type apiKeyTransport struct {
	apiKey       string
	apiKeyHeader string

	transport http.RoundTripper
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

	transport http.RoundTripper
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
