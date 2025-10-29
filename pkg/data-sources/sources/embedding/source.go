package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
)

type Config struct {
	Model        string
	BaseUrl      string
	ApiKey       string
	ApiKeyHeader string
	Timeout      time.Duration
}

type Source struct {
	ds     types.DataSource
	engine engines.Engine

	mu         sync.RWMutex
	isAttached bool
	config     Config
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isAttached {
		return sources.ErrDataSourceAttached
	}

	s.ds.Path, err = sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	u, err := url.Parse(s.ds.Path)
	if err != nil {
		return err
	}

	c := Config{
		Model:        u.Query().Get("model"),
		ApiKey:       u.Query().Get("api_key"),
		ApiKeyHeader: u.Query().Get("api_key_header"),
	}
	c.Timeout, err = time.ParseDuration(u.Query().Get("timeout"))
	if err != nil || c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
	if c.Model == "" {
		return errors.New("model is required in the data source path")
	}
	q := u.Query()
	q.Del("model")
	q.Del("api_key")
	q.Del("timeout")
	q.Del("api_key_header")
	u.RawQuery = q.Encode()

	c.BaseUrl = u.String()

	s.config = c
	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isAttached {
		return nil
	}
	s.isAttached = false
	return nil
}

type request struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

type Response struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

func (s *Source) CreateEmbedding(ctx context.Context, input string) (types.Vector, error) {
	vectors, err := s.CreateEmbeddings(ctx, []string{input})
	if err != nil {
		return nil, err
	}
	if len(vectors) != 1 {
		return nil, errors.New("failed to create embedding")
	}
	return vectors[0], nil
}

func (s *Source) CreateEmbeddings(ctx context.Context, input []string) ([]types.Vector, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.isAttached {
		return nil, sources.ErrDataSourceNotAttached
	}

	w := bytes.Buffer{}

	err := json.NewEncoder(&w).Encode(request{
		Input: input,
		Model: s.config.Model,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	rec, err := http.NewRequestWithContext(ctx, "POST", s.config.BaseUrl, &w)
	if err != nil {
		return nil, err
	}
	rec.Header.Set("Content-Type", "application/json")
	if s.config.ApiKeyHeader != "" {
		rec.Header.Set(s.config.ApiKeyHeader, s.config.ApiKey)
	}
	if s.config.ApiKeyHeader == "" && s.config.ApiKey != "" {
		rec.Header.Set("Authorization", "Bearer "+s.config.ApiKey)
	}

	resp, err := http.DefaultClient.Do(rec)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("embedding request for source %s failed: %s", s.Name(), resp.Status)
		return nil, errors.New("failed to create embedding")
	}

	var result Response
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	vectors := make([]types.Vector, len(result.Data))
	for i := range result.Data {
		vectors[i] = slices.Clone(result.Data[i].Embedding)
	}

	return vectors, nil
}
