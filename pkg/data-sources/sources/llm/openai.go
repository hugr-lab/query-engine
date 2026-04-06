// Package llm implements LLM data source types for AI model providers.
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/ratelimit"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

// OpenAISource implements Source + LLMSource for OpenAI-compatible APIs.
// Covers: OpenAI, Azure OpenAI, Ollama, LM Studio, vLLM, Mistral, Qwen, LiteLLM.
type OpenAISource struct {
	ds         types.DataSource
	engine     engines.Engine
	isAttached bool
	config     openAIConfig

	resolver    sources.DataSourceResolver
	limiter     *ratelimit.Limiter
	limiterOnce sync.Once
}

type openAIConfig struct {
	BaseURL      string
	Model        string
	ApiKey       string
	ApiKeyHeader string
	MaxTokens    int
	Timeout      time.Duration
	RPM          int    // max requests per minute (0 = unlimited)
	TPM          int    // max tokens per minute (0 = unlimited)
	RateStore    string // name of StoreSource for shared counters (empty = in-memory)
}

func NewOpenAI(ds types.DataSource, attached bool) (*OpenAISource, error) {
	return &OpenAISource{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *OpenAISource) Name() string             { return s.ds.Name }
func (s *OpenAISource) Definition() types.DataSource { return s.ds }
func (s *OpenAISource) Engine() engines.Engine   { return s.engine }
func (s *OpenAISource) IsAttached() bool         { return s.isAttached }
func (s *OpenAISource) ReadOnly() bool           { return true }

func (s *OpenAISource) ModelInfo() sources.ModelInfo {
	return sources.ModelInfo{
		Name:     s.ds.Name,
		Type:     "llm",
		Provider: "openai",
		Model:    s.config.Model,
	}
}

func (s *OpenAISource) Attach(_ context.Context, _ *db.Pool) error {
	if s.isAttached {
		return sources.ErrDataSourceAttached
	}

	path, err := sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	u, err := url.Parse(path)
	if err != nil {
		return err
	}

	s.config.Model = u.Query().Get("model")
	if s.config.Model == "" {
		return errors.New("model is required in the data source path")
	}
	s.config.ApiKey = u.Query().Get("api_key")
	s.config.ApiKeyHeader = u.Query().Get("api_key_header")

	if mt := u.Query().Get("max_tokens"); mt != "" {
		fmt.Sscanf(mt, "%d", &s.config.MaxTokens)
	}
	if s.config.MaxTokens == 0 {
		s.config.MaxTokens = 4096
	}

	s.config.Timeout, _ = time.ParseDuration(u.Query().Get("timeout"))
	if s.config.Timeout == 0 {
		s.config.Timeout = 60 * time.Second
	}

	if rpm := u.Query().Get("rpm"); rpm != "" {
		fmt.Sscanf(rpm, "%d", &s.config.RPM)
	}
	if tpm := u.Query().Get("tpm"); tpm != "" {
		fmt.Sscanf(tpm, "%d", &s.config.TPM)
	}
	s.config.RateStore = u.Query().Get("rate_store")

	// Strip query params to get base URL
	q := u.Query()
	q.Del("model")
	q.Del("api_key")
	q.Del("api_key_header")
	q.Del("max_tokens")
	q.Del("timeout")
	q.Del("rpm")
	q.Del("tpm")
	q.Del("rate_store")
	u.RawQuery = q.Encode()
	s.config.BaseURL = u.String()

	s.isAttached = true
	return nil
}

func (s *OpenAISource) Detach(_ context.Context, _ *db.Pool) error {
	s.isAttached = false
	return nil
}

// SetDataSourceResolver implements sources.SourceDataSourceUser.
func (s *OpenAISource) SetDataSourceResolver(resolver sources.DataSourceResolver) {
	s.resolver = resolver
}

// ensureLimiter lazily initializes the rate limiter on first use.
// Deferred because the Redis store may not be attached when the LLM source attaches.
func (s *OpenAISource) ensureLimiter() {
	s.limiterOnce.Do(func() {
		if s.config.RPM == 0 && s.config.TPM == 0 {
			return
		}
		var store sources.StoreSource
		if s.config.RateStore != "" && s.resolver != nil {
			if ds, err := s.resolver.Resolve(s.config.RateStore); err == nil {
				store, _ = ds.(sources.StoreSource)
			}
		}
		s.limiter = ratelimit.New(s.ds.Name, s.config.RPM, s.config.TPM, store)
	})
}

func (s *OpenAISource) CreateCompletion(ctx context.Context, prompt string, opts sources.LLMOptions) (*sources.LLMResult, error) {
	messages := []sources.LLMMessage{
		{Role: "user", Content: prompt},
	}
	return s.CreateChatCompletion(ctx, messages, opts)
}

func (s *OpenAISource) CreateChatCompletion(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.LLMResult, error) {
	if !s.isAttached {
		return nil, sources.ErrDataSourceNotAttached
	}

	s.ensureLimiter()
	if s.limiter != nil {
		if err := s.limiter.Check(ctx); err != nil {
			return nil, err
		}
	}

	maxTokens := opts.MaxTokens
	if maxTokens == 0 {
		maxTokens = s.config.MaxTokens
	}

	// Build request body
	reqBody := map[string]any{
		"model":      s.config.Model,
		"messages":   convertMessagesOpenAI(messages),
		"max_tokens": maxTokens,
	}
	if opts.Temperature > 0 {
		reqBody["temperature"] = opts.Temperature
	}
	if opts.TopP > 0 {
		reqBody["top_p"] = opts.TopP
	}
	if len(opts.Stop) > 0 {
		reqBody["stop"] = opts.Stop
	}
	if len(opts.Tools) > 0 {
		reqBody["tools"] = convertToolsOpenAI(opts.Tools)
	}
	if opts.ToolChoice != "" {
		reqBody["tool_choice"] = opts.ToolChoice
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.BaseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.config.ApiKeyHeader != "" {
		req.Header.Set(s.config.ApiKeyHeader, s.config.ApiKey)
	} else if s.config.ApiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.ApiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("OpenAI API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	result, err := parseOpenAIResponse(respBody, "openai")
	if err != nil {
		return nil, err
	}

	if s.limiter != nil {
		_ = s.limiter.Record(ctx, result.TotalTokens)
	}

	return result, nil
}

// --- OpenAI format helpers ---

func convertMessagesOpenAI(msgs []sources.LLMMessage) []map[string]any {
	result := make([]map[string]any, len(msgs))
	for i, m := range msgs {
		msg := map[string]any{"role": m.Role, "content": m.Content}
		if len(m.ToolCalls) > 0 {
			calls := make([]map[string]any, len(m.ToolCalls))
			for j, tc := range m.ToolCalls {
				args, _ := json.Marshal(tc.Arguments)
				calls[j] = map[string]any{
					"id":   tc.ID,
					"type": "function",
					"function": map[string]any{
						"name":      tc.Name,
						"arguments": string(args),
					},
				}
			}
			msg["tool_calls"] = calls
		}
		if m.ToolCallID != "" {
			msg["tool_call_id"] = m.ToolCallID
		}
		result[i] = msg
	}
	return result
}

func convertToolsOpenAI(tools []sources.LLMTool) []map[string]any {
	result := make([]map[string]any, len(tools))
	for i, t := range tools {
		result[i] = map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        t.Name,
				"description": t.Description,
				"parameters":  t.Parameters,
			},
		}
	}
	return result
}

func parseOpenAIResponse(body []byte, provider string) (*sources.LLMResult, error) {
	var resp struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Model string `json:"model"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	result := &sources.LLMResult{
		Model:            resp.Model,
		PromptTokens:     resp.Usage.PromptTokens,
		CompletionTokens: resp.Usage.CompletionTokens,
		TotalTokens:      resp.Usage.TotalTokens,
		Provider:         provider,
	}

	if len(resp.Choices) > 0 {
		choice := resp.Choices[0]
		result.Content = choice.Message.Content
		result.FinishReason = normalizeFinishReasonOpenAI(choice.FinishReason)

		for _, tc := range choice.Message.ToolCalls {
			var args any
			_ = json.Unmarshal([]byte(tc.Function.Arguments), &args)
			result.ToolCalls = append(result.ToolCalls, sources.LLMToolCall{
				ID:        tc.ID,
				Name:      tc.Function.Name,
				Arguments: args,
			})
		}
	}

	return result, nil
}

func normalizeFinishReasonOpenAI(reason string) string {
	switch reason {
	case "stop":
		return "stop"
	case "tool_calls":
		return "tool_use"
	case "length":
		return "length"
	default:
		return reason
	}
}

var (
	_ sources.Source              = (*OpenAISource)(nil)
	_ sources.LLMSource           = (*OpenAISource)(nil)
	_ sources.SourceDataSourceUser = (*OpenAISource)(nil)
)
