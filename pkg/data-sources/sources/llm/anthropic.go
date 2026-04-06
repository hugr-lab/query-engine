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

// AnthropicSource implements Source + LLMSource for the Anthropic Messages API.
type AnthropicSource struct {
	ds         types.DataSource
	engine     engines.Engine
	isAttached bool
	config     openAIConfig // reuse same config struct

	resolver    sources.DataSourceResolver
	limiter     *ratelimit.Limiter
	limiterOnce sync.Once
}

func NewAnthropic(ds types.DataSource, attached bool) (*AnthropicSource, error) {
	return &AnthropicSource{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *AnthropicSource) Name() string             { return s.ds.Name }
func (s *AnthropicSource) Definition() types.DataSource { return s.ds }
func (s *AnthropicSource) Engine() engines.Engine   { return s.engine }
func (s *AnthropicSource) IsAttached() bool         { return s.isAttached }
func (s *AnthropicSource) ReadOnly() bool           { return true }

func (s *AnthropicSource) ModelInfo() sources.ModelInfo {
	return sources.ModelInfo{
		Name:     s.ds.Name,
		Type:     "llm",
		Provider: "anthropic",
		Model:    s.config.Model,
	}
}

func (s *AnthropicSource) Attach(_ context.Context, _ *db.Pool) error {
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

	q := u.Query()
	q.Del("model")
	q.Del("api_key")
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

func (s *AnthropicSource) Detach(_ context.Context, _ *db.Pool) error {
	s.isAttached = false
	return nil
}

func (s *AnthropicSource) SetDataSourceResolver(resolver sources.DataSourceResolver) {
	s.resolver = resolver
}

func (s *AnthropicSource) ensureLimiter() {
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

func (s *AnthropicSource) CreateCompletion(ctx context.Context, prompt string, opts sources.LLMOptions) (*sources.LLMResult, error) {
	return s.CreateChatCompletion(ctx, []sources.LLMMessage{{Role: "user", Content: prompt}}, opts)
}

func (s *AnthropicSource) CreateChatCompletion(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.LLMResult, error) {
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

	// Separate system message
	var system string
	var apiMessages []map[string]any
	for _, m := range messages {
		if m.Role == "system" {
			system = m.Content
			continue
		}
		if m.Role == "tool" && m.ToolCallID != "" {
			apiMessages = append(apiMessages, map[string]any{
				"role": "user",
				"content": []map[string]any{{
					"type":        "tool_result",
					"tool_use_id": m.ToolCallID,
					"content":     m.Content,
				}},
			})
			continue
		}
		if m.Role == "assistant" && len(m.ToolCalls) > 0 {
			// Anthropic requires tool_use content blocks for assistant tool calls
			content := []map[string]any{}
			if m.Content != "" {
				content = append(content, map[string]any{"type": "text", "text": m.Content})
			}
			for _, tc := range m.ToolCalls {
				content = append(content, map[string]any{
					"type":  "tool_use",
					"id":    tc.ID,
					"name":  tc.Name,
					"input": tc.Arguments,
				})
			}
			apiMessages = append(apiMessages, map[string]any{"role": "assistant", "content": content})
			continue
		}
		apiMessages = append(apiMessages, map[string]any{"role": m.Role, "content": m.Content})
	}

	reqBody := map[string]any{
		"model":      s.config.Model,
		"messages":   apiMessages,
		"max_tokens": maxTokens,
	}
	if system != "" {
		reqBody["system"] = system
	}
	if opts.Temperature > 0 {
		reqBody["temperature"] = opts.Temperature
	}
	if len(opts.Tools) > 0 {
		tools := make([]map[string]any, len(opts.Tools))
		for i, t := range opts.Tools {
			tools[i] = map[string]any{
				"name":         t.Name,
				"description":  t.Description,
				"input_schema": t.Parameters,
			}
		}
		reqBody["tools"] = tools
	}
	if opts.ToolChoice != "" {
		switch opts.ToolChoice {
		case "auto":
			reqBody["tool_choice"] = map[string]any{"type": "auto"}
		case "none":
			reqBody["tool_choice"] = map[string]any{"type": "none"}
		default:
			reqBody["tool_choice"] = map[string]any{"type": "tool", "name": opts.ToolChoice}
		}
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.BaseURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", s.config.ApiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

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
		return nil, fmt.Errorf("Anthropic API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	result, err := parseAnthropicResponse(respBody)
	if err != nil {
		return nil, err
	}

	if s.limiter != nil {
		_ = s.limiter.Record(ctx, result.TotalTokens)
	}

	return result, nil
}

func parseAnthropicResponse(body []byte) (*sources.LLMResult, error) {
	var resp struct {
		Content []struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			ID    string `json:"id"`
			Name  string `json:"name"`
			Input any    `json:"input"`
		} `json:"content"`
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Usage      struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	result := &sources.LLMResult{
		Model:            resp.Model,
		PromptTokens:     resp.Usage.InputTokens,
		CompletionTokens: resp.Usage.OutputTokens,
		TotalTokens:      resp.Usage.InputTokens + resp.Usage.OutputTokens,
		Provider:         "anthropic",
	}

	switch resp.StopReason {
	case "end_turn":
		result.FinishReason = "stop"
	case "tool_use":
		result.FinishReason = "tool_use"
	case "max_tokens":
		result.FinishReason = "length"
	default:
		result.FinishReason = resp.StopReason
	}

	for _, block := range resp.Content {
		switch block.Type {
		case "text":
			result.Content += block.Text
		case "tool_use":
			result.ToolCalls = append(result.ToolCalls, sources.LLMToolCall{
				ID:        block.ID,
				Name:      block.Name,
				Arguments: block.Input,
			})
		}
	}

	return result, nil
}

var (
	_ sources.Source              = (*AnthropicSource)(nil)
	_ sources.LLMSource           = (*AnthropicSource)(nil)
	_ sources.SourceDataSourceUser = (*AnthropicSource)(nil)
)
