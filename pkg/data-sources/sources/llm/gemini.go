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
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

// GeminiSource implements Source + LLMSource for the Google Gemini API.
type GeminiSource struct {
	ds         types.DataSource
	engine     engines.Engine
	isAttached bool
	config     openAIConfig // reuse same config struct
}

func NewGemini(ds types.DataSource, attached bool) (*GeminiSource, error) {
	return &GeminiSource{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *GeminiSource) Name() string             { return s.ds.Name }
func (s *GeminiSource) Definition() types.DataSource { return s.ds }
func (s *GeminiSource) Engine() engines.Engine   { return s.engine }
func (s *GeminiSource) IsAttached() bool         { return s.isAttached }
func (s *GeminiSource) ReadOnly() bool           { return true }

func (s *GeminiSource) ModelInfo() sources.ModelInfo {
	return sources.ModelInfo{
		Name:     s.ds.Name,
		Type:     "llm",
		Provider: "gemini",
		Model:    s.config.Model,
	}
}

func (s *GeminiSource) Attach(_ context.Context, _ *db.Pool) error {
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
	q := u.Query()
	q.Del("model")
	q.Del("api_key")
	q.Del("max_tokens")
	q.Del("timeout")
	u.RawQuery = q.Encode()
	s.config.BaseURL = u.String()
	s.isAttached = true
	return nil
}

func (s *GeminiSource) Detach(_ context.Context, _ *db.Pool) error {
	s.isAttached = false
	return nil
}

func (s *GeminiSource) CreateCompletion(ctx context.Context, prompt string, opts sources.LLMOptions) (*sources.LLMResult, error) {
	return s.CreateChatCompletion(ctx, []sources.LLMMessage{{Role: "user", Content: prompt}}, opts)
}

func (s *GeminiSource) CreateChatCompletion(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.LLMResult, error) {
	if !s.isAttached {
		return nil, sources.ErrDataSourceNotAttached
	}
	maxTokens := opts.MaxTokens
	if maxTokens == 0 {
		maxTokens = s.config.MaxTokens
	}

	// Separate system instruction
	var systemInstruction *map[string]any
	var contents []map[string]any
	for _, m := range messages {
		if m.Role == "system" {
			si := map[string]any{"parts": []map[string]any{{"text": m.Content}}}
			systemInstruction = &si
			continue
		}
		role := m.Role
		if role == "assistant" {
			role = "model"
		}
		if m.Role == "tool" {
			contents = append(contents, map[string]any{
				"role": "function",
				"parts": []map[string]any{{
					"functionResponse": map[string]any{
						"name":     m.ToolCallID,
						"response": map[string]any{"result": m.Content},
					},
				}},
			})
			continue
		}
		parts := []map[string]any{{"text": m.Content}}
		for _, tc := range m.ToolCalls {
			parts = append(parts, map[string]any{
				"functionCall": map[string]any{"name": tc.Name, "args": tc.Arguments},
			})
		}
		contents = append(contents, map[string]any{"role": role, "parts": parts})
	}

	reqBody := map[string]any{
		"contents": contents,
		"generationConfig": map[string]any{
			"maxOutputTokens": maxTokens,
		},
	}
	if systemInstruction != nil {
		reqBody["system_instruction"] = *systemInstruction
	}
	if opts.Temperature > 0 {
		reqBody["generationConfig"].(map[string]any)["temperature"] = opts.Temperature
	}
	if len(opts.Tools) > 0 {
		decls := make([]map[string]any, len(opts.Tools))
		for i, t := range opts.Tools {
			decls[i] = map[string]any{
				"name":        t.Name,
				"description": t.Description,
				"parameters":  t.Parameters,
			}
		}
		reqBody["tools"] = []map[string]any{{"functionDeclarations": decls}}
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	// URL: {baseURL}/models/{model}:generateContent?key={api_key}
	apiURL := fmt.Sprintf("%s/models/%s:generateContent", s.config.BaseURL, url.PathEscape(s.config.Model))
	if s.config.ApiKey != "" {
		apiURL += "?key=" + url.QueryEscape(s.config.ApiKey)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

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
		return nil, fmt.Errorf("Gemini API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return parseGeminiResponse(respBody)
}

func parseGeminiResponse(body []byte) (*sources.LLMResult, error) {
	var resp struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text         string `json:"text"`
					FunctionCall *struct {
						Name string `json:"name"`
						Args any    `json:"args"`
					} `json:"functionCall"`
				} `json:"parts"`
			} `json:"content"`
			FinishReason string `json:"finishReason"`
		} `json:"candidates"`
		UsageMetadata struct {
			PromptTokenCount     int `json:"promptTokenCount"`
			CandidatesTokenCount int `json:"candidatesTokenCount"`
			TotalTokenCount      int `json:"totalTokenCount"`
		} `json:"usageMetadata"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	result := &sources.LLMResult{
		PromptTokens:     resp.UsageMetadata.PromptTokenCount,
		CompletionTokens: resp.UsageMetadata.CandidatesTokenCount,
		TotalTokens:      resp.UsageMetadata.TotalTokenCount,
		Provider:         "gemini",
	}

	if len(resp.Candidates) > 0 {
		c := resp.Candidates[0]
		switch c.FinishReason {
		case "STOP":
			result.FinishReason = "stop"
		case "MAX_TOKENS":
			result.FinishReason = "length"
		default:
			result.FinishReason = c.FinishReason
		}
		for _, part := range c.Content.Parts {
			if part.Text != "" {
				result.Content += part.Text
			}
			if part.FunctionCall != nil {
				result.ToolCalls = append(result.ToolCalls, sources.LLMToolCall{
					Name:      part.FunctionCall.Name,
					Arguments: part.FunctionCall.Args,
				})
				if result.FinishReason == "" {
					result.FinishReason = "tool_use"
				}
			}
		}
	}

	return result, nil
}

var (
	_ sources.Source    = (*GeminiSource)(nil)
	_ sources.LLMSource = (*GeminiSource)(nil)
)
