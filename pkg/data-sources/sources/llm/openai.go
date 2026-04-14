// Package llm implements LLM data source types for AI model providers.
package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
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

	rateLimitMixin
}

type openAIConfig struct {
	BaseURL          string
	Model            string
	ApiKey           string
	ApiKeyHeader     string
	MaxTokens        int
	ThinkingBudget   int // max thinking/reasoning tokens (0 = disabled)
	Timeout          time.Duration
	RPM              int    // max requests per minute (0 = unlimited)
	TPM              int    // max tokens per minute (0 = unlimited)
	RateStore        string // name of StoreSource for shared counters (empty = in-memory)
	UseResponsesAPI  bool   // use OpenAI Responses API instead of Chat Completions
	ReasoningSummary string // "auto", "concise", "detailed" (Responses API only)
	ReasoningEffort  string // "low", "medium", "high" (Responses API only)
}

func NewOpenAI(ds types.DataSource, attached bool) (*OpenAISource, error) {
	return &OpenAISource{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewDuckDB(),
	}, nil
}

func (s *OpenAISource) Name() string                 { return s.ds.Name }
func (s *OpenAISource) Definition() types.DataSource { return s.ds }
func (s *OpenAISource) Engine() engines.Engine       { return s.engine }
func (s *OpenAISource) IsAttached() bool             { return s.isAttached }
func (s *OpenAISource) ReadOnly() bool               { return true }

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
	if strings.HasPrefix(s.config.Model, "\"") && strings.HasSuffix(s.config.Model, "\"") {
		s.config.Model = strings.Trim(s.config.Model, "\"")
	}
	s.config.ApiKey = u.Query().Get("api_key")
	if strings.HasPrefix(s.config.ApiKey, "\"") && strings.HasSuffix(s.config.ApiKey, "\"") {
		s.config.ApiKey = strings.Trim(s.config.ApiKey, "\"")
	}
	s.config.ApiKeyHeader = u.Query().Get("api_key_header")

	if mt := u.Query().Get("max_tokens"); mt != "" {
		fmt.Sscanf(mt, "%d", &s.config.MaxTokens)
	}
	if s.config.MaxTokens == 0 {
		s.config.MaxTokens = 4096
	}

	if tb := u.Query().Get("thinking_budget"); tb != "" {
		fmt.Sscanf(tb, "%d", &s.config.ThinkingBudget)
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

	// Responses API detection
	if u.Query().Get("use_responses_api") == "true" {
		s.config.UseResponsesAPI = true
	} else if strings.Contains(u.Path, "/responses") {
		s.config.UseResponsesAPI = true
	}
	s.config.ReasoningSummary = u.Query().Get("reasoning_summary")
	s.config.ReasoningEffort = u.Query().Get("reasoning_effort")

	// Strip query params to get base URL
	q := u.Query()
	q.Del("model")
	q.Del("api_key")
	q.Del("api_key_header")
	q.Del("max_tokens")
	q.Del("timeout")
	q.Del("thinking_budget")
	q.Del("rpm")
	q.Del("tpm")
	q.Del("rate_store")
	q.Del("use_responses_api")
	q.Del("reasoning_summary")
	q.Del("reasoning_effort")
	u.RawQuery = q.Encode()
	s.config.BaseURL = u.String()

	s.isAttached = true
	return nil
}

func (s *OpenAISource) Detach(_ context.Context, _ *db.Pool) error {
	s.isAttached = false
	return nil
}

func (s *OpenAISource) CreateCompletion(ctx context.Context, prompt string, opts sources.LLMOptions) (*sources.LLMResult, error) {
	messages := []sources.LLMMessage{
		{Role: "user", Content: prompt},
	}
	return s.CreateChatCompletion(ctx, messages, opts)
}

func (s *OpenAISource) CreateChatCompletion(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.LLMResult, error) {
	if s.config.UseResponsesAPI {
		return s.createResponsesCompletion(ctx, messages, opts)
	}
	if !s.isAttached {
		return nil, sources.ErrDataSourceNotAttached
	}

	s.rateLimitMixin.ensureLimiter(s.ds.Name, s.config)
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
		"model":      strings.Trim(s.config.Model, "\""),
		"messages":   convertMessagesOpenAI(messages),
		"max_completion_tokens": maxTokens,
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
		params := t.Parameters
		// Ensure "required" key exists in parameters — some model templates
		// (e.g. Gemma4 in LM Studio) crash on missing "required" field.
		if pm, ok := params.(map[string]any); ok {
			if _, hasRequired := pm["required"]; !hasRequired {
				pm["required"] = []string{}
			}
		}
		result[i] = map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        t.Name,
				"description": t.Description,
				"parameters":  params,
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

func (s *OpenAISource) CreateChatCompletionStream(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions,
	onEvent func(event *sources.LLMStreamEvent) error) error {
	if s.config.UseResponsesAPI {
		return s.createResponsesStream(ctx, messages, opts, onEvent)
	}
	if !s.isAttached {
		return sources.ErrDataSourceNotAttached
	}

	s.rateLimitMixin.ensureLimiter(s.ds.Name, s.config)
	if s.limiter != nil {
		if err := s.limiter.Check(ctx); err != nil {
			return err
		}
	}

	maxTokens := opts.MaxTokens
	if maxTokens == 0 {
		maxTokens = s.config.MaxTokens
	}

	reqBody := map[string]any{
		"model":      strings.Trim(s.config.Model, "\""), // some providers (e.g. Ollama) require unquoted model names
		"messages":   convertMessagesOpenAI(messages),
		"max_completion_tokens": maxTokens,
		"stream":     true,
		// Ask the OpenAI-compatible endpoint to send token usage on the
		// final SSE chunk. Without this, the streaming response carries
		// no `usage` block (LM Studio, vLLM, llama.cpp and the official
		// OpenAI API all gate it on this option), so the LLMStreamEvent
		// `finish` event ends up with PromptTokens=0 / CompletionTokens=0
		// and downstream consumers can't display per-message cost.
		"stream_options": map[string]any{"include_usage": true},
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
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.BaseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.config.ApiKeyHeader != "" {
		req.Header.Set(s.config.ApiKeyHeader, s.config.ApiKey)
	} else if s.config.ApiKey != "" {
		req.Header.Set("Authorization", "Bearer "+s.config.ApiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OpenAI API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var totalPromptTokens, totalCompletionTokens int
	// pendingToolCalls accumulates tool call fragments keyed by delta index.
	// Each entry captures id/name from the first delta and concatenates
	// argument JSON fragments across subsequent deltas.
	type pendingToolCall struct {
		ID       string
		Name     string
		ArgsJSON strings.Builder
	}
	var pendingToolCalls []*pendingToolCall
	// finishEvent is captured when the model emits a finish_reason, but
	// emitted only AFTER the SSE stream ends. With `stream_options.include_usage`
	// the OpenAI-compatible server sends a trailing chunk with `choices: []`
	// and a `usage` block — that arrives strictly after the finish chunk, so
	// emitting the finish event eagerly would always lose the token counts.
	var finishEvent *sources.LLMStreamEvent

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					ToolCalls        []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
			Model string `json:"model"`
			Usage *struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}

		if chunk.Usage != nil {
			totalPromptTokens = chunk.Usage.PromptTokens
			totalCompletionTokens = chunk.Usage.CompletionTokens
		}

		if len(chunk.Choices) == 0 {
			continue
		}
		choice := chunk.Choices[0]

		// Accumulate tool call fragments per-index.
		// The first delta for each index carries id and function.name;
		// subsequent deltas carry only argument fragments.
		for _, tc := range choice.Delta.ToolCalls {
			for tc.Index >= len(pendingToolCalls) {
				pendingToolCalls = append(pendingToolCalls, &pendingToolCall{})
			}
			ptc := pendingToolCalls[tc.Index]
			if tc.ID != "" {
				ptc.ID = tc.ID
			}
			if tc.Function.Name != "" {
				ptc.Name = tc.Function.Name
			}
			ptc.ArgsJSON.WriteString(tc.Function.Arguments)
		}

		if choice.Delta.ReasoningContent != "" {
			if err := onEvent(&sources.LLMStreamEvent{
				Type:    "reasoning",
				Content: choice.Delta.ReasoningContent,
				Model:   chunk.Model,
			}); err != nil {
				return err
			}
		}

		if choice.Delta.Content != "" {
			if err := onEvent(&sources.LLMStreamEvent{
				Type:    "content_delta",
				Content: choice.Delta.Content,
				Model:   chunk.Model,
			}); err != nil {
				return err
			}
		}

		if choice.FinishReason != nil {
			// Capture the finish event but defer emitting it. The trailing
			// usage-only chunk that follows (when include_usage is set) will
			// update totalPromptTokens / totalCompletionTokens before we
			// flush below.
			finishEvent = &sources.LLMStreamEvent{
				Type:         "finish",
				Model:        chunk.Model,
				FinishReason: normalizeFinishReasonOpenAI(*choice.FinishReason),
			}
			if len(pendingToolCalls) > 0 {
				var calls []sources.LLMToolCall
				for _, ptc := range pendingToolCalls {
					var args any
					if s := ptc.ArgsJSON.String(); s != "" {
						_ = json.Unmarshal([]byte(s), &args)
					}
					calls = append(calls, sources.LLMToolCall{
						ID:        ptc.ID,
						Name:      ptc.Name,
						Arguments: args,
					})
				}
				b, _ := json.Marshal(calls)
				finishEvent.ToolCalls = string(b)
			}
		}
	}

	// Flush the finish event with the final accumulated token counts
	// (populated either from the finish chunk itself for providers that
	// inline `usage`, or from the trailing usage-only chunk for those that
	// honor `stream_options.include_usage`).
	if finishEvent != nil {
		finishEvent.PromptTokens = totalPromptTokens
		finishEvent.CompletionTokens = totalCompletionTokens
		if err := onEvent(finishEvent); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading SSE stream: %w", err)
	}

	if s.limiter != nil {
		_ = s.limiter.Record(ctx, totalPromptTokens+totalCompletionTokens)
	}

	return nil
}

var (
	_ sources.Source               = (*OpenAISource)(nil)
	_ sources.LLMSource            = (*OpenAISource)(nil)
	_ sources.LLMStreamingSource   = (*OpenAISource)(nil)
	_ sources.SourceDataSourceUser = (*OpenAISource)(nil)
)
