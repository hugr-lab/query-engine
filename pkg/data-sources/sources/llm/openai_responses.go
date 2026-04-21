package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

// createResponsesCompletion implements CreateChatCompletion using the OpenAI Responses API.
func (s *OpenAISource) createResponsesCompletion(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.LLMResult, error) {
	reqBody := buildResponsesRequest(s.config, messages, opts, false)

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
	setOpenAIHeaders(req, s.config)

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
		return nil, fmt.Errorf("OpenAI Responses API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return parseResponsesResponse(respBody)
}

// createResponsesStream implements CreateChatCompletionStream using the OpenAI Responses API.
func (s *OpenAISource) createResponsesStream(ctx context.Context, messages []sources.LLMMessage, opts sources.LLMOptions,
	onEvent func(event *sources.LLMStreamEvent) error) error {

	reqBody := buildResponsesRequest(s.config, messages, opts, true)

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", s.config.BaseURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	setOpenAIHeaders(req, s.config)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OpenAI Responses API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return parseResponsesStream(resp.Body, onEvent)
}

// buildResponsesRequest builds a Responses API request body from normalized messages.
func buildResponsesRequest(config openAIConfig, messages []sources.LLMMessage, opts sources.LLMOptions, stream bool) map[string]any {
	maxTokens := opts.MaxTokens
	if maxTokens == 0 {
		maxTokens = config.MaxTokens
	}

	reqBody := map[string]any{
		"model":            config.Model,
		"max_output_tokens": maxTokens,
	}

	if stream {
		reqBody["stream"] = true
	}

	// Convert messages to input
	input := convertMessagesResponses(messages)
	reqBody["input"] = input

	// Temperature
	if opts.Temperature > 0 {
		reqBody["temperature"] = opts.Temperature
	}

	// Reasoning config
	reasoning := map[string]any{}
	effort := config.ReasoningEffort
	if effort == "" {
		effort = "medium"
	}
	reasoning["effort"] = effort
	if config.ReasoningSummary != "" {
		reasoning["summary"] = config.ReasoningSummary
	}
	reqBody["reasoning"] = reasoning

	// Tools
	if len(opts.Tools) > 0 {
		tools := make([]map[string]any, len(opts.Tools))
		for i, t := range opts.Tools {
			tool := map[string]any{
				"type": "function",
				"name": t.Name,
			}
			if t.Description != "" {
				tool["description"] = t.Description
			}
			if t.Parameters != nil {
				tool["parameters"] = t.Parameters
			}
			tools[i] = tool
		}
		reqBody["tools"] = tools
	}

	return reqBody
}

// convertMessagesResponses converts normalized LLMMessages to Responses API input items.
func convertMessagesResponses(messages []sources.LLMMessage) []map[string]any {
	var input []map[string]any

	for _, m := range messages {
		switch m.Role {
		case "system":
			input = append(input, map[string]any{
				"type": "message",
				"role": "developer",
				"content": []map[string]any{
					{"type": "input_text", "text": m.Content},
				},
			})

		case "user":
			input = append(input, map[string]any{
				"type": "message",
				"role": "user",
				"content": []map[string]any{
					{"type": "input_text", "text": m.Content},
				},
			})

		case "assistant":
			// Assistant message with possible tool calls
			var content []map[string]any
			if m.Content != "" {
				content = append(content, map[string]any{
					"type": "output_text", "text": m.Content,
				})
			}
			if len(content) > 0 {
				input = append(input, map[string]any{
					"type": "message", "role": "assistant", "content": content,
				})
			}
			// Tool calls as separate function_call items
			for _, tc := range m.ToolCalls {
				argsStr := ""
				if tc.Arguments != nil {
					b, _ := json.Marshal(tc.Arguments)
					argsStr = string(b)
				}
				input = append(input, map[string]any{
					"type":      "function_call",
					"call_id":   tc.ID,
					"name":      tc.Name,
					"arguments": argsStr,
				})
			}

		case "tool":
			// Tool result → function_call_output
			input = append(input, map[string]any{
				"type":    "function_call_output",
				"call_id": m.ToolCallID,
				"output":  m.Content,
			})
		}
	}

	return input
}

// setOpenAIHeaders sets auth headers for OpenAI API requests.
func setOpenAIHeaders(req *http.Request, config openAIConfig) {
	req.Header.Set("Content-Type", "application/json")
	if config.ApiKey != "" {
		if config.ApiKeyHeader != "" {
			req.Header.Set(config.ApiKeyHeader, config.ApiKey)
		} else {
			req.Header.Set("Authorization", "Bearer "+config.ApiKey)
		}
	}
}

// parseResponsesResponse parses a non-streaming Responses API response.
func parseResponsesResponse(body []byte) (*sources.LLMResult, error) {
	var resp struct {
		ID     string `json:"id"`
		Model  string `json:"model"`
		Output []struct {
			Type    string `json:"type"`
			ID      string `json:"id"`
			Status  string `json:"status"`
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			// reasoning type
			Summary []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"summary"`
			// function_call type
			CallID    string `json:"call_id"`
			Name      string `json:"name"`
			Arguments string `json:"arguments"`
		} `json:"output"`
		Usage struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
			TotalTokens  int `json:"total_tokens"`
		} `json:"usage"`
		Status string `json:"status"`
		Error  *struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse responses API: %w", err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("OpenAI Responses API error: %s (%s)", resp.Error.Message, resp.Error.Code)
	}

	result := &sources.LLMResult{
		Model:            resp.Model,
		PromptTokens:     resp.Usage.InputTokens,
		CompletionTokens: resp.Usage.OutputTokens,
		TotalTokens:      resp.Usage.TotalTokens,
		Provider:         "openai",
		FinishReason:     "stop",
	}

	for _, item := range resp.Output {
		switch item.Type {
		case "message":
			for _, c := range item.Content {
				if c.Type == "output_text" {
					result.Content += c.Text
				}
			}
		case "reasoning":
			for _, s := range item.Summary {
				if s.Type == "summary_text" {
					result.Thinking += s.Text
				}
			}
		case "function_call":
			var args any
			if item.Arguments != "" {
				_ = json.Unmarshal([]byte(item.Arguments), &args)
			}
			result.ToolCalls = append(result.ToolCalls, sources.LLMToolCall{
				ID:        item.CallID,
				Name:      item.Name,
				Arguments: args,
			})
			result.FinishReason = "tool_use"
		}
	}

	return result, nil
}

// parseResponsesStream parses SSE events from a streaming Responses API response.
//
// Pending function calls are keyed by item_id (the stable handle OpenAI uses on
// function_call_arguments.delta / .done events). The external call_id — which
// the provider expects echoed back on the tool-result follow-up message — is
// captured once on response.output_item.added and emitted upward as
// LLMToolCall.ID.
func parseResponsesStream(body io.Reader, onEvent func(event *sources.LLMStreamEvent) error) error {
	type pendingFunctionCall struct {
		ItemID      string
		CallID      string
		Name        string
		OutputIndex int
		Args        strings.Builder
		FinalArgs   string
		Done        bool
	}

	var (
		accThinking     strings.Builder
		pendingByItemID = map[string]*pendingFunctionCall{}
		pendingOrder    []*pendingFunctionCall
	)

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}

		// Fields are unioned across event types; absent fields parse as
		// zero values. Comments note which events populate each field.
		var event struct {
			Type string `json:"type"`

			// output_text.delta, reasoning_summary_text.delta,
			// function_call_arguments.delta
			Delta string `json:"delta"`

			// function_call_arguments.delta, function_call_arguments.done,
			// output_text.delta, reasoning_summary_text.delta — item
			// correlation handle.
			ItemID string `json:"item_id"`

			// output_item.added, function_call_arguments.delta/done —
			// stable index within the response, used for ordered flush.
			OutputIndex int `json:"output_index"`

			// function_call_arguments.done — authoritative final args
			// string for a single function_call item.
			Arguments string `json:"arguments"`

			// output_item.added — item.id is the map key; item.call_id
			// is the external id surfaced later as LLMToolCall.ID.
			Item struct {
				ID     string `json:"id"`
				Type   string `json:"type"`
				CallID string `json:"call_id"`
				Name   string `json:"name"`
			} `json:"item"`

			// response.completed — terminal usage + model metadata.
			Response struct {
				Model string `json:"model"`
				Usage struct {
					InputTokens  int `json:"input_tokens"`
					OutputTokens int `json:"output_tokens"`
				} `json:"usage"`
			} `json:"response"`

			// reasoning_summary_text.* — unused today; parsed for
			// forward compatibility with richer reasoning events.
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			continue
		}

		switch event.Type {
		case "response.output_text.delta":
			if err := onEvent(&sources.LLMStreamEvent{
				Type:    "content_delta",
				Content: event.Delta,
			}); err != nil {
				return err
			}

		case "response.reasoning_summary_text.delta":
			accThinking.WriteString(event.Delta)
			if err := onEvent(&sources.LLMStreamEvent{
				Type:    "reasoning",
				Content: event.Delta,
			}); err != nil {
				return err
			}

		case "response.output_item.added":
			if event.Item.Type != "function_call" || event.Item.ID == "" {
				break
			}
			pc := &pendingFunctionCall{
				ItemID:      event.Item.ID,
				CallID:      event.Item.CallID,
				Name:        event.Item.Name,
				OutputIndex: event.OutputIndex,
			}
			pendingByItemID[event.Item.ID] = pc
			pendingOrder = append(pendingOrder, pc)

		case "response.function_call_arguments.delta":
			if pc := pendingByItemID[event.ItemID]; pc != nil {
				pc.Args.WriteString(event.Delta)
			}

		case "response.function_call_arguments.done":
			if pc := pendingByItemID[event.ItemID]; pc != nil {
				pc.FinalArgs = event.Arguments
				pc.Done = true
			}

		case "response.completed":
			ev := &sources.LLMStreamEvent{
				Type:             "finish",
				Model:            event.Response.Model,
				FinishReason:     "stop",
				PromptTokens:     event.Response.Usage.InputTokens,
				CompletionTokens: event.Response.Usage.OutputTokens,
				Thinking:         accThinking.String(),
			}

			if len(pendingOrder) > 0 {
				calls := make([]sources.LLMToolCall, 0, len(pendingOrder))
				for _, pc := range pendingOrder {
					// Prefer the authoritative final args from the .done
					// event; fall back to the accumulated delta stream.
					raw := pc.Args.String()
					if pc.Done {
						raw = pc.FinalArgs
					}
					var args any
					if raw != "" {
						_ = json.Unmarshal([]byte(raw), &args)
					}
					calls = append(calls, sources.LLMToolCall{
						ID:        pc.CallID,
						Name:      pc.Name,
						Arguments: args,
					})
				}
				b, _ := json.Marshal(calls)
				ev.ToolCalls = string(b)
				ev.FinishReason = "tool_use"
			}

			if err := onEvent(ev); err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}
