package types

// ModelInfo describes a registered AI model data source.
type ModelInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // "llm" or "embedding"
	Provider string `json:"provider"` // "openai", "anthropic", "gemini"
	Model    string `json:"model"`
}

// EmbeddingResult is an enriched embedding response with token count.
type EmbeddingResult struct {
	Vector       Vector `json:"vector"`
	PromptTokens int    `json:"prompt_tokens"`
	TokenCount   int    `json:"token_count"`
}

// EmbeddingsResult is a batch embedding response with total token count.
type EmbeddingsResult struct {
	Vectors      []Vector `json:"vectors"`
	TokenCount   int      `json:"token_count"`
	PromptTokens int      `json:"prompt_tokens"`
}

// LLMMessage is a single message in a chat conversation.
type LLMMessage struct {
	Role             string        `json:"role"`
	Content          string        `json:"content"`
	ToolCalls        []LLMToolCall `json:"tool_calls,omitempty"`
	ToolCallID       string        `json:"tool_call_id,omitempty"`
	ThoughtSignature string        `json:"thought_signature,omitempty"` // Gemini 2.5+: Part-level signature for the first functionCall
}

// LLMTool is a tool definition provided to the model.
type LLMTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

// LLMToolCall is a tool invocation from the model.
type LLMToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments any    `json:"arguments"`
}

// LLMOptions configures an LLM request.
type LLMOptions struct {
	MaxTokens      int       `json:"max_tokens,omitempty"`
	Temperature    float64   `json:"temperature,omitempty"`
	TopP           float64   `json:"top_p,omitempty"`
	Stop           []string  `json:"stop,omitempty"`
	Tools          []LLMTool `json:"tools,omitempty"`
	ToolChoice     string    `json:"tool_choice,omitempty"`
	ThinkingBudget int       `json:"thinking_budget,omitempty"` // Token budget for reasoning/thinking (Anthropic, Gemini)
}

// LLMStreamEvent is a single event from a streaming LLM completion.
type LLMStreamEvent struct {
	Type             string `json:"type"`              // content_delta, reasoning, tool_use, finish, error
	Content          string `json:"content"`           // Token content (content_delta, reasoning)
	Model            string `json:"model"`             // Model identifier
	FinishReason     string `json:"finish_reason"`     // stop, length, tool_use (finish only)
	ToolCalls        string `json:"tool_calls"`        // JSON-encoded tool calls (tool_use only)
	PromptTokens     int    `json:"prompt_tokens"`     // Input token count (finish only)
	CompletionTokens int    `json:"completion_tokens"` // Output token count (finish only)
	ThoughtSignature string `json:"thought_signature"` // Gemini 2.5+: Part-level signature (finish only)
}

// LLMResult is the normalized response from any LLM provider.
type LLMResult struct {
	Content          string        `json:"content"`
	Model            string        `json:"model"`
	FinishReason     string        `json:"finish_reason"`
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Provider         string        `json:"provider"`
	LatencyMs        int           `json:"latency_ms"`
	ToolCalls        []LLMToolCall `json:"tool_calls"`
	ThoughtSignature string        `json:"thought_signature,omitempty"` // Gemini 2.5+: Part-level signature
}
