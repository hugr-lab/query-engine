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
	Vector     []float64 `json:"vector"`
	TokenCount int    `json:"token_count"`
}

// EmbeddingsResult is a batch embedding response with total token count.
type EmbeddingsResult struct {
	Vectors    [][]float64 `json:"vectors"`
	TokenCount int      `json:"token_count"`
}

// LLMMessage is a single message in a chat conversation.
type LLMMessage struct {
	Role       string        `json:"role"`
	Content    string        `json:"content"`
	ToolCalls  []LLMToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
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
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Temperature float64   `json:"temperature,omitempty"`
	TopP        float64   `json:"top_p,omitempty"`
	Stop        []string  `json:"stop,omitempty"`
	Tools       []LLMTool `json:"tools,omitempty"`
	ToolChoice  string    `json:"tool_choice,omitempty"`
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
}
