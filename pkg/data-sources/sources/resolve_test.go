package sources

import (
	"testing"
)

func TestResolveProviderScheme(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    string
		wantErr bool
	}{
		{
			name: "openai llm",
			path: "openai://llm?model=gpt-4o&api_key=sk-test",
			want: "https://api.openai.com/v1/chat/completions?model=gpt-4o&api_key=sk-test",
		},
		{
			name: "openai embedding",
			path: "openai://embedding?model=text-embedding-3-small&api_key=sk-test",
			want: "https://api.openai.com/v1/embeddings?model=text-embedding-3-small&api_key=sk-test",
		},
		{
			name: "anthropic llm",
			path: "anthropic://llm?model=claude-sonnet-4-20250514&api_key=sk-ant-test",
			want: "https://api.anthropic.com/v1/messages?model=claude-sonnet-4-20250514&api_key=sk-ant-test",
		},
		{
			name: "gemini llm",
			path: "gemini://llm?model=gemini-2.5-flash&api_key=AIza-test",
			want: "https://generativelanguage.googleapis.com/v1beta?model=gemini-2.5-flash&api_key=AIza-test",
		},
		{
			name: "openai llm no query params",
			path: "openai://llm",
			want: "https://api.openai.com/v1/chat/completions",
		},
		{
			name: "http passthrough",
			path: "http://localhost:11434/v1/chat/completions?model=llama3",
			want: "http://localhost:11434/v1/chat/completions?model=llama3",
		},
		{
			name: "https passthrough",
			path: "https://api.openai.com/v1/chat/completions?model=gpt-4&api_key=sk-test",
			want: "https://api.openai.com/v1/chat/completions?model=gpt-4&api_key=sk-test",
		},
		{
			name:    "unsupported scheme",
			path:    "mistral://llm?model=mistral-large",
			wantErr: true,
		},
		{
			name:    "unsupported service for known provider",
			path:    "anthropic://embedding?model=test",
			wantErr: true,
		},
		{
			name: "empty path",
			path: "",
			want: "",
		},
		{
			name: "preserves multiple query params",
			path: "openai://llm?model=gpt-4o&api_key=sk-test&max_tokens=4096&timeout=30s&rpm=60",
			want: "https://api.openai.com/v1/chat/completions?model=gpt-4o&api_key=sk-test&max_tokens=4096&timeout=30s&rpm=60",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveProviderScheme(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ResolveProviderScheme() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ResolveProviderScheme() = %q, want %q", got, tt.want)
			}
		})
	}
}
