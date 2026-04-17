package sources

import (
	"fmt"
	"net/url"
)

// providerSchemes maps provider://service shortcuts to real API endpoint URLs.
var providerSchemes = map[string]string{
	"openai://llm":       "https://api.openai.com/v1/chat/completions",
	"openai://embedding": "https://api.openai.com/v1/embeddings",
	"anthropic://llm":    "https://api.anthropic.com/v1/messages",
	"gemini://llm":       "https://generativelanguage.googleapis.com/v1beta",
}

// ResolveProviderScheme checks if path uses a provider:// scheme and resolves
// it to the real API endpoint URL. Returns the path unchanged if it uses
// http:// or https://. Returns an error for unrecognized schemes.
func ResolveProviderScheme(path string) (string, error) {
	u, err := url.Parse(path)
	if err != nil {
		return path, nil
	}

	switch u.Scheme {
	case "http", "https", "":
		return path, nil
	}

	// Build the scheme://host key to look up in the mapping.
	key := u.Scheme + "://" + u.Host

	baseURL, ok := providerSchemes[key]
	if !ok {
		return "", fmt.Errorf("unsupported provider URL scheme %q; supported schemes: openai://llm, openai://embedding, anthropic://llm, gemini://llm", key)
	}

	// Rebuild with the resolved base URL and the original query parameters.
	if u.RawQuery != "" {
		return baseURL + "?" + u.RawQuery, nil
	}
	return baseURL, nil
}
